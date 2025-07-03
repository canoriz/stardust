use core::slice;
use std::cell::UnsafeCell;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{self, AtomicBool};
use std::task::{Context, Poll, Waker};

use futures::ready;
#[cfg(mloom)]
use loom::sync::{mpsc, Arc, Mutex, MutexGuard};
#[cfg(not(mloom))]
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use tokio::task::JoinHandle;

use tokio::sync::Notify;

#[cfg(test_flush_migrate_concurrent)]
use tokio::sync::Semaphore;
#[cfg(test_flush_migrate_concurrent)]
static DROP_CONCURRENT: Semaphore = Semaphore::const_new(0);

#[cfg(mloom)]
use loom::atomic::{AtomicBool, AtomicU32, Ordering};
#[cfg(not(mloom))]
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{info, warn};

use super::SubAbortHandle;

pub const MIN_ALLOC_SIZE: usize = 256 * 1024;
const MIN_ALLOC_SIZE_POWER: u32 = MIN_ALLOC_SIZE.ilog2();

const _: () = assert!(MIN_ALLOC_SIZE.next_power_of_two() == MIN_ALLOC_SIZE);

/// session cache
/// targets:
/// 1. allocate piece buffer for different sized piece (all power of 2)
/// 2. WRITE BACK and LOAD from file storage at proper time
/// 3. write back part of the piece if piece is big and cache is small
/// 4. calculate memory fragmentation ratio
/// 5. defragmentation when memory is fragmentated
/// 6. expand/shrink size of cache
#[derive(Clone)]
pub(crate) struct PieceBufPool {
    inner: Arc<PieceBufPoolImpl>,
}

// FreeBlock is NOT Clone. It's fundamentally a owned reference to
// free buffer
#[derive(Debug, Eq, PartialEq)]
struct FreeBlock {
    // TODO: offset and ptr is same thing? only store one?
    offset: usize,
    power: u32,

    // ptr is the pointer to buf[offset]
    ptr: *mut u8,
}

impl FreeBlock {
    // This is essentially Clone, use very carefully
    fn dup(&self) -> Self {
        Self {
            offset: self.offset,
            power: self.power,
            ptr: self.ptr,
        }
    }
}

struct FreeBlockHandle {
    // This is only a "reference", stores where this handle controlls,
    // we don't own this memory
    free_block: FreeBlock,
    // waits for FreeBlock drops
    invalidate_waiter: Vec<Arc<Notify>>,
    sub_manager_handle: Option<SubAbortHandle<PieceBuf>>,
}

impl Debug for FreeBlockHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FreeBlockHandle")
            .field("free_block", &self.free_block)
            .field("len(invalidate_waiter)", &self.invalidate_waiter.len())
            .field(
                "is_some(sub_manager_handle)",
                &self.sub_manager_handle.is_some(),
            )
            .finish()
    }
}

impl Hash for FreeBlock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.offset.hash(state);
        self.power.hash(state);
        self.ptr.hash(state);
    }
}

unsafe impl Send for FreeBlock {}
unsafe impl Sync for FreeBlock {}

struct BufTree {
    migrating: bool, // TODO: After carefully checking, use atomic and move out of BufTree(Mutex)
    flushing: u32,   // TODO: After carefully checking, use atomic and move out of BufTree(Mutex)
    loading: u32,

    buf: Vec<u8>,
    blk_tree: BlockTree,

    // TODO: maybe use a more efficient DS
    alloced: HashMap<PieceKey, FreeBlockHandle>,

    flush_after_migrate: Vec<WriteJob>,
}

impl BufTree {
    fn alloc(&mut self, power: u32, key: PieceKey) -> Result<FreeBlock, AllocErr> {
        // Can not alloc when concurrently migrating.
        // migrate_task() requires all use of PieceBuf stop, And pause is an async
        // operation, which means it does not always hold lock.
        // If pause done, but now a new PieceBuf is allocated, this PieceBuf
        // may be used, which violates the assumption of migrate_task() that all
        // uses are stopped.
        if self.migrating {
            return Err(AllocErr::IsMigrating);
        }
        self.migrating_alloc(power, key)
    }

    /// alloc regardless of migrating bit
    fn migrating_alloc(&mut self, power: u32, key: PieceKey) -> Result<FreeBlock, AllocErr> {
        if self.alloced.contains_key(&key) {
            for k in self.flush_after_migrate.iter() {
                if k.piece.0 == key {
                    return Err(AllocErr::Flushing);
                }
            }
            return Err(AllocErr::Allocated);
        }
        if self.blk_tree.tree.len() as u32 + MIN_ALLOC_SIZE_POWER - 1 < power {
            return Err(AllocErr::Oversize);
        }

        match self.blk_tree.split_down(power) {
            Some((offset, power)) => {
                let fb = FreeBlock {
                    offset,
                    power,
                    ptr: unsafe { self.buf.as_mut_ptr().add(offset) },
                };
                self.alloced.insert(
                    key,
                    FreeBlockHandle {
                        free_block: fb.dup(),
                        invalidate_waiter: Vec::new(),
                        sub_manager_handle: None,
                    },
                );
                Ok(fb)
            }
            None => Err(AllocErr::NoSpace),
        }
    }

    fn free(&mut self, key: &PieceKey) -> FreeBlockHandle {
        let fbh = self
            .alloced
            .remove(key)
            .expect("free should see piecebuf in alloced list");
        let b = &fbh.free_block;
        self.blk_tree.merge_up(b.offset, b.power);
        fbh
    }

    fn count_blk_of_power(&self) -> Vec<usize> {
        let mut alloc_n = vec![0; self.blk_tree.tree.len()];
        for (_, fbh) in self.alloced.iter() {
            alloc_n[(fbh.free_block.power - MIN_ALLOC_SIZE_POWER) as usize] += 1;
        }
        alloc_n
    }

    fn can_fit_n_if_migrated(&self, target_power: u32) -> usize {
        let alloc_n = self.count_blk_of_power();

        let total = self.buf.len();
        let mut can_fit = 0;
        let mut occupied_since = total;
        for (i, b) in alloc_n.iter().enumerate() {
            let power = (i as u32) + MIN_ALLOC_SIZE_POWER;
            let sz = 1usize << power;

            if *b > 0 {
                occupied_since -= sz * b;

                let hole_sz = occupied_since % sz;
                occupied_since -= hole_sz;
                can_fit += hole_sz >> target_power;
            }
        }

        can_fit += occupied_since >> target_power;
        can_fit
    }

    // WARN: only called after pause_all_before_migrate() returns
    // when all buf ref is freed
    // TODO: longterm: optimize this
    unsafe fn migrate(&mut self) {
        // clear all blk_tree
        self.blk_tree = init_block_tree(self.buf.len());

        let mut migrate_map = vec![0usize; self.buf.len() >> MIN_ALLOC_SIZE_POWER];

        #[cfg(test)]
        {
            println!("old allocated");
            println!("{:?}", self.alloced);
        }
        let mut block_powers: Vec<Vec<(PieceKey, FreeBlockHandle)>> = Vec::new();
        for _ in 0..self.blk_tree.tree.len() {
            block_powers.push(vec![]);
        }

        for (pk, fbh) in self.alloced.drain() {
            let power_index = (fbh.free_block.power - MIN_ALLOC_SIZE_POWER) as usize;
            block_powers[power_index].push((pk, fbh));
        }

        let mut new_alloced = HashMap::new();
        for p in block_powers.into_iter() {
            for (pk, mut fbh) in p.into_iter() {
                //NOTE: self.alloc will modify self.alloced, but we not use the modified "alloced"
                // variable anyway, we will replace a new one
                let new_fb = self
                    .migrating_alloc(fbh.free_block.power, pk.clone())
                    .expect("migrate alloc should ok");

                let begin_block_idx = fbh.free_block.offset >> MIN_ALLOC_SIZE_POWER;
                for (i, block) in (begin_block_idx
                    ..begin_block_idx + (1 << (fbh.free_block.power - MIN_ALLOC_SIZE_POWER)))
                    .enumerate()
                {
                    migrate_map[block] = (new_fb.offset >> MIN_ALLOC_SIZE_POWER) + i;
                }

                fbh.free_block = new_fb.dup();
                fbh.sub_manager_handle
                    .as_ref()
                    .expect("should have handle")
                    .migrate(new_fb.offset);
                new_alloced.insert(pk, fbh);
            }
        }
        self.alloced = new_alloced;
        #[cfg(test)]
        {
            println!("new allocated");
            println!("{:?}", self.alloced);
        }

        #[cfg(test)]
        {
            println!("new block tree");
            self.blk_tree.print();
        }

        let mut blk_offset = 0;
        while blk_offset < self.buf.len() >> MIN_ALLOC_SIZE_POWER {
            let dest = migrate_map[blk_offset];
            if dest == blk_offset || dest == 0 {
                blk_offset += 1;
            } else {
                if migrate_map[dest] == 0 {
                    // dest is not used
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            self.buf.as_mut_ptr().add(blk_offset * MIN_ALLOC_SIZE),
                            self.buf.as_mut_ptr().add(dest * MIN_ALLOC_SIZE),
                            MIN_ALLOC_SIZE,
                        );
                    }
                } else {
                    // dest used
                    unsafe {
                        std::ptr::swap_nonoverlapping(
                            self.buf.as_mut_ptr().add(blk_offset * MIN_ALLOC_SIZE),
                            self.buf.as_mut_ptr().add(dest * MIN_ALLOC_SIZE),
                            MIN_ALLOC_SIZE,
                        );
                    }
                }
                migrate_map.swap(blk_offset, dest);
            }
        }

        // now restore all PieceBuf to valid for creating new refs
        // be careful not to re-enable new ref if is invalidating in progress
        // If a piece is now invalidating, the allow_new_ref should be true
        // restoring pause_new_ref won't make new refs created
        for (_, fbh) in self.alloced.iter() {
            let handle = fbh.sub_manager_handle.as_ref().expect("should have handle");
            handle.reenable();
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MigrateErr {
    Unabortable,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InvalidateErr {
    MigrateInProgress,
    Unabortable,
}

struct WriteJob {
    f: DynFileImpl,
    offset: usize,
    piece: (PieceKey, usize),
    main_cache: Arc<PieceBufPoolImpl>,
}

impl WriteJob {
    fn do_io(self) -> io::Result<()> {
        // TODO: dead lock?
        let mut bf = self.f.lock().unwrap();

        let (key, len) = &self.piece;

        // we have flushing > 0, so even if out of mutex, no migrate task will
        // run and the pointer always valid
        let buf = {
            let buf_tree = self
                .main_cache
                .buf_tree
                .lock()
                .expect("do_io lock flushing-- should OK");
            assert!(buf_tree.flushing > 0);
            assert!(!buf_tree.migrating);
            let blk = buf_tree
                .alloced
                .get(key)
                .expect("key should exist in allocated list");
            let buf = unsafe { slice::from_raw_parts(blk.free_block.ptr, *len) };
            buf
        };

        let r = bf.write_all(self.offset, buf);

        // TODO: optimize this unlock-relock pattern
        self.main_cache.free(key, true);
        // let r = Ok(());
        warn!("write offset {} len {} result {r:?}", self.offset, len,);
        #[cfg(test)]
        println!("write offset {} len {} result {r:?}", self.offset, len,);
        r
        // job.write_tx.send(r);
    }
}

fn write_worker(r: mpsc::Receiver<WriteJob>) {
    loop {
        match r.recv() {
            Ok(job) => {
                job.do_io();
            }
            Err(e) => {
                warn!("write worker error {e:?}");
                return;
            }
        }
    }
}

struct PieceBufPoolImpl {
    buf_tree: Mutex<BufTree>,
    waiting_alloc: Mutex<VecDeque<AllocReq<PieceKey>>>,

    // TODO: maybe move into buf_tree
    // TODO: is this used?
    invalidating: AtomicU32,
    // write_worker: std::sync::mpsc::Sender<WriteJob>,
}

pub trait FileImpl: Send {
    fn write_all(&mut self, offset: usize, buf: &[u8]) -> io::Result<()>;
    fn read_all(&mut self, offset: usize, buf: &mut [u8]) -> io::Result<()>;
}

// TODO: FIXME: use BackFile and make inner of BackFile dyn
pub type DynFileImpl = Arc<Mutex<dyn FileImpl>>;

// This is !Clone
pub(crate) struct PieceBuf {
    b: FreeBlock,
    key: PieceKey,
    len: usize,
    dirty: bool,
    under_file: Option<DynFileImpl>,
    main_cache: Arc<PieceBufPoolImpl>,
}

// SAFETY: the reference of free buffer can be sent across threads.
// and dropped by a different thread than created one.
unsafe impl Send for PieceBuf {}
// SAFETY: PieceBuf's interface is read only
unsafe impl Sync for PieceBuf {}

impl Debug for PieceBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PieceBuf")
            .field("key", &self.key)
            .field("dirty", &self.dirty)
            .field("free_block", &self.b)
            .field("len", &self.len)
            .finish()
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) struct PieceKey {
    pub hash: Arc<[u8; 20]>,
    pub offset: usize,
}

impl Debug for PieceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PieceBuf")
            .field("hash", &format_args!("{:02x?}", self.hash))
            .field("offset", &self.offset)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum AllocErr {
    NoSpace,
    Allocated,
    Flushing, // TODO: maybe add Loading state?
    LoadErr(io::ErrorKind),
    Oversize,
    StartMigrating,
    IsMigrating,
}

pub trait AbortHandle
where
    Self: Send + Sync,
{
    async fn abort_all(&self);
}

/// This trait is capable of abortable use
pub trait FromPieceBuf
where
    Self: Sized,
{
    fn new_abort(p: PieceBuf) -> (Self, SubAbortHandle<PieceBuf>);
}

impl PieceBufPool {
    pub fn new(size: usize) -> Self {
        Self {
            inner: Arc::new(PieceBufPoolImpl::new(size)),
        }
    }

    /// alloc PieceBuf
    pub fn alloc(
        &self,
        key: PieceKey,
        under_file: Option<DynFileImpl>,
        load_from_file: bool,
        size: usize,
    ) -> Result<PieceBuf, AllocErr> {
        let post_work = |pb: PieceBuf, _: &mut BufTree| pb;
        self.alloc_general(key, under_file, load_from_file, size, post_work)
    }

    /// alloc abortable PieceBuf
    pub fn alloc_abort<T>(
        &self,
        key: PieceKey,
        under_file: Option<DynFileImpl>,
        load_from_file: bool,
        size: usize,
    ) -> Result<T, AllocErr>
    where
        T: FromPieceBuf,
    {
        let post_work = |pb: PieceBuf, tree: &mut BufTree| {
            let key = pb.key.clone();
            let (ret, abort_handle) = T::new_abort(pb);
            let fbh = tree
                .alloced
                .get_mut(&key)
                .expect("just alloced should exist");
            fbh.sub_manager_handle = Some(abort_handle);
            ret
        };
        self.alloc_general(key, under_file, load_from_file, size, post_work)
    }

    fn alloc_general<T, F>(
        &self,
        key: PieceKey,
        under_file: Option<DynFileImpl>,
        load_from_file: bool,
        size: usize,
        post_piecebuf: F,
    ) -> Result<T, AllocErr>
    where
        F: FnOnce(PieceBuf, &mut BufTree) -> T,
    {
        let size_fixed = if size < MIN_ALLOC_SIZE {
            MIN_ALLOC_SIZE
        } else {
            size.next_power_of_two()
        };
        debug_assert_eq!(size_fixed % MIN_ALLOC_SIZE, 0);
        let power = size_fixed.ilog2();

        #[cfg(mloom)]
        if under_file.is_some() {
            panic!("mloom mode can only use (under_file: None) because under_file: Some(_) requires tokio");
        }

        let (mut pb, mut buf_tree_guard) = {
            let mut buf_tree_guard = self
                .inner
                .buf_tree
                .lock()
                .expect("alloc sub lock should OK");

            match buf_tree_guard.alloc(power, key.clone()) {
                Ok(fb) => (
                    PieceBuf {
                        b: fb,
                        key,
                        len: size,
                        dirty: false,
                        under_file,
                        main_cache: self.inner.clone(),
                    },
                    buf_tree_guard,
                ),
                Err(r @ AllocErr::Allocated | r @ AllocErr::Oversize) => {
                    #[cfg(test)]
                    println!("poll key {:?} error reason: {r:?}", key.clone());
                    return Err(r);
                }
                Err(e) => {
                    if buf_tree_guard.flushing == 0
                        && buf_tree_guard.loading == 0
                        && !buf_tree_guard.migrating
                        && buf_tree_guard.can_fit_n_if_migrated(power) > 0
                    {
                        // TODO: do we need a from_async flag, so that only fire migrate_task
                        // when calling inside a async environment?
                        let pool_impl = self.inner.clone();
                        start_migrate_task(buf_tree_guard, pool_impl);
                        return Err(AllocErr::StartMigrating);
                    } else {
                        return Err(e);
                    }
                }
            }
        };

        match pb.under_file.clone() {
            Some(file) if load_from_file => {
                buf_tree_guard.loading += 1;
                drop(buf_tree_guard);

                // we are out of lock
                // but we are holding pb, and pb don't have abort handle
                // no one can moves us
                let mut f = file.lock().expect("alloc lock file should OK");
                f.read_all(pb.key.offset, pb.as_mut());

                let mut buf_tree_guard = self
                    .inner
                    .buf_tree
                    .lock()
                    .expect("alloc() re-lock after load should OK");
                let wrapped = post_piecebuf(pb, &mut buf_tree_guard);
                buf_tree_guard.loading -= 1;

                // TODO: FIXME: maybe wake alloc
                Ok(wrapped)
            }
            _ => Ok(post_piecebuf(pb, &mut buf_tree_guard)),
        }
    }

    /// async alloc PieceBuf
    ///
    /// Equivalent to
    /// ```ignore
    /// async async_alloc(&self, size: usize) -> PieceBuf
    /// ```
    ///
    /// wait until enough free space
    /// as long as size <= max size of buffer, waits until
    /// enough space, else return None
    // TODO: make this async fn, and make real impl to PieceBufPoolImpl
    pub fn async_alloc(
        &self,
        key: PieceKey,
        under_file: Option<DynFileImpl>,
        load_from_file: bool,
        size: usize,
    ) -> AllocPieceBufFut {
        AllocPieceBufFut {
            inner: AllocFutInner::Allocating(Allocating {
                key,
                under_file,
                load_from_file,
                size,
                pool: self,
                waiting: false,
                valid: Arc::new(AtomicU32::new(WAITING)),
            }),
        }
    }

    pub fn async_alloc_abort<T>(
        &self,
        key: PieceKey,
        under_file: Option<DynFileImpl>,
        load_from_file: bool,
        size: usize,
    ) -> AllocPieceBufAbortFut<T>
    where
        T: FromPieceBuf + Unpin,
    {
        AllocPieceBufAbortFut {
            inner: AllocFutInner::Allocating(Allocating {
                key,
                size,
                under_file,
                load_from_file,
                pool: self,
                waiting: false,
                valid: Arc::new(AtomicU32::new(WAITING)),
            }),
            _t: PhantomData,
        }
    }

    async fn invalidate(&self, key: PieceKey) -> Result<(), InvalidateErr> {
        self.inner.invalidate(key).await
    }
}

impl PieceBufPoolImpl {
    fn new(size: usize) -> Self {
        let size_fixed = size.next_multiple_of(MIN_ALLOC_SIZE);

        // let (job_tx, job_rx) = std::sync::mpsc::channel();

        // #[cfg(not(mloom))]
        // tokio::task::spawn_blocking(move || write_worker(job_rx));

        Self {
            buf_tree: Mutex::new(BufTree {
                migrating: false,
                flushing: 0,
                loading: 0,

                buf: vec![0u8; size_fixed],
                blk_tree: init_block_tree(size_fixed),
                alloced: HashMap::new(),
                flush_after_migrate: Vec::new(),
            }),
            waiting_alloc: Mutex::new(VecDeque::new()),
            invalidating: AtomicU32::new(0),
            // write_worker: job_tx,
        }
    }

    /// alloc FreeBlock
    fn alloc(&self, key: PieceKey, size: usize) -> Result<FreeBlock, AllocErr> {
        let size_fixed = if size < MIN_ALLOC_SIZE {
            MIN_ALLOC_SIZE
        } else {
            size.next_power_of_two()
        };
        debug_assert_eq!(size_fixed % MIN_ALLOC_SIZE, 0);
        let power = size_fixed.ilog2();

        let mut buf_tree_guard = self.buf_tree.lock().expect("alloc lock should OK");
        buf_tree_guard.alloc(power, key)
    }

    /// return PieceBuf to pool
    fn free(&self, key: &PieceKey, after_flush: bool) {
        {
            let mut buf_tree_guard = self.buf_tree.lock().expect("free lock should OK");
            let mut fh = buf_tree_guard.free(key);

            #[cfg(test)]
            println!("free piecebuf");

            for w in fh.invalidate_waiter.drain(0..) {
                #[cfg(test)]
                println!("invalidate_waker some {:?}", w);
                w.notify_one();
            }

            if after_flush {
                buf_tree_guard.flushing -= 1;
            }
            // buf_tree's lock dropped here, prevents deadlock
        }

        {
            let mut waiting_alloc = self
                .waiting_alloc
                .lock()
                .expect("waiting alloc lock should OK");

            wake_next_waiting_alloc(&mut waiting_alloc);
        }
    }

    // TODO: after invalidate, the block should not be allocated before
    // further operations like flush to disk / migrate
    // TODO: what if this gets dropped?
    async fn invalidate(&self, key: PieceKey) -> Result<(), InvalidateErr> {
        self.invalidating.fetch_add(1, Ordering::Acquire);
        let (wait_invalid, wait_abort) = {
            let mut buf_tree_guard = self.buf_tree.lock().expect("invalidate lock should OK");
            if buf_tree_guard.migrating {
                // TODO: FIXME: is this a must?
                return Err(InvalidateErr::MigrateInProgress);
            }
            if let Some(fbh) = buf_tree_guard.alloced.get_mut(&key) {
                let aborter = fbh.sub_manager_handle.clone();
                #[cfg(test)]
                println!("invalidate add wait_invalid");
                let waiter = Arc::new(Notify::new());
                fbh.invalidate_waiter.push(waiter.clone());
                (waiter, aborter)
            } else {
                return Ok(());
            }
            // TODO: tell this piece to stop all use of reference (async read/write)
            // need a reference to allocated PieceBuf
        };
        if let Some(aborter) = wait_abort {
            aborter.invalidate_all().await;
        } else {
            #[cfg(test)]
            println!("try to wait unabortable piece");
            // TODO: what if try to invalidate unabortable piece?
        }
        #[cfg(test)]
        println!("waiting wait_invalid {:?}", wait_invalid);
        wait_invalid.notified().await;
        self.invalidating.fetch_sub(1, Ordering::Release);
        Ok(())
        // let buf_tree_guard = self.buf_tree.lock().expect("invalidate lock should OK");
        // assert!(!buf_tree_guard.alloced.contains_key(&f));
    }

    async fn pause_all_before_migrate(&self) -> Result<(), MigrateErr> {
        #[cfg(test_flush_migrate_concurrent)]
        {
            use tokio::time;
            println!("{}", DROP_CONCURRENT.available_permits());
            _ = DROP_CONCURRENT.acquire_many(2).await;
            time::sleep(time::Duration::from_millis(10)).await;
        }

        let wait_abort = {
            let mut buf_tree_guard = self.buf_tree.lock().expect("pause lock should OK");
            assert_eq!(buf_tree_guard.migrating, true);
            let mut aborters = Vec::new();
            for (fb, fbh) in buf_tree_guard.alloced.iter_mut() {
                #[cfg(test)]
                println!("pause_all add wait_abort");

                let aborter = fbh.sub_manager_handle.clone();
                match aborter {
                    Some(a) => {
                        aborters.push(a);
                    }
                    None => {
                        #[cfg(test)]
                        println!("block cannot abort fb {:?}", fb);

                        return Err(MigrateErr::Unabortable);
                    }
                }
            }
            aborters
        };

        let mut ts = tokio::task::JoinSet::new();
        for a in wait_abort.into_iter() {
            ts.spawn(async move { a.abort_all().await });
        }
        ts.join_all().await;

        #[cfg(test)]
        println!("all aborted(paused)");
        Ok(())
    }

    // fn frag_status(&self) -> Option<f32> {
    //     if self.invalidating.load(Ordering::Relaxed) > 0 {
    //         return None;
    //     }
    //     let mut buf_tree_guard = self.buf_tree.lock().expect("frag status lock should OK");
    //     if self.invalidating.load(Ordering::Relaxed) > 0 {
    //         return None;
    //     }

    //     // TODO: move this to BufTree
    //     let total_len = buf_tree_guard.buf.len();
    //     let inuse_len = buf_tree_guard.alloced.keys().fold(0, |acc, fb| {
    //         debug_assert!(fb.valid.load(Ordering::Relaxed) == true);
    //         acc + (1usize << fb.power)
    //     });
    //     let free_len = total_len - inuse_len;

    //     for i in 0..buf_tree_guard.blk_tree.tree.len() {
    //         let mut allocable_num = 0;
    //         let mut base = 1usize << (i as u32 + MIN_ALLOC_SIZE_POWER);
    //         for blk in buf_tree_guard.blk_tree.tree.iter().skip(i) {
    //             allocable_num += blk.blk.len() * base;
    //             base <<= 1;
    //         }
    //         // TODO: theoritical max alloctable
    //         let ratio = (allocable_size as f32) / (free_len as f32);
    //     }

    //     None
    // }
}

impl Drop for PieceBuf {
    /// Must be very careful since PieceBuf maybe wrapped in struct
    /// which supports "abort". And we might be dropping some piece while
    /// concurrently migrating.
    // SAFETY: TODO: is this sound?
    fn drop(&mut self) {
        #[cfg(test)]
        println!("free PieceBuf {:?}", self);

        #[cfg(test_flush_migrate_concurrent)]
        {
            DROP_CONCURRENT.add_permits(1);
            println!("add permit");
        }

        // self.b may points to a DANGLING pointer, even if dirty == false or
        // migrating == false. But key is always valid since no other way except
        // drop() can purge a key, so the block associated with key always exist.
        if let Self {
            dirty: true,
            under_file: Some(f_impl),
            ..
        } = self
        {
            let main_cache = &self.main_cache;
            let mut buf_tree = main_cache
                .buf_tree
                .lock()
                .expect("PieceBuf drop lock flushing++ should OK");

            let job = WriteJob {
                f: f_impl.clone(),
                offset: self.key.offset,
                piece: (self.key.clone(), self.len),
                main_cache: self.main_cache.clone(),
            };

            buf_tree.flushing += 1;
            if buf_tree.migrating {
                // we should be in progress of pausing all reads
                // or after migrating done
                // we CAN'T be concurrently with real migrating process
                // because real migrating process holds BufTree lock!
                //
                // when pausing, pointers will not move, valid pointers are
                // always valid. But there may be more than one migrate() called
                // since drop() begin, our self.b might also be DANGLING

                buf_tree.flush_after_migrate.push(job);
            } else {
                // There are 2 possibilities:
                // 1. no migration at entire lifetime of drop()
                // 2. after entering drop(), migration is called and done
                //
                // In situation 2, the self.b maybe DANGLING, but key won't

                // TODO: FIXME: check return value of do_io
                tokio::task::spawn_blocking(move || job.do_io());

                #[cfg(debug_assertions)]
                println!("wake worker to flush dirty piece {:?}", self);
            }
        } else {
            // No matter self.b dangling or not, we don't touch these fields
            // at all. The key is always valid.
            self.main_cache.free(&self.key, false);
        }
    }
}

impl AsMut<[u8]> for PieceBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.set_dirty();
        unsafe { slice::from_raw_parts_mut(self.b.ptr, self.len) }
    }
}

impl AsRef<[u8]> for PieceBuf {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.b.ptr, self.len) }
    }
}

impl Deref for PieceBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl DerefMut for PieceBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl PieceBuf {
    // reset the underlying buf offset. only use this when you know
    // the offset(also ptr) is valid and "allocated" to this Piece
    // otherwise causing racing / wild pointer
    //
    // TODO: fixme: this should be private
    // maybe merge global.rs and piece.rs together
    pub(crate) unsafe fn reset_offset(&mut self, new_offset: usize) {
        // TODO: FIXME: check new_offset in range

        #[cfg(test)]
        println!("PieceBuf: migrate from {} to {}", self.b.offset, new_offset);

        self.b.ptr = self.b.ptr.sub(self.b.offset).add(new_offset);
        self.b.offset = new_offset;
    }

    pub(crate) fn set_dirty(&mut self) {
        self.dirty = true;
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn offset_len(&self) -> (usize, usize) {
        (
            self.b.offset / MIN_ALLOC_SIZE,
            1usize << (self.b.power - MIN_ALLOC_SIZE_POWER),
        )
    }
}

pub(crate) struct AllocReq<I> {
    pub key: I,
    pub waker: Waker,
    pub valid: Arc<AtomicU32>,
}

const DROPPED: u32 = 0b01;
const DONE: u32 = 0b010; // TODO: really need this?
const WAITING: u32 = 0;
const WAKING: u32 = 0b100;

pub(crate) fn wake_next_waiting_alloc<I: Debug>(waiting_alloc: &mut VecDeque<AllocReq<I>>) {
    while let Some(v) = waiting_alloc.pop_front() {
        #[cfg(test)]
        println!("try waking {:?}", v.key);
        match v
            .valid
            .compare_exchange(WAITING, WAKING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                v.waker.wake();
                #[cfg(test)]
                println!("wake waiting {:?}", v.key);
                break;
            }
            Err(actual) => {
                // if DONE, this future is Ready
                // if DROPPED, no one waiting future
                // pick next one
                debug_assert!(actual == DROPPED || actual == DONE || actual == WAKING);

                #[cfg(test)]
                println!("no wake {:?} because actual state {actual}", v.key);
            }
        }
    }
}

enum AllocFutInner<'a> {
    Allocating(Allocating<'a>),
    Loading(Loading<'a>),
}

impl AllocFutInner<'_> {
    fn poll_alloc<F, T>(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        post_piecebuf: F,
    ) -> Poll<Result<T, AllocErr>>
    where
        F: FnOnce(PieceBuf, &mut BufTree) -> T,
    {
        let fut = self.get_mut();
        match fut {
            AllocFutInner::Allocating(a) => {
                let alloc_res = ready!(Pin::new(&mut *a).poll_alloc(cx));
                let mut buf_tree_guard = a
                    .pool
                    .inner
                    .buf_tree
                    .lock()
                    .expect("poll_alloc loading lock should OK");
                match alloc_res {
                    Ok(mut pb) => {
                        match a.under_file.clone() {
                            Some(file) if a.load_from_file => {
                                buf_tree_guard.loading += 1;
                                let offset_before_load = buf_tree_guard
                                    .alloced
                                    .get(&a.key)
                                    .expect("should exist")
                                    .free_block
                                    .offset;
                                drop(buf_tree_guard);

                                // we are out of lock
                                // but we are holding pb, and pb don't have abort handle
                                // no one can moves us
                                let done = Arc::new(LoadJob(
                                    UnsafeCell::new(None),
                                    AtomicBool::new(false),
                                ));
                                let reader_done = done.clone();
                                let waker = cx.waker().clone();
                                tokio::task::spawn_blocking(move || {
                                    let mut f = file.lock().expect("alloc lock file should OK");
                                    let res = f.read_all(pb.key.offset, pb.as_mut());
                                    unsafe { reader_done.store(res) };
                                    atomic::fence(Ordering::Acquire); // make sure we set done first, then wake
                                    waker.wake()
                                });

                                *fut = AllocFutInner::Loading(Loading { pool: a.pool, done });
                                Poll::Pending
                            }
                            _ => {
                                let wrapped = post_piecebuf(pb, &mut buf_tree_guard);
                                Poll::Ready(Ok(wrapped))
                            }
                        }
                    }
                    Err(e) => {
                        warn!("alloc error {e:?}");
                        return Poll::Ready(Err(e));
                    }
                }
            }
            AllocFutInner::Loading(load) => Pin::new(load).poll_load(cx, post_piecebuf),
        }
        // if let Some(mut pb) = fut.res_piece_buf {
        //     let mut buf_tree_guard = self
        //         .pool
        //         .inner
        //         .buf_tree
        //         .lock()
        //         .expect("poll_alloc loading lock should OK");
        //     match fut.under_file.clone() {
        //         Some(file) if fut.load_from_file => {
        //             buf_tree_guard.loading += 1;
        //             let offset_before_load = buf_tree_guard
        //                 .alloced
        //                 .get(&fut.key)
        //                 .expect("should exist")
        //                 .free_block
        //                 .offset;
        //             drop(buf_tree_guard);

        //             // we are out of lock
        //             // but we are holding pb, and pb don't have abort handle
        //             // no one can moves us
        //             let mut f = file.lock().expect("alloc lock file should OK");
        //             // tokio::task::spawn_blocking(move {
        //             //     f.read_all(pb.key.offset, pb.as_mut());
        //             // });

        //             let mut buf_tree_guard = self
        //                 .pool
        //                 .inner
        //                 .buf_tree
        //                 .lock()
        //                 .expect("alloc() re-lock after load should OK");
        //             let offset_after_load = buf_tree_guard
        //                 .alloced
        //                 .get(&fut.key)
        //                 .expect("should exist")
        //                 .free_block
        //                 .offset;
        //             assert_eq!(offset_before_load, offset_after_load);
        //             let wrapped = post_piecebuf(pb, &mut buf_tree_guard);
        //             buf_tree_guard.loading -= 1;

        //             // TODO: FIXME: maybe wake alloc
        //             Poll::Ready(Ok(wrapped))
        //         }
        //         _ => Poll::Ready(Ok(post_piecebuf(pb, &mut buf_tree_guard))),
        //     }
        // } else {
        //     self.poll_alloc_inner(cx)
        // }
    }
}

struct Allocating<'a> {
    key: PieceKey,
    under_file: Option<DynFileImpl>,
    load_from_file: bool,
    pool: &'a PieceBufPool,
    waiting: bool,
    valid: Arc<AtomicU32>,
    size: usize,
}

impl Allocating<'_> {
    fn poll_alloc(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<PieceBuf, AllocErr>> {
        let fut = self.get_mut();
        #[cfg(test)]
        println!("poll key {:?}", fut.key);

        let mut waiting_alloc = fut.pool.inner.waiting_alloc.lock().unwrap();
        if !fut.waiting {
            if !waiting_alloc.is_empty() {
                waiting_alloc.push_back(AllocReq {
                    key: fut.key.clone(),
                    waker: cx.waker().clone(),
                    valid: fut.valid.clone(),
                });

                #[cfg(test)]
                println!("poll key {:?} pending 1", fut.key);

                fut.waiting = true;
                fut.valid.swap(WAITING, Ordering::Release);
                return Poll::Pending;
            }

            // alloc() holds both lock of block_tree and waiting_alloc
            match alloc_fn(fut.pool, fut.key.clone(), fut.under_file.clone(), fut.size) {
                ok @ Ok(_) => {
                    let old = fut.valid.swap(DONE, Ordering::Release);
                    debug_assert_eq!(old, WAITING);
                    #[cfg(test)]
                    println!("poll key {:?} ready 2", fut.key);
                    Poll::Ready(ok)
                }
                Err(r @ AllocErr::Allocated | r @ AllocErr::Oversize) => {
                    #[cfg(test)]
                    println!("poll key {:?} error reason: {r:?}", fut.key);
                    Poll::Ready(Err(r))
                }
                Err(e) => {
                    waiting_alloc.push_back(AllocReq {
                        key: fut.key.clone(),
                        waker: cx.waker().clone(),
                        valid: fut.valid.clone(),
                    });
                    fut.waiting = true;
                    #[cfg(test)]
                    println!("poll key {:?} pending 3, reason: {e:?}", fut.key);

                    fut.valid.swap(WAITING, Ordering::Release);
                    Poll::Pending
                }
            }
        } else {
            match alloc_fn(fut.pool, fut.key.clone(), fut.under_file.clone(), fut.size) {
                ok @ Ok(_) => {
                    let old = fut.valid.swap(DONE, Ordering::Release);
                    debug_assert!(old == WAITING || old == WAKING);

                    wake_next_waiting_alloc(&mut waiting_alloc);

                    #[cfg(test)]
                    println!("poll key {:?} ready 4", fut.key);
                    Poll::Ready(ok)
                }
                Err(r @ AllocErr::Allocated | r @ AllocErr::Oversize) => {
                    // No change to the VALID flag, flag is still WAKING.
                    // When this future dropped, a new waiting task is waked.
                    // But if future not dropped immediately, following
                    // allocations are blocked.
                    // So we wake one here.
                    // TODO: maybe we change VALID flag to DONE to reduce
                    // spurious wakes.
                    wake_next_waiting_alloc(&mut waiting_alloc);

                    #[cfg(test)]
                    println!("poll key {:?} error reason: {r:?}", fut.key);

                    Poll::Ready(Err(r))
                }
                Err(e) => {
                    waiting_alloc.push_front(AllocReq {
                        key: fut.key.clone(),
                        waker: cx.waker().clone(),
                        valid: fut.valid.clone(),
                    }); // try to be the first
                    #[cfg(test)]
                    println!("poll key {:?} pending 5 reason {e:?}", fut.key);

                    fut.valid.swap(WAITING, Ordering::Release);
                    Poll::Pending
                }
            }
        }
    }
}

impl Drop for Allocating<'_> {
    fn drop(&mut self) {
        // TODO: optimize this
        let state = self.valid.swap(DROPPED, Ordering::Acquire);

        #[cfg(test)]
        println!(
            "drop AllocPieceBufFut key {:?}, old-state {}",
            self.key, state
        );

        match state {
            WAKING => {
                // executor wants to wake us, but we are dropped before
                // being polled (if polled, state can't be WAKING)
                // wake another one
                let mut waiting_alloc = self.pool.inner.waiting_alloc.lock().unwrap();
                wake_next_waiting_alloc(&mut waiting_alloc);
            }
            w => debug_assert!(w == WAITING || w == DONE),
        }
    }
}

struct LoadJob(UnsafeCell<Option<io::ErrorKind>>, AtomicBool);
unsafe impl Sync for LoadJob {}
impl LoadJob {
    // make sure only call this once
    unsafe fn store(&self, r: io::Result<()>) {
        debug_assert!(!self.1.load(Ordering::Relaxed));
        if let Err(e) = r {
            *self.0.get() = Some(e.kind());
        }
        self.1.store(true, Ordering::Release); // make sure read done before atomic set
        atomic::fence(Ordering::Acquire);
    }

    // first option for done or not, second for err or not
    fn load(&self) -> Option<Option<io::ErrorKind>> {
        if self.1.load(Ordering::Acquire) {
            unsafe { Some(*self.0.get()) }
        } else {
            None
        }
    }
}

struct Loading<'a> {
    pool: &'a PieceBufPool,
    done: Arc<LoadJob>,
}

impl Loading<'_> {
    fn poll_load<F, T>(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        post_piecebuf: F,
    ) -> Poll<Result<T, AllocErr>>
    where
        F: FnOnce(PieceBuf, &mut BufTree) -> T,
    {
        let fut = self.get_mut();
        if let Some(r) = fut.done.load() {
            let mut buf_tree_guard = fut
                .pool
                .inner
                .buf_tree
                .lock()
                .expect("poll_alloc loading lock should OK");
            buf_tree_guard.loading -= 1;
            match r {
                None => {}
                Some(e) => Poll::Ready(Err(AllocErr::LoadErr(e))),
            }
        } else {
            Poll::Pending
        }
    }
}

#[must_use = "futures do nothing unless you poll them"]
pub(crate) struct AllocPieceBufFut<'a> {
    inner: AllocFutInner<'a>,
}

async fn migrate_task(pool_impl: Arc<PieceBufPoolImpl>) {
    if let Err(e) = pool_impl.pause_all_before_migrate().await {
        warn!("pause failed: {e:?}");
        #[cfg(test)]
        println!("pause failed: {e:?}");

        // pause failed, maybe some unabortable task
        // just wake one pending alloc request
        {
            let mut guard = pool_impl
                .buf_tree
                .lock()
                .expect("invalidate task lock should OK");
            guard.migrating = false;
        }
        {
            let mut waiting_alloc = pool_impl
                .waiting_alloc
                .lock()
                .expect("migrate task lock waiting alloc should OK");
            wake_next_waiting_alloc(&mut waiting_alloc);
        }
    } else {
        // all paused

        #[cfg(test)]
        println!("migrate_task: paused all pieceBufs");

        {
            let mut buf_tree_guard = pool_impl
                .buf_tree
                .lock()
                .expect("invalidate task lock should OK");
            unsafe {
                buf_tree_guard.migrate();
            }

            buf_tree_guard.migrating = false;

            for job in buf_tree_guard.flush_after_migrate.drain(0..) {
                #[cfg(test)]
                println!("migrate_task: migrating done, wake up one pending flush request");
                // TODO: FIXME: check return value of do_io
                tokio::task::spawn_blocking(move || job.do_io());
            }
        }
        {
            // migrate done, now wake up one pending alloc request

            #[cfg(test)]
            println!("migrate_task: migrating done, wake up one pending alloc request");

            let mut waiting_alloc = pool_impl
                .waiting_alloc
                .lock()
                .expect("migrate task lock waiting alloc should OK");
            wake_next_waiting_alloc(&mut waiting_alloc);
        }
    }
}

fn start_migrate_task(buf_tree: MutexGuard<'_, BufTree>, pool_impl: Arc<PieceBufPoolImpl>) {
    assert!(!buf_tree.migrating);
    assert!(buf_tree.flushing == 0);
    assert!(buf_tree.loading == 0);
    start_migrate_task_inner(buf_tree, pool_impl);
}

fn start_migrate_task_inner(
    mut buf_tree: MutexGuard<'_, BufTree>,
    pool_impl: Arc<PieceBufPoolImpl>,
) -> JoinHandle<()> {
    // must set migrating in sync code
    // so at most one migrate_task can run
    buf_tree.migrating = true;
    drop(buf_tree);
    tokio::spawn(migrate_task(pool_impl))
}

impl Future for AllocPieceBufFut<'_> {
    type Output = Result<PieceBuf, AllocErr>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner_fut = Pin::new(&mut self.get_mut().inner);
        let post_work = |pb: PieceBuf, _: &mut BufTree| pb;
        inner_fut.poll_alloc(cx, post_work)
    }
}

#[must_use = "futures do nothing unless you poll them"]
pub(crate) struct AllocPieceBufAbortFut<'a, T> {
    inner: AllocFutInner<'a>,
    _t: PhantomData<T>,
}

impl<T> Future for AllocPieceBufAbortFut<'_, T>
where
    T: FromPieceBuf + Unpin,
{
    type Output = Result<T, AllocErr>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner_fut = Pin::new(&mut self.get_mut().inner);
        let post_work = |pb: PieceBuf, tree: &mut BufTree| {
            let key = pb.key.clone();
            let (ret, abort_handle) = T::new_abort(pb);
            let fbh = tree
                .alloced
                .get_mut(&key)
                .expect("just alloced should exist");
            fbh.sub_manager_handle = Some(abort_handle);
            ret
        };
        let buf = ready!(inner_fut.poll_alloc(cx, post_work));
    }
}

#[derive(Debug, Eq, PartialEq)]
struct BlockList {
    size: usize,
    blk: BTreeSet<usize>,
}

#[derive(Debug, Eq, PartialEq)]
struct BlockTree {
    // TODO: perf: maybe use a data structure more efficient than BTreeSet
    tree: Vec<BlockList>,
}

type OffsetPower = (usize, u32);

impl BlockTree {
    /// target_power must >= MIN_ALLOC_SIZE_POWER
    fn split_down(&mut self, target_power: u32) -> Option<OffsetPower> {
        let (mut now_offset, mut now_order) = pop_first_vacant(&mut self.tree, target_power)?;

        let mut now_size = 1usize << (now_order as u32 + MIN_ALLOC_SIZE_POWER);
        let target_order = (target_power - MIN_ALLOC_SIZE_POWER) as usize;

        while now_order > target_order {
            // split this block then push this block to list of smaller blocks
            let left_blk = now_offset;
            now_order -= 1;
            self.tree[now_order].blk.insert(left_blk);
            now_size >>= 1;
            now_offset += now_size;
        }
        Some((now_offset, target_power))
        // unreachable!("should always find a hole");
    }

    /// returns a block and merges up
    fn merge_up(&mut self, offset: usize, block_size_power: u32) {
        let mut now_size = 1usize << block_size_power;
        let mut now_idx = (block_size_power - MIN_ALLOC_SIZE_POWER) as usize;
        let mut now_offset = offset;

        while now_idx < self.tree.len() {
            let buddy_blk = buddy_blk_of(now_offset, now_size);
            if self.tree[now_idx].blk.remove(&buddy_blk) {
                now_size <<= 1;
                now_idx += 1;
                now_offset = now_offset.min(buddy_blk);
            } else {
                self.tree[now_idx].blk.insert(now_offset);
                return;
            };
        }
        unreachable!("should always find a hole");
    }

    #[cfg(test)]
    fn print(&self) {
        println!("empty blocks:");
        for (i, blk) in self.tree.iter().enumerate() {
            println!("  power {}, {:?}", i as u32 + MIN_ALLOC_SIZE_POWER, blk);
        }
    }
}

fn pop_first_vacant(list: &mut Vec<BlockList>, target_power: u32) -> Option<(usize, usize)> {
    for (p, b) in list
        .iter_mut()
        .enumerate()
        .skip((target_power - MIN_ALLOC_SIZE_POWER) as usize)
    {
        if let Some(vacant_blk) = b.blk.pop_last() {
            return Some((vacant_blk, p));
        }
    }
    None
}

/// total_size must be multiple of MIN_ALLOC_SIZE
fn init_block_tree(total_size: usize) -> BlockTree {
    debug_assert_eq!(total_size % MIN_ALLOC_SIZE, 0);

    // TODO: maybe can use bit shift right loop, but this is init
    // so overhead is not a problem
    let max_block_size = prev_power_of_two(total_size);
    let max_power = max_block_size.ilog2();
    let mut t: Vec<BlockList> = (MIN_ALLOC_SIZE_POWER..=max_power)
        .into_iter()
        .map(|power| BlockList {
            size: 1usize << power,
            blk: BTreeSet::new(),
        })
        .collect();

    let mut block_size = max_block_size;
    let mut power = max_power;
    let mut offset = 0usize;
    while power >= MIN_ALLOC_SIZE_POWER {
        while offset + block_size <= total_size {
            t[(power - MIN_ALLOC_SIZE_POWER) as usize]
                .blk
                .insert(offset);
            offset += block_size;
        }
        power -= 1;
        block_size >>= 1;
    }
    BlockTree { tree: t }
}

// block_size must be power of 2
// offset must be multiple of block_size
#[inline]
fn buddy_blk_of(offset: usize, block_size: usize) -> usize {
    debug_assert!(block_size.is_power_of_two());
    debug_assert!(offset % block_size == 0);
    offset ^ block_size
}

#[inline]
fn prev_power_of_two(s: usize) -> usize {
    if s == 0 {
        panic!("s == 0");
    }
    if s.is_power_of_two() {
        s
    } else {
        s.next_power_of_two() >> 1
    }
}

#[cfg(test)]
mod test {
    use tokio::io::duplex;

    use crate::cache::AbortErr;
    use crate::cache::ArcCache;
    use crate::cache::AsyncAbortRead;
    use crate::cache::GetRefErr;

    use super::*;
    use tokio_test::task;

    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

    #[test]
    fn test_mul_of_2() {
        let cases = [
            (1, 1),
            (2, 2),
            (3, 2),
            (4, 4),
            (5, 4),
            (6, 4),
            (7, 4),
            (8, 8),
        ];
        for (i, o) in cases {
            assert_eq!((i, prev_power_of_two(i)), (i, o));
        }
    }

    #[test]
    fn test_init_block_list() {
        let r = init_block_tree(11 * MIN_ALLOC_SIZE);
        assert_eq!(
            r,
            BlockTree {
                tree: vec![
                    BlockList {
                        size: MIN_ALLOC_SIZE,
                        blk: BTreeSet::from([10 * MIN_ALLOC_SIZE])
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 1,
                        blk: BTreeSet::from([8 * MIN_ALLOC_SIZE])
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 2,
                        blk: BTreeSet::from([])
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 3,
                        blk: BTreeSet::from([0])
                    },
                ]
            }
        )
    }

    #[test]
    fn test_split() {
        let mut t = BlockTree {
            tree: vec![
                BlockList {
                    size: MIN_ALLOC_SIZE,
                    blk: BTreeSet::from([1, 3, 8].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 1,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 2,
                    blk: BTreeSet::from([4].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 3,
                    blk: BTreeSet::from([]),
                },
            ],
        };

        let r = t.split_down(MIN_ALLOC_SIZE_POWER + 1);
        assert_eq!(r, Some((6 * MIN_ALLOC_SIZE, MIN_ALLOC_SIZE_POWER + 1)));
        assert_eq!(
            t,
            BlockTree {
                tree: vec![
                    BlockList {
                        size: MIN_ALLOC_SIZE,
                        blk: BTreeSet::from([1, 3, 8].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 1,
                        blk: BTreeSet::from([4].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 2,
                        blk: BTreeSet::from([]),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 3,
                        blk: BTreeSet::from([]),
                    },
                ]
            }
        )
    }

    #[test]
    fn test_split2() {
        let mut v = BlockTree {
            tree: vec![
                BlockList {
                    size: MIN_ALLOC_SIZE,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 1,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 2,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 3,
                    blk: BTreeSet::from([0]),
                },
            ],
        };

        let r = v.split_down(MIN_ALLOC_SIZE_POWER);
        assert_eq!(r, Some((7 * MIN_ALLOC_SIZE, MIN_ALLOC_SIZE_POWER)));
        assert_eq!(
            v,
            BlockTree {
                tree: vec![
                    BlockList {
                        size: MIN_ALLOC_SIZE,
                        blk: BTreeSet::from([6].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 1,
                        blk: BTreeSet::from([4].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 2,
                        blk: BTreeSet::from([0].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 3,
                        blk: BTreeSet::from([]),
                    },
                ]
            }
        )
    }

    #[test]
    fn test_merge() {
        let mut v = BlockTree {
            tree: vec![
                BlockList {
                    size: MIN_ALLOC_SIZE,
                    blk: BTreeSet::from([1, 3, 8].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 1,
                    blk: BTreeSet::from([4].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 2,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 3,
                    blk: BTreeSet::from([]),
                },
            ],
        };

        v.merge_up(6 * MIN_ALLOC_SIZE, MIN_ALLOC_SIZE_POWER + 1);
        assert_eq!(
            v,
            BlockTree {
                tree: vec![
                    BlockList {
                        size: MIN_ALLOC_SIZE,
                        blk: BTreeSet::from([1, 3, 8].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 1,
                        blk: BTreeSet::from([]),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 2,
                        blk: BTreeSet::from([4].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 3,
                        blk: BTreeSet::from([]),
                    }
                ]
            },
        );
    }

    #[test]
    fn test_merge2() {
        let mut v = BlockTree {
            tree: vec![
                BlockList {
                    size: MIN_ALLOC_SIZE,
                    blk: BTreeSet::from([4, 10].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 1,
                    blk: BTreeSet::from([6, 8].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 2,
                    blk: BTreeSet::from([0].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 3,
                    blk: BTreeSet::from([]),
                },
            ],
        };

        v.merge_up(5 * MIN_ALLOC_SIZE, MIN_ALLOC_SIZE_POWER);
        assert_eq!(
            v,
            BlockTree {
                tree: vec![
                    BlockList {
                        size: MIN_ALLOC_SIZE,
                        blk: BTreeSet::from([10].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 1,
                        blk: BTreeSet::from([8].map(|i| i * MIN_ALLOC_SIZE)),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 2,
                        blk: BTreeSet::from([]),
                    },
                    BlockList {
                        size: MIN_ALLOC_SIZE << 3,
                        blk: BTreeSet::from([0]),
                    }
                ]
            },
        );
    }

    fn dup_free_block_handle(f: &FreeBlockHandle) -> FreeBlockHandle {
        FreeBlockHandle {
            free_block: f.free_block.dup(),
            invalidate_waiter: f.invalidate_waiter.clone(),
            sub_manager_handle: f.sub_manager_handle.clone(),
        }
    }

    fn key_and_fbh(key: PieceKey, f: FreeBlock) -> (PieceKey, FreeBlockHandle) {
        (
            key,
            FreeBlockHandle {
                free_block: f,
                invalidate_waiter: Vec::new(),
                sub_manager_handle: None,
            },
        )
    }

    #[test]
    fn test_optimal_n() {
        // - : occupied
        // * : vacant
        // - * - * * * - - * - -
        // 0 1 2 3 4 5 6 7 8 9 10
        let v = BlockTree {
            tree: vec![
                BlockList {
                    size: MIN_ALLOC_SIZE,
                    blk: BTreeSet::from([1, 3, 8].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 1,
                    blk: BTreeSet::from([4].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 2,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 3,
                    blk: BTreeSet::from([]),
                },
            ],
        };

        let mut buf = vec![0u8; 10 * MIN_ALLOC_SIZE];
        let base_ptr = buf.as_mut_ptr();
        let mut b = BufTree {
            migrating: false,
            flushing: 0,
            buf,
            blk_tree: v,
            flush_after_migrate: Vec::new(),
            alloced: HashMap::from([
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 0,
                    },
                    FreeBlock {
                        offset: 0,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: base_ptr,
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 1,
                    },
                    FreeBlock {
                        offset: 2 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(2 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 2,
                    },
                    FreeBlock {
                        offset: 6 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(6 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 3,
                    },
                    FreeBlock {
                        offset: 7 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(7 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 4,
                    },
                    FreeBlock {
                        offset: 9 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(9 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 5,
                    },
                    FreeBlock {
                        offset: 10 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(10 * MIN_ALLOC_SIZE) },
                    },
                ),
            ]),
        };

        assert_eq!(
            b.alloc(
                MIN_ALLOC_SIZE_POWER + 2,
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 6
                }
            ),
            Err(AllocErr::NoSpace) // this is BufTree.alloc(), will return NoSpace
        );
        let can_fit = b.can_fit_n_if_migrated(MIN_ALLOC_SIZE_POWER + 2);
        assert_eq!(can_fit, 1);
    }

    #[test]
    fn test_optimal_n_2() {
        // - : occupied
        // * : vacant
        // - - * * - - * * - - -
        // 0 1 2 3 4 5 6 7 8 9 10
        let v = BlockTree {
            tree: vec![
                BlockList {
                    size: MIN_ALLOC_SIZE,
                    blk: BTreeSet::from([10].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 1,
                    blk: BTreeSet::from([0, 4, 8].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 2,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 3,
                    blk: BTreeSet::from([]),
                },
            ],
        };

        let mut buf = vec![0u8; 11 * MIN_ALLOC_SIZE];
        let base_ptr = buf.as_mut_ptr();
        let mut b = BufTree {
            migrating: false,
            flushing: 0,
            buf,
            blk_tree: v,
            flush_after_migrate: Vec::new(),
            alloced: HashMap::from([
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 0,
                    },
                    FreeBlock {
                        offset: 0,
                        power: MIN_ALLOC_SIZE_POWER + 1,
                        ptr: base_ptr,
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 1,
                    },
                    FreeBlock {
                        offset: 4 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER + 1,
                        ptr: unsafe { base_ptr.add(4 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 2,
                    },
                    FreeBlock {
                        offset: 8 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER + 1,
                        ptr: unsafe { base_ptr.add(8 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 3,
                    },
                    FreeBlock {
                        offset: 10 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(10 * MIN_ALLOC_SIZE) },
                    },
                ),
            ]),
        };

        assert!(b
            .alloc(
                MIN_ALLOC_SIZE_POWER + 2,
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 6
                }
            )
            .is_err_and(|e| e == AllocErr::NoSpace));
        let can_fit = b.can_fit_n_if_migrated(MIN_ALLOC_SIZE_POWER + 2);
        assert_eq!(can_fit, 1);
    }

    #[test]
    fn test_optimal_n_3() {
        // - : occupied
        // * : vacant
        // * - * - * - * - * - -
        // 0 1 2 3 4 5 6 7 8 9 10
        let v = BlockTree {
            tree: vec![
                BlockList {
                    size: MIN_ALLOC_SIZE,
                    blk: BTreeSet::from([0, 2, 4, 6, 8].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 1,
                    blk: BTreeSet::from([].map(|i: usize| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 2,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 3,
                    blk: BTreeSet::from([]),
                },
            ],
        };

        let mut buf = vec![0u8; 11 * MIN_ALLOC_SIZE];
        let base_ptr = buf.as_mut_ptr();
        let mut b = BufTree {
            migrating: false,
            flushing: 0,
            buf,
            blk_tree: v,
            flush_after_migrate: Vec::new(),
            alloced: HashMap::from([
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 0,
                    },
                    FreeBlock {
                        offset: 1,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: base_ptr,
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 1,
                    },
                    FreeBlock {
                        offset: 3 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(3 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 2,
                    },
                    FreeBlock {
                        offset: 5 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(5 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 3,
                    },
                    FreeBlock {
                        offset: 7 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(7 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 4,
                    },
                    FreeBlock {
                        offset: 9 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(9 * MIN_ALLOC_SIZE) },
                    },
                ),
                key_and_fbh(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 5,
                    },
                    FreeBlock {
                        offset: 10 * MIN_ALLOC_SIZE,
                        power: MIN_ALLOC_SIZE_POWER,
                        ptr: unsafe { base_ptr.add(10 * MIN_ALLOC_SIZE) },
                    },
                ),
            ]),
        };

        assert!(b
            .alloc(
                MIN_ALLOC_SIZE_POWER + 2,
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 6
                }
            )
            .is_err_and(|e| e == AllocErr::NoSpace));
        assert!(b
            .alloc(
                MIN_ALLOC_SIZE_POWER + 1,
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 7
                }
            )
            .is_err_and(|e| e == AllocErr::NoSpace));
        let can_fit = b.can_fit_n_if_migrated(MIN_ALLOC_SIZE_POWER + 2);
        assert_eq!(can_fit, 1);
        let can_fit = b.can_fit_n_if_migrated(MIN_ALLOC_SIZE_POWER + 1);
        assert_eq!(can_fit, 2);

        // before migrate
        // - : occupied
        // * : vacant
        // * - * - * - * - * - -
        // 0 1 2 3 4 5 6 7 8 9 10
        //
        // after migrate
        // - : occupied
        // * : vacant
        // * * * * * - - - - - -
        // 0 1 2 3 4 5 6 7 8 9 10
        unsafe {
            b.migrate();
        }
        {
            let pk = PieceKey {
                hash: Arc::new([0; 20]),
                offset: 6,
            };
            let _res = b.alloc(MIN_ALLOC_SIZE_POWER + 2, pk.clone()).unwrap();
            b.free(&pk);
        }
        {
            let res1 = b
                .alloc(
                    MIN_ALLOC_SIZE_POWER + 1,
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 7,
                    },
                )
                .unwrap();
            let res2 = b
                .alloc(
                    MIN_ALLOC_SIZE_POWER + 1,
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 8,
                    },
                )
                .unwrap();
            assert!(b
                .alloc(
                    MIN_ALLOC_SIZE_POWER + 1,
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 9
                    }
                )
                .is_err_and(|e| e == AllocErr::NoSpace));
            assert!(b
                .alloc(
                    MIN_ALLOC_SIZE_POWER,
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 10
                    }
                )
                .is_ok());
        }
    }

    #[test]
    fn test_find_buddy() {
        assert_eq!(buddy_blk_of(10, 2), 8);
        assert_eq!(buddy_blk_of(9, 1), 8);
        assert_eq!(buddy_blk_of(4, 4), 0);
        assert_eq!(buddy_blk_of(0, 4), 4);

        assert_eq!(
            buddy_blk_of(10 * MIN_ALLOC_SIZE, 2 * MIN_ALLOC_SIZE),
            8 * MIN_ALLOC_SIZE
        );
        assert_eq!(
            buddy_blk_of(9 * MIN_ALLOC_SIZE, 1 * MIN_ALLOC_SIZE),
            8 * MIN_ALLOC_SIZE
        );
        assert_eq!(buddy_blk_of(4 * MIN_ALLOC_SIZE, 4 * MIN_ALLOC_SIZE), 0);
        assert_eq!(
            buddy_blk_of(0 * MIN_ALLOC_SIZE, 4 * MIN_ALLOC_SIZE),
            4 * MIN_ALLOC_SIZE
        );
    }

    #[cfg(mloom)]
    use loom::thread;

    use std::future::pending;
    #[cfg(not(mloom))]
    use std::thread;

    fn loom_test<F>(f: F)
    where
        F: Fn() + Sync + Send + 'static,
    {
        #[cfg(not(mloom))]
        f();

        #[cfg(mloom)]
        loom::model(f);
    }

    #[test]
    fn test_piece_buf() {
        loom_test(|| {
            let c = PieceBufPool::new(15 * MIN_ALLOC_SIZE);
            let sz1 = 4114;
            let sz2 = 5 * MIN_ALLOC_SIZE - 411;
            let mut b1 = c
                .alloc(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 0,
                    },
                    None,
                    sz1,
                )
                .unwrap();
            let mut b2 = c
                .alloc(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: 1,
                    },
                    None,
                    sz2,
                )
                .unwrap();
            assert_eq!(b1.len, sz1);
            assert_eq!(b2.len, sz2);
            assert_eq!(b1.as_mut().len(), sz1);
            for b in b1.as_mut().iter_mut() {
                *b = 0xff;
            }
            for b in b2.as_mut().iter_mut() {
                *b = 0xcc;
            }

            let guard = c.inner.buf_tree.lock().unwrap();
            for b in guard.buf.iter().skip(b1.b.offset).take(sz1) {
                assert_eq!(*b, 0xff);
            }
            for b in guard.buf.iter().skip(b2.b.offset).take(sz2) {
                assert_eq!(*b, 0xcc);
            }
        });
    }

    #[test]
    fn test_concurrent_piece_buf() {
        loom_test(|| {
            let c = PieceBufPool::new(15 * MIN_ALLOC_SIZE);
            let sz = 3 * MIN_ALLOC_SIZE - 112;

            let c1 = c.clone();
            let h1 = thread::spawn(move || {
                let mut pb = c1
                    .alloc(
                        PieceKey {
                            hash: Arc::new([0; 20]),
                            offset: 0,
                        },
                        None,
                        sz,
                    )
                    .unwrap();
                for b in pb.as_mut().iter_mut() {
                    *b = 0xff;
                }
                pb
            });

            let c2 = c.clone();
            let h2 = thread::spawn(move || {
                let mut pb = c2
                    .alloc(
                        PieceKey {
                            hash: Arc::new([0; 20]),
                            offset: 1,
                        },
                        None,
                        sz,
                    )
                    .unwrap();
                for b in pb.as_mut().iter_mut() {
                    *b = 0xcc;
                }
                pb
            });
            let pb1 = h1.join().unwrap();
            let pb2 = h2.join().unwrap();

            let guard = c.inner.buf_tree.lock().unwrap();
            for b in guard.buf.iter().skip(pb1.b.offset).take(sz) {
                assert_eq!(*b, 0xff);
            }
            for b in guard.buf.iter().skip(pb2.b.offset).take(sz) {
                assert_eq!(*b, 0xcc);
            }
        });
    }

    struct TestFileImpl {
        buf: Vec<u8>,
    }

    impl FileImpl for TestFileImpl {
        fn write_all(&mut self, offset: usize, buf: &[u8]) -> io::Result<()> {
            self.buf[offset..].copy_from_slice(buf);
            Ok(())
        }

        fn read_all(&mut self, _offset: usize, _buf: &mut [u8]) -> io::Result<()> {
            unimplemented!();
        }
    }

    impl TestFileImpl {
        fn new(size: usize) -> Self {
            Self {
                buf: vec![0u8; size],
            }
        }

        fn buf(&self) -> &[u8] {
            self.buf.as_slice()
        }
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_flush_dirty() {
        use tokio::time;

        let c = PieceBufPool::new(15 * MIN_ALLOC_SIZE);
        let sz1 = 4114;
        let buf = Arc::new(Mutex::new(TestFileImpl::new(sz1)));

        let mut b1 = c
            .alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 0,
                },
                Some(buf.clone()),
                sz1,
            )
            .unwrap();
        assert_eq!(b1.len, sz1);
        for b in b1.as_mut().iter_mut() {
            *b = 0xff;
        }
        drop(b1);

        time::sleep(time::Duration::from_millis(10)).await;
        for b in buf.lock().unwrap().buf().iter().take(sz1) {
            assert_eq!(*b, 0xff);
        }
    }

    #[cfg(not(mloom))]
    #[cfg(test_flush_migrate_concurrent)]
    #[tokio::test]
    async fn test_flush_migrate_concurrent() {
        use tokio::time;

        let c = PieceBufPool::new(15 * MIN_ALLOC_SIZE);

        let pk0 = PieceKey {
            hash: Arc::new([0; 20]),
            offset: 30,
        };
        let pk1 = PieceKey {
            hash: Arc::new([0; 20]),
            offset: 0,
        };
        let buf = Arc::new(Mutex::new(TestFileImpl::new(1155)));
        let p0 = c
            .async_alloc_abort::<ArcCache<_>>(pk0, Some(buf.clone()), 1155)
            .await
            .unwrap();
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(pk1, Some(buf.clone()), 1155)
            .await
            .unwrap();
        drop(p0);

        let (mut r1, w1) = duplex(MIN_ALLOC_SIZE);

        // before invalidate, buf should be all 0x00
        for b in buf.lock().unwrap().buf().iter().take(4) {
            assert_eq!(*b, 0x00);
        }

        r1.write(&[0xcc; 4]).await.unwrap();
        {
            let p1c = p1.clone();
            let mut w1_pin = Box::pin(w1);
            read_to_ref(&mut w1_pin, p1c.clone(), 0, 4).await;
        }
        {
            let r = p1.get_part_ref(0, 4).unwrap();
            println!("{:?}", r.as_ref());
        }

        time::sleep(time::Duration::from_millis(10)).await;
        let buf_guard = c.inner.buf_tree.lock().unwrap();
        let migrate_task = start_migrate_task_inner(buf_guard, c.inner.clone());
        drop(p1);
        // wait read task run
        _ = migrate_task.await;

        // wait flush task finish
        time::sleep(time::Duration::from_millis(10)).await;

        // after invalidate, dirty data should be flushed
        for b in buf.lock().unwrap().buf().iter().take(4) {
            assert_eq!(*b, 0xcc);
        }
    }

    #[tokio::test]
    #[cfg(not(mloom))]
    async fn test_invalidate_flush() {
        use tokio::time;

        let c = PieceBufPool::new(10 * MIN_ALLOC_SIZE);

        let pk1 = PieceKey {
            hash: Arc::new([0; 20]),
            offset: 0,
        };
        let buf = Arc::new(Mutex::new(TestFileImpl::new(2 * MIN_ALLOC_SIZE)));
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(pk1.clone(), Some(buf.clone()), 2 * MIN_ALLOC_SIZE)
            .await
            .unwrap();
        let (mut r1, w1) = duplex(MIN_ALLOC_SIZE);
        r1.write(&[0xcc; 4]).await.unwrap();
        {
            let p1c = p1.clone();
            tokio::spawn(async move {
                let mut w1_pin = Box::pin(w1);
                read_to_ref(&mut w1_pin, p1c.clone(), 0, 16384).await;
                read_to_ref(&mut w1_pin, p1c.clone(), 16384, 16384).await;
                read_to_ref(&mut w1_pin, p1c.clone(), 2 * 16384, 16384).await;
            });
        }

        // wait read task run
        time::sleep(time::Duration::from_millis(10)).await;

        // before invalidate, buf should be all 0x00
        for b in buf.lock().unwrap().buf().iter().take(4) {
            assert_eq!(*b, 0x00);
        }

        c.invalidate(pk1).await.unwrap();

        // wait flush task finish
        time::sleep(time::Duration::from_millis(10)).await;

        // after invalidate, dirty data should be flushed
        for b in buf.lock().unwrap().buf().iter().take(4) {
            assert_eq!(*b, 0xcc);
        }
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_wakeup() {
        let c = PieceBufPool::new(2 * MIN_ALLOC_SIZE);

        let mut ts = tokio::task::JoinSet::new();
        for size in [2, 1, 1, 2, 1, 2usize].into_iter().enumerate() {
            let c1 = c.clone();
            ts.spawn(async move {
                c1.async_alloc(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: size.0,
                    },
                    None,
                    size.1 * MIN_ALLOC_SIZE,
                )
                .await
            });
        }

        for _ in 0..ts.len() {
            _ = ts.join_next().await;
        }
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_abort_wakeup() {
        let c = PieceBufPool::new(2 * MIN_ALLOC_SIZE);

        let mut ts = tokio::task::JoinSet::new();
        for size in [2, 1, 1, 2, 1, 2usize].into_iter().enumerate() {
            let c1 = c.clone();
            ts.spawn(async move {
                c1.async_alloc_abort::<ArcCache<_>>(
                    PieceKey {
                        hash: Arc::new([0; 20]),
                        offset: size.0,
                    },
                    None,
                    size.1 * MIN_ALLOC_SIZE,
                )
                .await
            });
        }

        for _ in 0..ts.len() {
            _ = ts.join_next().await;
        }
    }

    async fn read_to_ref<T>(
        conn: &mut Pin<Box<T>>,
        piece: ArcCache<PieceBuf>,
        offset: usize,
        expect: usize,
    ) -> i32
    where
        T: AsyncRead,
    {
        let mut written = 0;
        let mut counter = 0;
        'outer: while written < expect {
            match piece.async_get_part_ref(offset, expect).await {
                Ok(ref mut refbuf) => {
                    while written < expect {
                        let read_fut = conn.read_abort(refbuf, written);
                        match read_fut.await {
                            Ok(n) => {
                                written += n;
                            }
                            Err(AbortErr::IO(_)) => {
                                panic!("");
                            }
                            Err(AbortErr::Aborted) => {
                                // go to next round
                                println!("read aborted");
                                counter += 1;
                                continue 'outer;
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("{e:?}");
                    break;
                }
            }
        }
        counter
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_abort_migrate() {
        let (mut r2, w2) = duplex(MIN_ALLOC_SIZE);
        _ = r2.write(&[0x22u8, 0x22]).await;
        let (mut r4, w4) = duplex(MIN_ALLOC_SIZE);
        _ = r4.write(&[0x44u8, 0x44]).await;

        let c = PieceBufPool::new(4 * MIN_ALLOC_SIZE);
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 1,
                },
                None,
                MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        let p2 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        let p3 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 3,
                },
                None,
                MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        let p4 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 4,
                },
                None,
                MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        let (rt1, rt2) = {
            let p2c = p2.clone();
            let rt1 = tokio::spawn(async move {
                let mut w2_pin = Box::pin(w2);
                read_to_ref(&mut w2_pin, p2c.clone(), 0, MIN_ALLOC_SIZE).await
            });
            let p4c = p4.clone();
            let rt2 = tokio::spawn(async move {
                let mut w4_pin = Box::pin(w4);
                read_to_ref(&mut w4_pin, p4c.clone(), 0, MIN_ALLOC_SIZE).await
            });
            (rt1, rt2)
        };
        drop(p1);
        drop(p3);

        // no enough space for consecutive 2 * MIN_ALLOC_SIZE
        // this sync alloc should return None
        // but a async task will be created and migrating blocks
        assert!(c
            .alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 9
                },
                None,
                2 * MIN_ALLOC_SIZE
            )
            .is_err_and(|e| e == AllocErr::StartMigrating));
        let p5 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 5,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        {
            _ = r2.write(&[0x22u8; MIN_ALLOC_SIZE - 2]).await;
            _ = r4.write(&[0x44u8; MIN_ALLOC_SIZE - 2]).await;
            let _n_abort1 = rt1.await.unwrap();
            let _n_abort2 = rt2.await.unwrap();
            // undeterminism, might not equal
            // assert_eq!(n_abort1, 1);
            // assert_eq!(n_abort2, 1);
            let ref2 = p2.get_part_ref(0, MIN_ALLOC_SIZE).unwrap();
            for i in ref2.as_ref() {
                assert_eq!(*i, 0x22);
            }
            let ref4 = p4.get_part_ref(0, MIN_ALLOC_SIZE).unwrap();
            for i in ref4.as_ref() {
                assert_eq!(*i, 0x44);
            }
        }
        drop(p2);
        drop(p4);
        let _p6 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 6,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        drop(p5);
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_abort_migrate_2() {
        let (mut r1, w1) = duplex(MIN_ALLOC_SIZE);
        _ = r1.write(&[0x11; 4]).await;
        let (mut r4, w4) = duplex(2 * MIN_ALLOC_SIZE);
        _ = r4.write(&[0x41; 4]).await;

        let c = PieceBufPool::new(11 * MIN_ALLOC_SIZE);
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 1,
                },
                None,
                MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 10
        let p2 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                4 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 4-7
        let p3 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 3,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 8-9
        let p4 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 4,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 2-3
        let (rt1, rt2) = {
            let p1c = p1.clone();
            let rt1 = tokio::spawn(async move {
                let mut w1_pin = Box::pin(w1);
                read_to_ref(&mut w1_pin, p1c, 0, MIN_ALLOC_SIZE).await
            });

            let p4c = p4.clone();
            let rt2 = tokio::spawn(async move {
                let mut w4_pin = Box::pin(w4);
                let n = read_to_ref(&mut w4_pin, p4c.clone(), 0, MIN_ALLOC_SIZE).await;
                n + read_to_ref(&mut w4_pin, p4c.clone(), MIN_ALLOC_SIZE, MIN_ALLOC_SIZE).await
            });
            (rt1, rt2)
        };
        drop(p2);
        drop(p3);

        // no enough space for consecutive 8 * MIN_ALLOC_SIZE
        // this sync alloc should return None
        // but a async task will be created and migrating blocks
        assert!(c
            .alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 9
                },
                None,
                8 * MIN_ALLOC_SIZE
            )
            .is_err_and(|e| e == AllocErr::StartMigrating)); // this is BufPool.alloc(), will return StartMigrating
        let p5 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 5,
                },
                None,
                8 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        {
            _ = r1.write(&[0x11; MIN_ALLOC_SIZE - 4]).await;
            _ = r4.write(&[0x41; MIN_ALLOC_SIZE - 4]).await;
            _ = r4.write(&[0x42; MIN_ALLOC_SIZE]).await;
            let _n_abort1 = rt1.await.unwrap();
            let _n_abort2 = rt2.await.unwrap();
            // undeterminism, might not equal
            // assert_eq!(n_abort1, 1);
            // assert_eq!(n_abort2, 1);

            let ref1 = p1.get_part_ref(0, MIN_ALLOC_SIZE).unwrap();
            for i in ref1.as_ref() {
                assert_eq!(*i, 0x11);
            }
            let ref4 = p4.get_part_ref(0, 1 * MIN_ALLOC_SIZE).unwrap();
            for (i, b) in ref4.as_ref().iter().enumerate() {
                assert_eq!((i, *b), (i, 0x41));
            }
            let ref4 = p4
                .get_part_ref(1 * MIN_ALLOC_SIZE, 1 * MIN_ALLOC_SIZE)
                .unwrap();
            for i in ref4.as_ref() {
                assert_eq!(*i, 0x42);
            }
        }
        drop(p1);
        drop(p4);
        let _p6 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 6,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        drop(p5);
    }

    /// migrate twice
    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_abort_migrate_3() {
        let (mut r, w) = duplex(MIN_ALLOC_SIZE);
        _ = r.write(&[0x11; 4]).await;

        let c = PieceBufPool::new(11 * MIN_ALLOC_SIZE);
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 1,
                },
                None,
                MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 10
        let p2 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                4 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 4-7
        let p3 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 3,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 8-9
        let p4 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 4,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 2-3
        let p5 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 5,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 0-1
        let rt4 = {
            let p4c = p4.clone();
            let rt4 = tokio::spawn(async move {
                let mut w4_pin = Box::pin(w);
                read_to_ref(&mut w4_pin, p4c, 0, MIN_ALLOC_SIZE).await
            });
            rt4
        };
        drop(p2);
        let p2 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 6-7
        drop(p3);
        drop(p5);

        // p1: 10
        // p2: 6-7
        // p4: 2-3
        // no enough space for consecutive 4 * MIN_ALLOC_SIZE
        // this sync alloc should return None
        // but a async task will be created and migrating blocks
        println!("{:?}", p1.piece_detail(|p| p.offset_len()).unwrap());
        println!("{:?}", p2.piece_detail(|p| p.offset_len()).unwrap());
        println!("{:?}", p4.piece_detail(|p| p.offset_len()).unwrap());

        let p5 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 5,
                },
                None,
                4 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();

        println!("b");
        {
            _ = r.write(&[0x11; 10]).await;
        }
        drop(p5);
        drop(p1);
        drop(p2);

        println!("{:?}", p4.piece_detail(|p| p.offset_len()).unwrap());
        let p5 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 5,
                },
                None,
                8 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap();
        println!("{:?}", p4.piece_detail(|p| p.offset_len()).unwrap());
        println!("{:?}", p5.piece_detail(|p| p.offset_len()).unwrap());
        {
            _ = r.write(&[0x11; 2 * MIN_ALLOC_SIZE - 14]).await;
            let _n_abort = rt4.await.unwrap();
            // assert_eq!(n_abort, 2) // due to undeterminism, this might not be 2

            let ref4 = p4.get_part_ref(0, MIN_ALLOC_SIZE).unwrap();
            for i in ref4.as_ref() {
                assert_eq!(*i, 0x11);
            }
        }
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_same_piece() {
        let c = PieceBufPool::new(11 * MIN_ALLOC_SIZE);
        let _p1 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 1,
                },
                None,
                MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 10
        let _p2 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                4 * MIN_ALLOC_SIZE,
            )
            .await
            .unwrap(); // at 4-7

        // even if no space, try to re-allocate a piece will return "Allocated"
        assert!(c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                40 * MIN_ALLOC_SIZE,
            )
            .await
            .is_err_and(|e| e == AllocErr::Allocated));
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_oversize() {
        let c = PieceBufPool::new(11 * MIN_ALLOC_SIZE);
        assert!(c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                40 * MIN_ALLOC_SIZE,
            )
            .await
            .is_err_and(|e| e == AllocErr::Oversize));
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_allocated_wake_next() {
        let c = PieceBufPool::new(11 * MIN_ALLOC_SIZE);
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 0,
                },
                None,
                4 * MIN_ALLOC_SIZE,
            )
            .await;
        let _p2 = c
            .async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 1,
                },
                None,
                4 * MIN_ALLOC_SIZE,
            )
            .await;
        let c3 = c.clone();
        let make_waiting = tokio::spawn(async move {
            c3.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 8,
                },
                None,
                4 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let c3 = c.clone();
        let p3 = tokio::spawn(async move {
            c3.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 8,
                },
                None,
                4 * MIN_ALLOC_SIZE,
            )
            .await
        }); // returns err(allocated)
        let c4 = c.clone();
        let p4 = tokio::spawn(async move {
            c4.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 4,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
        });
        tokio::spawn(async move {
            drop(p1);
        });
        let _p3_1 = make_waiting.await.unwrap().unwrap();
        assert!(p3.await.unwrap().is_err_and(|e| e == AllocErr::Allocated));
        assert!(p4.await.unwrap().is_ok());
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_drop_future() {
        let c = PieceBufPool::new(2 * MIN_ALLOC_SIZE);
        let (c0, c1, c2, c3, c4, c5) = (
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
        );

        let h0 = tokio::spawn(async move {
            c0.async_alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 0,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let r0 = h0.await.unwrap();

        let h1 = tokio::spawn(async move {
            c1.async_alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 1,
                },
                None,
                1 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let mut h2 = task::spawn(async move {
            c2.async_alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                1 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let mut h3 = task::spawn(async move {
            c3.async_alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 3,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let h4 = tokio::spawn(async move {
            c4.async_alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 4,
                },
                None,
                1 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let h5 = tokio::spawn(async move {
            c5.async_alloc(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 5,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
        });
        assert!(h2.poll().is_pending());
        drop(h2);
        assert!(h3.poll().is_pending());
        drop(h3);
        drop(r0);
        {
            assert!(h1.await.is_ok());
            assert!(h4.await.is_ok());
        }
        {
            assert!(h5.await.is_ok());
        }
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_abort_drop_future() {
        let c = PieceBufPool::new(2 * MIN_ALLOC_SIZE);
        let (c0, c1, c2, c3, c4, c5) = (
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
            c.clone(),
        );

        let h0 = tokio::spawn(async move {
            c0.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 0,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let r0 = h0.await.unwrap();

        let h1 = tokio::spawn(async move {
            c1.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 1,
                },
                None,
                1 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let mut h2 = task::spawn(async move {
            c2.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 2,
                },
                None,
                1 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let mut h3 = task::spawn(async move {
            c3.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 3,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let h4 = tokio::spawn(async move {
            c4.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 4,
                },
                None,
                1 * MIN_ALLOC_SIZE,
            )
            .await
        });
        let h5 = tokio::spawn(async move {
            c5.async_alloc_abort::<ArcCache<_>>(
                PieceKey {
                    hash: Arc::new([0; 20]),
                    offset: 5,
                },
                None,
                2 * MIN_ALLOC_SIZE,
            )
            .await
        });
        assert!(h2.poll().is_pending());
        drop(h2);
        assert!(h3.poll().is_pending());
        drop(h3);
        drop(r0);
        {
            assert!(h1.await.unwrap().is_ok());
            assert!(h4.await.unwrap().is_ok());
        }
        {
            assert!(h5.await.unwrap().is_ok());
        }
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_invalidate_piece_buf() {
        let c = PieceBufPool::new(10 * MIN_ALLOC_SIZE);

        let pk0 = PieceKey {
            hash: Arc::new([0; 20]),
            offset: 0,
        };
        let mut piece = c.async_alloc(pk0.clone(), None, 2 * MIN_ALLOC_SIZE).await;
        let ref_work = task::spawn(async move {
            let _ = piece.as_mut();
        });
        let mut invalid_fut1 = task::spawn(c.invalidate(pk0.clone()));
        let mut invalid_fut2 = task::spawn(c.invalidate(pk0.clone()));
        assert_eq!(invalid_fut1.poll(), Poll::Pending);
        assert_eq!(invalid_fut2.poll(), Poll::Pending);
        assert_eq!(invalid_fut1.poll(), Poll::Pending);
        assert_eq!(invalid_fut2.poll(), Poll::Pending);
        ref_work.await;
        assert_eq!(invalid_fut1.poll(), Poll::Ready(Ok(())));
        assert_eq!(invalid_fut2.poll(), Poll::Ready(Ok(())));
        println!("invalidated");
        assert!(c.invalidate(pk0.clone()).await.is_ok());
    }

    #[tokio::test]
    #[cfg(not(mloom))]
    async fn test_alloc_abort() {
        let c = PieceBufPool::new(10 * MIN_ALLOC_SIZE);

        let (pk1, pk2) = (
            PieceKey {
                hash: Arc::new([0; 20]),
                offset: 1,
            },
            PieceKey {
                hash: Arc::new([0; 20]),
                offset: 2,
            },
        );
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(pk1.clone(), None, 2 * MIN_ALLOC_SIZE)
            .await
            .unwrap();
        let p2 = c
            .async_alloc_abort::<ArcCache<_>>(pk2.clone(), None, 1 * MIN_ALLOC_SIZE)
            .await
            .unwrap();
        let (mut r1, w1) = duplex(MIN_ALLOC_SIZE);
        r1.write(&[0; 4]).await;
        let (mut r2, w2) = duplex(MIN_ALLOC_SIZE);
        r2.write(&[0; 4]).await;
        {
            let p1c = p1.clone();
            tokio::spawn(async move {
                let mut w1_pin = Box::pin(w1);
                read_to_ref(&mut w1_pin, p1c.clone(), 0, 16384).await;
                read_to_ref(&mut w1_pin, p1c.clone(), 16384, 16384).await;
                read_to_ref(&mut w1_pin, p1c.clone(), 2 * 16384, 16384).await;
            });
            let p2c = p2.clone();
            tokio::spawn(async move {
                let mut w2_pin = Box::pin(w2);
                read_to_ref(&mut w2_pin, p2c.clone(), 2 * 16384, 16384).await;
            });
        }

        c.invalidate(pk1).await;
        c.invalidate(pk2).await;

        // TODO: test p1, p2's inner Cache is None
        assert!(matches!(
            p1.get_part_ref(16384, 16384),
            Err(GetRefErr::Invalidated)
        ));
    }

    #[tokio::test]
    async fn test_alloc_abort2() {
        let c = PieceBufPool::new(10 * MIN_ALLOC_SIZE);
        let (pk1, pk2) = (
            PieceKey {
                hash: Arc::new([0; 20]),
                offset: 1,
            },
            PieceKey {
                hash: Arc::new([0; 20]),
                offset: 2,
            },
        );
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(pk1.clone(), None, 2 * MIN_ALLOC_SIZE)
            .await
            .unwrap();
        let p2 = c
            .async_alloc_abort::<ArcCache<_>>(pk2.clone(), None, 1 * MIN_ALLOC_SIZE)
            .await
            .unwrap();
        let (_r1, w1) = duplex(MIN_ALLOC_SIZE);
        let (_r2, w2) = duplex(MIN_ALLOC_SIZE);
        let (mut t1, mut t2) = {
            let p1c = p1.clone();
            let mut t1 = task::spawn(async move {
                let mut w1_pin = Box::pin(w1);
                read_to_ref(&mut w1_pin, p1c.clone(), 0, 16384).await;
            });
            let p2c = p2.clone();
            let mut t2 = task::spawn(async move {
                let mut w2_pin = Box::pin(w2);
                read_to_ref(&mut w2_pin, p2c.clone(), 2 * 16384, 16384).await;
            });
            assert_eq!(t1.poll(), Poll::Pending);
            assert_eq!(t2.poll(), Poll::Pending);

            (t1, t2)
        };
        assert_eq!(t1.poll(), Poll::Pending);
        assert_eq!(t2.poll(), Poll::Pending);
        let mut inv2 = task::spawn(c.invalidate(pk2));
        let mut inv1 = task::spawn(c.invalidate(pk1));
        assert_eq!(inv2.poll(), Poll::Pending);
        assert_eq!(inv2.poll(), Poll::Pending);
        t2.await;
        assert_eq!(inv2.poll(), Poll::Ready(Ok(())));
        assert_eq!(inv1.poll(), Poll::Pending);
        t1.await;
        _ = inv1.await;
    }
}

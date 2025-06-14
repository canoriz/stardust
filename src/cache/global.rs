use core::slice;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Div};
use std::pin::Pin;
use std::sync::MutexGuard;
use std::task::{Context, Poll, Waker};

#[cfg(mloom)]
use loom::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[cfg(mloom)]
use loom::atomic::{AtomicBool, AtomicU32, Ordering};
#[cfg(not(mloom))]
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::task::JoinHandle;
use tracing::warn;

#[cfg(not(mloom))]
use std::sync::{Arc, Mutex};

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
#[derive(Debug)]
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

impl Eq for FreeBlock {}
impl PartialEq for FreeBlock {
    fn eq(&self, other: &Self) -> bool {
        // only compare value, not valid
        self.offset == other.offset && self.power == other.power && self.ptr == other.ptr
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
    migrating: bool, // TODO: Maybe use atomic and move out of BufTree
    buf: Vec<u8>,
    blk_tree: BlockTree,

    // TODO: maybe use a more efficient DS
    // TODO: maybe use HashMap<FreeBlock, handle>
    alloced: HashMap<FreeBlock, FreeBlockHandle>,
}

impl BufTree {
    fn alloc(&mut self, power: u32) -> Option<FreeBlock> {
        // Can not alloc when concurrently migrating.
        // migrate_task() requires all use of PieceBuf stop, And pause is an async
        // operation, which means it does not always hold lock.
        // If pause done, but now a new PieceBuf is allocated, this PieceBuf
        // may be used, which violates the assumption of migrate_task() that all
        // uses are stopped.
        if self.migrating {
            return None;
        }
        self.migrating_alloc(power)
    }

    fn migrating_alloc(&mut self, power: u32) -> Option<FreeBlock> {
        self.blk_tree.split_down(power).map(|(offset, power)| {
            let fb = FreeBlock {
                offset,
                power,
                ptr: unsafe { self.buf.as_mut_ptr().add(offset) },
            };
            self.alloced.insert(
                fb.dup(),
                FreeBlockHandle {
                    free_block: fb.dup(),
                    invalidate_waiter: Vec::new(),
                    sub_manager_handle: None,
                },
            );
            fb
        })
    }

    fn free(&mut self, b: &FreeBlock) -> FreeBlockHandle {
        self.blk_tree.merge_up(b.offset, b.power);

        self.alloced
            .remove(b)
            .expect("free should see piecebuf in alloced list")
    }

    fn count_blk_of_power(&self) -> Vec<usize> {
        let mut alloc_n = vec![0; self.blk_tree.tree.len()];
        for (b, _) in self.alloced.iter() {
            alloc_n[(b.power - MIN_ALLOC_SIZE_POWER) as usize] += 1;
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

        let mut block_powers: Vec<Vec<(FreeBlock, FreeBlockHandle)>> = Vec::new();
        for _ in 0..self.blk_tree.tree.len() {
            block_powers.push(vec![]);
        }

        for (fb, fbh) in self.alloced.drain() {
            let power_index = (fb.power - MIN_ALLOC_SIZE_POWER) as usize;
            block_powers[power_index].push((fb, fbh));
        }

        let mut new_alloced = HashMap::new();
        for p in block_powers.into_iter() {
            for (fb, mut fbh) in p.into_iter() {
                //NOTE: self.alloc will modify self.alloced, but we not use the modified "alloced"
                // variable anyway, we will replace a new one
                let new_fb = self
                    .migrating_alloc(fb.power)
                    .expect("migrate alloc should ok");
                fbh.free_block = new_fb.dup();

                let begin_block_idx = fb.offset >> MIN_ALLOC_SIZE_POWER;
                for (i, block) in (begin_block_idx
                    ..begin_block_idx + (1 << (fb.power - MIN_ALLOC_SIZE_POWER)))
                    .enumerate()
                {
                    migrate_map[block] = (new_fb.offset >> MIN_ALLOC_SIZE_POWER) + i;
                }

                fbh.sub_manager_handle
                    .as_ref()
                    .expect("should have handle")
                    .migrate(new_fb.offset);
                new_alloced.insert(new_fb, fbh);
            }
        }
        self.alloced = new_alloced;

        #[cfg(test)]
        self.blk_tree.print();

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
        for (fb, fbh) in self.alloced.iter() {
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

struct PieceBufPoolImpl {
    buf_tree: Mutex<BufTree>,
    waiting_alloc: Mutex<VecDeque<AllocReq>>,

    // TODO: maybe move into buf_tree
    // TODO: is this used?
    invalidating: AtomicU32,
}

// This is !Clone
pub(crate) struct PieceBuf {
    b: FreeBlock,
    len: usize,
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
            .field("free_block", &self.b)
            .field("len", &self.len)
            .finish()
    }
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
    pub fn alloc(&self, size: usize) -> Option<PieceBuf> {
        let size_fixed = if size < MIN_ALLOC_SIZE {
            MIN_ALLOC_SIZE
        } else {
            size.next_power_of_two()
        };
        debug_assert_eq!(size_fixed % MIN_ALLOC_SIZE, 0);
        let power = size_fixed.ilog2();

        let mut buf_tree_guard = self.inner.buf_tree.lock().expect("alloc lock should OK");
        if let Some(fb) = buf_tree_guard.alloc(power) {
            Some(PieceBuf {
                b: fb,
                len: size,
                main_cache: self.inner.clone(),
            })
        } else if !buf_tree_guard.migrating && buf_tree_guard.can_fit_n_if_migrated(power) > 0 {
            // TODO: do we need a from_async flag, so that only fire migrate_task
            // when calling inside a async environment?
            let pool_impl = self.inner.clone();
            start_migrate_task(buf_tree_guard, pool_impl);
            None
        } else {
            None
        }
    }

    /// alloc PieceBuf
    pub fn alloc_abort<T>(&self, size: usize) -> Option<T>
    where
        T: FromPieceBuf,
    {
        // TODO: simplify repeat patterns
        let size_fixed = if size < MIN_ALLOC_SIZE {
            MIN_ALLOC_SIZE
        } else {
            size.next_power_of_two()
        };
        debug_assert_eq!(size_fixed % MIN_ALLOC_SIZE, 0);
        let power = size_fixed.ilog2();

        let mut buf_tree_guard = self
            .inner
            .buf_tree
            .lock()
            .expect("alloc sub lock should OK");
        if let Some(fb) = buf_tree_guard.alloc(power) {
            let (ret, abort_handle) = T::new_abort(PieceBuf {
                b: fb.dup(),
                len: size,
                main_cache: self.inner.clone(),
            });
            let fbh = buf_tree_guard
                .alloced
                .get_mut(&fb)
                .expect("just alloced should exist");
            fbh.sub_manager_handle = Some(abort_handle);
            Some(ret)
        } else if !buf_tree_guard.migrating && buf_tree_guard.can_fit_n_if_migrated(power) > 0 {
            // TODO: do we need a from_async flag, so that only fire migrate_task
            // when calling inside a async environment?
            let pool_impl = self.inner.clone();
            start_migrate_task(buf_tree_guard, pool_impl);
            None
        } else {
            None
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
    pub fn async_alloc(&self, id: usize, size: usize) -> AllocPieceBufFut {
        AllocPieceBufFut {
            inner: AllocFutInner {
                id,
                size,
                pool: self,
                waiting: false,
                valid: Arc::new(AtomicU32::new(WAITING)),
            },
        }
    }

    pub fn async_alloc_abort<T>(&self, id: usize, size: usize) -> AllocPieceBufAbortFut<T>
    where
        T: FromPieceBuf + Unpin,
    {
        AllocPieceBufAbortFut {
            inner: AllocFutInner {
                id,
                size,
                pool: self,
                waiting: false,
                valid: Arc::new(AtomicU32::new(WAITING)),
            },
            _t: PhantomData,
        }
    }

    async fn invalidate(&self, f: FreeBlock) -> Result<(), InvalidateErr> {
        self.inner.invalidate(f).await
    }
}

impl PieceBufPoolImpl {
    fn new(size: usize) -> Self {
        let size_fixed = size.next_multiple_of(MIN_ALLOC_SIZE);

        Self {
            buf_tree: Mutex::new(BufTree {
                migrating: false,
                buf: vec![0u8; size_fixed],
                blk_tree: init_block_tree(size_fixed),
                alloced: HashMap::new(),
            }),
            waiting_alloc: Mutex::new(VecDeque::new()),
            invalidating: AtomicU32::new(0),
        }
    }

    /// alloc FreeBlock
    fn alloc(&self, size: usize) -> Option<FreeBlock> {
        let size_fixed = if size < MIN_ALLOC_SIZE {
            MIN_ALLOC_SIZE
        } else {
            size.next_power_of_two()
        };
        debug_assert_eq!(size_fixed % MIN_ALLOC_SIZE, 0);
        let power = size_fixed.ilog2();

        let mut buf_tree_guard = self.buf_tree.lock().expect("alloc lock should OK");
        buf_tree_guard.alloc(power)
    }

    /// returns PieceBuf
    fn free(&self, b: &FreeBlock) {
        {
            let mut buf_tree_guard = self.buf_tree.lock().expect("alloc lock should OK");
            let mut fh = buf_tree_guard.free(b);

            #[cfg(test)]
            println!("free piecebuf");

            for w in fh.invalidate_waiter.drain(0..) {
                #[cfg(test)]
                println!("invalidate_waker some {:?}", w);
                w.notify_one();
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
    async fn invalidate(&self, f: FreeBlock) -> Result<(), InvalidateErr> {
        self.invalidating.fetch_add(1, Ordering::Acquire);
        let (wait_invalid, wait_abort) = {
            let mut buf_tree_guard = self.buf_tree.lock().expect("invalidate lock should OK");
            if buf_tree_guard.migrating {
                // TODO: FIXME: is this a must?
                return Err(InvalidateErr::MigrateInProgress);
            }
            if let Some(fbh) = buf_tree_guard.alloced.get_mut(&f) {
                let aborter = fbh.sub_manager_handle.clone();
                #[cfg(test)]
                println!("add wait_invalid");
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
        let wait_abort = {
            let mut buf_tree_guard = self.buf_tree.lock().expect("pause lock should OK");
            assert_eq!(buf_tree_guard.migrating, true);
            let mut aborters = Vec::new();
            for (fb, fbh) in buf_tree_guard.alloced.iter_mut() {
                #[cfg(test)]
                println!("add wait_invalid");

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
    fn drop(&mut self) {
        #[cfg(test)]
        println!("free PieceBuf {:?}", self);

        self.main_cache.free(&self.b);
    }
}

impl AsMut<[u8]> for PieceBuf {
    fn as_mut(&mut self) -> &mut [u8] {
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
}

pub(crate) struct AllocReq {
    pub id: usize,
    pub waker: Waker,
    pub valid: Arc<AtomicU32>,
}

const DROPPED: u32 = 0b01;
const DONE: u32 = 0b010; // TODO: really need this?
const WAITING: u32 = 0;
const WAKING: u32 = 0b100;

pub(crate) fn wake_next_waiting_alloc(waiting_alloc: &mut VecDeque<AllocReq>) {
    while let Some(v) = waiting_alloc.pop_front() {
        #[cfg(test)]
        println!("try waking {}", v.id);
        match v
            .valid
            .compare_exchange(WAITING, WAKING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                v.waker.wake();
                #[cfg(test)]
                println!("wake waiting {}", v.id);
                break;
            }
            Err(actual) => {
                // if DONE, this future is Ready
                // if DROPPED, no one waiting future
                // pick next one
                debug_assert!(actual == DROPPED || actual == DONE || actual == WAKING);

                #[cfg(test)]
                println!("no wake {} because actual state {actual}", v.id);
            }
        }
    }
}

struct AllocFutInner<'a> {
    id: usize,
    pool: &'a PieceBufPool,
    waiting: bool,
    valid: Arc<AtomicU32>,
    size: usize,
}

impl AllocFutInner<'_> {
    fn poll_alloc<F, T>(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        alloc_fn: F,
    ) -> Poll<T>
    where
        // since we only alloc at most once, use most relaxed FnOnce here
        // if we need to call alloc_fn more than once, use FnMut
        // technically we are all using member functions, not even closures
        // so Fn would be enough
        F: FnOnce(&PieceBufPool, usize) -> Option<T>,
    {
        let fut = self.get_mut();

        #[cfg(test)]
        println!("poll id {}", fut.id);

        let mut waiting_alloc = fut.pool.inner.waiting_alloc.lock().unwrap();
        if !fut.waiting {
            if !waiting_alloc.is_empty() {
                waiting_alloc.push_back(AllocReq {
                    id: fut.id,
                    waker: cx.waker().clone(),
                    valid: fut.valid.clone(),
                });

                #[cfg(test)]
                println!("poll id {} pending 1", fut.id);

                fut.waiting = true;
                fut.valid.swap(WAITING, Ordering::Release);
                return Poll::Pending;
            }

            // alloc() holds both lock of block_tree and waiting_alloc
            if let Some(bf) = alloc_fn(&fut.pool, fut.size) {
                let old = fut.valid.swap(DONE, Ordering::Release);
                debug_assert_eq!(old, WAITING);
                #[cfg(test)]
                println!("poll id {} ready 2", fut.id);
                Poll::Ready(bf)
            } else {
                waiting_alloc.push_back(AllocReq {
                    id: fut.id,
                    waker: cx.waker().clone(),
                    valid: fut.valid.clone(),
                });
                fut.waiting = true;
                #[cfg(test)]
                println!("poll id {} pending 3", fut.id);

                fut.valid.swap(WAITING, Ordering::Release);
                Poll::Pending
            }
        } else if let Some(bf) = alloc_fn(&fut.pool, fut.size) {
            let old = fut.valid.swap(DONE, Ordering::Release);
            debug_assert!(old == WAITING || old == WAKING);

            wake_next_waiting_alloc(&mut waiting_alloc);

            #[cfg(test)]
            println!("poll id {} ready 4", fut.id);
            Poll::Ready(bf)
        } else {
            waiting_alloc.push_front(AllocReq {
                id: fut.id,
                waker: cx.waker().clone(),
                valid: fut.valid.clone(),
            }); // try to be the first
            #[cfg(test)]
            println!("poll id {} pending 5", fut.id);

            fut.valid.swap(WAITING, Ordering::Release);
            Poll::Pending
        }
    }
}

impl Drop for AllocFutInner<'_> {
    fn drop(&mut self) {
        // TODO: optimize this
        let state = self.valid.swap(DROPPED, Ordering::Acquire);

        #[cfg(test)]
        println!("drop AllocPieceBufFut id {}, old-state {}", self.id, state);

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

pub(crate) struct AllocPieceBufFut<'a> {
    inner: AllocFutInner<'a>,
}

async fn migrate_task(pool_impl: Arc<PieceBufPoolImpl>) {
    // TODO: SAFETY: make sure all PieceBuf is migratable.
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

fn start_migrate_task(mut buf_tree: MutexGuard<'_, BufTree>, pool_impl: Arc<PieceBufPoolImpl>) {
    // must set migrating in sync code
    // so at most one migrate_task can run
    buf_tree.migrating = true;
    drop(buf_tree);
    tokio::spawn(migrate_task(pool_impl));
}

impl Future for AllocPieceBufFut<'_> {
    type Output = PieceBuf;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner_fut = Pin::new(&mut self.get_mut().inner);
        inner_fut.poll_alloc(cx, PieceBufPool::alloc)
    }
}

pub(crate) struct AllocPieceBufAbortFut<'a, T> {
    inner: AllocFutInner<'a>,
    _t: PhantomData<T>,
}

impl<T> Future for AllocPieceBufAbortFut<'_, T>
where
    T: FromPieceBuf + Unpin,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner_fut = Pin::new(&mut self.get_mut().inner);
        inner_fut.poll_alloc(cx, PieceBufPool::alloc_abort)
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
            if self.tree[now_idx as usize].blk.remove(&buddy_blk) {
                now_size <<= 1;
                now_idx += 1;
                now_offset = now_offset.min(buddy_blk);
            } else {
                self.tree[now_idx as usize].blk.insert(now_offset);
                return;
            };
        }
        unreachable!("should always find a hole");
    }

    #[cfg(test)]
    fn print(&self) {
        for (i, blk) in self.tree.iter().enumerate() {
            println!("power {}, {:?}", i as u32 + MIN_ALLOC_SIZE_POWER, blk);
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
    use crate::cache::ArcCache;

    use super::*;
    use futures::future;
    use tokio_test::task;

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

    fn fb_and_fbh(f: FreeBlock) -> (FreeBlock, FreeBlockHandle) {
        (
            f.dup(),
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
            buf,
            blk_tree: v,
            alloced: HashMap::from([
                fb_and_fbh(FreeBlock {
                    offset: 0,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: base_ptr,
                }),
                fb_and_fbh(FreeBlock {
                    offset: 2 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(2 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 6 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(6 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 7 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(7 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 9 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(9 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 10 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(10 * MIN_ALLOC_SIZE) },
                }),
            ]),
        };

        assert_eq!(b.alloc(MIN_ALLOC_SIZE_POWER + 2), None);
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
            buf,
            blk_tree: v,
            alloced: HashMap::from([
                fb_and_fbh(FreeBlock {
                    offset: 0,
                    power: MIN_ALLOC_SIZE_POWER + 1,
                    ptr: base_ptr,
                }),
                fb_and_fbh(FreeBlock {
                    offset: 4 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER + 1,
                    ptr: unsafe { base_ptr.add(4 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 8 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER + 1,
                    ptr: unsafe { base_ptr.add(8 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 10 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(10 * MIN_ALLOC_SIZE) },
                }),
            ]),
        };

        assert_eq!(b.alloc(MIN_ALLOC_SIZE_POWER + 2), None);
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
            buf,
            blk_tree: v,
            alloced: HashMap::from([
                fb_and_fbh(FreeBlock {
                    offset: 1,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: base_ptr,
                }),
                fb_and_fbh(FreeBlock {
                    offset: 3 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(3 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 5 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(5 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 7 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(7 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 9 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(9 * MIN_ALLOC_SIZE) },
                }),
                fb_and_fbh(FreeBlock {
                    offset: 10 * MIN_ALLOC_SIZE,
                    power: MIN_ALLOC_SIZE_POWER,
                    ptr: unsafe { base_ptr.add(10 * MIN_ALLOC_SIZE) },
                }),
            ]),
        };

        assert_eq!(b.alloc(MIN_ALLOC_SIZE_POWER + 2), None);
        assert_eq!(b.alloc(MIN_ALLOC_SIZE_POWER + 1), None);
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
            let res = b.alloc(MIN_ALLOC_SIZE_POWER + 2).unwrap();
            b.free(&res);
        }
        {
            let res1 = b.alloc(MIN_ALLOC_SIZE_POWER + 1).unwrap();
            let res2 = b.alloc(MIN_ALLOC_SIZE_POWER + 1).unwrap();
            assert!(b.alloc(MIN_ALLOC_SIZE_POWER + 1).is_none());
            assert!(b.alloc(MIN_ALLOC_SIZE_POWER).is_some());
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
            let mut b1 = c.alloc(sz1).unwrap();
            let mut b2 = c.alloc(sz2).unwrap();
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
                let mut pb = c1.alloc(sz).unwrap();
                for b in pb.as_mut().iter_mut() {
                    *b = 0xff;
                }
                pb
            });

            let c2 = c.clone();
            let h2 = thread::spawn(move || {
                let mut pb = c2.alloc(sz).unwrap();
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

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_wakeup() {
        let c = PieceBufPool::new(2 * MIN_ALLOC_SIZE);

        let mut ts = tokio::task::JoinSet::new();
        for size in [2, 1, 1, 2, 1, 2usize].into_iter().enumerate() {
            let c1 = c.clone();
            ts.spawn(async move { c1.async_alloc(size.0, size.1 * MIN_ALLOC_SIZE).await });
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
                c1.async_alloc_abort::<ArcCache<_>>(size.0, size.1 * MIN_ALLOC_SIZE)
                    .await
            });
        }

        for _ in 0..ts.len() {
            _ = ts.join_next().await;
        }
    }

    #[cfg(not(mloom))]
    #[tokio::test]
    async fn test_async_alloc_abort_migrate() {
        let c = PieceBufPool::new(4 * MIN_ALLOC_SIZE);
        let p1 = c.async_alloc_abort::<ArcCache<_>>(1, MIN_ALLOC_SIZE).await;
        let p2 = c.async_alloc_abort::<ArcCache<_>>(2, MIN_ALLOC_SIZE).await;
        let p3 = c.async_alloc_abort::<ArcCache<_>>(3, MIN_ALLOC_SIZE).await;
        let p4 = c.async_alloc_abort::<ArcCache<_>>(4, MIN_ALLOC_SIZE).await;
        {
            let mut ref2 = p2.get_part_ref(0, MIN_ALLOC_SIZE).unwrap();
            for i in ref2.as_mut() {
                *i = 0x22;
            }
            let mut ref4 = p4.get_part_ref(0, MIN_ALLOC_SIZE).unwrap();
            for i in ref4.as_mut() {
                *i = 0x44;
            }

            tokio::spawn(async move {
                let r = ref2.abortable_work(|_| pending::<()>()).await;
                assert_eq!(r, Err(future::Aborted));
            });
            tokio::spawn(async move {
                let r = ref4.abortable_work(|_| pending::<()>()).await;
                assert_eq!(r, Err(future::Aborted));
            });
        }
        drop(p1);
        drop(p3);

        // no enough space for consecutive 2 * MIN_ALLOC_SIZE
        // this sync alloc should return None
        // but a async task will be created and migrating blocks
        assert!(c.alloc(2 * MIN_ALLOC_SIZE).is_none());
        let p5 = c
            .async_alloc_abort::<ArcCache<_>>(5, 2 * MIN_ALLOC_SIZE)
            .await;
        {
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
        let p6 = c
            .async_alloc_abort::<ArcCache<_>>(6, 2 * MIN_ALLOC_SIZE)
            .await;
        drop(p5);
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

        let h0 = tokio::spawn(async move { c0.async_alloc(0, 2 * MIN_ALLOC_SIZE).await });
        let r0 = h0.await;

        let h1 = tokio::spawn(async move { c1.async_alloc(1, 1 * MIN_ALLOC_SIZE).await });
        let mut h2 = task::spawn(async move { c2.async_alloc(2, 1 * MIN_ALLOC_SIZE).await });
        let mut h3 = task::spawn(async move { c3.async_alloc(3, 2 * MIN_ALLOC_SIZE).await });
        let h4 = tokio::spawn(async move { c4.async_alloc(4, 1 * MIN_ALLOC_SIZE).await });
        let h5 = tokio::spawn(async move { c5.async_alloc(5, 2 * MIN_ALLOC_SIZE).await });
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
            c0.async_alloc_abort::<ArcCache<_>>(0, 2 * MIN_ALLOC_SIZE)
                .await
        });
        let r0 = h0.await;

        let h1 = tokio::spawn(async move {
            c1.async_alloc_abort::<ArcCache<_>>(1, 1 * MIN_ALLOC_SIZE)
                .await
        });
        let mut h2 = task::spawn(async move {
            c2.async_alloc_abort::<ArcCache<_>>(2, 1 * MIN_ALLOC_SIZE)
                .await
        });
        let mut h3 = task::spawn(async move {
            c3.async_alloc_abort::<ArcCache<_>>(3, 2 * MIN_ALLOC_SIZE)
                .await
        });
        let h4 = tokio::spawn(async move {
            c4.async_alloc_abort::<ArcCache<_>>(4, 1 * MIN_ALLOC_SIZE)
                .await
        });
        let h5 = tokio::spawn(async move {
            c5.async_alloc_abort::<ArcCache<_>>(5, 2 * MIN_ALLOC_SIZE)
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
    async fn test_invalidate_piece_buf() {
        let c = PieceBufPool::new(10 * MIN_ALLOC_SIZE);

        let mut piece = c.async_alloc(0, 2 * MIN_ALLOC_SIZE).await;
        let fb = piece.b.dup();
        let ref_work = task::spawn(async move {
            let _ = piece.as_mut();
        });
        let mut invalid_fut1 = task::spawn(c.invalidate(fb.dup()));
        let mut invalid_fut2 = task::spawn(c.invalidate(fb.dup()));
        assert_eq!(invalid_fut1.poll(), Poll::Pending);
        assert_eq!(invalid_fut2.poll(), Poll::Pending);
        assert_eq!(invalid_fut1.poll(), Poll::Pending);
        assert_eq!(invalid_fut2.poll(), Poll::Pending);
        ref_work.await;
        assert_eq!(invalid_fut1.poll(), Poll::Ready(Ok(())));
        assert_eq!(invalid_fut2.poll(), Poll::Ready(Ok(())));
        println!("invalidated");
        assert!(c.invalidate(fb.dup()).await.is_ok());
    }

    #[tokio::test]
    async fn test_alloc_abort() {
        let c = PieceBufPool::new(10 * MIN_ALLOC_SIZE);

        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(1, 2 * MIN_ALLOC_SIZE)
            .await;
        let p2 = c
            .async_alloc_abort::<ArcCache<_>>(1, 1 * MIN_ALLOC_SIZE)
            .await;
        // let p3 = c
        //     .async_alloc_abort::<ArcCache<_>>(1, 4 * MIN_ALLOC_SIZE)
        //     .await;
        let (fb1, fb2) = {
            let mut ref11 = p1.get_part_ref(0, 16384).unwrap();
            let mut ref12 = p1.get_part_ref(16384, 16384).unwrap();
            let mut ref13 = p1.get_part_ref(2 * 16384, 16384).unwrap();
            let mut ref21 = p2.get_part_ref(2 * 16384, 16384).unwrap();
            tokio::spawn(async move {
                let r = ref11.abortable_work(|_| pending::<()>()).await;
                assert_eq!(r, Err(future::Aborted));
            });
            tokio::spawn(async move {
                let r = ref12.abortable_work(|_| pending::<()>()).await;
                assert_eq!(r, Err(future::Aborted));
            });
            tokio::spawn(async move {
                let r = ref13.abortable_work(|_| pending::<()>()).await;
                assert_eq!(r, Err(future::Aborted));
            });
            tokio::spawn(async move {
                let r = ref21.abortable_work(|_| pending::<()>()).await;
                assert_eq!(r, Err(future::Aborted));
            });

            (
                p1.piece_detail(|p| p.b.dup()).unwrap(),
                p2.piece_detail(|p| p.b.dup()).unwrap(),
            )
        };

        c.invalidate(fb2).await;
        c.invalidate(fb1).await;

        // TODO: test p1, p2's inner Cache is None
        assert!(p1.get_part_ref(16384, 16384).is_none());
    }

    #[tokio::test]
    async fn test_alloc_abort2() {
        let c = PieceBufPool::new(10 * MIN_ALLOC_SIZE);
        let p1 = c
            .async_alloc_abort::<ArcCache<_>>(1, 2 * MIN_ALLOC_SIZE)
            .await;
        let p2 = c
            .async_alloc_abort::<ArcCache<_>>(1, 1 * MIN_ALLOC_SIZE)
            .await;
        let (fb1, fb2, mut t1, mut t2) = {
            // let p3 = c
            //     .async_alloc_abort::<ArcCache<_>>(1, 4 * MIN_ALLOC_SIZE)
            //     .await;
            let mut ref11 = p1.get_part_ref(0, 16384).unwrap();
            let mut ref21 = p2.get_part_ref(2 * 16384, 16384).unwrap();
            let mut t1 = task::spawn(async move {
                let r = ref11.abortable_work(|_| pending::<()>()).await;
                assert_eq!(r, Err(future::Aborted));
            });
            let mut t2 = task::spawn(async move {
                let r = ref21.abortable_work(|_| pending::<()>()).await;
                assert_eq!(r, Err(future::Aborted));
            });
            assert_eq!(t1.poll(), Poll::Pending);
            assert_eq!(t2.poll(), Poll::Pending);

            (
                p1.piece_detail(|p| p.b.dup()).unwrap(),
                p2.piece_detail(|p| p.b.dup()).unwrap(),
                t1,
                t2,
            )
        };
        assert_eq!(t1.poll(), Poll::Pending);
        assert_eq!(t2.poll(), Poll::Pending);
        let mut inv2 = task::spawn(c.invalidate(fb2));
        let mut inv1 = task::spawn(c.invalidate(fb1));
        assert_eq!(inv2.poll(), Poll::Pending);
        assert_eq!(inv2.poll(), Poll::Pending);
        t2.await;
        assert_eq!(inv2.poll(), Poll::Ready(Ok(())));
        assert_eq!(inv1.poll(), Poll::Pending);
        t1.await;
        inv1.await;
    }
}

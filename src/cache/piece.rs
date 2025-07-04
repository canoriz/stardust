use core::fmt;
use futures::task::AtomicWaker;
use pin_project::{pin_project, pinned_drop};
use std::fmt::Debug;
use std::task::Waker;
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    io,
    marker::PhantomPinned,
    mem::ManuallyDrop,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    pin::{pin, Pin},
    slice,
    sync::{atomic::AtomicU32, Weak},
    task::{Context, Poll},
    vec,
};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Notify;
use tracing::warn;

#[cfg(mloom)]
use loom::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc, Mutex,
};

#[cfg(not(mloom))]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use super::{global::FromPieceBuf, PieceBuf};
use super::{wake_next_waiting_alloc, AllocReq};
use super::{DONE, DROPPED, WAITING, WAKING};

type Peer = SocketAddr;
const BLOCKBITS: usize = 14;
const BLOCKSIZE: usize = 1 << BLOCKBITS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockState {
    Vacant,
    Alloced,
    InUse,
}

struct State {
    state: Vec<BlockState>,
    ref_cnt: u32, // TODO: use atomic?
}

impl State {
    // returns reference count after release
    // use very carefully, this must not be public api
    fn release_ref<T>(&mut self, r: &Ref<T>) -> u32
    where
        T: AsMut<[u8]>,
    {
        change_blockstate(
            &mut self.state,
            r.offset.from,
            r.offset.len,
            BlockState::Vacant,
        );
        self.ref_cnt -= 1;
        self.ref_cnt
    }
}

const ALLOW_NEW_REF: u32 = 0;
const PAUSE_NEW_REF: u32 = 0b1;
const DISALLOW_NEW_REF: u32 = 0b10;
const INVALIDATED: u32 = 0b100;

fn allow_state_to_err(allow_state: u32) -> Option<GetRefErr> {
    if allow_state == ALLOW_NEW_REF {
        None
    } else if allow_state & DISALLOW_NEW_REF > 0 {
        Some(GetRefErr::Disallowed)
    } else if allow_state & INVALIDATED > 0 {
        Some(GetRefErr::Invalidated)
    } else if allow_state & PAUSE_NEW_REF > 0 {
        Some(GetRefErr::Paused)
    } else {
        None
    }
}

struct BufState<T> {
    // TODO: maybe out of mutex
    allow_new_ref: AtomicU32,

    cache: Option<T>,
    state: State,
    abort_handle: HashMap<RefOffset, AbortHandle>,

    abort_waiter: Vec<Arc<Notify>>,
}

type OffsetLen = (usize, usize);
struct Cache<T> {
    // TODO: since all jobs access to cache,
    // maybe split lock to smaller granularity to reduce contention
    buf: Mutex<BufState<T>>,
    waiting_reqs: Mutex<VecDeque<AllocReq<OffsetLen>>>,
}

fn change_blockstate(v: &mut [BlockState], offset: usize, len: usize, state: BlockState) {
    let fixed_len = len.next_multiple_of(BLOCKSIZE);
    for s in v[offset >> BLOCKBITS..((offset + fixed_len) >> BLOCKBITS)].iter_mut() {
        *s = state;
    }
}

impl<T> Cache<T>
where
    T: AsMut<[u8]>,
{
    fn release_ref(&self, r: &Ref<T>) {
        let mut buf_guard = self.buf.lock().expect("release ref should lock OK");

        #[cfg(test)]
        println!("drop {:?}", r.offset);

        if buf_guard.state.release_ref(r) == 0 {
            for waiter in buf_guard.abort_waiter.drain(0..) {
                // assert_eq!(buf_guard.allow_new_ref.load(Ordering::Relaxed), false);
                waiter.notify_one();
            }
        }
    }

    fn allow_new_ref(&self, allow_new_ref: bool) {
        let buf_guard = self.buf.lock().expect("allow new ref should lock OK");
        if allow_new_ref {
            buf_guard
                .allow_new_ref
                .fetch_and(!DISALLOW_NEW_REF, Ordering::Release);
        } else {
            buf_guard
                .allow_new_ref
                .fetch_or(DISALLOW_NEW_REF, Ordering::Release);
        }
    }

    fn set_allow_state(&self, state: u32) -> u32 {
        let buf_guard = self.buf.lock().expect("set_allow_state should lock OK");
        buf_guard.allow_new_ref.swap(state, Ordering::AcqRel)
    }

    /// returns if allowed to add this work
    /// if return false, means this work is aborted now
    fn add_abortable_work(&self, offset: RefOffset, handle: AbortHandle) -> bool {
        let mut buf_guard = self.buf.lock().expect("add abortable work lock should OK");
        if buf_guard.allow_new_ref.load(Ordering::Relaxed) != ALLOW_NEW_REF {
            // TODO: return state
            return false;
        }
        buf_guard.abort_handle.insert(offset, handle);
        true
    }

    fn rm_abortable_work(&self, offset: &RefOffset) {
        let mut buf_guard = self.buf.lock().expect("add abortable work lock should OK");
        buf_guard.abort_handle.remove(offset);
    }

    /// abort_all trys to abort all pending read write
    /// but if some operation cannot be aborted
    /// these operations will continue
    async fn abort_all(&self) {
        let wait_abort = {
            let mut buf_guard = self.buf.lock().expect("abort all work");
            // TODO: FIXME: what if abort a already DISALLOW_NEW_REF cache?
            buf_guard
                .allow_new_ref
                .fetch_or(PAUSE_NEW_REF, Ordering::Relaxed);
            for (_, handle) in buf_guard.abort_handle.drain() {
                handle.abort();
            }

            #[cfg(test)]
            println!("in abort all ref_cnt {}", buf_guard.state.ref_cnt);

            if buf_guard.state.ref_cnt == 0 {
                return;
            } else {
                let aborted = Arc::new(Notify::new());
                buf_guard.abort_waiter.push(aborted.clone());
                aborted
            }
        };
        wait_abort.notified().await;
    }

    fn drop_piecebuf(&self) {
        let mut buf_guard = self.buf.lock().expect("drop piecebuf lock should OK");
        assert!(buf_guard.allow_new_ref.load(Ordering::Relaxed) > 0);
        assert!(buf_guard.allow_new_ref.load(Ordering::Relaxed) & PAUSE_NEW_REF > 0);
        assert_eq!(buf_guard.state.ref_cnt, 0);

        #[cfg(test)]
        println!("drop inner pieceBuf");

        buf_guard.cache.take();
    }

    fn unpause(&self) {
        self.buf
            .lock()
            .expect("unpause lock should OK")
            .allow_new_ref
            .fetch_and(!PAUSE_NEW_REF, Ordering::Relaxed);

        let mut waiting_reqs = self
            .waiting_reqs
            .lock()
            .expect("unpause lock waiting_reqs should OK");

        for w in waiting_reqs.drain(0..) {
            #[cfg(test)]
            println!("try waking {:?}", w.key);
            match w
                .valid
                .compare_exchange(WAITING, WAKING, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    w.waker.wake();
                    #[cfg(test)]
                    println!("wake waiting {:?}", w.key);
                    break;
                }
                Err(actual) => {
                    // if DONE, this future is Ready
                    // if DROPPED, no one waiting future
                    // pick next one
                    debug_assert!(actual == DROPPED || actual == DONE || actual == WAKING);

                    #[cfg(test)]
                    println!("no wake {:?} because actual state {actual}", w.key);
                }
            }
        }
    }
}

pub struct ArcCache<T> {
    inner: Arc<Cache<T>>,
}

impl<T> Clone for ArcCache<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl FromPieceBuf for ArcCache<PieceBuf> {
    // TODO: is this safe? require T: AsMut and taking ownership of T
    // size must be multiple of 16384
    fn new_abort(t: PieceBuf) -> (Self, SubAbortHandle<PieceBuf>) {
        let len = t.len();
        let fixed_len = len.next_multiple_of(BLOCKSIZE);
        assert_eq!(fixed_len % BLOCKSIZE, 0);
        let inner = Arc::new(Cache {
            buf: Mutex::new(BufState {
                allow_new_ref: AtomicU32::new(ALLOW_NEW_REF),

                cache: Some(t),
                state: State {
                    state: vec![BlockState::Vacant; fixed_len >> BLOCKBITS],
                    ref_cnt: 0,
                },
                abort_handle: HashMap::new(),
                abort_waiter: Vec::new(),
            }),
            waiting_reqs: Mutex::new(VecDeque::new()),
        });
        (
            Self {
                inner: inner.clone(),
            },
            SubAbortHandle {
                cache: Arc::downgrade(&inner),
            },
        )
    }
}

impl ArcCache<PieceBuf> {
    pub(crate) fn piece_detail<T, F>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&PieceBuf) -> T,
    {
        self.inner
            .buf
            .lock()
            .expect("piece detail lock should OK")
            .cache
            .as_ref()
            .map(|p| f(p))
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.inner
            .buf
            .lock()
            .expect("is_valid should lock OK")
            .cache
            .is_some()
    }
}

impl<T> ArcCache<T>
where
    T: AsMut<[u8]>,
{
    // TODO: is this safe? require T: AsMut and taking ownership of T
    // size must be multiple of 16384
    pub fn new(mut t: T) -> Self {
        let len = t.as_mut().len();
        let fixed_len = len.next_multiple_of(BLOCKSIZE);
        assert_eq!(fixed_len % BLOCKSIZE, 0);
        Self {
            inner: Arc::new(Cache {
                buf: Mutex::new(BufState {
                    allow_new_ref: AtomicU32::new(ALLOW_NEW_REF),

                    cache: Some(t),
                    state: State {
                        state: vec![BlockState::Vacant; fixed_len >> BLOCKBITS],
                        ref_cnt: 0,
                    },
                    abort_handle: HashMap::new(),
                    abort_waiter: Vec::new(),
                }),
                waiting_reqs: Mutex::new(VecDeque::new()),
            }),
        }
    }

    // pub fn expand(&mut self, new_size: usize) {
    //     todo!()
    // }
    // pub fn shrink(&mut self, new_size: usize) {
    //     todo!()
    // }

    // returns pre-allocated memory size
    pub fn pre_alloc(&self, peer: Peer, piece_size: usize, piece_index: usize) -> usize {
        todo!()
    }

    // block_offset must be multiple of 16384
    pub fn get_ref(&self, peer: Peer, piece_index: usize, block_offset: usize) -> Ref<T> {
        todo!()
    }

    pub fn disable_new_ref(&self) {
        self.inner.allow_new_ref(false);
    }

    pub async fn abort_all(&self) {
        self.inner.abort_all().await
    }

    // TODO: maybe not use (offset,length) but use
    // (offset_mutlple_of(block), len_multiple_of(block))
    // TODO: this needs a lot of tests
    // TODO: read|write mode
    pub fn get_part_ref(&self, offset: usize, len: usize) -> Result<Ref<T>, GetRefErr> {
        let fixed_len = len.next_multiple_of(BLOCKSIZE);
        assert_eq!(fixed_len % BLOCKSIZE, 0);

        let mut buf_guard = self.inner.buf.lock().expect("get part ref lock should OK");
        let allow_state = buf_guard.allow_new_ref.load(Ordering::Acquire);
        if let Some(err) = allow_state_to_err(allow_state) {
            return Err(err);
        }

        let buf = buf_guard.deref_mut();
        if let Some(ref mut cache) = buf.cache {
            if len + offset > cache.as_mut().len() {
                return Err(GetRefErr::RangeOverflow);
            }

            let range_state = &mut buf.state;
            range_state.ref_cnt += 1;

            let any_block_not_vacant = range_state.state
                [offset >> BLOCKBITS..(offset + fixed_len) >> BLOCKBITS]
                .iter()
                .any(|s| *s != BlockState::Vacant);
            if any_block_not_vacant {
                return Err(GetRefErr::InUse);
            }

            for s in
                range_state.state[offset >> BLOCKBITS..(offset + fixed_len) >> BLOCKBITS].iter_mut()
            {
                assert_eq!(*s, BlockState::Vacant);
                *s = BlockState::InUse;
            }

            Ok(Ref {
                offset: RefOffset {
                    from: offset,
                    ptr: unsafe { cache.as_mut().as_mut_ptr().add(offset) },
                    len,
                },
                main_cache: ManuallyDrop::new(self.inner.clone()),
            })
        } else {
            Err(GetRefErr::Invalidated)
        }
    }

    // TODO: maybe not use (offset,length) but use
    // (offset_mutlple_of(block), len_multiple_of(block))
    // TODO: this needs a lot of tests
    pub fn async_get_part_ref(&self, offset: usize, len: usize) -> AsyncPartRefFut<T> {
        AsyncPartRefFut {
            offset,
            len,
            valid: Arc::new(AtomicU32::new(WAITING)),
            cache: self,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GetRefErr {
    InUse,
    RangeOverflow,
    Invalidated,
    Paused,
    Disallowed,
}

#[must_use = "futures do nothing unless you poll them"]
pub(crate) struct AsyncPartRefFut<'a, T> {
    offset: usize,
    len: usize,
    valid: Arc<AtomicU32>,
    cache: &'a ArcCache<T>,
}

impl<T> Future for AsyncPartRefFut<'_, T>
where
    T: AsMut<[u8]>,
{
    type Output = Result<Ref<T>, GetRefErr>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let fut = self.get_mut();
        let fixed_len = fut.len.next_multiple_of(BLOCKSIZE);

        let mut buf_guard = fut
            .cache
            .inner
            .buf
            .lock()
            .expect("get part ref lock should OK");

        let allow_state = buf_guard.allow_new_ref.load(Ordering::Acquire);
        if allow_state & DISALLOW_NEW_REF > 0 {
            return Poll::Ready(Err(GetRefErr::Disallowed));
        }
        if allow_state & INVALIDATED > 0 {
            return Poll::Ready(Err(GetRefErr::Invalidated));
        }
        assert!(allow_state == ALLOW_NEW_REF || allow_state == PAUSE_NEW_REF);
        if allow_state == PAUSE_NEW_REF {
            fut.cache
                .inner
                .waiting_reqs
                .lock()
                .expect("async_get_part lock waiting_reqs should OK")
                .push_back(AllocReq {
                    key: (fut.offset, fut.len),
                    waker: cx.waker().clone(),
                    valid: fut.valid.clone(),
                });
            return Poll::Pending;
        }

        let buf = buf_guard.deref_mut();
        if let Some(ref mut cache) = buf.cache {
            if fut.len + fut.offset > cache.as_mut().len() {
                return Poll::Ready(Err(GetRefErr::RangeOverflow));
            }

            let range_state = &mut buf.state;
            range_state.ref_cnt += 1;

            let any_block_not_vacant = range_state.state
                [fut.offset >> BLOCKBITS..(fut.offset + fixed_len) >> BLOCKBITS]
                .iter()
                .any(|s| *s != BlockState::Vacant);
            if any_block_not_vacant {
                return Poll::Ready(Err(GetRefErr::InUse));
            }

            for s in range_state.state
                [fut.offset >> BLOCKBITS..(fut.offset + fixed_len) >> BLOCKBITS]
                .iter_mut()
            {
                assert_eq!(*s, BlockState::Vacant);
                *s = BlockState::InUse;
            }
            Poll::Ready(Ok(Ref {
                offset: RefOffset {
                    from: fut.offset,
                    ptr: unsafe { cache.as_mut().as_mut_ptr().add(fut.offset) },
                    len: fut.len,
                },
                main_cache: ManuallyDrop::new(fut.cache.inner.clone()),
            }))
        } else {
            return Poll::Ready(Err(GetRefErr::Invalidated));
        }
    }
}

impl<T> Drop for AsyncPartRefFut<'_, T> {
    fn drop(&mut self) {
        // TODO: optimize this
        let state = self.valid.swap(DROPPED, Ordering::Acquire);

        #[cfg(test)]
        println!("drop AllocPartRefFut, old-state {}", state);

        match state {
            WAKING => {
                // executor wants to wake us, but we are dropped before
                // being polled (if polled, state can't be WAKING)
                // wake another one
                let mut waiting_alloc = self.cache.inner.waiting_reqs.lock().unwrap();
                wake_next_waiting_alloc(&mut waiting_alloc);
            }
            w => debug_assert!(w == WAITING || w == DONE),
        }
    }
}

pub struct SubAbortHandle<T> {
    cache: Weak<Cache<T>>,
}

impl<T> Clone for SubAbortHandle<T> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
        }
    }
}

impl<T> SubAbortHandle<T>
where
    T: AsMut<[u8]>,
{
    // abort all pending read and reject all future get_ref request
    // drop inner Buf
    pub async fn invalidate_all(&self) {
        if let Some(a) = self.cache.upgrade() {
            a.set_allow_state(INVALIDATED);
            a.abort_all().await;
            a.drop_piecebuf();
        } else {
            warn!("invalidate_all upgrade failed, dropped in between?");
        }
    }

    // abort all pending read and reject all future get_ref request
    pub async fn abort_all(&self) {
        if let Some(a) = self.cache.upgrade() {
            a.abort_all().await;
        } else {
            warn!("abort upgrade failed, dropped in between?");
        }
    }

    // migrating done reenable get_ref request
    pub fn reenable(&self) {
        if let Some(a) = self.cache.upgrade() {
            a.unpause();
        } else {
            warn!("reenable upgrade failed, dropped in between?");
        }
    }
}

impl SubAbortHandle<PieceBuf> {
    pub fn migrate(&self, new_offset: usize) {
        if let Some(a) = self.cache.upgrade() {
            let mut guard = a.buf.lock().expect("abort handle migrate lock should OK");
            unsafe {
                guard
                    .cache
                    .as_mut()
                    .expect("at migrate, the piece should be valid")
                    .reset_offset(new_offset)
            };
        } else {
            warn!("some PieceBuf dropped while migrating, will this happen? this might indicating a bug!")
        }
    }
}

// this is a Writeable Ref
// TODO: maybe need a ReadOnly Ref
pub struct Ref<T>
where
    T: AsMut<[u8]>,
{
    offset: RefOffset,
    // manually_drop is used in extend_to_entire()
    // extend_or_entire acts as a "Drop"
    main_cache: ManuallyDrop<Arc<Cache<T>>>,
}
impl<T> Debug for Ref<T>
where
    T: Debug + AsMut<[u8]>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ref").field("offset", &self.offset).finish()
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct RefOffset {
    from: usize,
    ptr: *mut u8,

    len: usize,
}
unsafe impl Send for RefOffset {}
unsafe impl Sync for RefOffset {}

impl RefOffset {
    unsafe fn as_mut(&mut self) -> &mut [u8] {
        slice::from_raw_parts_mut(self.ptr, self.len)
    }

    unsafe fn as_ref(&self) -> &[u8] {
        slice::from_raw_parts(self.ptr, self.len)
    }
}

impl<T> Deref for Ref<T>
where
    T: AsMut<[u8]>,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for Ref<T>
where
    T: AsMut<[u8]>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

// TODO: this may be changed to AsMut<MaybeUninit<[u8]>>
impl<T> AsMut<[u8]> for Ref<T>
where
    T: AsMut<[u8]>,
{
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { self.offset.as_mut() }
    }
}

impl<T> AsRef<[u8]> for Ref<T>
where
    T: AsMut<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        unsafe { self.offset.as_ref() }
    }
}

unsafe impl<T> Send for Ref<T> where T: Send + AsMut<[u8]> {}

impl<T> Ref<T>
where
    T: AsMut<[u8]>,
{
    // pub fn as_alice(&mut self) -> &mut [u8] {
    //     // TODO: is this safe? is ptr valid (will inner address in arc change?)
    //     unsafe { slice::from_raw_parts_mut(self.ptr, self.len) }
    // }

    // pub fn as_slice_len(&mut self, len: usize) -> &mut [u8] {
    //     assert!(len <= self.len);
    //     // TODO: is this safe? is ptr valid (will inner address in arc change?)
    //     unsafe { slice::from_raw_parts_mut(self.ptr, len) }
    // }

    // extend block ref's length to full ref
    pub fn extend_to_entire(self) -> Option<Self> {
        let mut c = ManuallyDrop::new(self);
        // println!("enter");
        let success = {
            let mut buf_guard = c
                .main_cache
                .buf
                .lock()
                .expect("extend to entire should lock OK");
            let state = &mut buf_guard.state;
            // println!("{:?}", self.main_cache.ref_count);
            // let ref_cnt = self.main_cache.ref_count.load(Ordering::Acquire);
            // println!("cnt {}", state.ref_cnt);
            if state.ref_cnt != 1 {
                // println!("extend ref_cnt stop");
                state.release_ref(&c);
                None
            } else {
                // println!("extend ref_cnt go ahead {}", state.ref_cnt);

                for s in state.state.iter_mut() {
                    *s = BlockState::InUse;
                }
                let offset = RefOffset {
                    from: 0,
                    ptr: buf_guard
                        .cache
                        .as_mut()
                        .expect("at extend should be Some")
                        .as_mut()
                        .as_mut_ptr(),
                    len: buf_guard
                        .cache
                        .as_mut()
                        .expect("at extend should be Some")
                        .as_mut()
                        .len(),
                };
                assert_eq!(buf_guard.abort_handle.len(), 0);
                Some(offset)
            }
        };
        // println!("success: {success}");
        if let Some(offset) = success {
            c.offset = offset;
            Some(ManuallyDrop::into_inner(c))
        } else {
            #[cfg(test)]
            println!("manually drop arc");
            unsafe { ManuallyDrop::drop(&mut c.main_cache) };
            None
        }
    }

    pub fn len(&self) -> usize {
        self.offset.len
    }
}

impl<T> Drop for Ref<T>
where
    T: AsMut<[u8]>,
{
    fn drop(&mut self) {
        #[cfg(test)]
        println!("in drop, manually drop arc");
        self.main_cache.release_ref(self);
        unsafe { ManuallyDrop::drop(&mut self.main_cache) };
    }
}

/// A handle to an `Abortable` task.
#[derive(Debug, Clone)]
pub struct AbortHandle {
    inner: Arc<AbortInner>,
}

impl AbortHandle {
    /// Creates an (`AbortHandle`, `AbortRegistration`) pair which can be used
    /// to abort a running future or stream.
    ///
    /// This function is usually paired with a call to [`Abortable::new`].
    pub fn new_pair() -> (Self, AbortRegistration) {
        let inner = Arc::new(AbortInner {
            waker: AtomicWaker::new(),
            aborted: AtomicBool::new(false),
        });

        (
            Self {
                inner: inner.clone(),
            },
            AbortRegistration { inner },
        )
    }

    /// Abort the `Abortable` stream/future associated with this handle.
    ///
    /// Notifies the Abortable task associated with this handle that it
    /// should abort. Note that if the task is currently being polled on
    /// another thread, it will not immediately stop running. Instead, it will
    /// continue to run until its poll method returns.
    pub fn abort(&self) {
        self.inner.aborted.store(true, Ordering::Relaxed);
        self.inner.waker.wake();
    }

    /// Checks whether [`AbortHandle::abort`] was *called* on any associated
    /// [`AbortHandle`]s, which includes all the [`AbortHandle`]s linked with
    /// the same [`AbortRegistration`]. This means that it will return `true`
    /// even if:
    /// * `abort` was called after the task had completed.
    /// * `abort` was called while the task was being polled - the task may still be running and
    ///   will not be stopped until `poll` returns.
    ///
    /// This operation has a Relaxed ordering.
    pub fn is_aborted(&self) -> bool {
        self.inner.aborted.load(Ordering::Relaxed)
    }
}

/// A registration handle for an `Abortable` task.
/// Values of this type can be acquired from `AbortHandle::new` and are used
/// in calls to `Abortable::new`.
#[derive(Debug)]
pub struct AbortRegistration {
    pub(crate) inner: Arc<AbortInner>,
}

impl AbortRegistration {
    /// Create an [`AbortHandle`] from the given [`AbortRegistration`].
    ///
    /// The created [`AbortHandle`] is functionally the same as any other
    /// [`AbortHandle`]s that are associated with the same [`AbortRegistration`],
    /// such as the one created by [`AbortHandle::new_pair`].
    pub fn handle(&self) -> AbortHandle {
        AbortHandle {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AbortErr<T> {
    Aborted,
    IO(T),
}

impl<T> fmt::Display for AbortErr<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aborted => {
                write!(f, "`Abortable` future has been aborted")?;
            }
            Self::IO(t) => {
                write!(f, "{:?}", t)?;
            }
        }
        Ok(())
    }
}

impl<T> std::error::Error for AbortErr<T> where T: std::fmt::Debug {}

#[pin_project(PinnedDrop)]
#[must_use = "futures/streams do nothing unless you poll them"]
pub(crate) struct AbortableRead<'a, T: ?Sized, U>
where
    U: AsMut<[u8]>,
{
    reader: &'a mut T,
    buf: &'a mut Ref<U>,
    offset: usize,
    inner: Arc<AbortInner>,

    need_rm_abort_handle: bool,

    #[pin]
    _pin: PhantomPinned,
}

#[pinned_drop]
impl<T: ?Sized, U> PinnedDrop for AbortableRead<'_, T, U>
where
    U: AsMut<[u8]>,
{
    fn drop(self: Pin<&mut Self>) {
        let me = self.project();
        if *me.need_rm_abort_handle {
            me.buf.main_cache.rm_abortable_work(&me.buf.offset);
        }
    }
}

// Inner type storing the waker to awaken and a bool indicating that it
// should be aborted.
#[derive(Debug)]
pub(crate) struct AbortInner {
    pub(crate) waker: AtomicWaker,
    pub(crate) aborted: AtomicBool,
}

impl AbortInner {
    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Relaxed)
    }
}

impl<T, U> AbortableRead<'_, T, U>
where
    // TODO: use pin-project and lift Unpin restriction?
    T: AsyncRead + Unpin,
    U: AsMut<[u8]>,
{
    pub fn is_aborted(&self) -> bool {
        self.inner.is_aborted()
    }

    fn try_poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, AbortErr<io::Error>>> {
        // Check if the task has been aborted
        if self.is_aborted() {
            return Poll::Ready(Err(AbortErr::Aborted));
        }

        let me = self.project();
        // attempt to complete the task

        // TODO: use uninit ReadBuf?
        let mut buf = ReadBuf::new(&mut me.buf.as_mut()[*me.offset..]);
        if let Poll::Ready(r) = Pin::new(me.reader).poll_read(cx, &mut buf) {
            match r {
                Ok(_) => return Poll::Ready(Ok(buf.filled().len())),
                Err(e) => return Poll::Ready(Err(AbortErr::IO(e))),
            }
        }

        // Register to receive a wakeup if the task is aborted in the future
        me.inner.waker.register(cx.waker());

        // Check to see if the task was aborted between the first check and
        // registration.
        // Checking with `is_aborted` which uses `Relaxed` is sufficient because
        // `register` introduces an `AcqRel` barrier.
        if me.inner.is_aborted() {
            return Poll::Ready(Err(AbortErr::Aborted));
        }

        Poll::Pending
    }
}

impl<T, U> Future for AbortableRead<'_, T, U>
where
    // TODO: use pin-project and lift Unpin restriction?
    T: AsyncRead + Unpin,
    U: AsMut<[u8]>,
{
    type Output = Result<usize, AbortErr<io::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.try_poll(cx)
    }
}

pub trait AsyncAbortRead: AsyncRead {
    fn read_abort<'a, T>(
        &'a mut self,
        buf: &'a mut Ref<T>,
        offset: usize,
    ) -> AbortableRead<'a, Self, T>
    where
        T: AsMut<[u8]>,
        Self: AsyncRead,
    {
        let (handle, reg) = AbortHandle::new_pair();

        if buf
            .main_cache
            .add_abortable_work(buf.offset.clone(), handle.clone())
        {
            AbortableRead {
                reader: self,
                buf,
                offset,
                need_rm_abort_handle: true,

                inner: reg.inner,
                _pin: PhantomPinned,
            }
        } else {
            handle.abort();
            AbortableRead {
                reader: self,
                buf,
                offset,
                need_rm_abort_handle: false,

                inner: reg.inner,
                _pin: PhantomPinned,
            }
        }
    }
}

impl<R: AsyncRead + ?Sized> AsyncAbortRead for R {}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(mloom)]
    use loom::thread;

    #[cfg(not(mloom))]
    use std::thread;

    use tokio_test::task;

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
    fn test_cache_disjoint_ref() {
        loom_test(|| {
            let cache = ArcCache::new(vec![0u8; BLOCKSIZE * 16]);
            let ref5to7 = cache.get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE);
            let ref6to8 = cache.get_part_ref(6 * BLOCKSIZE, 2 * BLOCKSIZE);
            let ref9to11 = cache.get_part_ref(9 * BLOCKSIZE, 2 * BLOCKSIZE);
            assert!(ref5to7.is_ok());
            assert!(matches!(ref6to8, Err(GetRefErr::InUse)));
            assert!(ref9to11.is_ok());

            cache
                .inner
                .buf
                .lock()
                .unwrap()
                .state
                .state
                .iter()
                .enumerate()
                .for_each(|(i, s)| {
                    if i >= 5 && i < 7 || i >= 9 && i < 11 {
                        assert_eq!((i, *s), (i, BlockState::InUse))
                    } else {
                        assert_ne!((i, *s), (i, BlockState::InUse))
                    }
                });
            drop(ref5to7);
            drop(ref9to11);
            cache
                .inner
                .buf
                .lock()
                .unwrap()
                .state
                .state
                .iter()
                .enumerate()
                .for_each(|(i, s)| assert_eq!((i, *s), (i, BlockState::Vacant)));
        })
    }

    #[test]
    fn test_cache_get_last_ref() {
        loom_test(|| {
            let cache = ArcCache::new(vec![0u8; BLOCKSIZE * 15 + 201]);
            assert!(cache
                .get_part_ref(15 * BLOCKSIZE, 202)
                .is_err_and(|e| e == GetRefErr::RangeOverflow));
            let _ref_last = cache.get_part_ref(15 * BLOCKSIZE, 201).unwrap();
        })
    }

    #[tokio::test]
    async fn test_cache_async_get_ref_manual_runtime() {
        use tokio_test::task;

        let cache = ArcCache::new(vec![0u8; BLOCKSIZE * 16]);
        let ref5to7 = cache.async_get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE).await;
        assert!(ref5to7.is_ok());

        // mock migrating
        cache
            .inner
            .buf
            .lock()
            .unwrap()
            .allow_new_ref
            .fetch_or(PAUSE_NEW_REF, Ordering::Relaxed);

        let mut ref6to8 = task::spawn(cache.async_get_part_ref(6 * BLOCKSIZE, 2 * BLOCKSIZE));

        let mut ref9to11 = task::spawn(cache.async_get_part_ref(9 * BLOCKSIZE, 2 * BLOCKSIZE));
        let mut ref12to13 = task::spawn(cache.async_get_part_ref(12 * BLOCKSIZE, 1 * BLOCKSIZE));

        // poll will return pending
        assert!(matches!(ref9to11.poll(), Poll::Pending));
        assert!(matches!(ref12to13.poll(), Poll::Pending));
        assert!(matches!(ref9to11.poll(), Poll::Pending));
        assert!(matches!(ref12to13.poll(), Poll::Pending));
        assert!(matches!(ref6to8.poll(), Poll::Pending));

        cache.inner.unpause();

        assert!(matches!(ref6to8.poll(), Poll::Ready(Err(GetRefErr::InUse))));
        let r1213 = ref12to13.await;
        let r911 = ref9to11.await;

        cache
            .inner
            .buf
            .lock()
            .unwrap()
            .state
            .state
            .iter()
            .enumerate()
            .for_each(|(i, s)| {
                if i >= 5 && i < 7 || i >= 9 && i < 11 || i >= 12 && i < 13 {
                    assert_eq!((i, *s), (i, BlockState::InUse))
                } else {
                    assert_ne!((i, *s), (i, BlockState::InUse))
                }
            });
        drop(ref5to7);
        drop(r1213);
        drop(r911);
        cache
            .inner
            .buf
            .lock()
            .unwrap()
            .state
            .state
            .iter()
            .enumerate()
            .for_each(|(i, s)| assert_eq!((i, *s), (i, BlockState::Vacant)));
    }

    #[tokio::test]
    async fn test_cache_async_get_ref() {
        let cache = ArcCache::new(vec![0u8; BLOCKSIZE * 16]);
        let ref5to7 = cache.async_get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE).await;
        assert!(ref5to7.is_ok());

        // mock migrating
        cache
            .inner
            .buf
            .lock()
            .unwrap()
            .allow_new_ref
            .fetch_or(PAUSE_NEW_REF, Ordering::Relaxed);

        let c = cache.clone();
        let ref6to8 =
            tokio::spawn(async move { c.async_get_part_ref(6 * BLOCKSIZE, 2 * BLOCKSIZE).await });
        let c = cache.clone();
        let ref9to11 =
            tokio::spawn(async move { c.async_get_part_ref(9 * BLOCKSIZE, 2 * BLOCKSIZE).await });
        let c = cache.clone();
        let ref12to13 =
            tokio::spawn(async move { c.async_get_part_ref(12 * BLOCKSIZE, 1 * BLOCKSIZE).await });

        cache.inner.unpause();

        assert!(matches!(ref6to8.await.unwrap(), Err(GetRefErr::InUse)));
        let r1213 = ref12to13.await.unwrap();
        let r911 = ref9to11.await.unwrap();

        cache
            .inner
            .buf
            .lock()
            .unwrap()
            .state
            .state
            .iter()
            .enumerate()
            .for_each(|(i, s)| {
                if i >= 5 && i < 7 || i >= 9 && i < 11 || i >= 12 && i < 13 {
                    assert_eq!((i, *s), (i, BlockState::InUse))
                } else {
                    assert_ne!((i, *s), (i, BlockState::InUse))
                }
            });
        drop(ref5to7);
        drop(r1213);
        drop(r911);
        cache
            .inner
            .buf
            .lock()
            .unwrap()
            .state
            .state
            .iter()
            .enumerate()
            .for_each(|(i, s)| assert_eq!((i, *s), (i, BlockState::Vacant)));
    }

    #[test]
    fn test_concurrent_read_write() {
        loom_test(|| {
            let a = ArcCache::new(vec![0u8; 30 * BLOCKSIZE]);

            let a1 = a.clone();
            let h1 = thread::spawn(move || {
                let mut s_ref = a1.get_part_ref(0, 2 * BLOCKSIZE).expect("ok");
                let s = s_ref.as_mut();
                for b in s.iter_mut() {
                    *b = 1u8;
                }
            });

            let a2 = a.clone();
            let h2 = thread::spawn(move || {
                let mut s_ref = a2.get_part_ref(4 * BLOCKSIZE, 2 * BLOCKSIZE).expect("ok");
                let s = s_ref.as_mut();
                for b in s.iter_mut() {
                    *b = 3u8;
                }
            });
            let _ = h1.join();
            let _ = h2.join();

            let guard = a.inner.buf.lock().unwrap();

            guard.state.state.iter().enumerate().for_each(|(i, s)| {
                assert_eq!((i, *s), (i, BlockState::Vacant));
            });

            for (i, s) in guard.cache.as_ref().unwrap().iter().enumerate() {
                if (4 * BLOCKSIZE..6 * BLOCKSIZE).contains(&i) {
                    assert_eq!((i, *s), (i, 3u8))
                } else if (0..2 * BLOCKSIZE).contains(&i) {
                    assert_eq!((i, *s), (i, 1u8))
                }
            }
        });
    }

    #[test]
    fn test_extend_ref() {
        loom_test(|| {
            let cache = ArcCache::new(vec![0u8; BLOCKSIZE * 16]);
            let cache_clone = cache.clone();
            let cache_clone2 = cache.clone();
            let ref5to7 = cache.get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
            let ref8to9 = cache.get_part_ref(8 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
            cache
                .inner
                .buf
                .lock()
                .unwrap()
                .state
                .state
                .iter()
                .enumerate()
                .for_each(|(i, s)| {
                    println!("cc0{:?}", (i, *s));
                });

            let h1 = thread::spawn(move || {
                cache_clone.disable_new_ref();
                ref5to7.extend_to_entire()
            });

            let h2 = thread::spawn(move || {
                cache_clone2.disable_new_ref();
                ref8to9.extend_to_entire()
            });

            let ex1 = h1.join().unwrap();
            let ex2 = h2.join().unwrap();
            assert!(ex1.is_some() ^ ex2.is_some());

            let entire = ex1.or(ex2).unwrap();
            let mut guard = cache.inner.buf.lock().unwrap();

            guard.state.state.iter().enumerate().for_each(|(i, s)| {
                assert_eq!((i, *s), (i, BlockState::InUse));
            });

            assert_eq!(
                entire.offset.ptr,
                guard.cache.as_mut().unwrap().as_mut_ptr()
            );
            assert_eq!(entire.offset.from, 0);
            assert_eq!(entire.offset.len, guard.cache.as_mut().unwrap().len());
        });
    }

    #[test]
    fn test_disable_new_ref() {
        loom_test(|| {
            let cache = ArcCache::new(vec![0u8; BLOCKSIZE * 16]);
            let ref5to7 = cache.get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
            let ref8to9 = cache.get_part_ref(8 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
            cache.disable_new_ref();

            // new ref is banned
            let ref3to5 = cache.get_part_ref(3 * BLOCKSIZE, 2 * BLOCKSIZE);
            assert!(matches!(ref3to5, Err(GetRefErr::Disallowed)));

            // one ref still holds, no new ref
            drop(ref8to9);
            let ref3to5 = cache.get_part_ref(3 * BLOCKSIZE, 2 * BLOCKSIZE);
            assert!(matches!(ref3to5, Err(GetRefErr::Disallowed)));

            drop(ref5to7);
            // all ref reclaimed, new ref allowed
            let ref3to5 = cache.get_part_ref(3 * BLOCKSIZE, 2 * BLOCKSIZE);
            assert!(ref3to5.is_ok());
        });
    }

    #[tokio::test]
    async fn test_abort() {
        let cache = ArcCache::new(vec![0u8; BLOCKSIZE * 16]);
        let ref5to7 = cache.get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
        let ref8to9 = cache.get_part_ref(8 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
        let mut abort_fut = task::spawn(cache.abort_all());
        assert_eq!(abort_fut.poll(), Poll::Pending);
        assert_eq!(abort_fut.poll(), Poll::Pending);
        drop(ref5to7);
        assert_eq!(abort_fut.poll(), Poll::Pending);
        assert_eq!(abort_fut.poll(), Poll::Pending);
        drop(ref8to9);
        assert_eq!(abort_fut.poll(), Poll::Ready(()));
    }

    // #[test]
    // fn test_ref_count() {
    //     loom_test(|| {
    //         let cache = ArcCache::new(BLOCKSIZE * 16);
    //         let cache_clone = cache.clone();
    //         let cache_clone2 = cache.clone();
    //         let mut ref5to7 = cache.get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
    //         let mut ref8to9 = cache.get_part_ref(8 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
    //         // assert_eq!(cache.inner.ref_count.load(Ordering::Acquire), 2);
    //         drop(ref5to7);
    //         // assert_eq!(cache.inner.ref_count.load(Ordering::Acquire), 1);
    //         drop(ref8to9);
    //         // assert_eq!(cache.inner.ref_count.load(Ordering::Acquire), 0);

    //         // let h1 = thread::spawn(move || {
    //         //     cache_clone.disable_new_ref();
    //         //     ref5to7.extend_to_entire()
    //         // });

    //         // let h2 = thread::spawn(move || {
    //         //     cache_clone2.disable_new_ref();
    //         //     ref8to9.extend_to_entire()
    //         // });

    //         // let success1) = h1.join().unwrap();
    //         // let (r89, success2) = h2.join().unwrap();
    //         // dbg!(success1);
    //         // dbg!(success2);
    //         // assert!(success1 ^ success2);
    //         // if success1 {
    //         //     assert_eq!(r57.ptr, cache.inner.cache.as_ptr());
    //         //     assert_eq!(r57.from, 0);
    //         //     assert_eq!(r57.len, cache.inner.cache.len());
    //         //     assert_eq!(r89.len, 0);
    //         // } else {
    //         //     assert_eq!(r89.ptr, cache.inner.cache.as_ptr());
    //         //     assert_eq!(r89.from, 0);
    //         //     assert_eq!(r89.len, cache.inner.cache.len());
    //         //     assert_eq!(r57.len, 0);
    //         // }

    //         // cache
    //         //     .inner
    //         //     .state
    //         //     .lock()
    //         //     .unwrap()
    //         //     .iter()
    //         //     .enumerate()
    //         //     .for_each(|(i, s)| {
    //         //         assert_eq!((i, *s), (i, BlockState::InUse));
    //         //     });
    //     })
    // }
}

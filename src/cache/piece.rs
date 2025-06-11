use futures::future::{self, AbortHandle};
use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    mem::ManuallyDrop,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    slice,
    sync::Weak,
    task::{Poll, Waker},
    vec,
};
use tokio::sync::Notify;

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

struct BufState<T> {
    allow_new_ref: AtomicBool, // TODO: maybe out of Mutex

    cache: T,
    state: State,
    abort_handle: HashMap<RefOffset, AbortHandle>,

    abort_waiter: Vec<Arc<Notify>>,
}

struct Cache<T> {
    // TODO: since all jobs access to cache,
    // maybe split lock to smaller granularity to reduce contention
    buf: Mutex<BufState<T>>,
}

fn change_blockstate(v: &mut [BlockState], offset: usize, len: usize, state: BlockState) {
    for s in v[offset >> BLOCKBITS..(offset >> BLOCKBITS) + (len >> BLOCKBITS)].iter_mut() {
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
        self.buf
            .lock()
            .expect("allow new ref should lock OK")
            .allow_new_ref
            .store(allow_new_ref, Ordering::Release);
    }

    fn add_abortable_work(&self, offset: RefOffset, handle: AbortHandle) -> bool {
        let mut buf_guard = self.buf.lock().expect("add abortable work lock should OK");
        if !buf_guard.allow_new_ref.load(Ordering::Relaxed) {
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
            buf_guard.allow_new_ref.store(false, Ordering::Relaxed);
            for (_, handle) in buf_guard.abort_handle.drain() {
                handle.abort();
            }
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
    fn new_abort(mut t: PieceBuf) -> (Self, SubAbortHandle<PieceBuf>) {
        let size = t.as_mut().len();
        if size.trailing_zeros() < BLOCKSIZE.trailing_zeros() {
            // TODO: maybe not panic, fix size instead?
            panic!("cache size must be multiple of {BLOCKSIZE}")
        }
        assert_eq!(size % BLOCKSIZE, 0);
        let inner = Arc::new(Cache {
            buf: Mutex::new(BufState {
                allow_new_ref: AtomicBool::new(true),
                cache: t,
                state: State {
                    state: vec![BlockState::Vacant; size >> BLOCKBITS],
                    ref_cnt: 0,
                },
                abort_handle: HashMap::new(),
                abort_waiter: Vec::new(),
            }),
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
    pub(crate) fn piece_detail<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&PieceBuf) -> T,
    {
        f(&self
            .inner
            .buf
            .lock()
            .expect("piece detail lock should OK")
            .cache)
    }
}

impl<T> ArcCache<T>
where
    T: AsMut<[u8]>,
{
    // TODO: is this safe? require T: AsMut and taking ownership of T
    // size must be multiple of 16384
    pub fn new(mut t: T) -> Self {
        let size = t.as_mut().len();
        if size.trailing_zeros() < BLOCKSIZE.trailing_zeros() {
            // TODO: maybe not panic, fix size instead?
            panic!("cache size must be multiple of {BLOCKSIZE}")
        }
        assert_eq!(size % BLOCKSIZE, 0);
        Self {
            inner: Arc::new(Cache {
                buf: Mutex::new(BufState {
                    allow_new_ref: AtomicBool::new(true),
                    cache: t,
                    state: State {
                        state: vec![BlockState::Vacant; size >> BLOCKBITS],
                        ref_cnt: 0,
                    },
                    abort_handle: HashMap::new(),
                    abort_waiter: Vec::new(),
                }),
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
    pub fn get_part_ref(&self, offset: usize, len: usize) -> Option<Ref<T>> {
        if len.trailing_zeros() < BLOCKSIZE.trailing_zeros() {
            // TODO: maybe not panic, fix size instead?
            panic!("should be multiple of {BLOCKSIZE}");
        }

        let mut buf_guard = self.inner.buf.lock().expect("get part ref lock should OK");
        if !buf_guard.allow_new_ref.load(Ordering::Acquire) {
            return None;
        }
        {
            if len + offset > buf_guard.cache.as_mut().len() {
                // TODO: return error code
                return None;
            }

            let range_state = &mut buf_guard.state;
            range_state.ref_cnt += 1;

            let any_block_not_vacant = range_state.state
                [offset >> BLOCKBITS..(offset + len) >> BLOCKBITS]
                .iter()
                .any(|s| *s != BlockState::Vacant);
            if any_block_not_vacant {
                return None;
            }

            for s in range_state.state[offset >> BLOCKBITS..(offset + len) >> BLOCKBITS].iter_mut()
            {
                if *s != BlockState::Vacant {
                    return None;
                }
                *s = BlockState::InUse;
            }
        }

        Some(Ref {
            offset: RefOffset {
                from: offset,
                ptr: unsafe { buf_guard.cache.as_mut().as_mut_ptr().add(offset) },
                len,
            },
            main_cache: ManuallyDrop::new(self.inner.clone()),
        })
    }

    // TODO: maybe not use (offset,length) but use
    // (offset_mutlple_of(block), len_multiple_of(block))
    // TODO: this needs a lot of tests
    pub async fn async_get_part_ref(&self, offset: usize, len: usize) -> Option<Ref<T>> {
        if len.trailing_zeros() < BLOCKSIZE.trailing_zeros() {
            // TODO: maybe not panic, fix size instead?
            panic!("should be multiple of {BLOCKSIZE}");
        }

        let mut buf_guard = self.inner.buf.lock().expect("get part ref lock should OK");
        if !buf_guard.allow_new_ref.load(Ordering::Acquire) {
            return None;
        }
        {
            if len + offset > buf_guard.cache.as_mut().len() {
                // TODO: return error code
                return None;
            }

            let range_state = &mut buf_guard.state;
            range_state.ref_cnt += 1;

            let any_block_not_vacant = range_state.state
                [offset >> BLOCKBITS..(offset + len) >> BLOCKBITS]
                .iter()
                .any(|s| *s != BlockState::Vacant);
            if any_block_not_vacant {
                return None;
            }

            for s in range_state.state[offset >> BLOCKBITS..(offset + len) >> BLOCKBITS].iter_mut()
            {
                if *s != BlockState::Vacant {
                    return None;
                }
                *s = BlockState::InUse;
            }
        }

        Some(Ref {
            offset: RefOffset {
                from: offset,
                ptr: unsafe { buf_guard.cache.as_mut().as_mut_ptr().add(offset) },
                len,
            },
            main_cache: ManuallyDrop::new(self.inner.clone()),
        })
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
    pub async fn abort_all(&self) {
        if let Some(a) = self.cache.upgrade() {
            a.abort_all().await
        }
    }
}

pub struct Ref<T>
where
    T: AsMut<[u8]>,
{
    offset: RefOffset,
    // manually_drop is used in extend_to_entire()
    // extend_or_entire acts as a "Drop"
    main_cache: ManuallyDrop<Arc<Cache<T>>>,
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

    // SAFETY: fut ?must be CANCEL SAFE
    // Cancel unsafe will abort, but leaving a half-done state
    pub async fn abortable_work<'a, F, U>(
        &'a mut self,
        work: F,
    ) -> Result<<U as IntoFuture>::Output, future::Aborted>
    where
        F: FnOnce(&'a mut [u8]) -> U,
        U: IntoFuture,
    {
        let offset = self.offset.clone();
        let (abort_fut, handle) =
            future::abortable(work(unsafe { self.offset.as_mut() }).into_future());
        // TODO: FIXME: when abort, no new ref should be created
        if self.main_cache.add_abortable_work(offset.clone(), handle) {
            let res = abort_fut.await;
            self.main_cache.rm_abortable_work(&offset);
            res
        } else {
            Err(future::Aborted)
        }

        // TODO: will some new abortable work be created before rm_abortable_work?
        // we are holding &mut Ref so no new work?
    }

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
                    ptr: buf_guard.cache.as_mut().as_mut_ptr(),
                    len: buf_guard.cache.as_mut().len(),
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
            assert!(ref5to7.is_some());
            assert!(ref6to8.is_none());
            assert!(ref9to11.is_some());

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

            for (i, s) in guard.cache.iter().enumerate() {
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

            assert_eq!(entire.offset.ptr, guard.cache.as_mut_ptr());
            assert_eq!(entire.offset.from, 0);
            assert_eq!(entire.offset.len, guard.cache.len());
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
            assert!(ref3to5.is_none());

            // one ref still holds, no new ref
            drop(ref8to9);
            let ref3to5 = cache.get_part_ref(3 * BLOCKSIZE, 2 * BLOCKSIZE);
            assert!(ref3to5.is_none());

            drop(ref5to7);
            // all ref reclaimed, new ref allowed
            let ref3to5 = cache.get_part_ref(3 * BLOCKSIZE, 2 * BLOCKSIZE);
            assert!(ref3to5.is_some());
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

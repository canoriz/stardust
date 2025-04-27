use std::{any, marker::PhantomData, mem::MaybeUninit, net::SocketAddr, slice, vec};

#[cfg(mloom)]
use loom::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc, Mutex,
};

#[cfg(not(mloom))]
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc, Mutex,
};

type Peer = SocketAddr;
const BLOCKBITS: usize = 14;
const BLOCKSIZE: usize = 1 << BLOCKBITS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockState {
    Vacant,
    Alloced,
    InUse,
}

struct Cache {
    allow_new_ref: AtomicBool,
    ref_count: AtomicU32,
    cache: Vec<u8>,

    // TODO: since all jobs access to cache,
    // maybe split lock to smaller granularity to reduce contention
    state: Mutex<Vec<BlockState>>,
    // _t: PhantomData<T>,
}

impl Cache {
    fn change_state(&self, offset: usize, len: usize, state: BlockState) {
        for s in self.state.lock().expect("should ok")
            [offset >> BLOCKBITS..(offset >> BLOCKBITS) + (len >> BLOCKBITS)]
            .iter_mut()
        {
            *s = state;
        }
    }

    fn release_part_ref(&self, offset: usize, len: usize) {
        self.change_state(offset, len, BlockState::Vacant);
        self.ref_count.fetch_sub(1, Ordering::Release);
        println!("release part ref");
    }

    fn allow_new_ref(&self, allow_new_ref: bool) {
        self.allow_new_ref.store(allow_new_ref, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct ArcCache {
    inner: Arc<Cache>,
}

impl ArcCache {
    // size must be multiple of 16384
    pub fn new(size: usize) -> Self {
        if size.trailing_zeros() < BLOCKSIZE.trailing_zeros() {
            // TODO: maybe not panic, fix size instead?
            panic!("cache size must be multiple of {BLOCKSIZE}")
        }
        let size = size.next_multiple_of(BLOCKSIZE as usize);
        Self {
            inner: Arc::new(Cache {
                allow_new_ref: AtomicBool::new(true),
                ref_count: AtomicU32::new(0),
                cache: vec![0u8; size],
                state: Mutex::new(vec![BlockState::Vacant; size >> BLOCKBITS]),
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
    pub fn get_ref(&self, peer: Peer, piece_index: usize, block_offset: usize) -> Ref {
        todo!()
    }

    pub fn disable_new_ref(&self) {
        self.inner.allow_new_ref(false);
    }

    pub fn enable_new_ref(&self) {
        self.inner.allow_new_ref(true);
    }

    // TODO: maybe not use (offset,length) but use
    // (offset_mutlple_of(block), len_multiple_of(block))
    // TODO: this needs a lot of tests
    pub fn get_part_ref(&self, offset: usize, len: usize) -> Option<Ref> {
        if len.trailing_zeros() < BLOCKSIZE.trailing_zeros() {
            // TODO: maybe not panic, fix size instead?
            panic!("should be multiple of {BLOCKSIZE}");
        }

        if !self.inner.allow_new_ref.load(Ordering::Acquire) {
            return None;
        }

        let mut range_state = self.inner.state.lock().expect("should no error");
        if !self.inner.allow_new_ref.load(Ordering::Relaxed) {
            return None;
        }
        self.inner.ref_count.fetch_add(1, Ordering::Relaxed); // under mutex, can relax

        let any_block_not_vacant = range_state[offset >> BLOCKBITS..(offset + len) >> BLOCKBITS]
            .iter()
            .any(|s| *s != BlockState::Vacant);
        if any_block_not_vacant {
            return None;
        }

        for s in range_state[offset >> BLOCKBITS..(offset + len) >> BLOCKBITS].iter_mut() {
            if *s != BlockState::Vacant {
                return None;
            }
            *s = BlockState::InUse;
        }

        Some(Ref {
            from: offset,
            ptr: unsafe { self.inner.cache.as_ptr().add(offset) },
            len,
            main_cache: self.inner.clone(),
        })
    }

    fn release_part_ref(&self, offset: usize, len: usize) {
        self.inner.release_part_ref(offset, len);
    }

    fn change_state(&self, offset: usize, len: usize, state: BlockState) {
        self.inner.change_state(offset, len, state);
    }
}

// TODO: maybe implement Deref AsRef or something
// but it contains a ref to the main cache
pub struct Ref {
    from: usize,
    ptr: *const u8,

    len: usize,
    main_cache: Arc<Cache>,
}

unsafe impl Send for Ref {}

impl Ref {
    // TODO: is 'static safe? we are holding ref of arc, so
    // we can hold it as long as we want, so it's safe?
    pub fn to_slice(&mut self) -> &'static mut [u8] {
        // TODO: is this safe? is ptr valid (will inner address in arc change?)
        unsafe { slice::from_raw_parts_mut(self.ptr as *mut _, self.len) }
    }

    pub fn to_slice_len(&mut self, len: usize) -> &'static mut [u8] {
        assert!(len <= self.len);
        // TODO: is this safe? is ptr valid (will inner address in arc change?)
        unsafe { slice::from_raw_parts_mut(self.ptr as *mut _, len) }
    }

    // extend block ref to full ref
    // TODO: do we have a better way?
    // TODO: this needs many tests
    pub fn extend_to_entire(mut self) -> Option<Self> {
        println!("enter");
        let ref_cnt = self.main_cache.ref_count.compare_and_swap(1, Ordering::Acquire);
        println!("ref_cnt sub 1 before {ref_cnt}");
        if ref_cnt != 1 {
            println!("fail2 extend {ref_cnt}");
            return None;
        }

        // we are the only one reference
        // if disable_new_ref() called, no more ref created
        // this ref_count only decreases
        // since we are the only one, no one else can decrease this
        // TODO: what if enable_new_ref() called now?
        let success = {
            let mut state = self.main_cache.state.lock().unwrap();
            self.main_cache.ref_count.fetch_add(1, Ordering::Relaxed);
            println!("add 1");
            // if self.main_cache.ref_count.fetch_sub(1, Ordering::Relaxed) == 2 {
            for s in state.iter_mut() {
                *s = BlockState::InUse;
            }
            self.from = 0;
            self.ptr = self.main_cache.cache.as_ptr();
            self.len = self.main_cache.cache.len();
            println!("success extend");
            true
            // } else {
            //     println!("fail1 extend");
            //     false
            // }
        };
        if success {
            Some(self)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Ref {
    fn drop(&mut self) {
        self.main_cache.release_part_ref(self.from, self.len);
        println!("drop");
    }
}

trait BackFile {}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(mloom)]
    use loom::thread;

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
    fn test_cache_disjoint_ref() {
        loom_test(|| {
            let mut cache = ArcCache::new(BLOCKSIZE * 16);
            let ref5to7 = cache.get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE);
            let ref6to8 = cache.get_part_ref(6 * BLOCKSIZE, 2 * BLOCKSIZE);
            assert!(ref5to7.is_some());
            assert!(ref6to8.is_none());
            cache
                .inner
                .state
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .for_each(|(i, s)| {
                    if i >= 5 && i < 7 {
                        assert_eq!((i, *s), (i, BlockState::InUse))
                    } else {
                        assert_ne!((i, *s), (i, BlockState::InUse))
                    }
                });
        })
    }

    #[test]
    fn test_concurrent_read_write() {
        loom_test(|| {
            let a = ArcCache::new(30 * BLOCKSIZE);

            let a1 = a.clone();
            let h1 = thread::spawn(move || {
                let mut s_ref = a1.get_part_ref(0, 2 * BLOCKSIZE).expect("ok");
                let s = s_ref.to_slice();
                for b in s.iter_mut() {
                    *b = 1u8;
                }
            });

            let a2 = a.clone();
            let h2 = thread::spawn(move || {
                let mut s_ref = a2.get_part_ref(4 * BLOCKSIZE, 2 * BLOCKSIZE).expect("ok");
                let s = s_ref.to_slice();
                for b in s.iter_mut() {
                    *b = 3u8;
                }
            });
            let _ = h1.join();
            let _ = h2.join();

            a.inner
                .state
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .for_each(|(i, s)| {
                    assert_eq!((i, *s), (i, BlockState::Vacant));
                });

            for (i, s) in a.inner.cache.iter().enumerate() {
                if (4 * BLOCKSIZE..6 * BLOCKSIZE).contains(&i) {
                    assert_eq!((i, *s), (i, 3u8))
                } else if (0..2 * BLOCKSIZE).contains(&i) {
                    assert_eq!((i, *s), (i, 1u8))
                }
            }
        });
    }

    #[test]
    fn test_disable_new_ref() {
        loom_test(|| {
            let cache = ArcCache::new(BLOCKSIZE * 16);
            let cache_clone = cache.clone();
            let cache_clone2 = cache.clone();
            let ref5to7 = cache.get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
            let ref8to9 = cache.get_part_ref(8 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
            cache
                .inner
                .state
                .lock()
                .unwrap()
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
            cache
                .inner
                .state
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .for_each(|(i, s)| {
                    assert_eq!((i, *s), (i, BlockState::InUse));
                });

            match ex1 {
                Some(a) => {
                    assert_eq!(a.ptr, cache.inner.cache.as_ptr());
                    assert_eq!(a.from, 0);
                    assert_eq!(a.len, cache.inner.cache.len());
                }
                _ => {
                    let a = ex2.unwrap();
                    assert_eq!(a.ptr, cache.inner.cache.as_ptr());
                    assert_eq!(a.from, 0);
                    assert_eq!(a.len, cache.inner.cache.len());
                }
            }
        })
    }

    #[test]
    fn test_ref_count() {
        loom_test(|| {
            let cache = ArcCache::new(BLOCKSIZE * 16);
            let cache_clone = cache.clone();
            let cache_clone2 = cache.clone();
            let mut ref5to7 = cache.get_part_ref(5 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
            assert_eq!(cache.inner.ref_count.load(Ordering::Acquire), 1);
            let mut ref8to9 = cache.get_part_ref(8 * BLOCKSIZE, 2 * BLOCKSIZE).unwrap();
            assert_eq!(cache.inner.ref_count.load(Ordering::Acquire), 2);
            drop(ref5to7);
            assert_eq!(cache.inner.ref_count.load(Ordering::Acquire), 1);
            drop(ref8to9);
            assert_eq!(cache.inner.ref_count.load(Ordering::Acquire), 0);

            // let h1 = thread::spawn(move || {
            //     cache_clone.disable_new_ref();
            //     ref5to7.extend_to_entire()
            // });

            // let h2 = thread::spawn(move || {
            //     cache_clone2.disable_new_ref();
            //     ref8to9.extend_to_entire()
            // });

            // let success1) = h1.join().unwrap();
            // let (r89, success2) = h2.join().unwrap();
            // dbg!(success1);
            // dbg!(success2);
            // assert!(success1 ^ success2);
            // if success1 {
            //     assert_eq!(r57.ptr, cache.inner.cache.as_ptr());
            //     assert_eq!(r57.from, 0);
            //     assert_eq!(r57.len, cache.inner.cache.len());
            //     assert_eq!(r89.len, 0);
            // } else {
            //     assert_eq!(r89.ptr, cache.inner.cache.as_ptr());
            //     assert_eq!(r89.from, 0);
            //     assert_eq!(r89.len, cache.inner.cache.len());
            //     assert_eq!(r57.len, 0);
            // }

            // cache
            //     .inner
            //     .state
            //     .lock()
            //     .unwrap()
            //     .iter()
            //     .enumerate()
            //     .for_each(|(i, s)| {
            //         assert_eq!((i, *s), (i, BlockState::InUse));
            //     });
        })
    }
}

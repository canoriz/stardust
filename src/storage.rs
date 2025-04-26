use std::{any, marker::PhantomData, mem::MaybeUninit, net::SocketAddr, slice, vec};

#[cfg(mloom)]
use loom::{
    sync::{Arc, Mutex},
    thread,
};

#[cfg(not(mloom))]
use std::sync::{Arc, Mutex};

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

    // TODO: maybe not use (offset,length) but use
    // (offset_mutlple_of(block), len_multiple_of(block))
    pub fn get_part_ref(&self, offset: usize, len: usize) -> Option<Ref> {
        if len.trailing_zeros() < BLOCKSIZE.trailing_zeros() {
            // TODO: maybe not panic, fix size instead?
            panic!("should be multiple of {BLOCKSIZE}");
        }
        let any_block_not_vacant = self.inner.state.lock().expect("should no error")
            [offset >> BLOCKBITS..(offset + len) >> BLOCKBITS]
            .iter()
            .any(|s| *s != BlockState::Vacant);
        if any_block_not_vacant {
            return None;
        }

        for s in self.inner.state.lock().expect("should no error")
            [offset >> BLOCKBITS..(offset + len) >> BLOCKBITS]
            .iter_mut()
        {
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

    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Ref {
    fn drop(&mut self) {
        self.main_cache.release_part_ref(self.from, self.len);
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

    #[test]
    fn test_cache_disjoint_ref() {
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
    }

    #[test]
    #[cfg(mloom)]
    fn test_concurrent_read_write() {
        loom::model(
            || {
                // let v1 = Arc::new(AtomicUsize::new(0));
                // let v2 = v1.clone();
                test_concurrent_read_write_inner();
            }, // assert_eq!(0, v2.load(SeqCst));
        );
    }

    #[test]
    #[cfg(not(mloom))]
    fn test_concurrent_read_write() {
        test_concurrent_read_write_inner();
    }

    fn test_concurrent_read_write_inner() {
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
    }
}

use core::slice;
use std::collections::{BTreeSet, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::task::{Poll, Waker};

#[cfg(mloom)]
use loom::sync::{Arc, Mutex};
use tracing::{debug, warn};

#[cfg(not(mloom))]
use std::sync::{Arc, Mutex};

const MIN_ALLOC_SIZE: usize = 256 * 1024;
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

struct PieceBufPoolImpl {
    buf: Vec<u8>,
    block_tree: Mutex<BlockTree>,
    waiting_alloc: Mutex<VecDeque<AllocReq>>,
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

// FreeBlock is NOT Clone. It's fundamentally a owned reference to
// free buffer
#[derive(Debug)]
struct FreeBlock {
    // TODO: offset and ptr is same thing? only store one?
    offset: usize,
    power: u32,

    ptr: *const u8,
}

impl PieceBufPool {
    pub fn new(size: usize) -> Self {
        Self {
            inner: Arc::new(PieceBufPoolImpl::new(size)),
        }
    }

    /// alloc PieceBuf
    pub fn alloc(&self, size: usize) -> Option<PieceBuf> {
        self.inner.alloc(size).map(|fb| PieceBuf {
            b: fb,
            len: size,
            main_cache: self.inner.clone(),
        })
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
    pub fn async_alloc(&self, id: usize, size: usize) -> AllocPieceBufFut {
        AllocPieceBufFut {
            id,
            size,
            pool: self,
            waiting: false,
        }
    }
}

impl PieceBufPoolImpl {
    fn new(size: usize) -> Self {
        let size_fixed = size.next_multiple_of(MIN_ALLOC_SIZE);

        Self {
            buf: vec![0u8; size_fixed],
            block_tree: Mutex::new(init_block_tree(size_fixed)),
            waiting_alloc: Mutex::new(VecDeque::new()),
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

        let mut blk_tree = self.block_tree.lock().expect("alloc lock should OK");
        blk_tree.split_down(power).map(|(offset, power)| FreeBlock {
            offset,
            power,
            ptr: unsafe { self.buf.as_ptr().add(offset) },
        })
    }

    /// returns PieceBuf
    fn free(&self, b: &FreeBlock) {
        {
            let mut blk_tree = self.block_tree.lock().expect("alloc lock should OK");
            blk_tree.merge_up(b.offset, b.power);
            // blk_tree's lock dropped here, prevents dead_lock
        }

        let mut waiting_alloc = self
            .waiting_alloc
            .lock()
            .expect("waiting alloc lock should OK");
        if let Some(v) = waiting_alloc.pop_front() {
            // TODO: maybe add an atomic, only wake one pending alloc request
            v.waker.wake();
        }
    }
}

impl Drop for PieceBuf {
    fn drop(&mut self) {
        debug!("free PieceBuf {:?}", self);
        self.main_cache.free(&self.b);
    }
}

impl PieceBuf {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.b.ptr as *mut _, self.len) }
    }
}

impl AsRef<[u8]> for PieceBuf {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.b.ptr, self.len) }
    }
}

struct AllocReq {
    id: usize,
    waker: Waker,
}

pub(crate) struct AllocPieceBufFut<'a> {
    id: usize,
    pool: &'a PieceBufPool,
    waiting: bool,
    size: usize,
}

impl Future for AllocPieceBufFut<'_> {
    type Output = PieceBuf;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let fut = self.get_mut();

        let mut waiting_alloc = fut.pool.inner.waiting_alloc.lock().unwrap();
        if !fut.waiting {
            if !waiting_alloc.is_empty() {
                waiting_alloc.push_back(AllocReq {
                    id: fut.id,
                    waker: cx.waker().clone(),
                });
                fut.waiting = true;
                return Poll::Pending;
            }
            if let Some(bf) = fut.pool.alloc(fut.size) {
                // this holds both lock of block_tree and waiting_alloc
                Poll::Ready(bf)
            } else {
                waiting_alloc.push_back(AllocReq {
                    id: fut.id,
                    waker: cx.waker().clone(),
                });
                fut.waiting = true;
                Poll::Pending
            }
        } else if let Some(bf) = fut.pool.alloc(fut.size) {
            if let Some(v) = waiting_alloc.pop_front() {
                // TODO: maybe add an atomic, only wake one pending alloc request
                v.waker.wake();
            }
            Poll::Ready(bf)
        } else {
            waiting_alloc.push_front(AllocReq {
                id: fut.id,
                waker: cx.waker().clone(),
            }); // try to be the first
            Poll::Pending
        }
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
        let (mut now_offset, mut now_order) =
            if let Some(v) = pop_first_vacant(&mut self.tree, target_power) {
                v
            } else {
                return None;
            };

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
    use super::*;

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
            assert_eq!(b1.as_mut_slice().len(), sz1);
            for b in b1.as_mut_slice().iter_mut() {
                *b = 0xff;
            }
            for b in b2.as_mut_slice().iter_mut() {
                *b = 0xcc;
            }
            for b in c.inner.buf.iter().skip(b1.b.offset).take(sz1) {
                assert_eq!(*b, 0xff);
            }
            for b in c.inner.buf.iter().skip(b2.b.offset).take(sz2) {
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
                for b in pb.as_mut_slice().iter_mut() {
                    *b = 0xff;
                }
                pb
            });

            let c2 = c.clone();
            let h2 = thread::spawn(move || {
                let mut pb = c2.alloc(sz).unwrap();
                for b in pb.as_mut_slice().iter_mut() {
                    *b = 0xcc;
                }
                pb
            });
            let pb1 = h1.join().unwrap();
            let pb2 = h2.join().unwrap();

            for b in c.inner.buf.iter().skip(pb1.b.offset).take(sz) {
                assert_eq!(*b, 0xff);
            }
            for b in c.inner.buf.iter().skip(pb2.b.offset).take(sz) {
                assert_eq!(*b, 0xcc);
            }
        });
    }

    #[tokio::test]
    async fn test_async_piece_buf() {
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
}

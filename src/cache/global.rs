use std::collections::BTreeSet;

const MIN_ALLOC_SIZE: usize = 256 * 1024;
const MIN_ALLOC_SIZE_POWER: u32 = MIN_ALLOC_SIZE.ilog2();

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

/// session cache
/// targets:
/// 1. allocate piece buffer for different sized piece (all power of 2)
/// 2. WRITE BACK and LOAD from file storage at proper time
/// 3. write back part of the piece if piece is big and cache is small
/// 4. calculate memory fragmentation ratio
/// 5. defragmentation when memory is fragmentated
/// 6. expand/shrink size of cache
pub(crate) struct Cache {
    buf: Vec<u8>,
    blk: Vec<BlockList>,
}

pub(crate) struct PieceBuf {}

impl Cache {
    fn new(size: usize) -> Self {
        let size_fixed = size.next_multiple_of(MIN_ALLOC_SIZE);

        Self {
            buf: vec![0u8; size_fixed],
            blk: init_block_list(size_fixed),
        }
    }

    fn alloc(&mut self, size: usize) -> Option<PieceBuf> {
        let size_fixed = size.next_multiple_of(MIN_ALLOC_SIZE);
        let power = size_fixed.ilog2();

        let vacant_blk_id = (|| {
            for (p, b) in self
                .blk
                .iter()
                .enumerate()
                .skip((power - MIN_ALLOC_SIZE_POWER) as usize)
            {
                if b.blk.last().is_some() {
                    return Some(p);
                }
            }
            None
        })();

        if let Some(i) = vacant_blk_id {
            split_down(&mut self.blk, i, size_fixed);
            Some(PieceBuf {})
        } else {
            None
        }
    }

    fn return_piece(&mut self, p: PieceBuf) {
        todo!()
    }
}

#[derive(Debug, Eq, PartialEq)]
struct BlockList {
    size: usize,
    blk: BTreeSet<usize>,
}

fn init_block_list(total_size: usize) -> Vec<BlockList> {
    let max_block_size = prev_power_of_two(total_size);
    let max_power = max_block_size.ilog2();
    let mut v: Vec<BlockList> = (MIN_ALLOC_SIZE_POWER..=max_power)
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
            v[(power - MIN_ALLOC_SIZE_POWER) as usize]
                .blk
                .insert(offset);
            offset += block_size;
        }
        power -= 1;
        block_size >>= 1;
    }
    v
}

fn split_down(
    list: &mut Vec<BlockList>,
    since: usize,
    target_block_size: usize,
) -> Option<PieceBuf> {
    let now_size = 1usize << ((since as u32) + MIN_ALLOC_SIZE_POWER);
    let cur_offset = if let Some(b) = list[since].blk.pop_last() {
        b
    } else {
        return None;
    };
    let mut cur_blk = (cur_offset, now_size);

    while cur_blk.1 >= MIN_ALLOC_SIZE {
        let now_size = cur_blk.1;

        if now_size == target_block_size {
            return Some(PieceBuf {});
        } else {
            // split this block then push this block to list of smaller blocks
            let left_blk = cur_blk.0;
            cur_blk = (cur_blk.0 + now_size, now_size >> 1);
            list[since - 1].blk.insert(left_blk);
        }
    }
    unreachable!("should always find a hole");
}

// block_size must be power of 2
// offset must be multiple of block_size
#[inline]
fn buddy_blk_of(offset: usize, block_size: usize) -> usize {
    debug_assert!(block_size.is_power_of_two());
    debug_assert!(offset % block_size == 0);
    offset ^ block_size
}

fn merge_up(list: &mut Vec<BlockList>, offset: usize, block_size_power: u32) {
    let mut now_size = 1usize << block_size_power;
    let mut now_idx = (block_size_power - MIN_ALLOC_SIZE_POWER) as usize;
    let mut now_offset = offset;

    while now_idx < list.len() {
        let buddy_blk = buddy_blk_of(now_offset, now_size);
        if list[now_idx as usize].blk.remove(&buddy_blk) {
            now_size <<= 1;
            now_idx += 1;
            now_offset = now_offset.min(buddy_blk);
        } else {
            list[now_idx as usize].blk.insert(now_offset);
            return;
        };
    }
    unreachable!("should always find a hole");
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
        let r = init_block_list(11 * MIN_ALLOC_SIZE);
        assert_eq!(
            r,
            vec![
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
        )
    }

    #[test]
    fn test_split() {
        let mut v = vec![
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
        ];

        let r = split_down(&mut v, 2, 2 * MIN_ALLOC_SIZE);
        assert_eq!(
            v,
            vec![
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
        )
    }

    #[test]
    fn test_merge() {
        let mut v = vec![
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
        ];

        merge_up(&mut v, 6 * MIN_ALLOC_SIZE, MIN_ALLOC_SIZE_POWER + 1);
        assert_eq!(
            v,
            vec![
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
            ],
        );
    }

    #[test]
    fn test_merge2() {
        let mut v = vec![
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
        ];

        merge_up(&mut v, 5 * MIN_ALLOC_SIZE, MIN_ALLOC_SIZE_POWER);
        assert_eq!(
            v,
            vec![
                BlockList {
                    size: MIN_ALLOC_SIZE,
                    blk: BTreeSet::from([10].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 1,
                    blk: BTreeSet::from([1, 3, 8].map(|i| i * MIN_ALLOC_SIZE)),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 2,
                    blk: BTreeSet::from([]),
                },
                BlockList {
                    size: MIN_ALLOC_SIZE << 3,
                    blk: BTreeSet::from([0]),
                }
            ],
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
}

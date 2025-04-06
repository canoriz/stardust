mod heap;
use crate::protocol;
pub use crate::protocol::BitField;
use heap::Heap;
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::{info, warn};

const BLOCK_SIZE: u32 = 16384;

// TODO: maybe use peer_id instead of socketaddr?
pub trait Picker {
    fn peer_add(&mut self, peer: SocketAddr, b: BitField);
    fn peer_remove(&mut self, peer: &SocketAddr);
    fn pick_blocks(&mut self, peer: &SocketAddr, n_blocks: usize) -> BlockRequests;
    fn peer_have(&mut self, peer: &SocketAddr, piece: u32);
}

// many consecutive block ranges
#[derive(Debug)]
pub(crate) struct BlockRequests {
    pub piece_size: u32,
    pub range: Vec<BlockRange>,
}

// TODO: maybe change protocol::Request to use block-index
// question: how to represent a part 16kib request?
#[derive(Debug, Clone)]
pub struct BlockRange {
    // from and to are inclusive
    // TODO: maybe use block index? this [begin, len) pattern is strange
    from: protocol::Request,
    to: protocol::Request,
}

// TODO: can piece request cross PIECE boundry?
impl BlockRange {
    pub fn iter(&self, piece_size: u32) -> BlockRangeIter {
        let br = self.clone();
        BlockRangeIter {
            current_in_piece_offset: br.from.begin,
            current_piece: br.from.index,

            piece_size,
            br,
        }
    }
}

pub struct BlockRangeIter {
    current_piece: u32,
    current_in_piece_offset: u32,
    piece_size: u32,

    br: BlockRange,
}
// struct BlockRequestIter<T>
// where
//     T: Iterator,
// {
//     iter: T,
// }

// impl<T> BlockRequestIter<T>
// where
//     T: Iterator,
// {
//     fn new(iter: T) -> Self {
//         Self { iter }
//     }
// }

// impl BlockRequests {
//     fn iter<T>(&self) -> BlockRequestIter<T>
//     where
//         T: Iterator,
//         <T as Iterator>::Item: Iterator<Item = protocol::Request>,
//     {
//         let iter = self.range.iter().flat_map(|br| br.iter());
//         BlockRequestIter::new(iter)
//     }
// }
impl BlockRangeIter {
    fn one_request(&mut self, last_offset: u32) -> Option<<Self as Iterator>::Item> {
        if self.current_in_piece_offset < last_offset {
            if self.current_in_piece_offset + 16384 > last_offset {
                let step = last_offset - self.current_in_piece_offset;
                let res = protocol::Request {
                    index: self.current_piece,
                    begin: self.current_in_piece_offset,
                    len: step,
                };
                self.current_in_piece_offset += step;
                Some(res)
            } else {
                let res = protocol::Request {
                    index: self.current_piece,
                    begin: self.current_in_piece_offset,
                    len: 16384,
                };
                self.current_in_piece_offset += 16384;
                Some(res)
            }
        } else {
            None
        }
    }
}

impl Iterator for BlockRangeIter {
    type Item = protocol::Request;

    fn next(&mut self) -> Option<Self::Item> {
        let mut last_offset = self.piece_size;
        if self.current_piece == self.br.to.index {
            // this is the last piece
            last_offset = self.br.to.begin + self.br.to.len;
            return self.one_request(last_offset);
        }

        let piece_size = self.piece_size;
        if self.current_in_piece_offset >= piece_size {
            self.current_piece += 1;
            self.current_in_piece_offset = 0;
        }
        self.one_request(piece_size)
    }
}

#[derive(Debug)]
struct PartialRequestedPiece {
    // TODO: maybe use a more space/time efficient data structure?
    block_map: Vec<bool>,
}

// TODO: change this to PiecePicker<Heap>
// make piece picker a generic
// i.e. this is actually a block picker
pub struct HeapPiecePicker {
    heap: Heap<i32>, // let's say availability is type i32
    piece_size: u32,
    piece_total: u32,

    peer_field_map: HashMap<SocketAddr, BitField>,

    // index -> Piece
    partly_requested_pieces: HashMap<u32, PartialRequestedPiece>,
    // TODO: maybe need a Vec<UnackedPieces>
}

impl HeapPiecePicker {
    pub fn new(piece_total: u32, piece_size: u32) -> Self {
        Self {
            heap: Heap::new(piece_total as usize),
            piece_size,
            piece_total,
            peer_field_map: HashMap::new(),
            partly_requested_pieces: HashMap::new(),
        }
    }
}

// TODO: tests
// returns blockrequests and count of chosen blocks
fn choose_blocks_from_partly_requested_piece(
    prp: &mut PartialRequestedPiece,
    n_blocks: usize,
    piece_size: u32,
    piece_index: u32,
) -> (BlockRange, usize) {
    let mut end = piece_size >> 14;
    let mut begin = end;

    let mut in_middle = false;
    for (idx, b) in prp.block_map.iter_mut().enumerate() {
        if !in_middle && !*b {
            begin = idx as u32;
            in_middle = true;
        }
        // TODO: is this n_blocks condition correct?
        if in_middle && (*b || begin as usize + n_blocks <= idx) {
            end = idx as u32;
            break;
        } else {
            *b = true;
        }
    }

    if begin < end {
        dbg!(begin, end, begin + (n_blocks as u32) >= end);
        (
            BlockRange {
                // TODO: FIXME: last block length is not BLOCK_SIZE
                from: protocol::Request {
                    index: piece_index,
                    begin: begin * BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
                to: protocol::Request {
                    index: piece_index,
                    begin: (end - 1) * BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
            },
            (end - begin) as usize,
        )
    } else {
        const DUMMY: protocol::Request = protocol::Request {
            index: 0,
            begin: 0,
            len: 0,
        };
        (
            // TODO: maybe use some clever expression
            BlockRange {
                from: DUMMY,
                to: DUMMY,
            },
            0,
        )
    }
}

impl Picker for HeapPiecePicker {
    fn peer_add(&mut self, peer: SocketAddr, b: BitField) {
        let field_map = b
            .iter()
            .take(self.piece_total as usize)
            .enumerate()
            .filter(|(_, v)| *v)
            .map(|(i, _)| i);
        self.heap.increment_bulk(field_map, 1);
        self.peer_field_map.insert(peer, b);
    }

    fn peer_remove(&mut self, peer: &SocketAddr) {
        self.peer_field_map.remove(peer);
    }

    fn pick_blocks(&mut self, peer: &SocketAddr, n_blocks: usize) -> BlockRequests {
        let mut n_blocks = n_blocks;
        let mut res = Vec::new();
        if let Some(peer_field) = self.peer_field_map.get(peer) {
            'outer: for (index, p) in self.partly_requested_pieces.iter_mut() {
                if peer_field.get(*index) {
                    loop {
                        let (chosen, n) = choose_blocks_from_partly_requested_piece(
                            p,
                            n_blocks,
                            self.piece_size,
                            *index,
                        );
                        if n == 0 {
                            break;
                        }
                        res.push(chosen);
                        n_blocks -= n;
                        if n_blocks == 0 {
                            break 'outer;
                        }
                    }
                }
            }

            // TODO: optimize this
            // retain pieces that are not fully requested
            info!("partial request pieces {:?}", self.partly_requested_pieces);
            self.partly_requested_pieces
                .retain(|_, v| v.block_map.iter().any(|b| !*b));
            info!("partial request pieces {:?}", self.partly_requested_pieces);

            if n_blocks == 0 {
                return BlockRequests {
                    piece_size: self.piece_size,
                    range: res,
                };
            }

            loop {
                let chosen_piece = if let Some((index, _)) =
                    self.heap
                        .min_of_set(|i| peer_field.get(i as u32), None, None)
                {
                    *index as u32
                } else {
                    return BlockRequests {
                        piece_size: self.piece_size,
                        range: res,
                    };
                };

                self.heap.delete(chosen_piece as usize);

                // TODO: FIXME: last block length is not BLOCK_SIZE
                let n_blocks_in_piece = (self.piece_size >> 14) as usize;
                if n_blocks >= n_blocks_in_piece {
                    res.push(BlockRange {
                        // TODO: FIXME: last block length is not BLOCK_SIZE
                        from: protocol::Request {
                            index: chosen_piece,
                            begin: 0,
                            len: BLOCK_SIZE,
                        },
                        to: protocol::Request {
                            index: chosen_piece,
                            begin: self.piece_size - 16384,
                            len: BLOCK_SIZE,
                        },
                    });
                    n_blocks -= n_blocks_in_piece;
                } else {
                    // add this piece to partial piece list
                    let mut block_map = vec![false; n_blocks_in_piece];
                    for i in block_map.iter_mut().take(n_blocks) {
                        *i = true;
                    }
                    self.partly_requested_pieces
                        .insert(chosen_piece, PartialRequestedPiece { block_map });
                    res.push(BlockRange {
                        // TODO: FIXME: last block length is not BLOCK_SIZE
                        from: protocol::Request {
                            index: chosen_piece,
                            begin: 0,
                            len: BLOCK_SIZE,
                        },
                        to: protocol::Request {
                            index: chosen_piece,
                            begin: (n_blocks as u32 - 1) * BLOCK_SIZE,
                            len: BLOCK_SIZE,
                        },
                    });
                    n_blocks = 0;
                }

                if n_blocks == 0 {
                    return BlockRequests {
                        piece_size: self.piece_size,
                        range: res,
                    };
                }
            }
        } else {
            warn!("request blocks of peer {peer} which not exist in field_map");
            BlockRequests {
                piece_size: self.piece_size,
                range: Vec::new(),
            }
        }
    }

    fn peer_have(&mut self, peer: &SocketAddr, piece: u32) {
        if let Some(v) = self.peer_field_map.get_mut(peer) {
            v.set(piece);
        } else {
            panic!("set have for a un-stored peer, socket addr: {peer}");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_block_range_different_piece() {
        let br = BlockRange {
            from: protocol::Request {
                index: 5,
                begin: 3 * 16384,
                len: 16384,
            },
            to: protocol::Request {
                index: 7,
                begin: 9 * 16384,
                len: 163,
            },
        };
        assert!(br.iter(16 * 16384).any(|r| r
            == protocol::Request {
                index: 7,
                begin: 147456,
                len: 163
            }));
        assert!(br.iter(16 * 16384).all(|r| r
            != protocol::Request {
                index: 2,
                begin: 147456,
                len: 16384
            }));
    }

    #[test]
    fn test_block_range_same_piece() {
        let br = BlockRange {
            from: protocol::Request {
                index: 5,
                begin: 3 * 16384,
                len: 16384,
            },
            to: protocol::Request {
                index: 5,
                begin: 9 * 16384,
                len: 163,
            },
        };
        assert!(br.iter(16 * 16384).any(|r| r
            == protocol::Request {
                index: 5,
                begin: 147456,
                len: 163
            }));
        assert!(br.iter(16 * 16384).any(|r| r
            == protocol::Request {
                index: 5,
                begin: 3 * 16384,
                len: 16384
            }));
    }
}

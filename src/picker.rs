mod heap;
use crate::protocol;
pub use crate::protocol::BitField;
use heap::Heap;
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::warn;

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
    range: Vec<BlockRange>,
}

#[derive(Debug, Clone)]
struct BlockRange {
    // from and to are inclusive
    from: protocol::Request,
    to: protocol::Request,
}

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

struct BlockRangeIter {
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
    fn abc(&mut self, last_offset: u32) -> Option<<Self as Iterator>::Item> {
        if self.current_in_piece_offset < last_offset {
            if self.current_in_piece_offset + 16384 > last_offset {
                let step = last_offset - self.current_in_piece_offset;
                let res = protocol::Request {
                    index: self.current_piece,
                    begin: self.current_in_piece_offset,
                    len: step,
                };
                self.current_in_piece_offset += step;
                return Some(res);
            } else {
                let res = protocol::Request {
                    index: self.current_piece,
                    begin: self.current_in_piece_offset,
                    len: 16384,
                };
                self.current_in_piece_offset += 16384;
                return Some(res);
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
            return self.abc(last_offset);
        }

        let piece_size = self.piece_size;
        if self.current_in_piece_offset >= piece_size {
            self.current_piece += 1;
            self.current_in_piece_offset = 0;
        }
        self.abc(piece_size)
    }
}

struct PartialRequestedPieces {
    index: u32,

    // TODO: maybe use a more space efficient data structure?
    block_field: BitField,
}

// TODO: change this to PiecePicker<Heap>
// make piece picker a generic
// i.e. this is actually a block picker
pub struct HeapPiecePicker {
    heap: Heap<i32>, // let's say availability is type i32
    piece_size: u32,
    piece_total: u32,

    peer_field_map: HashMap<SocketAddr, BitField>,

    partly_requested_pieces: Vec<PartialRequestedPieces>,
    // TODO: maybe need a Vec<UnackedPieces>
}

impl HeapPiecePicker {
    pub fn new(piece_total: u32, piece_size: u32) -> Self {
        Self {
            heap: Heap::new(piece_total as usize),
            piece_size,
            piece_total,
            peer_field_map: HashMap::new(),
            partly_requested_pieces: Vec::new(),
        }
    }
}

impl Picker for HeapPiecePicker {
    fn peer_add(&mut self, peer: SocketAddr, b: BitField) {
        let field_map = b.iter().take(self.piece_total as usize).enumerate().filter(|(_, v)| *v).map(|(i, _)| i);
        self.heap.increment_bulk(field_map, 1);
        self.peer_field_map.insert(peer, b);
    }

    fn peer_remove(&mut self, peer: &SocketAddr) {
        self.peer_field_map.remove(peer);
    }

    fn pick_blocks(&mut self, peer: &SocketAddr, n_blocks: usize) -> BlockRequests {
        if let Some(peer_field) = self.peer_field_map.get(peer) {
            for p in self.partly_requested_pieces.iter_mut() {
                if peer_field.get(p.index) {
                    // TODO: try to get n blocks from this piece
                    // now just return
                    return BlockRequests {
                        range: vec![BlockRange {
                            from: protocol::Request {
                                index: p.index,
                                begin: 0,
                                len: 16384,
                            },
                            to: protocol::Request {
                                index: p.index,
                                begin: self.piece_size - 16384,
                                len: 16384,
                            },
                        }],
                    };
                }
            }

            match self
                .heap
                .min_of_set(|i| peer_field.get(i as u32), None, None)
            {
                Some((index, _)) => BlockRequests {
                    range: vec![BlockRange {
                        from: protocol::Request {
                            index: *index as u32,
                            begin: 0,
                            len: 16384,
                        },
                        to: protocol::Request {
                            index: *index as u32,
                            begin: self.piece_size - 16384,
                            len: 16384,
                        },
                    }],
                },
                None => BlockRequests { range: Vec::new() },
            }
        } else {
            warn!("request blocks of peer {peer} which not exist in field_map");
            BlockRequests { range: Vec::new() }
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

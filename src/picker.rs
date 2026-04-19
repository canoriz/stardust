use crate::bandwidth::Bandwidth;
pub use crate::protocol::BitField;
use crate::protocol::{self, Request};
use heap::Heap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::time;
use tracing::{debug, info, warn};

mod block_picker;
mod heap;
mod rarest_first;
pub use block_picker::{BlockPicker, BlockPickerDump, BlockStatus};
pub use rarest_first::Picker as RarestPicker;

const BLOCK_SIZE: u32 = 16384;
pub(crate) const BW_SLOT_SIZE: usize = 50;

impl PieceState {
    pub fn have(&self, index: u32) -> bool {
        match self {
            PieceState::HaveAll => true,
            PieceState::HaveNone => false,
            PieceState::Bitfield(b) => b.get(index),
        }
    }
}

#[derive(Debug)]
pub struct PeerPieceDetail {
    have: PieceState,
    choke: bool,
}

#[derive(Debug)]
pub enum PieceState {
    HaveAll,
    HaveNone,
    Bitfield(BitField),
}

impl PieceState {
    /// set index to 1
    pub fn set_have2(&mut self, index: u32) {
        match self {
            PieceState::HaveAll => {}
            PieceState::HaveNone => {
                let mut b = BitField::with_bit_len(index as usize);
                b.set(index, true);
                *self = PieceState::Bitfield(b);
            }
            PieceState::Bitfield(b) => {
                b.set(index, true);
            }
        }
    }

    /// set index to `have`. set or unset one index.
    pub fn set_have(&mut self, n_piece: usize, index: u32, have: bool) {
        match self {
            PieceState::HaveAll => {
                if !have {
                    let mut b = BitField::from(vec![true; n_piece]);
                    b.set(index, have);
                    *self = PieceState::Bitfield(b);
                }
            }
            PieceState::HaveNone => {
                if have {
                    let mut b = BitField::with_bit_len(n_piece);
                    b.set(index, have);
                    *self = PieceState::Bitfield(b);
                }
            }
            PieceState::Bitfield(b) => {
                b.set(index, have);
                let n_ones = b.count_ones();
                if n_ones == n_piece as u32 {
                    *self = PieceState::HaveAll;
                } else if n_ones == 0 {
                    *self = PieceState::HaveNone;
                }
            }
        }
    }

    pub fn as_bitfield(&self, n_piece: usize) -> BitField {
        match self {
            PieceState::HaveAll => BitField::from(vec![true; n_piece]),
            PieceState::HaveNone => BitField::from(vec![false; n_piece]),
            PieceState::Bitfield(b) => b.clone(),
        }
    }
}

impl PeerPieceDetail {
    pub fn have(&self, index: u32) -> bool {
        match &self.have {
            PieceState::HaveAll => true,
            PieceState::HaveNone => false,
            PieceState::Bitfield(b) => b.get(index),
        }
    }

    pub fn choke(&self, index: u32) -> bool {
        self.choke
    }
}

pub type PeerAddr = SocketAddr;

pub trait PiecePicker {
    type T;

    fn peer_add(&mut self, addr: PeerAddr, state: PieceState);
    fn peer_leave(&mut self, addr: &PeerAddr);
    fn peer_choke(&mut self, addr: &PeerAddr);
    fn peer_unchoke(&mut self, addr: &PeerAddr);

    /// called with peer send a HAVE to us
    fn peer_new_have(&mut self, addr: &PeerAddr, index: u32);

    /// show peer detail
    fn peer_detail(&mut self, addr: &PeerAddr) -> Option<&Self::T>;

    /// change selected piece set
    fn select(&mut self, index: u32, want: bool);

    /// returns this piece is selected or not
    fn selected(&self, index: u32) -> bool {
        self.selected_pieces().get(index)
    }

    fn selected_pieces(&self) -> &BitField;

    /// set we have/not have this piece
    fn set_have(&mut self, index: u32, have: bool);

    /// returns if we have this piece
    fn have(&self, index: u32) -> bool;

    /// returns the availability of this piece, i.e. how many peers have this piece
    /// TODO: maybe don't make this a trait method. Instead, we maintain our own counter
    /// TODO: also, maybe change signature to
    /// piece_availability(&self) -> Iterator<Item=(u32, usize)>
    fn piece_availability(&self, index: u32) -> usize;

    /// returns have piece bitfield
    fn have_pieces(&self) -> &BitField;

    /// Pick next piece, returns id
    /// and marks we have this piece
    /// If later we don't receive this
    /// call set_have(index, false) to reset it
    fn pick_next(&mut self, addr: &PeerAddr) -> Option<u32>;

    /// if we have all the piece we want
    fn is_finished(&mut self) -> bool;

    /// dump which piece we have
    fn dump(&self) -> PieceMap {
        PieceMap {
            selected: self.selected_pieces().clone(),
            have: self.have_pieces().clone(),
        }
    }

    /// load progress from dumped piece map
    fn load(&mut self, piece_map: PieceMap) {
        for (i, s) in piece_map.selected.iter().enumerate() {
            self.select(i as u32, s);
        }
        for (i, h) in piece_map.have.iter().enumerate() {
            self.set_have(i as u32, h);
        }
    }
}

/// Dumped piece map, must be size of N
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PieceMap {
    pub selected: BitField,
    pub have: BitField,
}

// TODO: maybe use peer_id instead of socketaddr?
// pub trait Picker {
//     fn peer_add(&mut self, peer: SocketAddr, b: BitField);
//     fn peer_remove(&mut self, peer: &SocketAddr);
//     fn pick_blocks(&mut self, peer: &SocketAddr, n_blocks: usize) -> BlockRequests;
//     fn peer_have(&mut self, peer: &SocketAddr, piece: u32);
//     fn blocks_received(&mut self, block: &BlockRange);

//     // these blocks(requested or not) will not come automatically
//     // need to re-request them
//     fn blocks_revoke(&mut self, block: &BlockRange);

//     fn piece_checked(&mut self, piece_index: u32);
// }

// many consecutive block ranges
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct BlockRequests {
    pub piece_size: u32,
    pub range: Vec<BlockRange>,
}

impl BlockRequests {
    pub fn len(&self) -> usize {
        self.range.iter().map(|r| r.len(self.piece_size)).sum()
    }

    pub fn none() -> Self {
        Self {
            piece_size: 0,
            range: Vec::new(),
        }
    }
}

// struct BlockRequestsIter<'a, T>
// where
//     T: Iterator<Item = BlockRange>,
// {
//     piece_size: u32,
//     last_piece_size: u32,
//     last_piece_index: u32,
//     br_iter: T,
//     pr_iter: Option<BlockRangeIter>,
// }

// impl<T> Iterator for BlockRequestsIter<'_, T>
// where
//     T: Iterator<Item = BlockRange>,
// {
//     type Item = protocol::Request;

//     fn next(&mut self) -> Option<Self::Item> {
//         if let Some(i) = self.pr_iter {
//             let nx = self.pr_iter.next();
//             if let Some(v) = nx {
//                 return Some(v);
//             } else {
//                 self.pr_iter = match self.br_iter.next() {
//                     Some(br) => {
//                         if br.
//                     }
//                 }
//             }
//         }
//         let br = self.br_iter.next();
//     }
// }

// Represents a continuous range of blocks
// TODO: maybe change protocol::Request to use block-index
// question: how to represent a part 16kib request?
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRange {
    // from and to are inclusive
    // TODO: maybe use block index? this [begin, len) pattern is strange
    from: protocol::Request,
    to: protocol::Request,
}

// TODO: can piece request cross PIECE boundry?
// YES!. transmission states that some torrent's piece size
// is not multiple of BLOCKSIZE, so leaving last block in piece
// smaller than BLOCKSIZE
impl BlockRange {
    // TODO: from 3-int tuple or some more sophisticated struct?
    pub fn one_block(index: u32, begin: u32, len: u32) -> Self {
        let a = protocol::Request { index, begin, len };
        Self { from: a, to: a }
    }

    pub fn iter(&self, piece_size: u32) -> BlockRangeIter {
        let br = self.clone();
        BlockRangeIter {
            current_in_piece_offset: br.from.begin,
            current_piece: br.from.index,

            piece_size,
            br,
        }
    }

    pub fn len(&self, piece_size: u32) -> usize {
        if self.from.index == self.to.index {
            (self.to.begin.saturating_sub(self.from.begin) / BLOCK_SIZE + 1) as usize
        } else if self.to.index > self.from.index {
            ((piece_size - self.from.begin) / BLOCK_SIZE
                + (self.to.begin / BLOCK_SIZE + 1)
                + (self.to.index - self.from.index - 1) * (piece_size / BLOCK_SIZE))
                as usize
        } else {
            0
        }
    }
}

pub struct BlockRangeIter {
    current_piece: u32,
    current_in_piece_offset: u32,
    piece_size: u32,

    br: BlockRange,
}

impl BlockRangeIter {
    fn one_request(&mut self, last_offset: u32) -> Option<<Self as Iterator>::Item> {
        if self.current_in_piece_offset < last_offset {
            let end = (self.current_in_piece_offset + BLOCK_SIZE).min(last_offset);
            let step = end - self.current_in_piece_offset;
            let res = protocol::Request {
                index: self.current_piece,
                begin: self.current_in_piece_offset,
                len: step,
            };
            self.current_in_piece_offset += step;
            Some(res)
        } else {
            None
        }
    }
}

impl Iterator for BlockRangeIter {
    type Item = protocol::Request;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_piece == self.br.to.index {
            // this is the last piece in range
            let last_offset = self.br.to.begin + self.br.to.len;
            return self.one_request(last_offset);
        }

        if self.current_in_piece_offset >= self.piece_size {
            self.current_piece += 1;
            self.current_in_piece_offset = 0;
        }
        self.one_request(self.piece_size)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PeerStatus {
    pub bitfield: BitField,
    pub n_timeout: usize,
    pub bandwidth: Bandwidth<BW_SLOT_SIZE>,

    pub n_in_flight: usize,
}

fn block_size(blk_index: u32, piece_size: u32) -> u32 {
    let normal_end = blk_index * BLOCK_SIZE + BLOCK_SIZE;
    if normal_end <= piece_size {
        BLOCK_SIZE
    } else {
        piece_size - blk_index * BLOCK_SIZE
    }
}

#[cfg(test)]
mod test {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[test]
    fn test_block_range_iter_many_pieces() {
        let br = BlockRange {
            from: protocol::Request {
                index: 5,
                begin: 3 * BLOCK_SIZE,
                len: BLOCK_SIZE,
            },
            to: protocol::Request {
                index: 7,
                begin: 9 * BLOCK_SIZE,
                len: 163,
            },
        };
        assert!(br.iter(16 * BLOCK_SIZE).any(|r| r
            == protocol::Request {
                index: 7,
                begin: 147456,
                len: 163
            }));
        assert!(br.iter(16 * BLOCK_SIZE).all(|r| r
            != protocol::Request {
                index: 2,
                begin: 147456,
                len: BLOCK_SIZE
            }));
        assert_eq!(br.len(16 * BLOCK_SIZE), 13 + 16 + 10);
    }

    #[test]
    fn test_block_range_iter_same_piece() {
        let br = BlockRange {
            from: protocol::Request {
                index: 5,
                begin: 3 * BLOCK_SIZE,
                len: BLOCK_SIZE,
            },
            to: protocol::Request {
                index: 5,
                begin: 9 * BLOCK_SIZE,
                len: 163,
            },
        };
        assert!(br.iter(16 * BLOCK_SIZE).any(|r| r
            == protocol::Request {
                index: 5,
                begin: 147456,
                len: 163
            }));
        assert!(br.iter(16 * BLOCK_SIZE).any(|r| r
            == protocol::Request {
                index: 5,
                begin: 3 * BLOCK_SIZE,
                len: BLOCK_SIZE
            }));
        assert_eq!(br.len(16 * BLOCK_SIZE), 7);
    }

    #[test]
    fn test_block_range_iter_same_block() {
        let br = BlockRange {
            from: protocol::Request {
                index: 5,
                begin: 9 * BLOCK_SIZE,
                len: 163,
            },
            to: protocol::Request {
                index: 5,
                begin: 9 * BLOCK_SIZE,
                len: 163,
            },
        };
        assert!(br.iter(16 * BLOCK_SIZE).all(|r| r
            == protocol::Request {
                index: 5,
                begin: 9 * BLOCK_SIZE,
                len: 163,
            }));
        assert_eq!(br.len(16 * BLOCK_SIZE), 1)
    }

    const fn generate_peer(ip: u32) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::from_bits(ip)), 1)
    }

    // test helper: check if BlockRequests is with in index range and size == total
    // if not, assert failed
    fn check_block_requests(br: &BlockRequests, index_with_in: &[u32], total: usize) {
        let piece_size = br.piece_size;
        println!("block requests: {br:?}");
        for k in br.range.iter() {
            for b in k.iter(piece_size) {
                assert!(index_with_in.iter().any(|i| *i == b.index));
            }
        }
        assert_eq!(
            br.range
                .iter()
                .map(|k| k.iter(piece_size).count())
                .sum::<usize>(),
            total
        );
    }
}

// pub struct PiecePicker {
//     heap: Heap,
// }

// impl PiecePicker {
//     fn new(piece_count: usize) -> Self {
//         Self {
//             heap: Heap::new(piece_count),
//         }
//     }
// }
use crate::protocol;
pub use crate::protocol::BitField;
use std::net::SocketAddr;

pub trait Picker {
    fn peer_add(&mut self, peer: &SocketAddr, b: &BitField);
    fn peer_remove(&mut self, peer: &SocketAddr);
    fn peer_reset_bitfield(&mut self, peer: &SocketAddr, b: &BitField);
    fn pick_blocks(&mut self, peer: &SocketAddr, n_blocks: usize);
    fn peer_have(&mut self, peer: &SocketAddr, piece: u32);
}

// request a range from peer
// from and to must be aligned with block size (except last block)
// specified by BitTorrent protocol

// from and to are inclusive
#[derive(Debug)]
pub(crate) struct BlockRange {
    // from and to are inclusive
    from: protocol::Request,
    to: protocol::Request,
}

impl Iterator for BlockRange {
    type Item = protocol::Request;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

mod heap;
pub use heap::Picker as HeapPicker;

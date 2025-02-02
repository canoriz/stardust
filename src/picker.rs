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
pub use crate::protocol::BitField;
use std::net::SocketAddr;

pub trait Picker {
    fn new() -> Self;
    fn add_peer(&mut self, peer: &SocketAddr, b: BitField);
    fn rm_peer(&mut self, peer: &SocketAddr);
    fn pick_blocks(&mut self, peer: &SocketAddr, n_blocks: usize);
    fn peer_set_have(&mut self, peer: &SocketAddr, new_have: usize);
}

mod heap;
pub use heap::Picker as HeapPicker;

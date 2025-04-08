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
    fn received_blocks(&mut self, block: BlockRange);

    // these blocks(requested or not) will not come automatically
    // need to re-request them
    fn reschedule_blocks(&mut self, block: BlockRange);
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

impl BlockRangeIter {
    fn one_request(&mut self, last_offset: u32) -> Option<<Self as Iterator>::Item> {
        if self.current_in_piece_offset < last_offset {
            if self.current_in_piece_offset + BLOCK_SIZE > last_offset {
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
                    len: BLOCK_SIZE,
                };
                self.current_in_piece_offset += BLOCK_SIZE;
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

#[derive(PartialEq, Debug, Clone)]
enum BlockStatus {
    NotRequested,
    Requested(SocketAddr), // TODO: maybe use a Rc here to save space?
    Received,
}

#[derive(Debug, Clone)]
struct PartialRequestedPiece {
    // TODO: maybe use a more space/time efficient data structure?
    all_requested_before: usize,
    block_map: Vec<BlockStatus>,
}

impl PartialRequestedPiece {
    fn is_all_received(&self) -> bool {
        // TODO: optimize use a all_received_before?
        self.block_map.iter().all(|v| *v == BlockStatus::Received)
    }

    fn is_all_requested(&self) -> bool {
        // TODO: optimize use a all_received_before?
        self.block_map
            .iter()
            .skip(self.all_requested_before)
            .all(|b| match b {
                BlockStatus::Requested(_) => true,
                _ => false,
            })
    }

    fn set_block_status(&mut self, block: &protocol::Request, status: BlockStatus) {
        let block_index = block.begin >> 14;
        for b in self
            .block_map
            .iter_mut()
            .skip(block_index as usize)
            .take(block.len as usize)
        {
            *b = status.clone();
        }
        match status {
            BlockStatus::NotRequested => {
                if self.all_requested_before >= block_index as usize {
                    self.all_requested_before = block_index as usize
                }
            }
            BlockStatus::Requested(_) | BlockStatus::Received => {
                if self.all_requested_before == block_index as usize {
                    self.all_requested_before = block_index as usize
                }
            }
        }
    }
}

struct PartialRequestedPieces(HashMap<u32, PartialRequestedPiece>);

impl PartialRequestedPieces {
    // request picking n_blocks blocks from partial picked pieces
    // returns number of blocks REMAINS to be picked
    // cannot make this a method of PiecePicker because mut and immut ref problem
    fn pick_blocks_from_partial_pieces(
        &mut self,
        peer: &SocketAddr,
        peer_field: &BitField,
        mut n_blocks: usize,
        piece_size: u32,
        picked: &mut Vec<BlockRange>,
    ) -> usize {
        'outer: for (index, p) in self.0.iter_mut() {
            if peer_field.get(*index) {
                loop {
                    if n_blocks == 0 {
                        // no more to pick
                        break 'outer;
                    }
                    let (chosen, n) = choose_blocks_from_a_partial_requested_piece(
                        peer, p, n_blocks, piece_size, *index,
                    );
                    if n == 0 {
                        // every block in this piece have been picked
                        // jump to next partial piece
                        break;
                    }
                    picked.push(chosen);
                    n_blocks -= n;
                }
            }
        }

        n_blocks
    }
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
    partly_requested_pieces: PartialRequestedPieces,

    // TODO: fix this HashMap's V type
    // TODO: maybe change to a Vec<UnackedPieces>
    fully_requested_pieces: HashMap<u32, PartialRequestedPiece>,
}

impl HeapPiecePicker {
    pub fn new(piece_total: u32, piece_size: u32) -> Self {
        Self {
            heap: Heap::new(piece_total as usize),
            piece_size,
            piece_total,
            peer_field_map: HashMap::new(),

            partly_requested_pieces: PartialRequestedPieces(HashMap::new()),
            fully_requested_pieces: HashMap::new(),
        }
    }
}

// TODO: tests
// returns blockrequests and count of chosen blocks
fn choose_blocks_from_a_partial_requested_piece(
    peer: &SocketAddr,
    prp: &mut PartialRequestedPiece,
    n_blocks: usize,
    piece_size: u32,
    piece_index: u32,
) -> (BlockRange, usize) {
    let mut end = piece_size >> 14;
    let mut begin = end;

    let mut in_middle = false;
    for (idx, b) in prp
        .block_map
        .iter_mut()
        .enumerate()
        .skip(prp.all_requested_before)
    {
        if !in_middle && *b == BlockStatus::NotRequested {
            begin = idx as u32;
            in_middle = true;
        }
        // TODO: is this n_blocks condition correct?
        if in_middle && ((*b != BlockStatus::NotRequested) || begin as usize + n_blocks <= idx) {
            prp.all_requested_before = idx;
            end = idx as u32;
            break;
        } else {
            *b = BlockStatus::Requested(peer.clone());
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

// request picking n_blocks from unpicked pieces
// returns number of blocks REMAINS to be picked
fn pick_blocks_from_heap(
    heap: &mut Heap<i32>,
    peer: &SocketAddr,
    peer_field: &BitField,
    mut n_blocks: usize,
    piece_size: u32,
    picked: &mut Vec<BlockRange>,
    partials: &mut PartialRequestedPieces,
    fullys: &mut HashMap<u32, PartialRequestedPiece>,
) -> usize {
    while n_blocks > 0 {
        let chosen_piece = if let Some((index, _)) = heap.min_of_set(
            |i| {
                peer_field.get(i as u32)
                // TODO: contains_key is inefficient. maybe use another field_map?
                    && !partials.0.contains_key(&(i as u32))
                    && !fullys.contains_key(&(i as u32))
            },
            None,
            None,
        ) {
            *index as u32
        } else {
            // if every piece the peer have been already picked
            // can pick nothing, just return
            return n_blocks;
        };
        info!("picked piece {chosen_piece} from heap");

        // TODO: FIXME: last block length is not BLOCK_SIZE
        let n_blocks_in_piece = (piece_size >> 14) as usize;
        if n_blocks >= n_blocks_in_piece {
            picked.push(BlockRange {
                // TODO: FIXME: last block length is not BLOCK_SIZE
                from: protocol::Request {
                    index: chosen_piece,
                    begin: 0,
                    len: BLOCK_SIZE,
                },
                to: protocol::Request {
                    index: chosen_piece,
                    begin: piece_size - BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
            });

            let block_map = vec![BlockStatus::Requested(peer.clone()); n_blocks_in_piece];
            fullys.insert(
                chosen_piece,
                PartialRequestedPiece {
                    block_map,
                    all_requested_before: n_blocks_in_piece,
                },
            );

            n_blocks -= n_blocks_in_piece;
        } else {
            // add this piece to partial piece list
            let mut block_map = vec![BlockStatus::NotRequested; n_blocks_in_piece];
            for i in block_map.iter_mut().take(n_blocks) {
                *i = BlockStatus::Requested(peer.clone());
            }
            partials.0.insert(
                chosen_piece,
                PartialRequestedPiece {
                    block_map,
                    all_requested_before: n_blocks,
                },
            );
            picked.push(BlockRange {
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
    }
    assert_eq!(n_blocks, 0);
    n_blocks
}

impl Picker for HeapPiecePicker {
    fn peer_add(&mut self, peer: SocketAddr, b: BitField) {
        if self.peer_field_map.contains_key(&peer) {
            // if peer previously stored, remove it first
            // TODO: will remove cause big overhead
            self.peer_remove(&peer);
        }

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
        if let Some(field_map) = self.peer_field_map.get(peer) {
            let field_map = field_map
                .iter()
                .take(self.piece_total as usize)
                .enumerate()
                .filter(|(_, v)| *v)
                .map(|(i, _)| i);
            self.heap.decrement_bulk(field_map, 0);
        }
        self.peer_field_map.remove(peer);
        // TODO: reset all block status in fully requested and partial requested

        let should_remove_from_partial = self
            .partly_requested_pieces
            .0
            .iter_mut()
            .filter_map(|(k, v)| {
                let mut has = false;
                for (bk, bv) in v.block_map.iter_mut().enumerate().rev() {
                    if *bv == BlockStatus::Requested(*peer) {
                        *bv = BlockStatus::NotRequested;
                        v.all_requested_before = *k as usize;
                        has = true;
                    }
                }

                if has && v.all_requested_before == 0 {
                    // now we need to check if all blocks in this piece is NotRequested
                    v.block_map
                        .iter()
                        .all(|bv| *bv == BlockStatus::NotRequested)
                        .then_some(*k)
                } else {
                    None
                }
            })
            .collect::<Vec<u32>>();

        for k in should_remove_from_partial {
            self.partly_requested_pieces.0.remove(&k);
        }

        enum MoveTo {
            Partial(u32),
            Remove(u32),
        }

        let should_move_from_fully = self
            .fully_requested_pieces
            .iter_mut()
            .filter_map(|(k, v)| {
                let mut has = false;
                for (bk, bv) in v.block_map.iter_mut().enumerate().rev() {
                    if *bv == BlockStatus::Requested(*peer) {
                        *bv = BlockStatus::NotRequested;
                        v.all_requested_before = *k as usize;
                        has = true;
                    }
                }

                if has {
                    if v.all_requested_before == 0 {
                        // now we need to check if all blocks in this piece is NotRequested
                        v.block_map
                            .iter()
                            .all(|bv| *bv == BlockStatus::NotRequested)
                            .then_some(MoveTo::Remove(*k))
                    } else {
                        Some(MoveTo::Partial(*k))
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<MoveTo>>();

        for k in should_move_from_fully {
            match k {
                MoveTo::Partial(k) => {
                    let v = self
                        .fully_requested_pieces
                        .remove(&k)
                        .expect("MoveToPartial k should be in fully pieces");
                    debug_assert!(!self.partly_requested_pieces.0.contains_key(&k));
                    self.partly_requested_pieces.0.insert(k, v);
                }
                MoveTo::Remove(k) => {
                    self.fully_requested_pieces.remove(&k);
                }
            }
        }
    }

    // TODO: change this struct to reuse return request's Vec buffer?
    fn pick_blocks(&mut self, peer: &SocketAddr, n_blocks: usize) -> BlockRequests {
        let mut n_blocks = n_blocks;
        let mut picked = Vec::new();
        if let Some(peer_field) = self.peer_field_map.get(peer) {
            n_blocks = self
                .partly_requested_pieces
                .pick_blocks_from_partial_pieces(
                    peer,
                    peer_field,
                    n_blocks,
                    self.piece_size,
                    &mut picked,
                );

            info!(
                "partial request pieces {:?}",
                self.partly_requested_pieces.0
            );

            n_blocks = pick_blocks_from_heap(
                &mut self.heap,
                peer,
                peer_field,
                n_blocks,
                self.piece_size,
                &mut picked,
                &mut self.partly_requested_pieces,
                &mut self.fully_requested_pieces,
            );

            // TODO: 3 optimizations:
            // duplicate should_move_from_partial_to_fully check
            // optimize the clone Pieces then deleted it logic
            // only trigger this logic when some partial pieces may change to fully piece
            for k in self
                .partly_requested_pieces
                .0
                .iter()
                .filter(|(_, v)| v.is_all_requested())
                .map(|(k, _)| *k)
                .collect::<Vec<u32>>()
                .iter()
            {
                // TODO: optimize this Clone, maybe use Rc/Arc<T>?
                let v = self
                    .partly_requested_pieces
                    .0
                    .remove(&k)
                    .expect(&format!("k should in partly requested pieces"));
                self.fully_requested_pieces.insert(*k, v);
            }

            info!(
                "after move, partial request pieces {:?}",
                self.partly_requested_pieces.0
            );
            info!(
                "after move, fully request pieces {:?}",
                self.fully_requested_pieces
            );

            return BlockRequests {
                piece_size: self.piece_size,
                range: picked,
            };
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
            self.heap.increment_or(piece as usize, 1);
        } else {
            // if this peer is not stored, assume it is a new peer
            // TODO: maybe need a method for creating bitfield with given length?
            let mut b = BitField::new(vec![0u8; (self.piece_total >> 3) as usize]);
            b.set(piece);
            self.peer_add(*peer, b);
            warn!("set have for a un-stored peer, socket addr: {peer}");
        }
    }

    // TODO: maybe use &BlockRange?
    fn received_blocks(&mut self, block: BlockRange) {
        for blk in block.iter(self.piece_size) {
            let block_index = blk.begin >> 14;
            if let Some(p) = self.partly_requested_pieces.0.get_mut(&blk.index) {
                info!("received blocks of partial requested piece {block:?}");
                debug_assert_eq!(block_index / BLOCK_SIZE, block_index);
                p.set_block_status(&blk, BlockStatus::Received);
            } else if let Some(p) = self.fully_requested_pieces.get_mut(&blk.index) {
                info!("received blocks of fully requested piece {block:?}");
                debug_assert!(!self.partly_requested_pieces.0.contains_key(&blk.index));
                debug_assert_eq!(block_index / BLOCK_SIZE, block_index);
                p.set_block_status(&blk, BlockStatus::Received);
                if p.is_all_received() {
                    info!("piece blk.index is fully received");
                }
            } else {
                // some un-requested blocks come
                info!("received blocks of not requested piece {block:?}");
                let n_blocks_in_piece = (self.piece_size >> 14) as usize;
                debug_assert_eq!(block_index / BLOCK_SIZE, block_index);
                let mut p = PartialRequestedPiece {
                    block_map: vec![BlockStatus::NotRequested; n_blocks_in_piece],
                    all_requested_before: 0,
                };
                p.set_block_status(&blk, BlockStatus::Received);

                // TODO: maybe check moved to fully requested pieces
                // currently not doing it, let next pick call do it
                self.partly_requested_pieces.0.insert(blk.index, p);
            }
        }
    }

    fn reschedule_blocks(&mut self, block: BlockRange) {
        for blk in block.iter(self.piece_size) {
            let block_index = blk.begin >> 14;
            if let Some(p) = self.partly_requested_pieces.0.get_mut(&blk.index) {
                info!("reschedule blocks of partial requested piece {block:?}");
                debug_assert_eq!(block_index / BLOCK_SIZE, block_index);
                for b in p
                    .block_map
                    .iter_mut()
                    .skip(block_index as usize)
                    .take(blk.len as usize)
                {
                    *b = BlockStatus::NotRequested; // TODO: maybe introduce finer types?
                }
                p.all_requested_before = block_index as usize;
            } else if let Some(p) = self.fully_requested_pieces.get_mut(&blk.index) {
                info!("reschedule blocks of fully requested piece {block:?}");
                debug_assert!(!self.partly_requested_pieces.0.contains_key(&blk.index));
                debug_assert_eq!(block_index / BLOCK_SIZE, block_index);

                for b in p
                    .block_map
                    .iter_mut()
                    .skip(block_index as usize)
                    .take(blk.len as usize)
                {
                    *b = BlockStatus::NotRequested;
                }
                p.all_requested_before = block_index as usize;

                let v = self
                    .fully_requested_pieces
                    .remove(&blk.index)
                    .expect("blk.index should exist in fully requested pieces");
                self.partly_requested_pieces.0.insert(blk.index, v);
            } else {
                // these blocks are never requested anyway
                // why caller called this on never requested blocks?
                // this may indicate deeper bugs
                // panic!
                panic!("reschedule blocks of not requested piece {block:?}");
            }
        }
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
    }

    fn generate_peer(ip: u32) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::from_bits(ip)), 1)
    }

    #[test]
    fn test_pick_blocks() {
        const PIECE_TOTAL: u32 = 30;
        let u8len = (PIECE_TOTAL >> 3 + 1) as usize;
        let mut picker = HeapPiecePicker::new(PIECE_TOTAL, BLOCK_SIZE * 14);

        let p1 = generate_peer(1);
        let p2 = generate_peer(2);
        // first peer has all piece
        picker.peer_add(p1, BitField::new(vec![0xffu8; u8len]));
        // peer 2 has all some piece
        picker.peer_add(
            p2,
            BitField::new(
                vec![0xffu8; 10]
                    .into_iter()
                    .chain(vec![0u8; u8len - 10].into_iter())
                    .collect(),
            ),
        );
        let a = picker.pick_blocks(&p2, 112);
        println!("{a:?}");
    }
}

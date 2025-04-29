mod heap;
use crate::protocol;
pub use crate::protocol::BitField;
use heap::Heap;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use tracing::{debug, info, warn};

const BLOCK_SIZE: u32 = 16384;

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
#[derive(Debug)]
pub(crate) struct BlockRequests {
    pub piece_size: u32,
    pub range: Vec<BlockRange>,
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
    // TODO: from 3-int tuple or some more sophisticated struct?
    pub fn one_block(index: u32, begin: u32, len: u32) -> Self {
        let a = protocol::Request { index, begin, len };
        Self {
            from: a.clone(),
            to: a,
        }
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

// TODO: maybe introduce finer BlockStatus types like Revoked/Rejected?
#[derive(PartialEq, Debug, Clone, Copy)]
enum BlockStatus {
    NotRequested,
    Requested(SocketAddr), // TODO: maybe use a Rc here to save space?
    Received,              // TODO: maybe record which peer sends us this block?
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

    fn is_none_requested(&self) -> bool {
        if self.all_requested_before > 0 {
            false
        } else {
            self.block_map
                .iter()
                .skip(self.all_requested_before)
                .all(|b| match b {
                    BlockStatus::NotRequested => true,
                    _ => false,
                })
        }
    }

    fn is_all_requested(&self) -> bool {
        self.block_map
            .iter()
            .skip(self.all_requested_before)
            .all(|b| match b {
                BlockStatus::Requested(_) => true,
                _ => false,
            })
    }

    fn update_requested_before(
        all_requested_before: &mut usize,
        block_index: usize,
        status: &BlockStatus,
    ) {
        match status {
            BlockStatus::NotRequested => {
                if *all_requested_before >= block_index {
                    *all_requested_before = block_index;
                }
            }
            BlockStatus::Requested(_) | BlockStatus::Received => {
                if *all_requested_before == block_index {
                    *all_requested_before = block_index + 1;
                }
            }
        }
    }

    fn set_one_block_status(&mut self, block: &protocol::Request, status: BlockStatus) {
        let block_index = block.begin >> 14; // TODO: change this const
        self.set_one_block_status_by_block_index(block_index as usize, status);
    }

    fn set_one_block_status_by_block_index(&mut self, block_index: usize, status: BlockStatus) {
        self.block_map[block_index] = status.clone();
        Self::update_requested_before(
            &mut self.all_requested_before,
            block_index as usize,
            &status,
        );
    }

    // apply f to every block
    fn map_block_status<F>(&mut self, f: F)
    where
        F: Fn(u32, &BlockStatus) -> BlockStatus,
    {
        for (i, bs) in self.block_map.iter_mut().enumerate() {
            *bs = f(i as u32, bs);
            Self::update_requested_before(&mut self.all_requested_before, i, bs);
        }
    }
}

// using BTreeMap because we can iterate piece index in order
// which makes tests predictable
struct PartialRequestedPieces(BTreeMap<u32, PartialRequestedPiece>);

impl PartialRequestedPieces {
    // request picking n_blocks blocks from partial picked pieces
    // returns number of blocks REMAINS to be picked
    // cannot make this a method of PiecePicker because mut and immut ref problem
    fn pick_blocks_from_partial_pieces(
        &mut self,
        peer: &SocketAddr,
        peer_field: &BitField,
        mut n_blocks: usize,
        normal_piece_size: u32, // TODO: maybe use a Fn(index: u32) -> (piecesize: u32)?
        last_piece_index: u32,
        last_piece_size: u32,
        picked: &mut Vec<BlockRange>,
    ) -> usize {
        // TODO: pick pieces from picked ratio high to low
        'outer: for (index, p) in self.0.iter_mut() {
            if peer_field.get(*index) {
                loop {
                    if n_blocks == 0 {
                        // no more to pick
                        break 'outer;
                    }
                    // if is last piece or not, set piece_size
                    let piece_size = if *index == last_piece_index {
                        last_piece_size
                    } else {
                        normal_piece_size
                    };
                    let (chosen, n) = choose_blocks_from_a_partial_requested_piece(
                        peer, p, n_blocks, *index, piece_size,
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
    last_piece_size: u32,

    peer_field_map: HashMap<SocketAddr, BitField>,

    // index -> Piece
    partly_requested_pieces: PartialRequestedPieces,

    // TODO: fix this HashMap's V type
    // TODO: maybe change to a Vec<UnackedPieces>
    fully_requested_pieces: HashMap<u32, PartialRequestedPiece>,
}

impl HeapPiecePicker {
    pub fn new(total_length: usize, piece_size: u32) -> Self {
        let n_full_piece = total_length / (piece_size as usize);
        let full_piece_total_size = n_full_piece * (piece_size as usize);
        let (piece_total, last_piece_size) = if full_piece_total_size == total_length {
            (n_full_piece, 0)
        } else {
            (
                n_full_piece + 1,
                (total_length - full_piece_total_size) as u32,
            )
        };

        Self {
            heap: Heap::new(piece_total as usize),
            piece_size,
            piece_total: piece_total as u32,
            peer_field_map: HashMap::new(),
            last_piece_size,

            partly_requested_pieces: PartialRequestedPieces(BTreeMap::new()),
            fully_requested_pieces: HashMap::new(),
        }
    }
}

fn block_size(blk_index: u32, piece_size: u32) -> u32 {
    let normal_end = blk_index * BLOCK_SIZE + BLOCK_SIZE;
    if normal_end <= piece_size {
        BLOCK_SIZE
    } else {
        piece_size - blk_index * BLOCK_SIZE
    }
}

// returns blockrequests and count of chosen blocks
fn choose_blocks_from_a_partial_requested_piece(
    peer: &SocketAddr,
    prp: &mut PartialRequestedPiece,
    n_blocks: usize,
    piece_index: u32,
    piece_size: u32,
) -> (BlockRange, usize) {
    let mut end = prp.block_map.len() as u32;
    let mut begin = end;

    assert!(end * BLOCK_SIZE >= piece_size);
    assert!((end - 1) * BLOCK_SIZE < piece_size);

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
        assert!(begin + (n_blocks as u32) >= end);
        (
            BlockRange {
                from: protocol::Request {
                    index: piece_index,
                    begin: begin * BLOCK_SIZE,
                    len: block_size(begin, piece_size),
                },
                to: protocol::Request {
                    index: piece_index,
                    begin: (end - 1) * BLOCK_SIZE,
                    len: block_size(end - 1, piece_size),
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
    normal_piece_size: u32, // TODO: maybe use a Fn(index: u32) -> (piecesize: u32)?
    last_piece_index: u32,
    last_piece_size: u32,
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

        // calc n blocks base on last piece or not
        let (n_blocks_in_piece, piece_size) = if chosen_piece == last_piece_index {
            (
                last_piece_size.div_ceil(BLOCK_SIZE) as usize,
                last_piece_size,
            )
        } else {
            ((normal_piece_size >> 14) as usize, normal_piece_size)
        };

        if n_blocks >= n_blocks_in_piece {
            picked.push(BlockRange {
                from: protocol::Request {
                    index: chosen_piece,
                    begin: 0,
                    len: block_size(0, piece_size),
                },
                to: protocol::Request {
                    index: chosen_piece,
                    begin: BLOCK_SIZE * (n_blocks_in_piece - 1) as u32,
                    len: block_size((n_blocks_in_piece - 1) as u32, piece_size),
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

            n_blocks -= (n_blocks_in_piece as usize);
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
                // TODO: test pick last piece and last block
                from: protocol::Request {
                    index: chosen_piece,
                    begin: 0,
                    len: block_size(0, piece_size),
                },
                to: protocol::Request {
                    index: chosen_piece,
                    begin: (n_blocks as u32 - 1) * BLOCK_SIZE,
                    len: block_size((n_blocks - 1) as u32, piece_size),
                },
            });
            n_blocks = 0;
        }
    }
    assert_eq!(n_blocks, 0);
    n_blocks
}

impl HeapPiecePicker {
    pub fn peer_add(&mut self, peer: SocketAddr, b: BitField) {
        if self.peer_field_map.contains_key(&peer) {
            // TODO: remove picker blocks first? And maintain picked blocks
            // new bitfield have?
            warn!("called peer_add of an existing peer {peer}");
            return;
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

    pub fn peer_mark_not_requested(&mut self, peer: &SocketAddr) {
        // TODO: perf: use a more efficient datastructure
        // which can only iterate over blocks requested by peer
        let should_remove_from_partial = self
            .partly_requested_pieces
            .0
            .iter_mut()
            .filter_map(|(k, v)| {
                let mark_unreqested = |_: u32, b: &BlockStatus| {
                    if b == &BlockStatus::Requested(*peer) {
                        BlockStatus::NotRequested
                    } else {
                        *b
                    }
                };
                v.map_block_status(mark_unreqested);

                v.is_none_requested().then_some(*k)
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
                        v.all_requested_before = bk;
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

    pub fn peer_remove(&mut self, peer: &SocketAddr) {
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

        self.peer_mark_not_requested(peer);
    }

    // TODO: change this struct to reuse return request's Vec buffer?
    pub fn pick_blocks(&mut self, peer: &SocketAddr, n_blocks: usize) -> BlockRequests {
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
                    self.piece_total - 1,
                    self.last_piece_size,
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
                self.piece_total - 1,
                self.last_piece_size,
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

            debug!(
                "after move, partial request pieces {:?}",
                self.partly_requested_pieces.0
            );
            debug!(
                "after move, fully request pieces {:?}",
                self.fully_requested_pieces
            );
            warn!(
                "n fully request pieces: {}/{}",
                self.fully_requested_pieces.len(),
                self.piece_total,
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

    pub fn peer_have(&mut self, peer: &SocketAddr, piece: u32) {
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

    // receive one block, returns if any piece is all received
    #[must_use = "returns fully received pieces"]
    pub fn block_received(&mut self, blk: protocol::Request) -> Option<u32> {
        let block_index = blk.begin >> 14;
        if let Some(p) = self.partly_requested_pieces.0.get_mut(&blk.index) {
            info!("received blocks of partial requested piece {blk:?}");
            debug_assert_eq!(blk.begin / BLOCK_SIZE, block_index);
            p.set_one_block_status(&blk, BlockStatus::Received);
            p.is_all_received().then_some(blk.index)
        } else if let Some(p) = self.fully_requested_pieces.get_mut(&blk.index) {
            info!("received blocks of fully requested piece {blk:?}");
            debug_assert!(!self.partly_requested_pieces.0.contains_key(&blk.index));
            debug_assert_eq!(blk.begin / BLOCK_SIZE, block_index);
            p.set_one_block_status(&blk, BlockStatus::Received);
            if p.is_all_received() {
                info!("piece {} is fully received", blk.index);
                Some(blk.index)
            } else {
                None
            }
        } else {
            // some un-requested blocks come
            info!("received blocks of not requested piece {blk:?}");
            let n_blocks_in_piece = (self.piece_size >> 14) as usize;
            debug_assert_eq!(blk.begin / BLOCK_SIZE, block_index);
            let mut p = PartialRequestedPiece {
                block_map: vec![BlockStatus::NotRequested; n_blocks_in_piece],
                all_requested_before: 0,
            };
            p.set_one_block_status(&blk, BlockStatus::Received);

            let is_all_received = p.is_all_received();
            if is_all_received || p.is_all_requested() {
                self.fully_requested_pieces.insert(blk.index, p);
            } else {
                self.partly_requested_pieces.0.insert(blk.index, p);
            }
            is_all_received.then_some(blk.index)
        }
    }

    pub fn blocks_received(&mut self, block: &BlockRange) {
        for blk in block.iter(self.piece_size) {
            self.block_received(blk);
        }
    }

    pub fn blocks_revoke(&mut self, block: &BlockRange) {
        for blk in block.iter(self.piece_size) {
            let block_index = blk.begin >> 14;
            if let Some(p) = self.partly_requested_pieces.0.get_mut(&blk.index) {
                info!("reschedule blocks of partial requested piece {block:?}");
                debug_assert_eq!(blk.begin / BLOCK_SIZE, block_index);

                p.set_one_block_status(&blk, BlockStatus::NotRequested);
            } else if let Some(p) = self.fully_requested_pieces.get_mut(&blk.index) {
                info!("reschedule blocks of fully requested piece {block:?}");
                debug_assert!(!self.partly_requested_pieces.0.contains_key(&blk.index));
                debug_assert_eq!(blk.begin / BLOCK_SIZE, block_index);

                p.set_one_block_status(&blk, BlockStatus::NotRequested);

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

        self.partly_requested_pieces
            .0
            .retain(|_, v| !v.is_none_requested());
    }

    pub fn piece_checked(&mut self, piece_index: u32) {
        warn!(
            "piece {piece_index} received, unpicked {} partial {}, fully {}",
            self.heap.len(),
            self.partly_requested_pieces.0.len(),
            self.fully_requested_pieces.len()
        );
        let in_fully = self.fully_requested_pieces.remove(&piece_index);
        if in_fully.is_none() {
            warn!("checked not request piece {piece_index}");
        }
        let in_heap = self.heap.delete(piece_index as usize);
        if in_heap.is_none() {
            warn!("checked piece {piece_index} not in heap");
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
                assert!(index_with_in.iter().find(|i| **i == b.index).is_some());
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

    fn check_picker_partial_and_fully<const N: usize, const M: usize>(
        picker: &HeapPiecePicker,
        partial_indices: [u32; N],
        fully_indices: [u32; M],
    ) {
        {
            let mut pi = picker
                .partly_requested_pieces
                .0
                .keys()
                .map(|k| *k)
                .collect::<Vec<u32>>();
            pi.sort();
            let mut pi_exp = partial_indices.clone();
            pi_exp.sort();
            assert_eq!(pi, pi_exp);
        }

        {
            let mut fi = picker
                .fully_requested_pieces
                .keys()
                .map(|k| *k)
                .collect::<Vec<u32>>();
            fi.sort();
            let mut fi_exp = fully_indices.clone();
            fi_exp.sort();
            assert_eq!(fi, fi_exp);
        }
    }

    #[test]
    fn test_receive_last_block() {
        const PIECE_TOTAL: u32 = 30;
        const TOTAL_LENGTH: usize = (PIECE_TOTAL * BLOCK_SIZE * 14 - 3 * (BLOCK_SIZE + 1)) as usize;

        // make a length not multiple of block size
        let mut picker = HeapPiecePicker::new(TOTAL_LENGTH, BLOCK_SIZE * 14);

        let addr1 = generate_peer(1);
        picker.peer_add(
            addr1,
            BitField::from({
                let mut v = vec![false; PIECE_TOTAL as usize];
                v[PIECE_TOTAL as usize - 1] = true;
                assert_eq!(v.len(), PIECE_TOTAL as usize);
                v
            }),
        );

        const LAST_PIECE_SIZE: u32 = TOTAL_LENGTH as u32 % (14 * BLOCK_SIZE);

        let blks = picker.pick_blocks(&addr1, 15);

        {
            let mut complete = None;
            for br in &blks.range {
                for b in br.iter(LAST_PIECE_SIZE) {
                    complete = complete.or(picker.block_received(b));
                }
            }
            assert_eq!(complete, Some(PIECE_TOTAL - 1));
        }

        {
            // if picker not set complete, should also return piece index
            let mut complete = None;
            for br in &blks.range {
                for b in br.iter(LAST_PIECE_SIZE) {
                    complete = complete.or(picker.block_received(b));
                }
            }
            assert_eq!(complete, Some(PIECE_TOTAL - 1));
        }
    }

    #[test]
    fn test_pick_blocks() {
        const PIECE_TOTAL: u32 = 30;

        const TOTAL_LENGTH: usize = (PIECE_TOTAL * BLOCK_SIZE * 14 - 3 * (BLOCK_SIZE + 1)) as usize;
        // make a length not multiple of block size
        let mut picker = HeapPiecePicker::new(TOTAL_LENGTH, BLOCK_SIZE * 14);

        // i-th peer has first i piece
        let peers: Vec<_> = (1..)
            .take(PIECE_TOTAL as usize)
            .map(|i| {
                (
                    generate_peer(i),
                    BitField::from({
                        let mut v = vec![true; i as usize];
                        v.append(&mut vec![false; PIECE_TOTAL as usize - i as usize]);
                        assert_eq!(v.len(), PIECE_TOTAL as usize);
                        v
                    }),
                )
            })
            .collect();

        for (peer, map) in peers.clone() {
            picker.peer_add(peer, map);
        }

        // test choose one piece
        let first_pick_4 = {
            let pick4 = picker.pick_blocks(&peers[4].0, 10);
            check_block_requests(&pick4, &[4], 10);
            check_picker_partial_and_fully(&picker, [4], []);
            pick4
        };

        // test choose from partial
        let pick_3blk_in_4 = {
            let pick4 = picker.pick_blocks(&peers[4].0, 3);
            check_block_requests(&pick4, &[4], 3);
            check_picker_partial_and_fully(&picker, [4], []);
            pick4
        };

        {
            // test choose from partial and heap
            let pick4 = picker.pick_blocks(&peers[4].0, 18);
            check_block_requests(&pick4, &[2, 3, 4], 18);
            check_picker_partial_and_fully(&picker, [2], [3, 4]);

            // now piece 3, 4 fully picked, piece 2 picked 3/14

            // test revoke blocks
            for br in &pick_3blk_in_4.range {
                println!("revoke {br:?}");
                picker.blocks_revoke(br);
                check_picker_partial_and_fully(&picker, [2, 4], [3]);
            }

            // now piece 3 fully picked, piece 2 picked 3/14, piece 4 picked 11/14

            // pick 20 more
            // piece 2,4 should be fully picked, piece 1 should be picked 6/14
            let pick4 = picker.pick_blocks(&peers[4].0, 20);
            check_block_requests(&pick4, &[2, 4, 1], 20);
            check_picker_partial_and_fully(&picker, [1], [4, 2, 3]);

            // revoke last 20, now piece 1 fully revoked
            // piece 2: 3/14, piece 4: 11/14
            for br in &pick4.range {
                println!("revoke {br:?}");
                picker.blocks_revoke(br);
            }
            println!("partial {:?}", picker.partly_requested_pieces.0);
            println!("fully {:?}", picker.fully_requested_pieces);
            check_picker_partial_and_fully(&picker, [2, 4], [3]);
        }

        // piece 2: 3/14, piece 4: 11/14
        // test another peer
        {
            // peer6 should first pick partial picked piece 2
            let pick6 = picker.pick_blocks(&peers[6].0, 10);
            check_block_requests(&pick6, &[2], 10);
            check_picker_partial_and_fully(&picker, [2, 4], [3]);

            // peer6 should pick piece 2, 4 and 6
            let pick6 = picker.pick_blocks(&peers[6].0, 10);
            check_block_requests(&pick6, &[2, 4, 6], 10);
            check_picker_partial_and_fully(&picker, [6], [2, 4, 3]);
        }

        // test block received
        {
            // test receive un-requested block
            picker.blocks_received(&BlockRange {
                from: protocol::Request {
                    index: 7,
                    begin: 13 * BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
                to: protocol::Request {
                    index: 7,
                    begin: 13 * BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
            });
            println!("partial {:?}", picker.partly_requested_pieces.0);
            println!("fully {:?}", picker.fully_requested_pieces);
            check_picker_partial_and_fully(&picker, [6, 7], [2, 4, 3]);

            // test receive block of partial piece
            picker.blocks_received(&BlockRange {
                from: protocol::Request {
                    index: 6,
                    begin: 1 * BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
                to: protocol::Request {
                    index: 6,
                    begin: 1 * BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
            });
            println!("partial {:?}", picker.partly_requested_pieces.0);
            println!("fully {:?}", picker.fully_requested_pieces);
            check_picker_partial_and_fully(&picker, [6, 7], [2, 4, 3]);

            for br in &pick_3blk_in_4.range {
                println!("revoke {br:?}");
                picker.blocks_received(br);
            }
            for br in &first_pick_4.range {
                println!("revoke {br:?}");
                picker.blocks_received(br);
            }

            // receive the last block of piece 4
            picker.blocks_received(&BlockRange {
                from: protocol::Request {
                    index: 4,
                    begin: 13 * BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
                to: protocol::Request {
                    index: 4,
                    begin: 13 * BLOCK_SIZE,
                    len: BLOCK_SIZE,
                },
            });

            println!("partial {:?}", picker.partly_requested_pieces.0);
            println!("fully {:?}", picker.fully_requested_pieces);

            picker.piece_checked(4);
            check_picker_partial_and_fully(&picker, [6, 7], [2, 3]);
        }

        // check pick last piece
        {
            println!("test pick last piece");
            let pick30 = picker.pick_blocks(&peers[29].0, 50);
            check_block_requests(&pick30, &[6, 7, 27, 29, 28], 50);
            let last_piece_length = TOTAL_LENGTH % ((BLOCK_SIZE as usize) * 14);
            let last_block_len = last_piece_length % (BLOCK_SIZE as usize);
            let last_block_begin = last_piece_length - last_block_len;
            assert!(pick30.range.iter().any(|br| {
                for pr in br.iter(pick30.piece_size) {
                    if pr.index == picker.piece_total - 1
                        && pr.begin == last_block_begin as u32
                        && pr.len == last_block_len as u32
                    {
                        return true;
                    }
                }
                return false;
            }));
            println!("{pick30:?}");
            check_picker_partial_and_fully(&picker, [27], [2, 3, 6, 7, 28, 29]);
        };
    }

    #[test]
    fn test_choose_from_partial() {
        const ADDR: SocketAddr = generate_peer(1);
        let mut prp = PartialRequestedPiece {
            all_requested_before: 0,
            block_map: vec![
                BlockStatus::Received,
                BlockStatus::NotRequested,
                BlockStatus::NotRequested,
                BlockStatus::NotRequested,
            ],
        };
        let last_block_size = 401;
        let (br, n) = choose_blocks_from_a_partial_requested_piece(
            &ADDR,
            &mut prp,
            4,
            1,
            3 * BLOCK_SIZE + last_block_size,
        );
        // assert_eq!(from: Request { index: 1, begin: 16384, len: 16384 }, to: Request { index: 1, begin: 49152, len: 15983 } });
        assert_eq!(n, 3);
        assert_eq!(
            br.from,
            protocol::Request {
                index: 1,
                begin: BLOCK_SIZE,
                len: 16384
            }
        );
        assert_eq!(
            br.to,
            protocol::Request {
                index: 1,
                begin: 3 * BLOCK_SIZE,
                len: last_block_size,
            }
        );
    }

    #[test]
    fn test_peer_mark_not_requested() {
        let peer1 = generate_peer(1);
        let peer2 = generate_peer(2);
        let mut picker = HeapPiecePicker::new(16 * 16 * BLOCK_SIZE as usize, 16 * BLOCK_SIZE);
        picker.peer_add(peer1, BitField::from(vec![true; 16]));
        picker.pick_blocks(&peer1, 5);
        picker.peer_add(peer2, BitField::from(vec![true; 16]));
        picker.pick_blocks(&peer2, 13);
        check_picker_partial_and_fully(&picker, [1], [0]);

        picker.peer_mark_not_requested(&peer2);
        check_picker_partial_and_fully(&picker, [0], []);
        let blockmap = picker.partly_requested_pieces.0.get(&0).unwrap();
        assert_eq!(blockmap.all_requested_before, 5);
        for (i, b) in blockmap.block_map[0..5].iter().enumerate() {
            assert_eq!((i, b), (i, &BlockStatus::Requested(peer1)));
        }
        for (i, b) in blockmap.block_map[5..].iter().enumerate() {
            assert_eq!((i, b), (i, &BlockStatus::NotRequested));
        }
    }

    #[test]
    fn test_maintain_blockstatus() {
        let peer1 = generate_peer(1);
        let mut bs = PartialRequestedPiece {
            all_requested_before: 0,
            block_map: vec![BlockStatus::NotRequested; 16],
        };
        assert!(bs.is_none_requested());
        assert!(!bs.is_all_requested());

        bs.set_one_block_status_by_block_index(3, BlockStatus::Requested(peer1));
        assert!(!bs.is_all_requested());
        assert!(!bs.is_none_requested());

        for i in 0..3 {
            bs.set_one_block_status_by_block_index(i, BlockStatus::Requested(peer1));
        }

        // now 0,1,2,3 are requested
        assert!(!bs.is_all_requested());
        assert!(!bs.is_none_requested());
    }

    #[test]
    fn test_maintain_blockstatus2() {
        let peer1 = generate_peer(1);
        let bs = PartialRequestedPiece {
            all_requested_before: 0,
            block_map: vec![
                BlockStatus::NotRequested,
                BlockStatus::Requested(peer1),
                BlockStatus::NotRequested,
            ],
        };
        assert!(!bs.is_none_requested());
        assert!(!bs.is_all_requested());
    }

    #[test]
    fn test_maintain_blockstatus3() {
        let peer1 = generate_peer(1);
        let bs = PartialRequestedPiece {
            all_requested_before: 0,
            block_map: vec![BlockStatus::Requested(peer1), BlockStatus::Requested(peer1)],
        };
        assert!(!bs.is_none_requested());
        assert!(bs.is_all_requested());
    }

    #[test]
    fn test_maintain_blockstatus_map() {
        let peer1 = generate_peer(1);
        let peer2 = generate_peer(2);
        let mut bs = PartialRequestedPiece {
            all_requested_before: 0,
            block_map: vec![
                BlockStatus::NotRequested,
                BlockStatus::Requested(peer1),
                BlockStatus::Requested(peer2),
                BlockStatus::NotRequested,
            ],
        };
        assert!(!bs.is_none_requested());
        assert!(!bs.is_all_received());

        bs.map_block_status(|_, bs| {
            if bs == &BlockStatus::Requested(peer1) {
                BlockStatus::NotRequested
            } else {
                *bs
            }
        });
        assert_eq!(
            bs.block_map,
            vec![
                BlockStatus::NotRequested,
                BlockStatus::NotRequested,
                BlockStatus::Requested(peer2),
                BlockStatus::NotRequested,
            ]
        );
        assert!(!bs.is_none_requested());
        assert!(!bs.is_all_requested());

        bs.map_block_status(|_, bs| {
            if bs == &BlockStatus::Requested(peer2) {
                BlockStatus::NotRequested
            } else {
                *bs
            }
        });
        assert_eq!(
            bs.block_map,
            vec![
                BlockStatus::NotRequested,
                BlockStatus::NotRequested,
                BlockStatus::NotRequested,
                BlockStatus::NotRequested,
            ]
        );
        assert!(bs.is_none_requested());
        assert!(!bs.is_all_requested());
    }
}

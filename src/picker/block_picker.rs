use serde::{Deserialize, Serialize};
use tracing::info;

use super::{
    BitField, BlockRange, BlockRequests, PeerAddr, PeerPieceDetail, PieceMap, PiecePicker,
    PieceState,
};
use crate::{math_helper::piece_total_and_last_size, protocol::Request};
use std::{collections::BTreeMap, time};

const BLOCK_SIZE: usize = 16384;

fn two_mins_ago() -> time::Instant {
    time::Instant::now() - time::Duration::from_mins(2)
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone, Copy)]
pub enum BlockStatus {
    NotRequested,
    Requested {
        addr: PeerAddr,
        #[serde(skip)]
        #[serde(default = "two_mins_ago")]
        at: time::Instant,
    },
    Received, // TODO: maybe record which peer sends us this block?
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct PieceBlocks {
    piece_index: u32,

    // size of last_block, useful for last piece
    last_block_size: usize,

    /// piece index from 0 to all_requested_or_received_before(exclusive)
    /// are all requested
    all_request_or_received_before: usize,

    /// number of blocks are requested plus received
    requested_or_received_count: usize,

    /// number of blocks are received
    received_count: usize,

    block_map: Vec<BlockStatus>,
}

impl PieceBlocks {
    fn is_all_received(&mut self) -> bool {
        self.received_count == self.block_map.len()
    }

    fn is_all_not_requested(&self) -> bool {
        self.requested_or_received_count == 0
    }

    fn is_all_requested_or_received(&mut self) -> bool {
        self.requested_or_received_count == self.block_map.len()
    }

    /// try to pick n blocks, return blocks and how many blocks picked
    fn pick(&mut self, peer: PeerAddr, n: usize) -> Option<(BlockRange, usize)> {
        let mut from = None;
        let mut to = None;
        let mut count = 0;
        let n_blocks = self.block_map.len();

        for (i, b) in self
            .block_map
            .iter_mut()
            .enumerate()
            .skip(self.all_request_or_received_before)
        {
            if count >= n {
                break;
            }
            match b {
                BlockStatus::NotRequested => {
                    *b = BlockStatus::Requested {
                        addr: peer,
                        at: time::Instant::now(),
                    };
                    self.all_request_or_received_before = i + 1;
                    let req = Some(Request {
                        index: self.piece_index,
                        begin: (i * BLOCK_SIZE) as u32,
                        len: if i + 1 == n_blocks {
                            self.last_block_size as u32
                        } else {
                            BLOCK_SIZE as u32
                        },
                    });
                    if from.is_none() {
                        from = req;
                    } else {
                        to = req;
                    }
                    count += 1;
                    self.requested_or_received_count += 1;
                }
                BlockStatus::Requested { .. } => {
                    self.all_request_or_received_before = i + 1;
                }
                BlockStatus::Received => {
                    self.all_request_or_received_before = i + 1;
                }
            }
        }

        match (from, to) {
            (None, _) => {
                assert!(self.all_request_or_received_before == n_blocks || count == n);
                None
            }
            (Some(f), None) => Some((BlockRange { from: f, to: f }, count)),
            (Some(f), Some(t)) => Some((BlockRange { from: f, to: t }, count)),
        }
    }

    /// inform some block is received, returns
    fn receive(&mut self, req: Request) {
        // input req must be valid
        let b_index = (req.begin as usize) / BLOCK_SIZE;
        let b = &mut self.block_map[b_index];
        match b {
            BlockStatus::NotRequested => {
                self.received_count += 1;
                self.requested_or_received_count += 1;
            }
            BlockStatus::Requested { .. } => {
                self.received_count += 1;
            }
            BlockStatus::Received => {}
        }
        *b = BlockStatus::Received;
    }

    /// inform some block request is rejected or no response, and
    /// should be send to other peers
    fn revoke(&mut self, peer: &PeerAddr, req: Request) {
        let b_index = (req.begin as usize) / BLOCK_SIZE;
        let b = &mut self.block_map[b_index];
        #[cfg(test)]
        println!("{b:?}");
        match b {
            BlockStatus::Requested { addr, .. } if addr == peer => {
                // A peer can only be revoked if it's requested before
                *b = BlockStatus::NotRequested;
                if self.all_request_or_received_before > b_index {
                    self.all_request_or_received_before = b_index;
                }
                self.requested_or_received_count -= 1;
            }
            BlockStatus::Requested { .. } => {}
            BlockStatus::NotRequested => {}
            BlockStatus::Received => {}
        }
    }

    /// change state of `Requested` blocks to `NotRequested` if
    /// condition fulfils
    fn revoke_all_requested_if<F>(&mut self, cond: F)
    where
        F: Fn(&BlockStatus) -> bool,
    {
        for (i, b) in self.block_map.iter_mut().enumerate().rev() {
            match b {
                BlockStatus::Requested { .. } => {
                    if cond(b) {
                        *b = BlockStatus::NotRequested;
                        self.requested_or_received_count -= 1;
                        if self.all_request_or_received_before > i {
                            self.all_request_or_received_before = i;
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

type Picker = dyn PiecePicker<T = PeerPieceDetail> + Send;
type PieceIndex = u32;
pub struct BlockPicker {
    // total piece number
    n: usize,
    piece_size: usize,

    // last piece length
    last_length: usize,

    // piece picker,
    piece_picker: Box<Picker>,

    // pieces whose blocks are not all requested
    requesting: BTreeMap<PieceIndex, PieceBlocks>,

    // pieces whose blocks that all requested and waiting receiving
    receiving: BTreeMap<PieceIndex, PieceBlocks>,

    // previous time the timeout request check runs
    prev_time_check: time::Instant,

    no_response_timeout: time::Duration,
}

impl BlockPicker {
    pub fn new(
        total_size: usize,
        piece_size: usize,
        piece_picker: Box<Picker>,
        no_response_timeout: time::Duration,
    ) -> Self {
        let (n, last_length) = piece_total_and_last_size(total_size, piece_size);
        Self {
            n,
            piece_size,
            last_length,
            piece_picker,
            requesting: BTreeMap::new(),
            receiving: BTreeMap::new(),
            prev_time_check: time::Instant::now(),
            no_response_timeout,
        }
    }

    #[inline]
    fn n_blocks_and_last_block_size(&self, index: u32) -> (usize, usize) {
        let piece_size = if index as usize + 1 == self.n {
            self.last_length
        } else {
            self.piece_size
        };
        let n_blocks = (piece_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let last_block_size = BLOCK_SIZE - (n_blocks * BLOCK_SIZE - piece_size);
        (n_blocks, last_block_size)
    }

    #[inline]
    fn piece_block_of(&self, index: u32) -> PieceBlocks {
        let (n_blocks, last_block_size) = self.n_blocks_and_last_block_size(index);
        PieceBlocks {
            piece_index: index,
            last_block_size,
            all_request_or_received_before: 0,
            block_map: vec![BlockStatus::NotRequested; n_blocks],
            requested_or_received_count: 0,
            received_count: 0,
        }
    }

    /// Pick n blocks from peer, returns picked blocks and number of picked blocks
    pub fn pick_blocks(&mut self, peer: &PeerAddr, n: usize) -> (BlockRequests, usize) {
        if self.prev_time_check.elapsed() > self.no_response_timeout {
            self.revoke_unrespond(self.no_response_timeout);
            self.prev_time_check = time::Instant::now();
        }

        let mut remain = n;
        let peer_status = self
            .piece_picker
            .peer_detail(peer)
            .expect("the peer to pick block from should exist in piece_picker");

        let mut ret = Vec::new();
        for (index, blocks) in &mut self.requesting {
            if remain > 0 && peer_status.have(*index) {
                while let Some((blks, n_picked)) = blocks.pick(*peer, remain) {
                    remain -= n_picked;
                    ret.push(blks);
                }
            }
        }

        while remain > 0 {
            if let Some(index) = self.piece_picker.pick_next(peer) {
                let mut blocks = self.piece_block_of(index);

                if let Some((blks, n_picked)) = blocks.pick(*peer, remain) {
                    remain -= n_picked;
                    ret.push(blks);
                }
                self.requesting.insert(index, blocks);
            } else {
                break;
            }
        }

        for (index, blocks) in self.requesting.iter_mut() {
            if blocks.is_all_requested_or_received() {
                self.receiving.insert(*index, blocks.clone());
            }
        }
        self.requesting
            .retain(|_, b| !b.is_all_requested_or_received());

        (
            BlockRequests {
                piece_size: self.piece_size as u32,
                range: ret,
            },
            n - remain,
        )
    }

    /// Call then some block request is rejected, and request for that block
    /// should be send to other peers again.
    pub fn peer_reject_block(&mut self, peer: &PeerAddr, req: Request) {
        if let Some(b) = self.receiving.get_mut(&req.index) {
            b.revoke(peer, req);
            if !b.is_all_received() {
                self.requesting.insert(req.index, b.clone());
                self.receiving.remove(&req.index);
            }
        } else if let Some(b) = self.requesting.get_mut(&req.index) {
            b.revoke(peer, req);
            if b.is_all_not_requested() {
                self.piece_picker.set_have(req.index, false);
                self.requesting.remove(&req.index);
            }
        } else {
            info!("revoke not requested/already checked piece");
        }
    }

    fn check_block_validity(&self, req: &Request) -> bool {
        let index = req.index;
        if index >= (self.n as u32) {
            info!("invalid req index {index} > {}", self.n);
            return false;
        }
        if req.begin % (BLOCK_SIZE as u32) != 0 {
            info!("invalid req begin");
            return false;
        }
        let (n_blocks, last_block_size) = self.n_blocks_and_last_block_size(index);
        let expect_len = if req.begin / (BLOCK_SIZE as u32) + 1 == n_blocks as u32 {
            last_block_size
        } else {
            BLOCK_SIZE
        };
        if req.len != expect_len as u32 {
            info!("invalid req len expect {expect_len} get {}", req.len);
            return false;
        }
        true
    }
    /// Called when a piece is received, returns if a piece is fully received
    pub fn receive_block(&mut self, req: Request) -> Option<u32> {
        if !self.check_block_validity(&req) {
            return None;
        }

        let index = req.index;
        if let Some(b) = self.receiving.get_mut(&index) {
            b.receive(req);
            b.is_all_received().then(|| index)
        } else if let Some(b) = self.requesting.get_mut(&index) {
            b.receive(req);
            if b.is_all_received() {
                self.requesting.remove(&index);
                Some(index)
            } else {
                if b.is_all_requested_or_received() {
                    self.receiving.insert(index, b.clone());
                    self.requesting.remove(&index);
                }
                None
            }
        } else if self.piece_picker.selected(index) && !self.piece_picker.have(index) {
            let mut b = self.piece_block_of(index);
            b.receive(req);
            // only receive one block must be partial requested
            self.requesting.insert(index, b);
            None
        } else {
            // blocks we didn't select or already have
            None
        }
    }

    /// Call when some piece is verified
    pub fn piece_verified(&mut self, index: u32, success: bool) {
        self.receiving.remove(&index);
        self.requesting.remove(&index);
        self.piece_picker.set_have(index, success);
    }

    /// Mark blocks as `NotRequested` if they are `Requested` and did not respond
    /// longer than timeout
    fn revoke_unrespond(&mut self, timeout: time::Duration) {
        let no_response = |b: &BlockStatus| match b {
            BlockStatus::Requested { at, .. } => at.elapsed() > timeout,
            _ => false,
        };
        for (index, blocks) in self.receiving.iter_mut() {
            blocks.revoke_all_requested_if(no_response);
            if !blocks.is_all_requested_or_received() {
                self.requesting.insert(*index, blocks.clone());
            }
        }
        self.receiving
            .retain(|_, b| b.is_all_requested_or_received());

        for (index, blocks) in self.requesting.iter_mut() {
            blocks.revoke_all_requested_if(no_response);
            if blocks.is_all_not_requested() {
                self.piece_picker.set_have(*index, false);
            }
        }
        self.requesting.retain(|_, b| !b.is_all_not_requested());
    }

    /// check if we want this block, if want, returns previous
    /// state of this block
    pub fn want_block(&mut self, req: Request) -> Option<BlockStatus> {
        if !self.check_block_validity(&req) {
            println!("unwant because invalid {req:?}");
            return None;
        }

        let index = req.index;
        if !self.selected(index) {
            println!("unwant because {index} not selected");
            return None;
        }

        if self.have(index) {
            println!("unwant because have {index}");
            return None;
        }

        if let Some(b) = self.requesting.get(&index) {
            let s = &b.block_map[req.begin as usize / BLOCK_SIZE];
            match s {
                BlockStatus::NotRequested | BlockStatus::Requested { .. } => {
                    return Some(*s);
                }
                BlockStatus::Received => {
                    println!("unwant because received");
                    return None;
                }
            }
        }

        if let Some(b) = self.receiving.get(&index) {
            let s = &b.block_map[req.begin as usize / BLOCK_SIZE];
            match s {
                BlockStatus::Requested { .. } => {
                    return Some(*s);
                }
                BlockStatus::Received => {
                    println!("unwant because received2");
                    return None;
                }
                _ => unreachable!(),
            }
        }

        // we selected, but we did not request it, or we mark this block as
        // not requested because of timeout
        return Some(BlockStatus::NotRequested);
    }

    /// Are all selected pieces downloaded and verified?
    pub fn is_finished(&mut self) -> bool {
        self.receiving.is_empty() && self.requesting.is_empty() && self.piece_picker.is_finished()
    }

    /// dump current status
    pub fn dump(&mut self) -> BlockPickerDump {
        BlockPickerDump {
            receiving: self.receiving.clone(),
            requesting: self.requesting.clone(),
            piece_map: self.piece_picker.dump(),
            no_response_timeout: self.no_response_timeout,
        }
    }

    /// load progress
    pub fn load_progress(&mut self, dump: BlockPickerDump) {
        self.receiving = dump.receiving;
        self.requesting = dump.requesting;
        self.piece_picker.load(dump.piece_map);
        self.no_response_timeout = dump.no_response_timeout;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockPickerDump {
    // pieces whose blocks are not all requested
    pub requesting: BTreeMap<PieceIndex, PieceBlocks>,

    // pieces whose blocks that all requested and waiting receiving
    pub receiving: BTreeMap<PieceIndex, PieceBlocks>,

    pub piece_map: PieceMap,

    pub no_response_timeout: time::Duration,
}

impl BlockPicker {
    pub fn n_pieces(&self) -> usize {
        self.n
    }

    pub fn peer_add(&mut self, addr: PeerAddr, state: PieceState) {
        self.piece_picker.peer_add(addr, state);
    }

    pub fn peer_leave(&mut self, addr: &PeerAddr) {
        self.piece_picker.peer_leave(addr);
    }

    pub fn peer_choke(&mut self, addr: &PeerAddr) {
        let requested_peer = |b: &BlockStatus| match b {
            BlockStatus::Requested { addr: peer, .. } => addr == peer,
            _ => false,
        };

        for (i, b) in self.receiving.iter_mut() {
            b.revoke_all_requested_if(requested_peer);
            if !b.is_all_requested_or_received() {
                self.requesting.insert(*i, b.clone());
            }
        }
        self.receiving
            .retain(|_, b| b.is_all_requested_or_received());

        for (i, b) in self.requesting.iter_mut() {
            b.revoke_all_requested_if(requested_peer);
            if b.is_all_not_requested() {
                self.piece_picker.set_have(*i, false);
            }
        }
        self.requesting.retain(|_, b| !b.is_all_not_requested());
    }

    pub fn peer_unchoke(&mut self, addr: &PeerAddr) {
        self.piece_picker.peer_unchoke(addr);
    }

    /// called with peer send a HAVE to us
    pub fn peer_new_have(&mut self, addr: &PeerAddr, index: u32) {
        self.piece_picker.peer_new_have(addr, index);
    }

    /// change the selected piece set
    pub fn select(&mut self, index: u32, selected: bool) {
        self.piece_picker.select(index, selected);
        if !selected {
            self.receiving.remove(&index);
            self.requesting.remove(&index);
        }
    }

    /// returns if we want this piece(piece in selected set)
    pub fn selected(&self, index: u32) -> bool {
        self.piece_picker.selected(index)
    }

    /// returns all selected pieces
    pub fn selected_pieces(&self) -> &BitField {
        self.piece_picker.selected_pieces()
    }

    /// returns if we have this piece
    pub fn have(&self, index: u32) -> bool {
        self.piece_picker.have(index)
            && !self.receiving.contains_key(&index)
            && !self.requesting.contains_key(&index)
    }

    /// set we have this piece
    pub fn set_have(&mut self, index: u32, have: bool) {
        self.piece_picker.set_have(index, have);
        if have {
            self.receiving.remove(&index);
            self.requesting.remove(&index);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::picker::BitField;

    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    const PEER1: PeerAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1);
    const PEER2: PeerAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2);
    const PEER3: PeerAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3);

    #[test]
    fn test_block_pieces() {
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: 4133,
            all_request_or_received_before: 0,
            requested_or_received_count: 0,
            received_count: 0,
            block_map: vec![BlockStatus::NotRequested; 50],
        };

        {
            let picked = b.pick(PEER1, 30);
            let exp = Some((
                BlockRange {
                    from: Request {
                        index: 0,
                        begin: 0,
                        len: 16384,
                    },
                    to: Request {
                        index: 0,
                        begin: 29 * 16384,
                        len: 16384,
                    },
                },
                30,
            ));
            assert_eq!(picked, exp);
            assert_eq!(b.all_request_or_received_before, 30);
            assert_eq!(b.requested_or_received_count, 30);
        }
        {
            let picked = b.pick(PEER1, 30);
            let exp = Some((
                BlockRange {
                    from: Request {
                        index: 0,
                        begin: 30 * 16384,
                        len: 16384,
                    },
                    to: Request {
                        index: 0,
                        begin: 49 * 16384,
                        len: 4133,
                    },
                },
                20,
            ));
            assert_eq!(picked, exp);
            assert_eq!(b.all_request_or_received_before, 50);
            assert_eq!(b.requested_or_received_count, 50);
            assert!(b.is_all_requested_or_received());
            assert!(!b.is_all_received());
        }
        {
            // test receive
            for i in 0..10 {
                b.receive(Request {
                    index: 0,
                    begin: i * 16384,
                    len: 16384,
                });
            }
            b.receive(Request {
                index: 0,
                begin: 15 * 16384,
                len: 16384,
            });
            assert_eq!(b.received_count, 11);
            assert_eq!(b.requested_or_received_count, 50);
        }
        {
            // test revoke
            b.revoke(
                &PEER1,
                Request {
                    index: 0,
                    begin: 17 * 16384,
                    len: 16384,
                },
            );
            assert!(!b.is_all_requested_or_received());
            assert!(!b.is_all_received());
            assert_eq!(b.all_request_or_received_before, 17);
            assert_eq!(b.received_count, 11);
            assert_eq!(b.requested_or_received_count, 49);
        }
        {
            // test revoke others should fail
            b.revoke(
                &PEER2,
                Request {
                    index: 0,
                    begin: 32 * 16384,
                    len: 16384,
                },
            );
            assert!(!b.is_all_requested_or_received());
            assert!(!b.is_all_received());
            assert_eq!(b.all_request_or_received_before, 17);
            assert_eq!(b.received_count, 11);
            assert_eq!(b.requested_or_received_count, 49);
        }
        {
            b.revoke_all_requested_if(|b| match b {
                BlockStatus::Requested { .. } => true,
                _ => false,
            });
            assert!(!b.is_all_requested_or_received());
            assert_eq!(b.all_request_or_received_before, 10);
            assert_eq!(b.received_count, 11);
            assert_eq!(b.requested_or_received_count, 11);
        }
    }

    #[test]
    fn test_block_picker_pick_select() {
        use crate::picker::RarestPicker;
        const PIECE_SIZE: usize = 16384 * 10;
        const TOTAL_SIZE: usize = 16384 * 10 * 10 + 1500;
        let p = Box::new(RarestPicker::new(TOTAL_SIZE, PIECE_SIZE));
        let mut b = BlockPicker::new(TOTAL_SIZE, PIECE_SIZE, p, time::Duration::from_secs(10));
        b.peer_add(PEER1, PieceState::HaveAll);
        b.peer_add(
            PEER2,
            PieceState::Bitfield(BitField::from(vec![true, false, false, true, true, true])),
        );
        // b.peer_add(
        //     PEER3,
        //     PieceState::Bitfield(BitField::from(vec![false, false, false, true, true, true])),
        // );
        for i in 0..6 {
            b.select(i, true);
        }

        {
            let picked = b.pick_blocks(&PEER1, 15);
            let exp = BlockRequests {
                piece_size: PIECE_SIZE as u32,
                range: vec![
                    BlockRange {
                        from: Request {
                            index: 1,
                            begin: 0,
                            len: 16384,
                        },
                        to: Request {
                            index: 1,
                            begin: 9 * 16384,
                            len: 16384,
                        },
                    },
                    BlockRange {
                        from: Request {
                            index: 2,
                            begin: 0,
                            len: 16384,
                        },
                        to: Request {
                            index: 2,
                            begin: 4 * 16384,
                            len: 16384,
                        },
                    },
                ],
            };
            assert_eq!(picked.0, exp);
            assert_eq!(picked.1, 15);
        }

        // test pick2
        {
            let picked = b.pick_blocks(&PEER2, 1);
            let exp = BlockRequests {
                piece_size: PIECE_SIZE as u32,
                range: vec![BlockRange {
                    from: Request {
                        index: 0,
                        begin: 0,
                        len: 16384,
                    },
                    to: Request {
                        index: 0,
                        begin: 0,
                        len: 16384,
                    },
                }],
            };
            assert_eq!(picked.0, exp);
        }

        // test un-select
        {
            b.select(2, false);
            let picked = b.pick_blocks(&PEER1, 5);
            let exp = BlockRequests {
                piece_size: PIECE_SIZE as u32,
                range: vec![BlockRange {
                    from: Request {
                        index: 0,
                        begin: 16384,
                        len: 16384,
                    },
                    to: Request {
                        index: 0,
                        begin: 5 * 16384,
                        len: 16384,
                    },
                }],
            };
            assert_eq!(picked.0, exp);
            assert!(!b.receiving.contains_key(&2));
            assert!(!b.requesting.contains_key(&2));
            assert!(!b.selected(2));
        }
    }

    fn test_block_picker_choke_unchoke() {
        // b.peer_leave(addr);
        // b.peer_choke(addr);
        // b.peer_unchoke(addr);
        // b.peer_new_have(addr, index);
        // b.piece_verified();
        // b.piece_verified();
        // b.check_block_validity(req)
        // b.have(index);
        // b.is_finished();
    }
}

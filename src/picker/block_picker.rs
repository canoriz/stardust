use serde::{Deserialize, Serialize};
use tracing::{info, trace};

use super::{
    BitField, BlockRange, BlockRequests, PeerAddr, PeerPieceDetail, PieceMap, PiecePicker,
    PieceState,
};
use crate::{
    cache::simple_buffer::{JointIndex, POOL_SIZE, SUB_PIECE_SIZE},
    math_helper::piece_total_and_last_size,
    protocol::Request,
};
use std::{
    collections::{BTreeMap, HashMap},
    time,
};

const BLOCK_SIZE: usize = 16384;
const NO_RESPONSE_TIMEOUT: time::Duration = time::Duration::from_secs(90);

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PickedDetail {
    pub pick_time: time::Instant,
    pub n_in_flight_when_picked: usize,
    pub expected_response_time: time::Duration,
}

impl PickedDetail {
    fn new(
        pick_time: time::Instant,
        n_in_flight_when_picked: usize,
        avg_speed: f32,
        rtt: time::Duration,
    ) -> Self {
        Self {
            pick_time,
            n_in_flight_when_picked,
            expected_response_time: expected_response_time(avg_speed, n_in_flight_when_picked, rtt),
        }
    }

    fn expected_recv_at(&self) -> time::Instant {
        self.pick_time + self.expected_response_time
    }
}

fn expected_response_time(
    avg_speed: f32,
    n_in_flight: usize,
    rtt: time::Duration,
) -> time::Duration {
    if avg_speed <= f32::EPSILON {
        return NO_RESPONSE_TIMEOUT;
    }

    (rtt + time::Duration::from_secs_f32(((n_in_flight + 1) * 16384) as f32 / avg_speed))
        .min(NO_RESPONSE_TIMEOUT)
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub enum BlockStatus {
    NotRequested {
        #[serde(skip)]
        revoked: HashMap<PeerAddr, PickedDetail>,
    },
    Requested {
        // TODO: when serializing, make all requested state to NotRequested
        // avoid canceling not requested blocks
        #[serde(skip)]
        requested: HashMap<PeerAddr, PickedDetail>,

        // blocks that are requested but revoked(because of timeout or have received from other peer)
        #[serde(skip)]
        revoked: HashMap<PeerAddr, PickedDetail>,
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

    /// sub pieces receive count
    sub_receive_count: Vec<usize>,
}

impl PieceBlocks {
    fn is_all_received(&self) -> bool {
        self.received_count == self.block_map.len()
    }

    /// check if the sub piece of the "in_piece_offset" is completed
    fn is_sub_all_received(&self, in_piece_offset: u32) -> bool {
        let index = (in_piece_offset / SUB_PIECE_SIZE) as usize;
        // Convert the sub-piece byte range to block-index range.
        let blocks_per_sub = (SUB_PIECE_SIZE as usize) / BLOCK_SIZE;
        let from = index * blocks_per_sub;
        let to = ((index + 1) * blocks_per_sub).min(self.block_map.len());
        self.sub_receive_count[index] == to - from
    }

    fn is_all_not_requested(&self) -> bool {
        self.requested_or_received_count == 0
    }

    fn is_all_requested_or_received(&mut self) -> bool {
        self.requested_or_received_count == self.block_map.len()
    }

    /// Reset all Requested blocks to NotRequested, updating bookkeeping.
    /// Returns true if any block was changed.
    /// Used on session restore: after loading a dump, `requested` maps inside
    /// Requested variants are empty (skipped by serde), so we must treat those
    /// blocks as not yet requested.
    fn reset_requested_blocks(&mut self) -> bool {
        let mut changed = false;
        for block in self.block_map.iter_mut() {
            if matches!(block, BlockStatus::Requested { .. }) {
                *block = BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                };
                self.requested_or_received_count -= 1;
                changed = true;
            }
        }
        if changed {
            // Recompute frontier: how many leading blocks are all Received
            self.all_request_or_received_before = self
                .block_map
                .iter()
                .take_while(|b| matches!(b, BlockStatus::Received))
                .count();
        }
        changed
    }

    /// Try to pick n blocks, return blocks and how many blocks picked.
    /// If in endgame mode, take a duplicate request limit, we want blocks
    /// requested count is evenly distributed, e.g. A and B block are both
    /// requested from 3 peers, not A block requested 5 peers
    /// while B from only 1 peer.
    /// TODO: optimize: if we really can not pick any block (all sent requests
    /// to this peer), then notify caller to fail soon
    fn pick(
        &mut self,
        peer: PeerAddr,
        n: usize,
        n_in_flight: &mut usize,
        avg_speed: f32,
        rtt: time::Duration,
        repick_option: RepickOption,
    ) -> Option<(BlockRange, usize)> {
        let mut from = None;
        let mut to = None;
        let mut count = 0;
        let n_blocks = self.block_map.len();

        let repick_limit = repick_option.repick_limit;
        if repick_limit > 1 || repick_option.endgame {
            for (i, b) in self.block_map.iter_mut().enumerate() {
                if count >= n {
                    break;
                }
                let req = Some(Request {
                    index: self.piece_index,
                    begin: (i * BLOCK_SIZE) as u32,
                    len: if i + 1 == n_blocks {
                        self.last_block_size as u32
                    } else {
                        BLOCK_SIZE as u32
                    },
                });
                match b {
                    BlockStatus::NotRequested { revoked } => {
                        let now = time::Instant::now();
                        // TODO: todo!("does this really happen in endgame mode?");
                        *b = BlockStatus::Requested {
                            requested: HashMap::from([(
                                peer,
                                PickedDetail::new(now, *n_in_flight, avg_speed, rtt),
                            )]),
                            revoked: revoked.clone(), // TODO: optimize clone
                        };
                        *n_in_flight += 1;
                        self.all_request_or_received_before = i + 1;
                        if from.is_none() {
                            from = req;
                        } else {
                            to = req;
                        }
                        count += 1;
                        self.requested_or_received_count += 1;
                    }
                    BlockStatus::Requested {
                        requested, revoked, ..
                    } => {
                        if self.all_request_or_received_before <= i {
                            self.all_request_or_received_before = i + 1;
                        }

                        // if number of requests that are in-flight and not timeout-ed are
                        // less than repick limit, request a new one
                        const REPICK_NO_RESPONSE_TIMEOUT_CAP: time::Duration =
                            time::Duration::from_secs(5);
                        let now = time::Instant::now();
                        let should_repick = requested.values().all(|detail| {
                            // repick block in 2 cases
                            // 1. this block is picked by known slow peers
                            // 2. peer should be fast, but are slow in reality
                            detail.expected_response_time > time::Duration::from_millis(2000)
                                || now.duration_since(detail.pick_time)
                                    > REPICK_NO_RESPONSE_TIMEOUT_CAP
                        });
                        if !requested.contains_key(&peer)
                            && (should_repick || repick_option.endgame)
                        {
                            if should_repick {
                                trace!(
                                    "{peer} repick {req:?} because we are faster {:?}",
                                    requested
                                        .iter()
                                        .map(|(p, t)| (p, t.pick_time.elapsed()))
                                        .collect::<Vec<_>>()
                                );
                            }
                            if repick_option.endgame {
                                trace!("{peer} in endgame mode, repick {req:?}",);
                            }
                            count += 1;
                            requested
                                .insert(peer, PickedDetail::new(now, *n_in_flight, avg_speed, rtt));
                            *n_in_flight += 1;
                            if from.is_none() {
                                from = req;
                            } else {
                                to = req;
                            }
                        } else if from.is_some() {
                            // not continuous, should break
                            break;
                        }
                    }
                    BlockStatus::Received => {
                        if self.all_request_or_received_before <= i {
                            self.all_request_or_received_before = i + 1;
                        }
                        if from.is_some() {
                            // not continuous, should break
                            break;
                        }
                    }
                }
            }
        } else {
            for (i, b) in self
                .block_map
                .iter_mut()
                .enumerate()
                .skip(self.all_request_or_received_before)
            {
                if count >= n {
                    break;
                }
                let req = Some(Request {
                    index: self.piece_index,
                    begin: (i * BLOCK_SIZE) as u32,
                    len: if i + 1 == n_blocks {
                        self.last_block_size as u32
                    } else {
                        BLOCK_SIZE as u32
                    },
                });
                match b {
                    BlockStatus::NotRequested { revoked } => {
                        let pick_time = time::Instant::now();
                        *b = BlockStatus::Requested {
                            requested: HashMap::from([(
                                peer,
                                PickedDetail::new(pick_time, *n_in_flight, avg_speed, rtt),
                            )]),
                            revoked: revoked.clone(),
                        };
                        *n_in_flight += 1;
                        if from.is_none() {
                            from = req;
                        } else {
                            to = req;
                        }
                        count += 1;
                        self.requested_or_received_count += 1;
                        self.all_request_or_received_before = i + 1;
                    }
                    BlockStatus::Requested { requested, .. } => {
                        self.all_request_or_received_before = i + 1;
                        if requested.is_empty() {
                            *n_in_flight += 1;
                            if from.is_none() {
                                from = req;
                            } else {
                                to = req;
                            }
                            count += 1;
                        } else if from.is_some() {
                            // not continuous, should break
                            break;
                        }
                    }
                    BlockStatus::Received => {
                        self.all_request_or_received_before = i + 1;
                        if from.is_some() {
                            // not continuous, should break
                            break;
                        }
                    }
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

    /// inform some block is received, returns all being requested peers
    /// requested for that block
    fn receive(&mut self, req: Request) -> Vec<PeerAddr> {
        // input req must be valid
        let b_index = (req.begin as usize) / BLOCK_SIZE;
        let b = &mut self.block_map[b_index];
        match b {
            BlockStatus::NotRequested { .. } => {
                self.received_count += 1;
                self.requested_or_received_count += 1;
                self.sub_receive_count[(b_index * BLOCK_SIZE) / (SUB_PIECE_SIZE as usize)] += 1;
                *b = BlockStatus::Received;
                vec![]
            }
            BlockStatus::Requested { requested, .. } => {
                self.received_count += 1;
                self.sub_receive_count[(b_index * BLOCK_SIZE) / (SUB_PIECE_SIZE as usize)] += 1;
                let ret = requested.keys().map(|x| *x).collect();
                *b = BlockStatus::Received;
                ret
            }
            BlockStatus::Received => vec![],
        }
    }

    /// inform some block request is rejected or no response, and
    /// should be send to other peers
    fn revoke(&mut self, peer: &PeerAddr, req: Request) {
        let b_index = (req.begin as usize) / BLOCK_SIZE;
        let b = &mut self.block_map[b_index];
        #[cfg(test)]
        println!("{b:?}");
        match b {
            BlockStatus::Requested { requested, revoked } => {
                // A peer can only be revoked if it's requested before
                if let Some(v) = requested.remove(peer) {
                    revoked.insert(*peer, v);
                }
                revoked.retain(|_, t| t.pick_time.elapsed() < NO_RESPONSE_TIMEOUT);
                if requested.is_empty() {
                    *b = BlockStatus::NotRequested {
                        revoked: revoked.clone(),
                    };
                    if self.all_request_or_received_before > b_index {
                        self.all_request_or_received_before = b_index;
                    }
                    self.requested_or_received_count -= 1;
                }
            }
            BlockStatus::NotRequested { revoked } => {
                revoked.retain(|_, t| t.pick_time.elapsed() < NO_RESPONSE_TIMEOUT);
            }
            BlockStatus::Received => {}
        }
    }

    /// revoke request of one peer for all `Requested` which fulfils condition
    fn revoke_all_requested_if<F>(
        &mut self,
        remove: F,
        revoked_reqs: &mut HashMap<PeerAddr, Vec<Request>>,
    ) where
        F: Fn(&PeerAddr, &time::Instant) -> bool,
    {
        let n_blocks = self.block_map.len();
        for (i, b) in self.block_map.iter_mut().enumerate().rev() {
            let req = Request {
                index: self.piece_index,
                begin: (i * BLOCK_SIZE) as u32,
                len: if i + 1 == n_blocks {
                    self.last_block_size
                } else {
                    BLOCK_SIZE
                } as u32,
            };
            match b {
                BlockStatus::Requested {
                    requested, revoked, ..
                } => {
                    requested.retain(|p, t| {
                        if remove(p, &t.pick_time) {
                            info!(
                                "revoke block {req:?} from {p}, after {:?}",
                                t.pick_time.elapsed()
                            );
                            revoked_reqs
                                .entry(*p)
                                .and_modify(|r| r.push(req))
                                .or_insert(vec![req]);
                            revoked.entry(*p).or_insert(t.clone());
                            false
                        } else {
                            true
                        }
                    });
                    revoked.retain(|_, t| t.pick_time.elapsed() < NO_RESPONSE_TIMEOUT);
                    if requested.is_empty() {
                        *b = BlockStatus::NotRequested {
                            revoked: revoked.clone(),
                        };
                        self.requested_or_received_count -= 1;
                        if self.all_request_or_received_before > i {
                            self.all_request_or_received_before = i;
                        }
                    }
                }
                BlockStatus::NotRequested { revoked } => {
                    revoked.retain(|_, t| t.pick_time.elapsed() < NO_RESPONSE_TIMEOUT);
                }
                _ => {}
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct RepickOption {
    // The upper limit of how many times a block may be requested from
    // different peers
    repick_limit: usize,

    // if in endgame mode
    endgame: bool,
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

    endgame: bool,
}

/// if some piece or sub piece is completed
pub struct PieceComplete {
    /// if piece completes
    pub piece: bool,
    /// if sub piece completes
    pub sub_piece: bool,
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
            endgame: false,
        }
    }

    #[inline]
    fn n_blocks_and_last_block_size(&self, index: u32) -> (usize, usize) {
        let piece_size = self.piece_size(index);
        let n_blocks = (piece_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let last_block_size = BLOCK_SIZE - (n_blocks * BLOCK_SIZE - piece_size);
        (n_blocks, last_block_size)
    }

    #[inline]
    pub fn piece_size(&self, index: u32) -> usize {
        if index as usize + 1 == self.n {
            self.last_length
        } else {
            self.piece_size
        }
    }

    #[inline]
    fn piece_block_of(&self, index: u32) -> PieceBlocks {
        let (n_blocks, last_block_size) = self.n_blocks_and_last_block_size(index);
        let sub_sz = SUB_PIECE_SIZE as usize;
        PieceBlocks {
            piece_index: index,
            last_block_size,
            all_request_or_received_before: 0,
            block_map: vec![
                BlockStatus::NotRequested {
                    revoked: HashMap::new()
                };
                n_blocks
            ],
            requested_or_received_count: 0,
            received_count: 0,
            sub_receive_count: vec![0; (n_blocks * BLOCK_SIZE + sub_sz - 1) / sub_sz],
        }
    }

    pub fn get_block_status(&self, req: &Request) -> Option<&BlockStatus> {
        if !self.check_block_validity(&req) {
            return None;
        }

        if let Some(b) = self.requesting.get(&req.index) {
            Some(&b.block_map[req.begin as usize / BLOCK_SIZE])
        } else if let Some(b) = self.receiving.get(&req.index) {
            Some(&b.block_map[req.begin as usize / BLOCK_SIZE])
        } else {
            None
        }
    }

    pub fn get_rtt(
        &self,
        peer: &PeerAddr,
        req: &Request,
        recv_time: time::Instant,
    ) -> Option<time::Duration> {
        if let Some(b) = self.get_block_status(req) {
            match b {
                BlockStatus::NotRequested { revoked } => {
                    if let Some(p) = revoked.get(peer) {
                        Some(recv_time.duration_since(p.pick_time))
                    } else {
                        None
                    }
                }
                BlockStatus::Requested { requested, revoked } => {
                    if let Some(p) = revoked.get(peer) {
                        Some(recv_time.duration_since(p.pick_time))
                    } else if let Some(p) = requested.get(peer) {
                        Some(recv_time.duration_since(p.pick_time))
                    } else {
                        None
                    }
                }
                BlockStatus::Received => None,
            }
        } else {
            None
        }
    }

    pub fn get_inflight_when_sent(&self, peer: &PeerAddr, req: &Request) -> Option<usize> {
        if let Some(b) = self.get_block_status(req) {
            match b {
                BlockStatus::NotRequested { revoked } => {
                    if let Some(p) = revoked.get(peer) {
                        Some(p.n_in_flight_when_picked)
                    } else {
                        None
                    }
                }
                BlockStatus::Requested { requested, revoked } => {
                    if let Some(p) = revoked.get(peer) {
                        Some(p.n_in_flight_when_picked)
                    } else if let Some(p) = requested.get(peer) {
                        Some(p.n_in_flight_when_picked)
                    } else {
                        None
                    }
                }
                BlockStatus::Received => None,
            }
        } else {
            None
        }
    }

    pub fn get_expected_response_time(
        &self,
        peer: &PeerAddr,
        req: &Request,
    ) -> Option<time::Duration> {
        if let Some(b) = self.get_block_status(req) {
            match b {
                BlockStatus::NotRequested { revoked } => {
                    revoked.get(peer).map(|p| p.expected_response_time)
                }
                BlockStatus::Requested { requested, revoked } => revoked
                    .get(peer)
                    .or_else(|| requested.get(peer))
                    .map(|p| p.expected_response_time),
                BlockStatus::Received => None,
            }
        } else {
            None
        }
    }

    /// Pick n blocks from peer, returns
    /// (
    ///  picked blocks,
    ///  number of picked blocks,
    ///  no response blocks
    /// )
    pub fn pick_blocks(
        &mut self,
        peer: &PeerAddr,
        n: usize,
        mut n_in_flight: usize,
        avg_speed: f32,
        rtt: time::Duration,
        revoked: &mut HashMap<PeerAddr, Vec<Request>>,
        n_cache_vacant: usize,
    ) -> (BlockRequests, usize) {
        if self.prev_time_check.elapsed() >= time::Duration::from_secs(1) {
            self.revoke_unrespond(revoked);
            self.prev_time_check = time::Instant::now();
        }

        let rush_mode = n_cache_vacant < POOL_SIZE / 2;
        let endgame = self.update_endgame();
        let repick_option = if endgame {
            RepickOption {
                repick_limit: 1,
                endgame: true,
            }
        } else if rush_mode {
            RepickOption {
                repick_limit: 7, // TODO: set a proper repick limit
                endgame: false,
            }
        } else {
            RepickOption {
                repick_limit: 1,
                endgame: false,
            }
        };
        info!("{peer} endgame {endgame}, rush {}", rush_mode);

        let mut remain = n;
        let peer_status = if let Some(h) = self.piece_picker.peer_detail(peer) {
            h
        } else {
            info!("pick_blocks: peer {peer} not registered in piece_picker, skipping");
            return (
                BlockRequests {
                    piece_size: self.piece_size as u32,
                    range: Vec::new(),
                },
                0,
            );
        };

        // pick blocks starting from pieces that have fewest block not requested
        let piece_index_order = |m: &BTreeMap<u32, PieceBlocks>| {
            let mut piece_order = m
                .iter()
                .map(|(i, b)| (*i, b.received_count))
                .collect::<Vec<_>>();
            piece_order.sort_by(|(_, recv_a), (_, recv_b)| recv_a.cmp(recv_b).reverse());
            piece_order
        };

        // TODO: reuse vector
        let mut ret = Vec::new();
        let pick_blocks_from_requesting =
            |ret: &mut Vec<BlockRange>,
             pieces: &mut BTreeMap<u32, PieceBlocks>,
             peer_status: &PeerPieceDetail,
             remain: &mut usize,
             n_in_flight: &mut usize,
             avg_speed: f32,
             rtt: time::Duration,
             piece_size: usize| {
                for index in piece_index_order(pieces).iter().map(|(i, _)| i) {
                    let blocks = &mut pieces.get_mut(index).expect("must exist");
                    assert!(!endgame);
                    if *remain <= 0 {
                        break;
                    }
                    if peer_status.have(*index) {
                        while let Some((blks, n_picked)) =
                            blocks.pick(*peer, *remain, n_in_flight, avg_speed, rtt, repick_option)
                        {
                            *remain -= n_picked;
                            let pb: Vec<_> = blks.iter(piece_size as u32).collect();
                            trace!("pick piece {index} from peer {peer} (requesting), picked blks: {pb:?}");
                            ret.push(blks);
                        }
                    }
                }
            };

        // if in normal more, pick requesting pieces
        // if in rush mode, pick receiving pieces first, then requesting pieces, try to finish pieces asap
        if !rush_mode {
            pick_blocks_from_requesting(
                &mut ret,
                &mut self.requesting,
                &peer_status,
                &mut remain,
                &mut n_in_flight,
                avg_speed,
                rtt,
                self.piece_size,
            );
            for (index, blocks) in self.requesting.iter_mut() {
                if blocks.is_all_requested_or_received() {
                    self.receiving.insert(*index, blocks.clone());
                }
            }
            self.requesting
                .retain(|_, b| !b.is_all_requested_or_received());
        }

        let endgame = self.update_endgame();
        let peer_status = if let Some(h) = self.piece_picker.peer_detail(peer) {
            h
        } else {
            info!("pick_blocks: peer {peer} not registered in piece_picker after initial picks; returning {} picked", n - remain);
            return (
                BlockRequests {
                    piece_size: self.piece_size as u32,
                    range: ret,
                },
                n - remain,
            );
        };

        if remain > 0 && endgame {
            // the fewest number of peers we have requested for a given block among all receiving blocks
            let from = self
                .receiving
                .iter()
                .filter_map(|(_, p)| {
                    p.block_map
                        .iter()
                        .filter_map(|b| match b {
                            BlockStatus::NotRequested { .. } => Some(0),
                            BlockStatus::Requested { requested, .. } => Some(requested.len()),
                            BlockStatus::Received => None,
                        })
                        .min()
                })
                .min();

            // If in endgame mode, we re-requesting requested blocks
            // In endgame mode, duplicate requested count of every block should be put evenly,
            // since in endgame mode, remaining candidates should be few(TODO: fact check)
            // use a simple approach
            // TODO: set dynamic upper limit, optimize impossible pick(if all blocks requested before
            // simply add limit does not work
            // maybe add a BTreeSet to maintain this
            if let Some(from) = from {
                for limit in from..=from.saturating_add(1) {
                    let repick_option = RepickOption {
                        repick_limit: limit,
                        endgame: true,
                    };
                    for index in piece_index_order(&self.receiving).iter().map(|(i, _)| i) {
                        let blocks = &mut self.receiving.get_mut(index).expect("must exist");
                        if remain <= 0 {
                            break;
                        }
                        if peer_status.have(*index) {
                            while let Some((blks, n_picked)) = blocks.pick(
                                *peer,
                                remain,
                                &mut n_in_flight,
                                avg_speed,
                                rtt,
                                repick_option,
                            ) {
                                remain -= n_picked;
                                let pb: Vec<_> = blks.iter(self.piece_size as u32).collect();
                                trace!("pick piece {index} from peer {peer} (requested endgame), picked blks: {pb:?}");
                                ret.push(blks);
                            }
                        }
                    }
                }
            }
            info!("remain 1 {remain}");
        } else if remain > 0 && rush_mode {
            for index in piece_index_order(&self.receiving).iter().map(|(i, _)| i) {
                let blocks = &mut self.receiving.get_mut(index).expect("must exist");
                if remain <= 0 {
                    break;
                }
                if peer_status.have(*index) {
                    while let Some((blks, n_picked)) = blocks.pick(
                        *peer,
                        remain,
                        &mut n_in_flight,
                        avg_speed,
                        rtt,
                        repick_option,
                    ) {
                        remain -= n_picked;
                        let pb: Vec<_> = blks.iter(self.piece_size as u32).collect();
                        trace!("{peer} pick piece {index} (pick_next), inflight {n_in_flight}");
                        trace!("{peer} (pick_next), picked blks: {pb:?}, repick_option: {repick_option:?}");
                        ret.push(blks);
                    }
                }
            }
            info!("remain 2 {remain}");
        }

        if rush_mode {
            pick_blocks_from_requesting(
                &mut ret,
                &mut self.requesting,
                &peer_status,
                &mut remain,
                &mut n_in_flight,
                avg_speed,
                rtt,
                self.piece_size,
            );
            for (index, blocks) in self.requesting.iter_mut() {
                if blocks.is_all_requested_or_received() {
                    self.receiving.insert(*index, blocks.clone());
                }
            }
            self.requesting
                .retain(|_, b| !b.is_all_requested_or_received());
        }

        let endgame = self.update_endgame();
        // if self.rush_mode() {
        //     for (index, blocks) in self.receiving.iter().chain(self.requesting.iter()) {
        //         let n_received = blocks.received_count;
        //         let n_requesting = blocks
        //             .block_map
        //             .iter()
        //             .filter(|b| matches!(b, BlockStatus::Requested { .. }))
        //             .count();
        //         let n_to_request = blocks
        //             .block_map
        //             .iter()
        //             .filter(|b| matches!(b, BlockStatus::NotRequested { .. }))
        //             .count();
        //         let least_waiting_time = blocks
        //             .block_map
        //             .iter()
        //             .filter_map(|b| {
        //                 if let BlockStatus::Requested { requested, .. } = b {
        //                     requested
        //                         .iter()
        //                         .map(|(p, t)| t.pick_time)
        //                         .reduce(|ta, tb| ta.max(tb))
        //                 } else {
        //                     None
        //                 }
        //             })
        //             .reduce(|va, vb| va.max(vb));
        //         let most_waiting_time = blocks
        //             .block_map
        //             .iter()
        //             .filter_map(|b| {
        //                 if let BlockStatus::Requested { requested, .. } = b {
        //                     requested
        //                         .iter()
        //                         .map(|(_, t)| t.pick_time)
        //                         .reduce(|ta, tb| ta.min(tb))
        //                 } else {
        //                     None
        //                 }
        //             })
        //             .reduce(|va, vb| va.min(vb));
        //         debug!(
        //             "{peer} in rush mode detail: piece {}, to request {}, requesting {}, received {}, requested least time {:?}, most time {:?}",
        //             index, n_to_request, n_requesting, n_received, least_waiting_time.map(|x| x.elapsed()), most_waiting_time.map(|x| x.elapsed()),
        //         );
        //     }
        // }
        let strict_rush_mode = n_cache_vacant == 0;
        while remain > 0 && !strict_rush_mode {
            if let Some(index) = self.piece_picker.pick_next(peer) {
                assert!(!endgame);
                let mut blocks = self.piece_block_of(index);

                if let Some((blks, n_picked)) = blocks.pick(
                    *peer,
                    remain,
                    &mut n_in_flight,
                    avg_speed,
                    rtt,
                    repick_option,
                ) {
                    remain -= n_picked;
                    let pb: Vec<_> = blks.iter(self.piece_size as u32).collect();
                    trace!(
                        "{peer} rush mode extra {} pick piece {} (pick_next), inflight {}",
                        rush_mode,
                        index,
                        n_in_flight
                    );
                    trace!(
                        "{peer} rush mode extra {} picked blks: {pb:?}, {:?}",
                        rush_mode,
                        blocks.block_map
                    );
                    ret.push(blks);
                }
                assert!(!self.requesting.contains_key(&index));
                self.requesting.insert(index, blocks);
            } else {
                break;
            }
            info!("remain 3 {remain}");
        }
        if (rush_mode || endgame) && remain > 0 {
            info!(
                "{peer} rush mode {} endgame {} causing less picking, remain {remain}",
                rush_mode, endgame
            );
            for (index, blocks) in &self.receiving {
                trace!(
                    "{peer} rush mode, receiving piece {index}, blocks: {:?}",
                    blocks.block_map
                );
            }
            for (index, blocks) in &self.requesting {
                trace!(
                    "{peer} rush mode {}, endgame {} requesting piece {index}, blocks: {:?}",
                    rush_mode,
                    endgame,
                    blocks.block_map,
                );
            }
        }

        (
            BlockRequests {
                piece_size: self.piece_size as u32,
                range: ret,
            },
            n - remain,
        )
    }

    /// Call when some block request is rejected, and request for that block
    /// should be send to other peers again.
    pub fn peer_reject_block(&mut self, peer: &PeerAddr, req: Request) {
        if let Some(b) = self.receiving.get_mut(&req.index) {
            assert!(b.is_all_requested_or_received(), "{}", req.index);
            assert!(!self.requesting.contains_key(&req.index), "{}", req.index);
            assert!(self.piece_picker.have(req.index), "{}", req.index);
            b.revoke(peer, req);
            if !b.is_all_requested_or_received() {
                let b = self.receiving.remove(&req.index).unwrap();
                self.requesting.insert(req.index, b);
            } else if b.is_all_not_requested() {
                self.piece_picker.set_have(req.index, false);
                self.receiving.remove(&req.index);
            }
        } else if let Some(b) = self.requesting.get_mut(&req.index) {
            assert!(!self.receiving.contains_key(&req.index), "{}", req.index);
            assert!(self.piece_picker.have(req.index), "{}", req.index);
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

    /// Called when a piece is received, returns
    /// (
    /// if a piece is fully received,
    /// revoked requests,
    /// )
    pub fn receive_block(&mut self, req: Request) -> (PieceComplete, Vec<PeerAddr>) {
        let ji = JointIndex::from(req);
        if !self.check_block_validity(&req) {
            return (
                PieceComplete {
                    piece: false,
                    sub_piece: false,
                },
                vec![],
            );
        }

        if let Some(b) = self.requesting.get_mut(&ji.index()) {
            let r = b.receive(req);
            if b.is_all_requested_or_received() {
                let b = self.requesting.remove(&ji.index()).unwrap();
                self.receiving.insert(ji.index(), b);
            } else {
                return (
                    PieceComplete {
                        piece: false,
                        sub_piece: b.is_sub_all_received(req.begin),
                    },
                    r,
                );
            }
        }

        if let Some(b) = self.receiving.get_mut(&ji.index()) {
            let r = b.receive(req);
            (
                PieceComplete {
                    piece: b.is_all_received(),
                    sub_piece: b.is_sub_all_received(req.begin),
                },
                r,
            )
        } else if self.piece_picker.selected(ji.index()) && !self.piece_picker.have(ji.index()) {
            let mut b = self.piece_block_of(ji.index());
            let r = b.receive(req);
            let sub_complete = b.is_sub_all_received(req.begin);
            // only receive one block must be partial requested
            // TODO: FIXME: there should be no way that piece only have one block, huh?
            // otherwise we may move that to received
            self.requesting.insert(ji.index(), b);
            // notify piece_picker this piece is downloading
            self.piece_picker.set_have(ji.index(), true);
            (
                PieceComplete {
                    piece: false,
                    sub_piece: sub_complete,
                },
                r,
            )
        } else {
            // blocks we didn't select or already have
            (
                PieceComplete {
                    piece: false,
                    sub_piece: false,
                },
                vec![],
            )
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
    fn revoke_unrespond(&mut self, revoked: &mut HashMap<PeerAddr, Vec<Request>>) {
        let no_response = |_: &PeerAddr, at: &time::Instant| at.elapsed() > NO_RESPONSE_TIMEOUT;
        for (index, blocks) in self.receiving.iter_mut() {
            blocks.revoke_all_requested_if(no_response, revoked);
            if !blocks.is_all_requested_or_received() {
                self.requesting.insert(*index, blocks.clone());
            }
        }
        self.receiving
            .retain(|_, b| b.is_all_requested_or_received());

        for (index, blocks) in self.requesting.iter_mut() {
            blocks.revoke_all_requested_if(no_response, revoked);
            if blocks.is_all_not_requested() {
                let has_revoked_history = blocks.block_map.iter().any(|s| {
                    if let BlockStatus::NotRequested { revoked } = s {
                        // TODO: optimize, do not loop over block_map, store a revoked_count
                        !revoked.is_empty()
                    } else {
                        false
                    }
                });
                if !has_revoked_history {
                    self.piece_picker.set_have(*index, false);
                }
            }
        }
        self.requesting.retain(|_, b| {
            !b.is_all_not_requested()
                || b.block_map.iter().any(|s| {
                    if let BlockStatus::NotRequested { revoked } = s {
                        // TODO: optimize, do not loop over block_map, store a revoked_count
                        !revoked.is_empty()
                    } else {
                        false
                    }
                })
        });
    }

    fn update_endgame(&mut self) -> bool {
        let old = self.endgame;
        self.endgame = self.requesting.is_empty() && self.piece_picker.is_finished();
        if old != self.endgame {
            info!("switch endgame from {old} to {}", self.endgame);
            info!("requesting pieces?: {:?}", self.requesting.keys());
            info!("picker finished?: {}", self.piece_picker.is_finished());
        }
        self.endgame
    }

    /// check if we want this block
    pub fn want_block(&mut self, req: Request) -> bool {
        if !self.check_block_validity(&req) {
            info!("unwant because invalid {req:?}");
            return false;
        }

        let index = req.index;
        if !self.selected(index) {
            info!("unwant because {index} not selected");
            return false;
        }

        if self.have(index) {
            info!("unwant because have {index}");
            return false;
        }

        if let Some(b) = self.requesting.get(&index) {
            let s = &b.block_map[req.begin as usize / BLOCK_SIZE];
            match s {
                BlockStatus::NotRequested { .. } | BlockStatus::Requested { .. } => {
                    return true;
                }
                BlockStatus::Received => {
                    info!("unwant because received");
                    return false;
                }
            }
        }

        if let Some(b) = self.receiving.get(&index) {
            let s = &b.block_map[req.begin as usize / BLOCK_SIZE];
            match s {
                BlockStatus::Requested { .. } => {
                    return true;
                }
                BlockStatus::Received => {
                    info!("unwant because received2");
                    return false;
                }
                _ => unreachable!(),
            }
        }

        // we selected, but we did not request it, or we mark this block as
        // not requested because of timeout
        return true;
    }

    /// returns the number of peers that have this piece.
    pub fn piece_availability(&self, index: u32) -> usize {
        self.piece_picker.piece_availability(index)
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

        // Normalize Requested → NotRequested: after restore the `requested`
        // maps are empty (serde-skipped), so no peer is tracked for those
        // blocks. Reset them so they will be re-requested from scratch.
        // Pieces in `receiving` that regain NotRequested blocks are moved
        // back to `requesting` so the pick loop can reach them.
        let mut to_move: Vec<u32> = Vec::new();
        for (&idx, piece) in self.receiving.iter_mut() {
            if piece.reset_requested_blocks() {
                to_move.push(idx);
            }
        }
        for idx in to_move {
            let piece = self.receiving.remove(&idx).unwrap();
            self.requesting.insert(idx, piece);
        }
        for piece in self.requesting.values_mut() {
            piece.reset_requested_blocks();
        }

        // Pieces with zero received and zero requested have no progress at all.
        // Remove them from `requesting` so they won't block the pick loop.
        // They will be picked again via piece_picker's pick_next as peers reconnect.
        let mut to_return: Vec<u32> = Vec::new();
        for (&idx, piece) in self.requesting.iter() {
            if piece.is_all_not_requested() {
                to_return.push(idx);
            }
        }
        for idx in to_return {
            self.requesting.remove(&idx);
            self.piece_picker.set_have(idx, false);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockPickerDump {
    // pieces whose blocks are not all requested
    requesting: BTreeMap<PieceIndex, PieceBlocks>,

    // pieces whose blocks that all requested and waiting receiving
    receiving: BTreeMap<PieceIndex, PieceBlocks>,

    pub piece_map: PieceMap,

    pub no_response_timeout: time::Duration,
}

impl BlockPicker {
    pub fn n_pieces(&self) -> usize {
        self.n
    }

    pub fn our_state(&self) -> PieceState {
        let state = self.piece_picker.dump().have;
        let ones = state.count_ones() as usize;
        if ones == self.n {
            PieceState::HaveAll
        } else if ones == 0 {
            PieceState::HaveNone
        } else {
            PieceState::Bitfield(state)
        }
    }

    pub fn peer_add(&mut self, addr: PeerAddr, state: PieceState) {
        self.piece_picker.peer_add(addr, state);
    }

    pub fn peer_leave(&mut self, addr: &PeerAddr) {
        // Revoke all blocks requested by this peer so they become available immediately.
        let requested_peer = |p: &PeerAddr, _: &time::Instant| p == addr;
        let mut revoked = HashMap::new();
        for (i, b) in self.receiving.iter_mut() {
            b.revoke_all_requested_if(requested_peer, &mut revoked);
            if !b.is_all_requested_or_received() {
                self.requesting.insert(*i, b.clone());
            }
        }
        self.receiving
            .retain(|_, b| b.is_all_requested_or_received());
        for (i, b) in self.requesting.iter_mut() {
            b.revoke_all_requested_if(requested_peer, &mut revoked);
            if b.is_all_not_requested() {
                self.piece_picker.set_have(*i, false);
            }
        }
        self.requesting.retain(|_, b| !b.is_all_not_requested());
        self.piece_picker.peer_leave(addr);
    }

    /// Returns true if this piece is in the receiving set (all blocks requested or some received,
    /// waiting for completion or hash verification).
    pub fn is_piece_wait_check(&self, index: u32) -> bool {
        self.receiving
            .get(&index)
            .map(|b| b.is_all_received())
            .unwrap_or(false)
    }

    pub fn peer_choke(&mut self, peer: &PeerAddr) {
        let requested_peer = |p: &PeerAddr, _: &time::Instant| p == peer;

        let mut revoked = HashMap::new();
        for (i, b) in self.receiving.iter_mut() {
            b.revoke_all_requested_if(requested_peer, &mut revoked);
            if !b.is_all_requested_or_received() {
                self.requesting.insert(*i, b.clone());
                info!("345");
            }
        }
        self.receiving
            .retain(|_, b| b.is_all_requested_or_received());

        for (i, b) in self.requesting.iter_mut() {
            b.revoke_all_requested_if(requested_peer, &mut revoked);
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

    /// returns if we have this sub-piece
    pub fn have_sub(&self, ji: JointIndex) -> bool {
        let index = ji.index();
        if self.have(index) {
            return true;
        }
        self.requesting
            .get(&index)
            .map(|b| b.is_sub_all_received(ji.in_piece_offset() as u32))
            .unwrap_or(false)
            || self
                .receiving
                .get(&index)
                .map(|b| b.is_sub_all_received(ji.in_piece_offset() as u32))
                .unwrap_or(false)
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

    fn picked_detail_at(pick_time: time::Instant, n_in_flight_when_picked: usize) -> PickedDetail {
        PickedDetail::new(
            pick_time,
            n_in_flight_when_picked,
            BLOCK_SIZE as f32,
            time::Duration::ZERO,
        )
    }

    #[test]
    fn test_block_pieces() {
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: 4133,
            all_request_or_received_before: 0,
            requested_or_received_count: 0,
            received_count: 0,
            block_map: vec![
                BlockStatus::NotRequested {
                    revoked: HashMap::new()
                };
                50
            ],
            sub_receive_count: vec![0],
        };

        {
            let picked = b.pick(
                PEER1,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                },
            );
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
            let picked = b.pick(
                PEER1,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                },
            );
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
            let mut revoked = HashMap::new();
            b.revoke_all_requested_if(|_, _| true, &mut revoked);
            assert!(!b.is_all_requested_or_received());
            assert_eq!(b.all_request_or_received_before, 10);
            assert_eq!(b.received_count, 11);
            assert_eq!(b.requested_or_received_count, 11);
        }
    }

    #[test]
    fn test_block_pieces_non_consecutive() {
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: 4133,
            all_request_or_received_before: 0,
            requested_or_received_count: 1,
            received_count: 0,
            block_map: vec![
                BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                },
                BlockStatus::Requested {
                    requested: HashMap::from([(PEER1, picked_detail_at(time::Instant::now(), 0))]),
                    revoked: HashMap::new(),
                },
                BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                },
            ],
            sub_receive_count: vec![0],
        };
        {
            let picked = b.pick(
                PEER1,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                },
            );
            let exp = Some((
                BlockRange {
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
                },
                1,
            ));
            assert_eq!(picked, exp);
            assert_eq!(b.all_request_or_received_before, 2);
            assert_eq!(b.requested_or_received_count, 2);
        }
    }

    fn two_mins_ago() -> time::Instant {
        time::Instant::now() - time::Duration::from_mins(2)
    }

    #[test]
    fn test_block_pieces_endgame() {
        // TODO: FIXME: this is not endgame test
        // should set endgame=true in option.
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: 4133,
            all_request_or_received_before: 0,
            requested_or_received_count: 2,
            received_count: 0,
            block_map: vec![
                BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                },
                BlockStatus::Requested {
                    requested: HashMap::from([(PEER1, picked_detail_at(two_mins_ago(), 0))]),
                    revoked: HashMap::new(),
                },
                BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                },
                BlockStatus::Received,
                BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                },
            ],
            sub_receive_count: vec![0],
        };
        {
            let picked = b.pick(
                PEER2,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                RepickOption {
                    repick_limit: 2,
                    endgame: false,
                },
            );
            let exp = Some((
                BlockRange {
                    from: Request {
                        index: 0,
                        begin: 0,
                        len: 16384,
                    },
                    to: Request {
                        index: 0,
                        begin: 2 * 16384,
                        len: 16384,
                    },
                },
                3,
            ));
            assert_eq!(picked, exp);
            assert_eq!(b.all_request_or_received_before, 4);
            assert_eq!(b.requested_or_received_count, 4);
        }
        {
            let picked = b.pick(
                PEER2,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                RepickOption {
                    repick_limit: 2,
                    endgame: false,
                },
            );
            let exp = Some((
                BlockRange {
                    from: Request {
                        index: 0,
                        begin: 4 * 16384,
                        len: 4133,
                    },
                    to: Request {
                        index: 0,
                        begin: 4 * 16384,
                        len: 4133,
                    },
                },
                1,
            ));
            assert_eq!(picked, exp);
            assert_eq!(b.all_request_or_received_before, 5);
            assert_eq!(b.requested_or_received_count, 5);
        }
        {
            let picked = b.pick(
                PEER1,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                RepickOption {
                    repick_limit: 2,
                    endgame: false,
                },
            );
            let exp = Some((
                BlockRange {
                    from: Request {
                        index: 0,
                        begin: 2 * 16384,
                        len: 16384,
                    },
                    to: Request {
                        index: 0,
                        begin: 2 * 16384,
                        len: 16384,
                    },
                },
                1,
            ));
            assert_eq!(picked, exp);
            assert_eq!(b.all_request_or_received_before, 5);
            assert_eq!(b.requested_or_received_count, 5);
        }
        {
            let picked = b.pick(
                PEER3,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                RepickOption {
                    repick_limit: 2,
                    endgame: false,
                },
            );
            // PEER 3 should not take over because existing requests are still expected sooner.
            let exp = None;
            assert_eq!(picked, exp);
            assert_eq!(b.all_request_or_received_before, 5);
            assert_eq!(b.requested_or_received_count, 5);
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
            let mut revoked = HashMap::new();
            let picked = b.pick_blocks(
                &PEER1,
                15,
                15,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                &mut revoked,
                POOL_SIZE,
            );
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
            let mut revoked = HashMap::new();
            let picked = b.pick_blocks(
                &PEER2,
                1,
                1,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                &mut revoked,
                POOL_SIZE,
            );
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
            let mut revoked = HashMap::new();
            let picked = b.pick_blocks(
                &PEER1,
                5,
                5,
                BLOCK_SIZE as f32,
                time::Duration::ZERO,
                &mut revoked,
                POOL_SIZE,
            );
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

    #[test]
    fn test_fast_peer_can_take_over_slow_requested_block() {
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: BLOCK_SIZE,
            all_request_or_received_before: 1,
            requested_or_received_count: 1,
            received_count: 0,
            block_map: vec![BlockStatus::Requested {
                requested: HashMap::from([(
                    PEER1,
                    PickedDetail::new(time::Instant::now(), 4, 1024.0, time::Duration::ZERO),
                )]),
                revoked: HashMap::new(),
            }],
            sub_receive_count: vec![0],
        };

        let picked = b.pick(
            PEER2,
            1,
            &mut 0,
            BLOCK_SIZE as f32 * 64.0,
            time::Duration::ZERO,
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
        );

        assert_eq!(picked.map(|(_, n)| n), Some(1));
        match &b.block_map[0] {
            BlockStatus::Requested { requested, .. } => {
                assert!(requested.contains_key(&PEER1));
                assert!(requested.contains_key(&PEER2));
            }
            _ => panic!("block should stay requested"),
        }
    }

    #[test]
    fn test_known_slow_peer_triggers_repick() {
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: BLOCK_SIZE,
            all_request_or_received_before: 1,
            requested_or_received_count: 1,
            received_count: 0,
            block_map: vec![BlockStatus::Requested {
                requested: HashMap::from([(
                    PEER1,
                    PickedDetail::new(time::Instant::now(), 0, 4096.0, time::Duration::ZERO),
                )]),
                revoked: HashMap::new(),
            }],
            sub_receive_count: vec![0],
        };

        let picked = b.pick(
            PEER2,
            1,
            &mut 0,
            BLOCK_SIZE as f32,
            time::Duration::ZERO,
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
        );

        assert_eq!(picked.map(|(_, n)| n), Some(1));
        match &b.block_map[0] {
            BlockStatus::Requested { requested, .. } => {
                assert!(requested.contains_key(&PEER1));
                assert!(requested.contains_key(&PEER2));
            }
            _ => panic!("block should stay requested"),
        }
    }

    #[test]
    fn test_stale_request_triggers_repick() {
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: BLOCK_SIZE,
            all_request_or_received_before: 1,
            requested_or_received_count: 1,
            received_count: 0,
            block_map: vec![BlockStatus::Requested {
                requested: HashMap::from([(
                    PEER1,
                    picked_detail_at(time::Instant::now() - time::Duration::from_secs(6), 0),
                )]),
                revoked: HashMap::new(),
            }],
            sub_receive_count: vec![0],
        };

        let picked = b.pick(
            PEER2,
            1,
            &mut 0,
            BLOCK_SIZE as f32,
            time::Duration::ZERO,
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
        );

        assert_eq!(picked.map(|(_, n)| n), Some(1));
        match &b.block_map[0] {
            BlockStatus::Requested { requested, .. } => {
                assert!(requested.contains_key(&PEER1));
                assert!(requested.contains_key(&PEER2));
            }
            _ => panic!("block should stay requested"),
        }
    }

    #[test]
    fn test_fresh_normal_speed_request_does_not_repick() {
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: BLOCK_SIZE,
            all_request_or_received_before: 1,
            requested_or_received_count: 1,
            received_count: 0,
            block_map: vec![BlockStatus::Requested {
                requested: HashMap::from([(
                    PEER1,
                    PickedDetail::new(
                        time::Instant::now(),
                        0,
                        BLOCK_SIZE as f32,
                        time::Duration::ZERO,
                    ),
                )]),
                revoked: HashMap::new(),
            }],
            sub_receive_count: vec![0],
        };

        let picked = b.pick(
            PEER2,
            1,
            &mut 0,
            BLOCK_SIZE as f32,
            time::Duration::ZERO,
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
        );

        assert_eq!(picked, None);
        match &b.block_map[0] {
            BlockStatus::Requested { requested, .. } => {
                assert!(requested.contains_key(&PEER1));
                assert!(!requested.contains_key(&PEER2));
            }
            _ => panic!("block should stay requested"),
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

    // ---- Performance benchmarks ----

    /// Helper: create a BlockPicker with `n_pieces` pieces, optionally with some pieces
    /// already in requesting/receiving state, and `n_peers` peers added.
    fn make_block_picker_with_state(
        n_pieces: usize,
        piece_size: usize,
        n_peers: usize,
        n_requesting: usize,
        n_receiving: usize,
    ) -> (BlockPicker, Vec<PeerAddr>) {
        use crate::picker::RarestPicker;
        let total_size = n_pieces * piece_size;
        let p = Box::new(RarestPicker::new(total_size, piece_size));
        let mut bp = BlockPicker::new(total_size, piece_size, p, time::Duration::from_secs(10));

        let mut peers = Vec::new();
        for i in 0..n_peers {
            let addr = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, (i / 256) as u8, (i % 256) as u8)),
                (6881 + i) as u16,
            );
            bp.peer_add(addr, PieceState::HaveAll);
            peers.push(addr);
        }

        for i in 0..n_pieces {
            bp.select(i as u32, true);
        }

        // Simulate some pieces in requesting state
        let mut revoked = HashMap::new();
        if n_requesting > 0 && !peers.is_empty() {
            let (_, _) = bp.pick_blocks(
                &peers[0],
                n_requesting * (piece_size / BLOCK_SIZE + 1),
                0,
                BLOCK_SIZE as f32,
                time::Duration::from_millis(100),
                &mut revoked,
                POOL_SIZE,
            );
        }

        // Move some to receiving state
        for i in 0..n_receiving.min(n_requesting) {
            let idx = i as u32;
            if let Some(blocks) = bp.requesting.remove(&idx) {
                bp.receiving.insert(idx, blocks);
            }
        }

        (bp, peers)
    }

    #[test]
    fn perf_pick_blocks_normal_mode() {
        // Simulate: 500 total pieces, 32 blocks/piece (512KB pieces), 12 peers
        // Some pieces already in-flight
        const N_PIECES: usize = 500;
        const PIECE_SIZE: usize = 16384 * 32; // 512KB
        const N_PEERS: usize = 12;
        const N_REQUESTING: usize = 20;
        const N_RECEIVING: usize = 10;
        const ITERATIONS: usize = 5000;

        let (mut bp, peers) =
            make_block_picker_with_state(N_PIECES, PIECE_SIZE, N_PEERS, N_REQUESTING, N_RECEIVING);

        let start = std::time::Instant::now();
        for i in 0..ITERATIONS {
            let peer = &peers[i % N_PEERS];
            let mut revoked = HashMap::new();
            let _ = bp.pick_blocks(
                peer,
                0, // ticker only: n=0
                100,
                BLOCK_SIZE as f32,
                time::Duration::from_millis(100),
                &mut revoked,
                POOL_SIZE,
            );
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / ITERATIONS as u32;

        eprintln!(
            "perf_pick_blocks_normal_mode: {} iterations, total {:?}, per call {:?}",
            ITERATIONS, elapsed, per_call
        );
        // Expect < 10µs per call for n=0 pick (ticker path)
        assert!(
            per_call < std::time::Duration::from_micros(100),
            "pick_blocks(n=0) too slow: {:?}/call",
            per_call
        );
    }

    #[test]
    fn perf_pick_blocks_with_picks() {
        // Simulate picking blocks in normal mode: 7 requesting pieces, pick 1 at a time
        // (realistic scenario: 219 in-flight blocks across ~7 pieces)
        const N_PIECES: usize = 500;
        const PIECE_SIZE: usize = 16384 * 32;
        const N_PEERS: usize = 12;
        const N_PREPICK: usize = 200;
        const ITERATIONS: usize = 5000;

        let (mut bp, peers) = make_block_picker_with_state(N_PIECES, PIECE_SIZE, N_PEERS, 0, 0);

        // Pre-pick some blocks to simulate in-flight state
        let mut revoked = HashMap::new();
        let _ = bp.pick_blocks(
            &peers[0],
            N_PREPICK,
            0,
            BLOCK_SIZE as f32,
            time::Duration::from_millis(100),
            &mut revoked,
            POOL_SIZE,
        );
        eprintln!(
            "perf_pick_blocks_with_picks setup: {} requesting, {} receiving",
            bp.requesting.len(),
            bp.receiving.len()
        );

        // Measure n=0 path (ticker only, no actual picks)
        let start = std::time::Instant::now();
        for i in 0..ITERATIONS {
            let peer = &peers[i % N_PEERS];
            let mut revoked = HashMap::new();
            let _ = bp.pick_blocks(
                peer,
                0,
                N_PREPICK,
                BLOCK_SIZE as f32,
                time::Duration::from_millis(100),
                &mut revoked,
                POOL_SIZE,
            );
        }
        let elapsed_n0 = start.elapsed();
        let per_call_n0 = elapsed_n0 / ITERATIONS as u32;

        // Measure n=1 path (pick 1 block per call, also receive blocks to keep state realistic)
        let start = std::time::Instant::now();
        let mut max_req = 0usize;
        let mut max_recv = 0usize;
        let mut block_cursor = 0u32; // track which blocks we've "received"
        for i in 0..ITERATIONS {
            let peer = &peers[i % N_PEERS];
            let mut revoked = HashMap::new();
            let _ = bp.pick_blocks(
                peer,
                1,
                200,
                BLOCK_SIZE as f32,
                time::Duration::from_millis(100),
                &mut revoked,
                POOL_SIZE,
            );
            // Simulate receiving work: receive 1 block for every pick
            // (balance pick and receive to maintain steady-state)
            let piece_idx = block_cursor / 32;
            let block_idx = block_cursor % 32;
            let req = Request {
                index: piece_idx,
                begin: block_idx * BLOCK_SIZE as u32,
                len: BLOCK_SIZE as u32,
            };
            if bp.want_block(req) {
                let _ = bp.receive_block(req);
            }
            block_cursor += 1;

            max_req = max_req.max(bp.requesting.len());
            max_recv = max_recv.max(bp.receiving.len());
        }
        let elapsed_n1 = start.elapsed();
        let per_call_n1 = elapsed_n1 / ITERATIONS as u32;

        eprintln!(
            "perf_pick_blocks_with_picks: {} iterations\n  n=0 path: total {:?}, per call {:?}\n  n=1 path: total {:?}, per call {:?}\n  max requesting: {}, max receiving: {}",
            ITERATIONS, elapsed_n0, per_call_n0, elapsed_n1, per_call_n1, max_req, max_recv
        );
    }

    #[test]
    fn perf_receive_block() {
        // Benchmark receive_block throughput
        const N_PIECES: usize = 100;
        const PIECE_SIZE: usize = 16384 * 32;
        const N_PEERS: usize = 4;

        let (mut bp, peers) = make_block_picker_with_state(N_PIECES, PIECE_SIZE, N_PEERS, 0, 0);

        // First pick enough blocks to fill requesting
        let mut revoked = HashMap::new();
        for p in &peers {
            let _ = bp.pick_blocks(
                p,
                1000,
                0,
                BLOCK_SIZE as f32,
                time::Duration::from_millis(100),
                &mut revoked,
                POOL_SIZE,
            );
        }

        // Now benchmark receiving blocks
        let n_blocks_per_piece = PIECE_SIZE / BLOCK_SIZE;
        let total_blocks = N_PIECES * n_blocks_per_piece;
        let start = std::time::Instant::now();
        let mut received_count = 0;
        for piece_idx in 0..N_PIECES as u32 {
            for block_idx in 0..n_blocks_per_piece as u32 {
                let req = Request {
                    index: piece_idx,
                    begin: block_idx * BLOCK_SIZE as u32,
                    len: BLOCK_SIZE as u32,
                };
                let _ = bp.receive_block(req);
                received_count += 1;
            }
        }
        let elapsed = start.elapsed();
        let per_block = elapsed / received_count;

        eprintln!(
            "perf_receive_block: {} blocks, total {:?}, per block {:?}",
            received_count, elapsed, per_block
        );
        assert!(
            per_block < std::time::Duration::from_micros(10),
            "receive_block too slow: {:?}/block",
            per_block
        );
    }

    #[test]
    fn perf_revoke_unrespond() {
        // Benchmark revoke_unrespond with many in-flight pieces
        const N_PIECES: usize = 200;
        const PIECE_SIZE: usize = 16384 * 32;
        const N_PEERS: usize = 12;
        const ITERATIONS: usize = 1000;

        let (mut bp, peers) = make_block_picker_with_state(N_PIECES, PIECE_SIZE, N_PEERS, 0, 0);

        // Fill up requesting and receiving
        let mut revoked = HashMap::new();
        for p in &peers {
            let _ = bp.pick_blocks(
                p,
                500,
                0,
                BLOCK_SIZE as f32,
                time::Duration::from_millis(100),
                &mut revoked,
                POOL_SIZE,
            );
        }
        eprintln!(
            "revoke_unrespond setup: {} requesting, {} receiving",
            bp.requesting.len(),
            bp.receiving.len()
        );

        let start = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            let mut revoked = HashMap::new();
            bp.revoke_unrespond(&mut revoked);
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / ITERATIONS as u32;

        eprintln!(
            "perf_revoke_unrespond: {} iterations ({}req + {}recv pieces), total {:?}, per call {:?}",
            ITERATIONS,
            bp.requesting.len(),
            bp.receiving.len(),
            elapsed,
            per_call
        );
        assert!(
            per_call < std::time::Duration::from_millis(1),
            "revoke_unrespond too slow: {:?}/call",
            per_call
        );
    }

    #[test]
    fn perf_piece_index_order_sort() {
        // Benchmark the piece_index_order sort that happens on every pick_blocks call
        use std::collections::BTreeMap;
        const N_PIECES: usize = 200;
        const ITERATIONS: usize = 5000;

        let mut map = BTreeMap::new();
        for i in 0..N_PIECES as u32 {
            map.insert(
                i,
                PieceBlocks {
                    piece_index: i,
                    last_block_size: BLOCK_SIZE,
                    all_request_or_received_before: 0,
                    requested_or_received_count: (i as usize) % 32,
                    received_count: (i as usize) % 16,
                    block_map: vec![
                        BlockStatus::NotRequested {
                            revoked: HashMap::new(),
                        };
                        32
                    ],
                    sub_receive_count: vec![0],
                },
            );
        }

        let start = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            let mut piece_order: Vec<_> = map.iter().map(|(i, b)| (*i, b.received_count)).collect();
            piece_order.sort_by(|(_, recv_a), (_, recv_b)| recv_a.cmp(recv_b).reverse());
            std::hint::black_box(&piece_order);
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / ITERATIONS as u32;

        eprintln!(
            "perf_piece_index_order_sort: {} pieces, {} iterations, total {:?}, per call {:?}",
            N_PIECES, ITERATIONS, elapsed, per_call
        );
        assert!(
            per_call < std::time::Duration::from_micros(100),
            "piece_index_order sort too slow: {:?}/call",
            per_call
        );
    }
}

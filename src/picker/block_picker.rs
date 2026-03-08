use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::{
    BitField, BlockRange, BlockRequests, PeerAddr, PeerPieceDetail, PieceMap, PiecePicker,
    PieceState,
};
use crate::{
    bandwidth::RTT,
    math_helper::piece_total_and_last_size,
    protocol::{Piece, Request},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time,
};

const BLOCK_SIZE: usize = 16384;
const NO_RESPONSE_TIMEOUT: time::Duration = time::Duration::from_secs(90);

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PickedDetail {
    pub pick_time: time::Instant,
    pub n_in_flight_when_picked: usize,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Clone)]
pub enum BlockStatus {
    NotRequested,
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
                    BlockStatus::NotRequested => {
                        // TODO: todo!("does this really happen in endgame mode?");
                        *b = BlockStatus::Requested {
                            requested: HashMap::from([(
                                peer,
                                PickedDetail {
                                    pick_time: time::Instant::now(),
                                    n_in_flight_when_picked: *n_in_flight,
                                },
                            )]),
                            revoked: HashMap::new(),
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
                        let (first_eta, min_elapsed) = requested
                            .iter()
                            .map(|(p, t)| {
                                let elapsed = t.pick_time.elapsed();
                                let est_rtt =
                                    if let Some(rtt) = repick_option.alt_timeout.get(p).copied() {
                                        elapsed.max(rtt)
                                    } else {
                                        time::Duration::from_secs(10)
                                    };
                                (t.pick_time + est_rtt, elapsed)
                            })
                            // TODO: use reduce instead of fold
                            .fold(
                                (
                                    time::Instant::now() + time::Duration::from_secs(10),
                                    time::Duration::from_secs(10),
                                ),
                                |acc, (eta, elapsed)| (acc.0.min(eta), acc.1.min(elapsed)),
                            );
                        let our_rtt = repick_option
                            .alt_timeout
                            .get(&peer)
                            .map(|d| *d)
                            .unwrap_or(time::Duration::from_secs(10));

                        const FIVE_SECS: time::Duration = time::Duration::from_secs(5);

                        let old_remain_time =
                            first_eta.saturating_duration_since(time::Instant::now());
                        if (old_remain_time > 2 * our_rtt
                            || min_elapsed > FIVE_SECS
                            || repick_option.endgame)
                            && !requested.contains_key(&peer)
                        {
                            if old_remain_time > our_rtt * 2 {
                                debug!(
                                    "{peer} estimated remain time of {req:?} {:?} is much higher than est new rtt {:?} repick",
                                    old_remain_time, our_rtt,
                                );
                            }
                            if min_elapsed > FIVE_SECS {
                                debug!(
                                    "{peer} min elapsed time of {req:?} is {min_elapsed:?}, higher than {FIVE_SECS:?}, repick",
                                );
                            }
                            if repick_option.endgame {
                                debug!("{peer} in endgame mode, repick {req:?}",);
                            }
                            // if !addr.contains_key(&peer) {
                            count += 1;
                            info!("{peer:?} repick {req:?}, requested {requested:?}, revoked: {revoked:?}");
                            // TODO: FIXME: this cause rtt wrongly thinks response is to second request,
                            // should be first request's response
                            requested.insert(
                                peer,
                                PickedDetail {
                                    pick_time: time::Instant::now(),
                                    n_in_flight_when_picked: *n_in_flight,
                                },
                            );
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
                    BlockStatus::NotRequested => {
                        *b = BlockStatus::Requested {
                            requested: HashMap::from([(
                                peer,
                                PickedDetail {
                                    pick_time: time::Instant::now(),
                                    n_in_flight_when_picked: *n_in_flight,
                                },
                            )]),
                            revoked: HashMap::new(),
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
            BlockStatus::NotRequested => {
                self.received_count += 1;
                self.requested_or_received_count += 1;
                *b = BlockStatus::Received;
                vec![]
            }
            BlockStatus::Requested { requested, .. } => {
                self.received_count += 1;
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
                if requested.is_empty() && revoked.is_empty() {
                    *b = BlockStatus::NotRequested;
                    if self.all_request_or_received_before > b_index {
                        self.all_request_or_received_before = b_index;
                    }
                    self.requested_or_received_count -= 1;
                }
            }
            BlockStatus::NotRequested => {}
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
                                "revoke block {req:?} from {p}, issued at {t:?}, after {:?}",
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
                    if requested.is_empty() && revoked.is_empty() {
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

#[derive(Copy, Clone, Debug)]
struct RepickOption<'a> {
    // The upper limit of how many times a block may be requested from
    // different peers
    repick_limit: usize,

    // if in endgame mode
    endgame: bool,

    // The alternative timeout duration.
    // Once a peer did not response Piece or Reject
    // in this period, we conclude they will not respond forever
    // forget that request, and request other peers for this block again
    alt_timeout: &'a HashMap<PeerAddr, time::Duration>,
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

    pub fn get_rtt(&self, peer: &PeerAddr, req: &Request) -> Option<time::Duration> {
        if let Some(b) = self.get_block_status(req) {
            match b {
                BlockStatus::NotRequested => None,
                BlockStatus::Requested { requested, revoked } => {
                    if let Some(p) = revoked.get(peer) {
                        Some(p.pick_time.elapsed())
                    } else if let Some(p) = requested.get(peer) {
                        Some(p.pick_time.elapsed())
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
                BlockStatus::NotRequested => None,
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

    fn rush_mode(&self) -> bool {
        let working_set_size = self.receiving.len() + self.requesting.len();
        // swap IO is too frequent
        const WORKING_SET_LIMIT: usize = 30;
        working_set_size > WORKING_SET_LIMIT
    }

    fn strict_rush_mode(&self) -> bool {
        let working_set_size = self.receiving.len() + self.requesting.len();
        // swap IO is too frequent
        use crate::cache::simple_buffer::POOL_SIZE;
        working_set_size > POOL_SIZE
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
        rtts: &HashMap<PeerAddr, time::Duration>,
        n: usize,
        mut n_in_flight: usize,
        revoked: &mut HashMap<PeerAddr, Vec<Request>>,
    ) -> (BlockRequests, usize) {
        self.revoke_unrespond(
            rtts.get(peer)
                .map_or(time::Duration::from_secs(5), |x| *x)
                .max(time::Duration::from_secs(5)),
            revoked,
        );
        self.prev_time_check = time::Instant::now();

        let endgame = self.update_endgame();
        let repick_option = if endgame {
            RepickOption {
                repick_limit: 1,
                endgame: true,
                alt_timeout: rtts,
            }
        } else if self.rush_mode() {
            RepickOption {
                repick_limit: 7, // TODO: set a proper repick limit
                endgame: false,
                alt_timeout: rtts,
            }
        } else {
            RepickOption {
                repick_limit: 1,
                endgame: false,
                alt_timeout: &HashMap::new(),
            }
        };
        info!("{peer} endgame {endgame}, rush {}", self.rush_mode());

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
        for index in piece_index_order(&self.requesting).iter().map(|(i, _)| i) {
            let blocks = &mut self.requesting.get_mut(index).expect("must exist");
            assert!(!endgame);
            if remain <= 0 {
                break;
            }
            if peer_status.have(*index) {
                while let Some((blks, n_picked)) =
                    blocks.pick(*peer, remain, &mut n_in_flight, repick_option)
                {
                    remain -= n_picked;
                    let pb: Vec<_> = blks.iter(self.piece_size as u32).collect();
                    debug!("pick piece {index} from peer {peer} (requesting), picked blks: {pb:?}");
                    ret.push(blks);
                }
            }
            info!("remain 0 {remain}");
        }

        for (index, blocks) in self.requesting.iter_mut() {
            if blocks.is_all_requested_or_received() {
                self.receiving.insert(*index, blocks.clone());
            }
        }
        self.requesting
            .retain(|_, b| !b.is_all_requested_or_received());

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
            let from = self
                .receiving
                .iter()
                .map(|(_, p)| {
                    p.block_map
                        .iter()
                        .map(|b| match b {
                            BlockStatus::NotRequested => 0,
                            BlockStatus::Requested { requested, .. } => requested.len(),
                            BlockStatus::Received => usize::MAX,
                        })
                        .min()
                        .unwrap_or(usize::MAX)
                })
                .min()
                .unwrap_or(2);

            // If in endgame mode, we re-requesting requested blocks
            // In endgame mode, duplicate requested count of every block should be put evenly,
            // since in endgame mode, remaining candidates should be few(TODO: fact check)
            // use a simple approach
            // TODO: set dynamic upper limit, optimize impossible pick(if all blocks requested before
            // simply add limit does not work
            // maybe add a BTreeSet to maintain this
            for limit in from..=from + 1 {
                let repick_option = RepickOption {
                    repick_limit: limit,
                    alt_timeout: rtts,
                    endgame: true,
                };
                for index in piece_index_order(&self.receiving).iter().map(|(i, _)| i) {
                    let blocks = &mut self.receiving.get_mut(index).expect("must exist");
                    if remain <= 0 {
                        break;
                    }
                    if peer_status.have(*index) {
                        while let Some((blks, n_picked)) =
                            blocks.pick(*peer, remain, &mut n_in_flight, repick_option)
                        {
                            remain -= n_picked;
                            let pb: Vec<_> = blks.iter(self.piece_size as u32).collect();
                            debug!("pick piece {index} from peer {peer} (requested endgame), picked blks: {pb:?}");
                            ret.push(blks);
                        }
                    }
                }
            }
            info!("remain 1 {remain}");
        } else if remain > 0 && repick_option.repick_limit > 1 {
            for index in piece_index_order(&self.receiving).iter().map(|(i, _)| i) {
                let blocks = &mut self.receiving.get_mut(index).expect("must exist");
                if remain <= 0 {
                    break;
                }
                if peer_status.have(*index) {
                    while let Some((blks, n_picked)) =
                        blocks.pick(*peer, remain, &mut n_in_flight, repick_option)
                    {
                        remain -= n_picked;
                        let pb: Vec<_> = blks.iter(self.piece_size as u32).collect();
                        debug!("{peer} pick piece {index} (pick_next), inflight {n_in_flight}");
                        debug!("{peer} (pick_next), picked blks: {pb:?}, repick_option: {repick_option:?}");
                        ret.push(blks);
                    }
                }
            }
            info!("remain 2 {remain}");
        }

        let endgame = self.update_endgame();
        if self.rush_mode() {
            for (index, blocks) in self.receiving.iter().chain(self.requesting.iter()) {
                let n_received = blocks.received_count;
                let n_requesting = blocks
                    .block_map
                    .iter()
                    .filter(|b| matches!(b, BlockStatus::Requested { .. }))
                    .count();
                let n_to_request = blocks
                    .block_map
                    .iter()
                    .filter(|b| matches!(b, BlockStatus::NotRequested))
                    .count();
                let least_waiting_time = blocks
                    .block_map
                    .iter()
                    .filter_map(|b| {
                        if let BlockStatus::Requested { requested, .. } = b {
                            requested
                                .iter()
                                .map(|(p, t)| t.pick_time)
                                .reduce(|ta, tb| ta.max(tb))
                        } else {
                            None
                        }
                    })
                    .reduce(|va, vb| va.max(vb));
                let most_waiting_time = blocks
                    .block_map
                    .iter()
                    .filter_map(|b| {
                        if let BlockStatus::Requested { requested, .. } = b {
                            requested
                                .iter()
                                .map(|(p, t)| t.pick_time)
                                .reduce(|ta, tb| ta.min(tb))
                        } else {
                            None
                        }
                    })
                    .reduce(|va, vb| va.min(vb));
                debug!(
                    "{peer} in rush mode detail: piece {}, to request {}, requesting {}, received {}, requested least time {:?}, most time {:?}",
                    index, n_to_request, n_requesting, n_received, least_waiting_time.map(|x| x.elapsed()), most_waiting_time.map(|x| x.elapsed()),
                );
            }
        }
        while remain > 0 && !self.strict_rush_mode() {
            let rush_mode = self.rush_mode();
            if let Some(index) = self.piece_picker.pick_next(peer) {
                assert!(!endgame);
                let mut blocks = self.piece_block_of(index);

                if let Some((blks, n_picked)) =
                    blocks.pick(*peer, remain, &mut n_in_flight, repick_option)
                {
                    remain -= n_picked;
                    let pb: Vec<_> = blks.iter(self.piece_size as u32).collect();
                    debug!(
                        "{peer} rush mode extra {} pick piece {} (pick_next), inflight {}",
                        rush_mode, index, n_in_flight
                    );
                    debug!(
                        "{peer} rush mode extra {} picked blks: {pb:?}, {:?}",
                        rush_mode, blocks.block_map
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
        if (self.rush_mode() || endgame) && remain > 0 {
            info!(
                "{peer} rush mode {} endgame {} causing less picking, remain {remain}",
                self.rush_mode(),
                endgame
            );
            for (index, blocks) in &self.receiving {
                debug!(
                    "{peer} rush mode, receiving piece {index}, blocks: {:?}",
                    blocks.block_map
                );
            }
            for (index, blocks) in &self.requesting {
                debug!(
                    "{peer} rush mode {}, endgame {} requesting piece {index}, blocks: {:?}",
                    self.rush_mode(),
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
    pub fn receive_block(&mut self, req: Request) -> (Option<u32>, Vec<PeerAddr>) {
        if !self.check_block_validity(&req) {
            return (None, vec![]);
        }

        let index = req.index;
        if let Some(b) = self.receiving.get_mut(&index) {
            let r = b.receive(req);
            (b.is_all_received().then(|| index), r)
        } else if let Some(b) = self.requesting.get_mut(&index) {
            let r = b.receive(req);
            if b.is_all_received() {
                let b = self.requesting.remove(&index).unwrap();
                self.receiving.insert(index, b);
                (Some(index), r)
            } else {
                if b.is_all_requested_or_received() {
                    let b = self.requesting.remove(&index).unwrap();
                    self.receiving.insert(index, b);
                }
                (None, r)
            }
        } else if self.piece_picker.selected(index) && !self.piece_picker.have(index) {
            let mut b = self.piece_block_of(index);
            let r = b.receive(req);
            // only receive one block must be partial requested
            self.requesting.insert(index, b);
            // notify piece_picker this piece is downloading
            self.piece_picker.set_have(index, true);
            (None, r)
        } else {
            // blocks we didn't select or already have
            (None, vec![])
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
    fn revoke_unrespond(
        &mut self,
        timeout: time::Duration,
        revoked: &mut HashMap<PeerAddr, Vec<Request>>,
    ) {
        let no_response = |_: &PeerAddr, at: &time::Instant| at.elapsed() > timeout;
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
                self.piece_picker.set_have(*index, false);
            }
        }
        self.requesting.retain(|_, b| !b.is_all_not_requested());
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
            println!("unwant because invalid {req:?}");
            return false;
        }

        let index = req.index;
        if !self.selected(index) {
            println!("unwant because {index} not selected");
            return false;
        }

        if self.have(index) {
            println!("unwant because have {index}");
            return false;
        }

        if let Some(b) = self.requesting.get(&index) {
            let s = &b.block_map[req.begin as usize / BLOCK_SIZE];
            match s {
                BlockStatus::NotRequested | BlockStatus::Requested { .. } => {
                    return true;
                }
                BlockStatus::Received => {
                    println!("unwant because received");
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
                    println!("unwant because received2");
                    return false;
                }
                _ => unreachable!(),
            }
        }

        // we selected, but we did not request it, or we mark this block as
        // not requested because of timeout
        return true;
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
        self.piece_picker.peer_leave(addr);
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
    use crate::{
        bandwidth::{ALPHA, BETA},
        picker::BitField,
    };

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
            let picked = b.pick(
                PEER1,
                30,
                &mut 0,
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                    alt_timeout: &HashMap::new(),
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
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                    alt_timeout: &HashMap::new(),
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
                BlockStatus::NotRequested,
                BlockStatus::Requested {
                    requested: HashMap::from([(
                        PEER1,
                        PickedDetail {
                            pick_time: time::Instant::now(),
                            n_in_flight_when_picked: 0,
                        },
                    )]),
                    revoked: HashMap::new(),
                },
                BlockStatus::NotRequested,
            ],
        };
        {
            let picked = b.pick(
                PEER1,
                30,
                &mut 0,
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                    alt_timeout: &HashMap::new(),
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
        let mut b = PieceBlocks {
            piece_index: 0,
            last_block_size: 4133,
            all_request_or_received_before: 0,
            requested_or_received_count: 2,
            received_count: 0,
            block_map: vec![
                BlockStatus::NotRequested,
                BlockStatus::Requested {
                    requested: HashMap::from([(
                        PEER1,
                        PickedDetail {
                            pick_time: two_mins_ago(),
                            n_in_flight_when_picked: 0,
                        },
                    )]),
                    revoked: HashMap::new(),
                },
                BlockStatus::NotRequested,
                BlockStatus::Received,
                BlockStatus::NotRequested,
            ],
        };
        {
            let picked = b.pick(
                PEER2,
                30,
                &mut 0,
                RepickOption {
                    repick_limit: 2,
                    endgame: false,
                    alt_timeout: &HashMap::new(),
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
                RepickOption {
                    repick_limit: 2,
                    endgame: false,
                    alt_timeout: &HashMap::new(),
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
                RepickOption {
                    repick_limit: 2,
                    endgame: false,
                    alt_timeout: &HashMap::new(),
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
            assert_eq!(b.all_request_or_received_before, 5);
            assert_eq!(b.requested_or_received_count, 5);
        }
        {
            let picked = b.pick(
                PEER3,
                30,
                &mut 0,
                RepickOption {
                    repick_limit: 2,
                    endgame: false,
                    alt_timeout: &HashMap::new(),
                },
            );
            // PEER 3 should skip block 0, 1 because they are already requested to PEER 1 and 2
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

        let rtts = HashMap::new();

        {
            let mut revoked = HashMap::new();
            let picked = b.pick_blocks(&PEER1, &rtts, 0, 15, &mut revoked);
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
            let picked = b.pick_blocks(&PEER2, &rtts, 0, 1, &mut revoked);
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
            let picked = b.pick_blocks(&PEER1, &rtts, 0, 5, &mut revoked);
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

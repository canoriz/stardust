use serde::{Deserialize, Serialize};
use tracing::{info, trace};

use super::{
    BitField, BlockRange, BlockRequests, PeerAddr, PeerPieceDetail, PickMode, PieceMap,
    PiecePicker, PieceState,
};
use crate::{
    cache::simple_buffer::{JointIndex, POOL_SIZE, SUB_PIECE_SIZE},
    data_structure::IndexTree,
    file_tracker::FileTracker,
    math_helper::piece_total_and_last_size,
    metadata::Metadata,
    protocol::Request,
    slot_accounting::{Accounting, PeerCost},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time,
};

#[cfg(feature = "blocktrace")]
use crate::blocktrace;
#[cfg(feature = "metrics")]
use crate::metrics;

const BLOCK_SIZE: usize = 16384;
const NO_RESPONSE_TIMEOUT: time::Duration = time::Duration::from_secs(90);

/// Repick deadline for a any requester if cache is above water level
const REPICK_AFTER_PICK: time::Duration = time::Duration::from_secs(5);

/// Grace period for a block picked by slow peers
const SLOW_TAKEOVER_DELAY: time::Duration = time::Duration::from_secs(5);

/// Repick available for a peer does not respond after delay of expectation
const REPICK_RESPONSE_DELAY: time::Duration = time::Duration::from_millis(2000);

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PickedDetail {
    pub pick_time: tokio::time::Instant,

    /// The peer's category when this request was issued
    pub slow_at_pick: bool,
    pub n_in_flight_when_picked: usize,
    pub expected_response_time: time::Duration,

    // None until the holder has a real RTT sample
    pub rtt: Option<time::Duration>,
}

impl PickedDetail {
    fn new(
        pick_time: tokio::time::Instant,
        n_in_flight_when_picked: usize,
        avg_speed: f32,
        rtt: Option<time::Duration>,
        slow_at_pick: bool,
    ) -> Self {
        Self {
            pick_time,
            slow_at_pick,
            n_in_flight_when_picked,
            expected_response_time: expected_response_time(avg_speed, n_in_flight_when_picked, rtt),
            rtt,
        }
    }

    fn expected_recv_at(&self) -> tokio::time::Instant {
        self.pick_time + self.expected_response_time
    }

    /// The instant the block this detail was picked for becomes repick-eligible:
    fn repick_eligible_at(&self) -> tokio::time::Instant {
        (self.expected_recv_at() + REPICK_RESPONSE_DELAY).min(self.pick_time + REPICK_AFTER_PICK)
    }
}

/// Picker-owned reverse index; block_map remains the source of request details.
/// tracks which peer occupies which block
#[derive(Default)]
struct BlockOccupy {
    /// Active and revoked requests ordered by their original pick time.
    /// Revocation keeps the entry; a new pick replaces it.
    by_peer: HashMap<PeerAddr, IndexTree<(PieceIndex, BlockIndex), tokio::time::Instant>>,
}

impl BlockOccupy {
    fn insert(
        &mut self,
        peer: PeerAddr,
        key: (PieceIndex, BlockIndex),
        picked_at: tokio::time::Instant,
    ) {
        self.by_peer.entry(peer).or_default().insert(key, picked_at);
    }

    fn remove_one(&mut self, peer: PeerAddr, key: (PieceIndex, BlockIndex)) {
        if let Some(requests) = self.by_peer.get_mut(&peer) {
            requests.remove(&key);
            if requests.is_empty() {
                self.by_peer.remove(&peer);
            }
        }
    }

    /// remove this requests entry of all peer, called when a block is received
    fn remove_all(&mut self, (piece, block): (PieceIndex, BlockIndex), status: &BlockStatus) {
        let revoked = match status {
            BlockStatus::Requested { requested, revoked } => {
                for peer in requested.keys() {
                    self.remove_one(*peer, (piece, block));
                }
                revoked
            }
            BlockStatus::NotRequested { revoked } => revoked,
            BlockStatus::Received => return,
        };
        for peer in revoked.keys() {
            self.remove_one(*peer, (piece, block));
        }
    }
}

/// A block is repick-eligible only once every current requester is overdue, so
/// its deadline is the latest per-requester `repick_eligible_at`.
fn repick_deadline_of(requested: &HashMap<PeerAddr, PickedDetail>) -> Option<tokio::time::Instant> {
    requested
        .values()
        .map(PickedDetail::repick_eligible_at)
        .max()
}

/// Deadline for requests issued by slow peers
fn takeover_deadline_of(
    slow_peers: &HashSet<PeerAddr>,
    status: &BlockStatus,
) -> Option<tokio::time::Instant> {
    if let BlockStatus::Requested { requested, .. } = status
        && requested
            .iter()
            .all(|(peer, detail)| detail.slow_at_pick || slow_peers.contains(peer))
    {
        requested
            .values()
            .map(|detail| detail.pick_time)
            .max()
            .map(|pick_time| pick_time + SLOW_TAKEOVER_DELAY)
    } else {
        None
    }
}

fn expected_response_time(
    avg_speed: f32,
    n_in_flight: usize,
    rtt: Option<time::Duration>,
) -> time::Duration {
    let Some(rtt) = rtt else {
        // just optimistically expect response will come quickly
        return REPICK_AFTER_PICK;
    };
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
        #[serde(skip)]
        requested: HashMap<PeerAddr, PickedDetail>,

        // blocks that are requested but revoked(because of timeout or have received from other peer)
        #[serde(skip)]
        revoked: HashMap<PeerAddr, PickedDetail>,
    },
    Received, // TODO: maybe record which peer sends us this block?
}

/// Per-sub-piece progress: `received` gates completion; the arrival pair and
/// `blocker` carry the `lift` bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SubPieceState {
    received: usize,
    /// Shared by both cost ledgers: first request to completion. Restored partial
    /// progress (or an unsolicited first arrival) starts at observation time.
    last_accounted_at: Option<tokio::time::Instant>,
    // the picker's clock is the tokio one everywhere, so tests advance it
    /// When the previous block arrived, or when the `blocker` picked the last one
    /// up if that came later.
    second_last: Option<tokio::time::Instant>,
    last: Option<tokio::time::Instant>,
    /// Earliest still-outstanding requester of the last-arriving block; `lift`
    /// bills this sub-piece's last wait to it.
    blocker: Option<PeerAddr>,
}

impl SubPieceState {
    fn new() -> Self {
        Self {
            received: 0,
            last_accounted_at: None,
            second_last: None,
            last: None,
            blocker: None,
        }
    }
}

/// The sub-piece's last wait: how long its last block was the only outstanding
/// block, and the earliest still-outstanding requester of that block. The wait
/// is charged to that requester from when it picked the block up, so a rescuer
/// pays only for its own hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastWait {
    pub wait: time::Duration,
    pub blocker: Option<PeerAddr>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[serde(into = "PieceBlockDump", from = "PieceBlockDump")]
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

    /// book keeping of per-block repick deadline, used for repick and endgame
    repick_deadline: IndexTree<BlockIndex, tokio::time::Instant>,

    /// per-sub-piece receive count and last-wait bookkeeping
    subs: Vec<SubPieceState>,
}

/// Serialized form of `PieceBlocks`: all `Requested` blocks are normalized to
/// `NotRequested`, and `repick_deadline` plus derived counts are rebuilt on load.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct PieceBlockDump {
    piece_index: u32,
    last_block_size: usize,
    block_map: Vec<BlockStatus>,
    sub_receive_count: Vec<usize>,
}

impl From<PieceBlocks> for PieceBlockDump {
    fn from(pb: PieceBlocks) -> Self {
        let block_map = pb
            .block_map
            .into_iter()
            .map(|b| match b {
                BlockStatus::Requested { .. } | BlockStatus::NotRequested { .. } => {
                    BlockStatus::NotRequested {
                        revoked: HashMap::new(),
                    }
                }
                other => other,
            })
            .collect();
        Self {
            piece_index: pb.piece_index,
            last_block_size: pb.last_block_size,
            block_map,
            sub_receive_count: pb.subs.iter().map(|s| s.received).collect(),
        }
    }
}

impl From<PieceBlockDump> for PieceBlocks {
    fn from(d: PieceBlockDump) -> Self {
        let received_count = d
            .block_map
            .iter()
            .filter(|b| matches!(b, BlockStatus::Received))
            .count();
        let all_request_or_received_before = d
            .block_map
            .iter()
            .take_while(|b| matches!(b, BlockStatus::Received))
            .count();
        Self {
            piece_index: d.piece_index,
            last_block_size: d.last_block_size,
            all_request_or_received_before,
            requested_or_received_count: received_count,
            received_count,
            block_map: d.block_map,
            repick_deadline: IndexTree::new(),
            // arrival instants of already-received blocks are not persisted
            subs: d
                .sub_receive_count
                .into_iter()
                .map(|received| SubPieceState {
                    received,
                    ..SubPieceState::new()
                })
                .collect(),
        }
    }
}

impl PieceBlocks {
    /// Settle one sub-piece before changing its missing blocks or request owners.
    /// A shared timestamp makes repeated settlement in one pick batch a no-op.
    /// Accept disjoint fields so callers can keep iterating the repick tree.
    fn settle_sub(
        subs: &mut [SubPieceState],
        block_map: &[BlockStatus],
        sub: usize,
        account: &mut Accounting,
        now: tokio::time::Instant,
    ) {
        let blocks_per_sub = SUB_PIECE_SIZE as usize / BLOCK_SIZE;
        let from = sub * blocks_per_sub;
        let to = (from + blocks_per_sub).min(block_map.len());
        let state = &mut subs[sub];
        let blocks = &block_map[from..to];
        let Some(last) = state.last_accounted_at else {
            return;
        };
        let missing = blocks.len() - state.received;
        if missing == 0 {
            state.last_accounted_at = None;
            return;
        }
        if now == last {
            return;
        }
        state.last_accounted_at = Some(now);
        let elapsed = now.duration_since(last).as_secs_f64();
        let per_block = elapsed / missing as f64;
        let fair_per_block = elapsed / blocks.len() as f64;
        // Attribute unowned shares to the scheduler without changing peer weights.
        for block in blocks {
            match block {
                BlockStatus::Received => account.scheduler_fair_st += fair_per_block,
                BlockStatus::Requested { requested, .. } if !requested.is_empty() => {
                    let share = per_block / requested.len() as f64;
                    let fair_share = fair_per_block / requested.len() as f64;
                    for peer in requested.keys() {
                        account.add_cost(*peer, share, fair_share);
                    }
                }
                _ => {
                    account.scheduler_blocker_st += per_block;
                    account.scheduler_fair_st += fair_per_block;
                }
            }
        }
    }

    /// Settle every sub-piece for sampling, rotation and piece removal.
    fn settle_account(&mut self, account: &mut Accounting, now: tokio::time::Instant) {
        for sub in 0..self.subs.len() {
            Self::settle_sub(&mut self.subs, &self.block_map, sub, account, now);
        }
    }
}

impl PieceBlocks {
    /// Restored partial progress starts a new interval at restore time. Neither
    /// saved timestamps nor offline time are charged; complete/empty subs stay idle.
    fn restart_slot_clocks(&mut self, now: tokio::time::Instant) {
        let blocks_per_sub = SUB_PIECE_SIZE as usize / BLOCK_SIZE;
        for (sub, state) in self.subs.iter_mut().enumerate() {
            let len = (self.block_map.len() - sub * blocks_per_sub).min(blocks_per_sub);
            state.last_accounted_at = (state.received > 0 && state.received < len).then_some(now);
        }
    }

    fn request_at(&self, block: BlockIndex) -> Request {
        Request {
            index: self.piece_index,
            begin: (block * BLOCK_SIZE) as u32,
            len: if block + 1 == self.block_map.len() {
                self.last_block_size as u32
            } else {
                BLOCK_SIZE as u32
            },
        }
    }

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
        self.subs[index].received == to - from
    }

    /// Take the sub-piece's arrival pair as its last wait; `None` if fewer than
    /// two blocks have landed or the pair was already taken.
    fn take_last_wait(&mut self, in_piece_offset: u32) -> Option<LastWait> {
        let sub = (in_piece_offset / SUB_PIECE_SIZE) as usize;
        let st = self.subs.get_mut(sub)?;
        let last = st.last.take()?;
        let second_last = st.second_last.take()?;
        // `second_last` was already clamped to the blocker's own pick.
        Some(LastWait {
            wait: last - second_last,
            blocker: st.blocker,
        })
    }

    fn is_all_not_requested(&self) -> bool {
        self.requested_or_received_count == 0
    }

    fn is_all_requested_or_received(&mut self) -> bool {
        self.requested_or_received_count == self.block_map.len()
    }

    /// Test helper: build a `PieceBlocks` from just `block_map`, deriving every
    /// cursor field automatically so tests need not hand-compute them.
    #[cfg(test)]
    fn from_block_map(
        piece_index: u32,
        last_block_size: usize,
        block_map: Vec<BlockStatus>,
    ) -> Self {
        let mut pb = PieceBlocks {
            piece_index,
            last_block_size,
            all_request_or_received_before: 0,
            requested_or_received_count: 0,
            received_count: 0,
            block_map,
            repick_deadline: IndexTree::new(),
            subs: Vec::new(),
        };
        pb.rebuild_cursors();
        pb
    }

    /// Test helper: recompute every derived cursor field from `block_map`
    /// (`*_count`, the contiguous requested/received prefix, `subs`,
    /// and `repick_deadline` from each block's requesters).
    #[cfg(test)]
    fn rebuild_cursors(&mut self) {
        self.all_request_or_received_before = self
            .block_map
            .iter()
            .take_while(|b| matches!(b, BlockStatus::Requested { .. } | BlockStatus::Received))
            .count();
        self.received_count = self
            .block_map
            .iter()
            .filter(|b| matches!(b, BlockStatus::Received))
            .count();
        self.requested_or_received_count = self
            .block_map
            .iter()
            .filter(|b| matches!(b, BlockStatus::Requested { .. } | BlockStatus::Received))
            .count();

        let blocks_per_sub = (SUB_PIECE_SIZE as usize) / BLOCK_SIZE;
        let mut subs = vec![SubPieceState::new(); self.block_map.len().div_ceil(blocks_per_sub)];
        let mut repick = IndexTree::new();
        for (i, b) in self.block_map.iter().enumerate() {
            match b {
                BlockStatus::Received => subs[i / blocks_per_sub].received += 1,
                BlockStatus::Requested { requested, .. } => {
                    if let Some(deadline) = repick_deadline_of(requested) {
                        repick.insert(i, deadline);
                    }
                }
                BlockStatus::NotRequested { .. } => {}
            }
        }
        self.subs = subs;
        self.repick_deadline = repick;
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
        rtt: Option<time::Duration>,
        slow_at_pick: bool,
        repick_option: RepickOption,
        picked_out: &mut Vec<BlockRange>,
        account: &mut Accounting,
        block_occupy: &mut BlockOccupy,
        mut on_repick: impl FnMut(BlockIndex, tokio::time::Instant),
        #[cfg(feature = "metrics")] torrent: &str,
        #[cfg(feature = "blocktrace")] bt_torrent_id: Option<i64>,
        #[cfg(feature = "blocktrace")] bt_peer_id: Option<i64>,
    ) -> usize {
        // The caller can exhaust its budget and then call again for another range.
        // Neither fresh picks nor repicks may change state when the budget is zero.
        if n == 0 {
            return 0;
        }

        let mut count = 0;
        let n_blocks = self.block_map.len();
        let now = tokio::time::Instant::now();
        let blocks_per_sub = SUB_PIECE_SIZE as usize / BLOCK_SIZE;

        while self.all_request_or_received_before < self.block_map.len() && count < n {
            let mut from = None;
            let mut to = None;
            for i in self.all_request_or_received_before..n_blocks {
                if count >= n {
                    break;
                }
                let will_pick = match &self.block_map[i] {
                    BlockStatus::NotRequested { .. } => true,
                    BlockStatus::Requested { requested, .. } => requested.is_empty(),
                    BlockStatus::Received => false,
                };
                if will_pick {
                    let sub = i / blocks_per_sub;
                    Self::settle_sub(&mut self.subs, &self.block_map, sub, account, now);
                    self.subs[sub].last_accounted_at.get_or_insert(now);
                }
                let req = Some(self.request_at(i));
                let b = &mut self.block_map[i];
                match b {
                    BlockStatus::NotRequested { revoked } => {
                        let detail =
                            PickedDetail::new(now, *n_in_flight, avg_speed, rtt, slow_at_pick);
                        let deadline = detail.repick_eligible_at();

                        #[cfg(feature = "blocktrace")]
                        if let (Some(tid), Some(pid)) = (bt_torrent_id, bt_peer_id) {
                            let mut ev = blocktrace::BlockEvent::at(
                                tid,
                                pid,
                                self.piece_index as i64,
                                i as i64,
                                blocktrace::EventKind::Pick,
                            );
                            ev.exp_us = Some(detail.expected_response_time.as_micros() as i64);
                            ev.n_inflight = Some(detail.n_in_flight_when_picked as i64);
                            blocktrace::record_block_event(ev);
                        }

                        revoked.remove(&peer);
                        block_occupy.insert(peer, (self.piece_index, i), now);
                        *b = BlockStatus::Requested {
                            requested: HashMap::from([(peer, detail)]),
                            revoked: std::mem::take(revoked),
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
                        self.repick_deadline.insert(i, deadline);
                    }
                    BlockStatus::Requested { requested, revoked } => {
                        self.all_request_or_received_before = i + 1;
                        if requested.is_empty() {
                            let detail =
                                PickedDetail::new(now, *n_in_flight, avg_speed, rtt, slow_at_pick);
                            let deadline = detail.repick_eligible_at();

                            #[cfg(feature = "blocktrace")]
                            if let (Some(tid), Some(pid)) = (bt_torrent_id, bt_peer_id) {
                                let mut ev = blocktrace::BlockEvent::at(
                                    tid,
                                    pid,
                                    self.piece_index as i64,
                                    i as i64,
                                    blocktrace::EventKind::Pick,
                                );
                                ev.exp_us = Some(detail.expected_response_time.as_micros() as i64);
                                ev.n_inflight = Some(detail.n_in_flight_when_picked as i64);
                                blocktrace::record_block_event(ev);
                            }

                            revoked.remove(&peer);
                            block_occupy.insert(peer, (self.piece_index, i), now);
                            requested.insert(peer, detail);
                            *n_in_flight += 1;
                            if from.is_none() {
                                from = req;
                            } else {
                                to = req;
                            }
                            count += 1;
                            // block is already counted in requested_or_received_count
                            self.repick_deadline.insert(i, deadline);
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
            match (from, to) {
                (None, _) => {
                    assert!(self.all_request_or_received_before == n_blocks || count == n);
                }
                (Some(f), None) => {
                    picked_out.push(BlockRange { from: f, to: f });
                }
                (Some(f), Some(t)) => {
                    picked_out.push(BlockRange { from: f, to: t });
                }
            }
        }

        let repick_limit = repick_option.repick_limit;
        if repick_limit <= 1 && !repick_option.endgame {
            #[cfg(feature = "metrics")]
            metrics::record_requests(torrent, repick_option.mode_label(), count as u64, 0);
            return count;
        }

        // TODO: Enforce repick_limit against requested.len() to bound duplicate
        // requests and make endgame's request-count rounds distribute them evenly.
        let repick_begin = picked_out.len();
        // The tree is ordered by deadline. In rush mode `take_while` stops at the
        // first block whose deadline is still in the future; in endgame mode the
        // predicate is always true so every requested block is considered.
        for (i, _) in self
            .repick_deadline
            .iter(false)
            .take_while(|(_, t)| repick_option.endgame || **t <= now)
        {
            if count >= n {
                break;
            }
            let i = *i;
            if let BlockStatus::Requested { requested, .. } = &self.block_map[i]
                && !requested.contains_key(&peer)
            {
                let sub = i / blocks_per_sub;
                Self::settle_sub(&mut self.subs, &self.block_map, sub, account, now);
            }
            let req = self.request_at(i);
            let b = &mut self.block_map[i];
            match b {
                BlockStatus::Requested { requested, revoked } => {
                    if !requested.contains_key(&peer) {
                        if repick_option.endgame {
                            trace!("{peer:?} in endgame mode, repick {req:?}",);
                        } else {
                            trace!(
                                "{peer:?} repick {req:?} because we are faster {:?}",
                                requested
                                    .iter()
                                    .map(|(p, t)| (p, t.pick_time.elapsed()))
                                    .collect::<Vec<_>>()
                            );
                        }
                        count += 1;
                        let detail =
                            PickedDetail::new(now, *n_in_flight, avg_speed, rtt, slow_at_pick);

                        #[cfg(feature = "blocktrace")]
                        if let (Some(tid), Some(pid)) = (bt_torrent_id, bt_peer_id) {
                            let mut ev = blocktrace::BlockEvent::at(
                                tid,
                                pid,
                                self.piece_index as i64,
                                i as i64,
                                blocktrace::EventKind::Repick,
                            );
                            ev.reason = Some(if repick_option.endgame {
                                blocktrace::RepickReason::Endgame
                            } else {
                                blocktrace::RepickReason::Faster
                            });
                            ev.exp_us = Some(detail.expected_response_time.as_micros() as i64);
                            ev.n_inflight = Some(detail.n_in_flight_when_picked as i64);
                            blocktrace::record_block_event(ev);
                        }

                        revoked.remove(&peer);
                        block_occupy.insert(peer, (self.piece_index, i), now);
                        requested.insert(peer, detail);
                        on_repick(i, now);
                        *n_in_flight += 1;
                        picked_out.push(BlockRange { from: req, to: req });
                    }
                }
                _ => unreachable!("should only see requested blocks in repick tree"),
            }
        }

        for br in &picked_out[repick_begin..] {
            let i = (br.from.begin / BLOCK_SIZE as u32) as usize;
            // Just repicked, so the block is Requested with an extra requester;
            // its deadline is the latest per-requester eligibility across all.
            let BlockStatus::Requested { requested, .. } = &self.block_map[i] else {
                unreachable!("just-repicked block must be Requested");
            };
            if let Some(deadline) = repick_deadline_of(requested) {
                self.repick_deadline.insert(i, deadline);
            }
        }

        #[cfg(feature = "metrics")]
        {
            let repick = (picked_out.len() - repick_begin) as u64;
            let new_pick = count as u64 - repick;
            metrics::record_requests(torrent, repick_option.mode_label(), new_pick, repick);
        }

        count
    }

    /// inform some block is received, returns all being requested peers
    /// requested for that block
    fn receive(
        &mut self,
        req: Request,
        account: &mut Accounting,
        block_occupy: &mut BlockOccupy,
    ) -> Vec<PeerAddr> {
        let now = tokio::time::Instant::now();
        // input req must be valid
        let b_index = (req.begin as usize) / BLOCK_SIZE;
        let sub = (b_index * BLOCK_SIZE) / (SUB_PIECE_SIZE as usize);
        // A duplicate must not move the last wait: only the first arrival of a
        // block bounds the sub-piece's wait.
        if matches!(self.block_map[b_index], BlockStatus::Received) {
            return vec![];
        }
        let blocks_per_sub = SUB_PIECE_SIZE as usize / BLOCK_SIZE;
        Self::settle_sub(&mut self.subs, &self.block_map, sub, account, now);

        // `blocker` travels with `last`, so it always names the last-arriving
        // block's requester; revoked peers are already pruned from `requested`.
        let (blocker, pick_at) = match &self.block_map[b_index] {
            BlockStatus::Requested { requested, .. } => {
                match requested.iter().min_by_key(|(_, d)| d.pick_time) {
                    Some((a, d)) => (Some(*a), Some(d.pick_time)),
                    None => (None, None),
                }
            }
            BlockStatus::NotRequested { .. } => (None, None),
            BlockStatus::Received => unreachable!("duplicates returned above"),
        };
        let st = &mut self.subs[sub];
        st.blocker = blocker;
        // The wait may only start when its requester took the block on: a
        // rescuer must not pay for the gap that preceded its pick.
        st.second_last = match (st.last, pick_at) {
            (Some(prev), Some(pick_at)) => Some(prev.max(pick_at)),
            (prev, _) => prev,
        };
        st.last = Some(now);
        st.received += 1;
        let from = sub * blocks_per_sub;
        let to = (from + blocks_per_sub).min(self.block_map.len());
        if st.received == to - from {
            st.last_accounted_at = None;
        } else {
            // A late/unsolicited block can create partial progress without a
            // recorded pick. Start now; never invent an earlier request time.
            st.last_accounted_at.get_or_insert(now);
        }

        block_occupy.remove_all((self.piece_index, b_index), &self.block_map[b_index]);
        let b = &mut self.block_map[b_index];
        match b {
            BlockStatus::NotRequested { .. } => {
                self.received_count += 1;
                self.requested_or_received_count += 1;
                *b = BlockStatus::Received;
                vec![]
            }
            BlockStatus::Requested { requested, .. } => {
                self.received_count += 1;
                let ret = requested.keys().map(|x| *x).collect();
                *b = BlockStatus::Received;
                self.repick_deadline.remove(&b_index);
                ret
            }
            BlockStatus::Received => unreachable!("duplicates returned above"),
        }
    }

    /// inform some block request is rejected or no response, and
    /// should be send to other peers
    fn revoke(&mut self, peer: &PeerAddr, req: Request, account: &mut Accounting) {
        let b_index = (req.begin as usize) / BLOCK_SIZE;
        if let BlockStatus::Requested { requested, .. } = &self.block_map[b_index]
            && requested.contains_key(peer)
        {
            let blocks_per_sub = SUB_PIECE_SIZE as usize / BLOCK_SIZE;
            let sub = b_index / blocks_per_sub;
            Self::settle_sub(
                &mut self.subs,
                &self.block_map,
                sub,
                account,
                tokio::time::Instant::now(),
            );
        }

        let b = &mut self.block_map[b_index];
        match b {
            BlockStatus::Requested { requested, revoked } => {
                // A peer can only be revoked if it's requested before
                let mut did_remove = false;
                if let Some(v) = requested.remove(peer) {
                    revoked.insert(*peer, v);
                    did_remove = true;
                }
                if requested.is_empty() {
                    *b = BlockStatus::NotRequested {
                        revoked: std::mem::take(revoked),
                    };
                    if self.all_request_or_received_before > b_index {
                        self.all_request_or_received_before = b_index;
                    }
                    self.requested_or_received_count -= 1;
                    self.repick_deadline.remove(&b_index);
                } else if did_remove {
                    // block still has other requesters: re-derive its deadline
                    // from the remaining ones (the removed one may or may not
                    // be the latest-eligible)
                    if let Some(deadline) = repick_deadline_of(requested) {
                        self.repick_deadline.insert(b_index, deadline);
                    }
                }
            }
            BlockStatus::NotRequested { .. } | BlockStatus::Received => {}
        }
    }

    /// Finish a rejected request and discard its history and index entry.
    fn reject(
        &mut self,
        peer: &PeerAddr,
        req: Request,
        account: &mut Accounting,
        block_occupy: &mut BlockOccupy,
    ) {
        self.revoke(peer, req, account);

        let block = req.begin as usize / BLOCK_SIZE;
        match &mut self.block_map[block] {
            BlockStatus::Requested { revoked, .. } | BlockStatus::NotRequested { revoked } => {
                revoked.remove(peer);
            }
            BlockStatus::Received => {}
        }
        block_occupy.remove_one(*peer, (self.piece_index, block));
    }

    /// revoke request of one peer for all `Requested` which fulfills condition
    #[cfg(test)]
    fn revoke_all_requested_if<F>(
        &mut self,
        remove: F,
        revoked_reqs: &mut HashMap<PeerAddr, Vec<Revoke>>,
        account: &mut Accounting,
    ) where
        F: Fn(&PeerAddr, &tokio::time::Instant) -> bool,
    {
        let now = tokio::time::Instant::now();
        // A removal can make the caller drop the whole piece, including other
        // sub-pieces with no active requesters.
        let needs_settlement = self.is_all_not_requested()
            || self.block_map.iter().any(|b| match b {
                BlockStatus::Requested { requested, .. } => {
                    requested.iter().any(|(p, d)| remove(p, &d.pick_time))
                }
                _ => false,
            });
        if needs_settlement {
            self.settle_account(account, now);
        }

        for i in (0..self.block_map.len()).rev() {
            let req = self.request_at(i);
            let b = &mut self.block_map[i];
            match b {
                BlockStatus::Requested {
                    requested, revoked, ..
                } => {
                    let before = requested.len();
                    requested.retain(|p, t| {
                        if remove(p, &t.pick_time) {
                            info!(
                                "revoke block {req:?} from {p:?}, after {:?}",
                                t.pick_time.elapsed()
                            );
                            revoked_reqs
                                .entry(*p)
                                .or_default()
                                .push(Revoke::Timeout(req));
                            revoked.entry(*p).or_insert(t.clone());
                            false
                        } else {
                            true
                        }
                    });
                    let removed_any = requested.len() != before;
                    if requested.is_empty() {
                        *b = BlockStatus::NotRequested {
                            revoked: std::mem::take(revoked),
                        };
                        self.requested_or_received_count -= 1;
                        if self.all_request_or_received_before > i {
                            self.all_request_or_received_before = i;
                        }
                        self.repick_deadline.remove(&i);
                    } else if removed_any {
                        // some (not all) requesters were revoked: re-derive the
                        // deadline from the remaining ones
                        if let Some(deadline) = repick_deadline_of(requested) {
                            self.repick_deadline.insert(i, deadline);
                        }
                    }
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

#[cfg(feature = "metrics")]
impl RepickOption {
    /// Scheduling-mode label for `requests_sent_total`.
    fn mode_label(&self) -> &'static str {
        if self.endgame {
            "endgame"
        } else if self.repick_limit > 1 {
            "rush"
        } else {
            "normal"
        }
    }
}

type Picker = dyn PiecePicker<T = PeerPieceDetail> + Send;
type PieceIndex = u32;
type BlockIndex = usize;
type Unrequested = usize;
type Unreceived = usize;
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
    // Lightweight scheduling indexes; PieceBlocks remains owned by the maps above.
    requesting_order: IndexTree<PieceIndex, Unrequested>,
    receiving_order: IndexTree<PieceIndex, Unreceived>,

    // Shared by slow peers; fully requested pieces remain here until verified.
    slow_pieces: HashMap<PieceIndex, PieceBlocks>,
    slow_order: IndexTree<PieceIndex, Unreceived>,
    slow_peers: HashSet<PeerAddr>,
    /// Slow block requests left in fast pieces, indexed by takeover deadline.
    slow_blocks: IndexTree<(PieceIndex, BlockIndex), tokio::time::Instant>,

    // previous time the timeout request check runs
    prev_time_check: tokio::time::Instant,
    // Previous picker-maintenance tick, checked when selecting blocks.
    prev_tick: tokio::time::Instant,

    no_response_timeout: time::Duration,

    /// Peer windows, owned by this picker and updated before block mutations.
    accounting: Accounting,
    block_occupy: BlockOccupy,

    endgame: bool,

    // tracks per-file completion by counting verified pieces against each
    // file's piece span; queried for the startup `.part` rename
    file_tracker: FileTracker,

    #[cfg(feature = "metrics")]
    torrent_hex: String,
}

/// A picker request removed from its original peer, with the required connection action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Revoke {
    Timeout(Request),
    SlowTakeover(Request),
}

impl Revoke {
    pub fn request(&self) -> Request {
        match self {
            Self::Timeout(request) | Self::SlowTakeover(request) => *request,
        }
    }
}

/// if some piece or sub piece is completed
pub struct PieceComplete {
    /// if piece completes
    pub piece: bool,
    /// if sub piece completes
    pub sub_piece: bool,
}

impl BlockPicker {
    fn piece(&self, index: PieceIndex) -> Option<&PieceBlocks> {
        self.requesting
            .get(&index)
            .or_else(|| self.receiving.get(&index))
            .or_else(|| self.slow_pieces.get(&index))
    }

    fn piece_mut(&mut self, index: PieceIndex) -> Option<&mut PieceBlocks> {
        self.piece_state_mut(index).map(|(piece, _, _)| piece)
    }

    fn piece_state_mut(
        &mut self,
        index: PieceIndex,
    ) -> Option<(&mut PieceBlocks, &mut Accounting, &mut BlockOccupy)> {
        let piece = self
            .requesting
            .get_mut(&index)
            .or_else(|| self.receiving.get_mut(&index))
            .or_else(|| self.slow_pieces.get_mut(&index))?;
        Some((piece, &mut self.accounting, &mut self.block_occupy))
    }

    fn pieces(&self) -> impl Iterator<Item = (&PieceIndex, &PieceBlocks)> {
        self.requesting
            .iter()
            .chain(self.receiving.iter())
            .chain(self.slow_pieces.iter())
    }

    fn remove_from_slow_blocks(&mut self, index: PieceIndex, block: Option<BlockIndex>) {
        if let Some(block) = block {
            self.slow_blocks.remove(&(index, block));
        } else {
            let count = self.piece(index).map_or(0, |piece| piece.block_map.len());
            for block in 0..count {
                self.slow_blocks.remove(&(index, block));
            }
        }
    }

    /// One of the requesters of this block leaves, this block may
    /// move to or remove from slow_blocks
    /// If blocks in a fast piece are now all requested to slow peers
    /// add to slow blocks
    fn update_slow_blocks(&mut self, index: PieceIndex, block: Option<BlockIndex>) {
        let fast_piece = self
            .requesting
            .get(&index)
            .or_else(|| self.receiving.get(&index));
        let is_fast_piece = fast_piece.is_some();
        let Some(piece) = fast_piece.or_else(|| self.slow_pieces.get(&index)) else {
            return;
        };

        let range = block.map_or(0..piece.block_map.len(), |b| b..b + 1);
        for block in range {
            let key = (index, block);
            let status = &piece.block_map[block];
            match status {
                BlockStatus::Requested { requested, .. } if requested.len() > 0 => {
                    let deadline = is_fast_piece
                        .then(|| takeover_deadline_of(&self.slow_peers, status))
                        .flatten();
                    if let Some(deadline) = deadline {
                        self.slow_blocks.insert(key, deadline);
                    }
                }
                _ => {
                    self.slow_blocks.remove(&key);
                }
            }
        }
    }

    fn remove_piece(&mut self, index: PieceIndex) {
        self.settle_piece(index);
        self.remove_from_slow_blocks(index, None);
        if let Some(piece) = self
            .requesting
            .get(&index)
            .or_else(|| self.receiving.get(&index))
            .or_else(|| self.slow_pieces.get(&index))
        {
            for (block, status) in piece.block_map.iter().enumerate() {
                self.block_occupy.remove_all((index, block), status);
            }
        }

        self.requesting.remove(&index);
        self.receiving.remove(&index);
        self.slow_pieces.remove(&index);
        self.requesting_order.remove(&index);
        self.receiving_order.remove(&index);
        self.slow_order.remove(&index);
    }

    /// Maintain requesting/receiving/slow_pieces and their orders
    /// DOES NOT maintain slow_blocks
    fn update_piece_state_and_order(&mut self, index: PieceIndex) {
        if self
            .requesting
            .get(&index)
            .is_some_and(|p| p.requested_or_received_count == p.block_map.len())
        {
            let p = self.requesting.remove(&index).unwrap();
            self.receiving.insert(index, p);
        }

        if self
            .receiving
            .get(&index)
            .is_some_and(|p| p.requested_or_received_count < p.block_map.len())
        {
            let p = self.receiving.remove(&index).unwrap();
            self.requesting.insert(index, p);
        }

        if let Some(p) = self.slow_pieces.get(&index) {
            if p.is_all_received() {
                let piece = self.slow_pieces.remove(&index).unwrap();
                self.receiving.insert(index, piece);
            }
        }

        if let Some(p) = self.requesting.get(&index) {
            self.requesting_order
                .insert(index, p.block_map.len() - p.requested_or_received_count);
        } else {
            self.requesting_order.remove(&index);
        }
        if let Some(p) = self.receiving.get(&index) {
            self.receiving_order
                .insert(index, p.block_map.len() - p.received_count);
        } else {
            self.receiving_order.remove(&index);
        }
        if let Some(p) = self.slow_pieces.get(&index) {
            self.slow_order
                .insert(index, p.block_map.len() - p.received_count);
        } else {
            self.slow_order.remove(&index);
        }
    }

    /// Bulk changes and progress restoration rebuild the derived indexes.
    fn rebuild_piece_order(&mut self) {
        self.requesting_order = IndexTree::new();
        self.receiving_order = IndexTree::new();
        self.slow_order = IndexTree::new();
        for (&index, blocks) in &self.requesting {
            self.requesting_order.insert(
                index,
                blocks.block_map.len() - blocks.requested_or_received_count,
            );
        }
        for (&index, blocks) in &self.receiving {
            self.receiving_order
                .insert(index, blocks.block_map.len() - blocks.received_count);
        }
        for (&index, blocks) in &self.slow_pieces {
            if !blocks.is_all_received() {
                self.slow_order
                    .insert(index, blocks.block_map.len() - blocks.received_count);
            }
        }
    }

    /// Return aged (ST, fair ST) per accepted block, hiding the aging windows.
    pub(crate) fn peer_slot_cost(&self, peer: &PeerAddr) -> PeerCost {
        self.accounting
            .peers
            .get(peer)
            .map_or_default(|slot_time| slot_time.per_block())
    }

    /// Called only for a useful, non-duplicate block from a live connection.
    pub(crate) fn record_slot_block(&mut self, peer: &PeerAddr) {
        self.accounting.accept_block(*peer);
    }

    /// Settle all active pieces to the same timestamp.
    pub(crate) fn settle_all_piece(&mut self, now: tokio::time::Instant) {
        for p in self
            .requesting
            .values_mut()
            .chain(self.receiving.values_mut())
            .chain(self.slow_pieces.values_mut())
        {
            p.settle_account(&mut self.accounting, now);
        }
    }

    /// Settle one piece to the current time without stopping its clock.
    fn settle_piece(&mut self, index: u32) {
        if let Some((piece, accounting, _)) = self.piece_state_mut(index) {
            piece.settle_account(accounting, tokio::time::Instant::now());
        }
    }
}

impl BlockPicker {
    pub fn new(
        meta: &Metadata,
        piece_picker: Box<Picker>,
        no_response_timeout: time::Duration,
    ) -> Self {
        let total_size = meta.len();
        let piece_size = meta.regular_piece_size();
        let (n, last_length) = piece_total_and_last_size(total_size, piece_size);
        Self {
            n,
            piece_size,
            last_length,
            piece_picker,
            requesting: BTreeMap::new(),
            receiving: BTreeMap::new(),
            slow_pieces: HashMap::new(),
            slow_order: IndexTree::new(),
            slow_peers: HashSet::new(),
            slow_blocks: IndexTree::new(),
            requesting_order: IndexTree::new(),
            receiving_order: IndexTree::new(),
            prev_time_check: tokio::time::Instant::now(),
            prev_tick: tokio::time::Instant::now(),
            no_response_timeout,
            endgame: false,
            accounting: Accounting::default(),
            block_occupy: BlockOccupy::default(),
            file_tracker: FileTracker::new(meta),
            #[cfg(feature = "metrics")]
            torrent_hex: hex::encode(meta.info_hash),
        }
    }

    /// Test-only constructor for a single logical file spanning `total_size`,
    /// avoiding the need to build a full `Metadata`.
    #[cfg(test)]
    pub fn new_single_file(
        total_size: u64,
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
            slow_pieces: HashMap::new(),
            slow_order: IndexTree::new(),
            slow_peers: HashSet::new(),
            slow_blocks: IndexTree::new(),
            requesting_order: IndexTree::new(),
            receiving_order: IndexTree::new(),
            prev_time_check: tokio::time::Instant::now(),
            prev_tick: tokio::time::Instant::now(),
            no_response_timeout,
            endgame: false,
            accounting: Accounting::default(),
            block_occupy: BlockOccupy::default(),
            file_tracker: FileTracker::from_lengths(piece_size as u64, [total_size]),
            #[cfg(feature = "metrics")]
            torrent_hex: String::new(),
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
        let n_subs = (n_blocks * BLOCK_SIZE).div_ceil(sub_sz);
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
            repick_deadline: IndexTree::new(),
            subs: vec![SubPieceState::new(); n_subs],
        }
    }

    /// Last wait of the sub-piece at `in_piece_offset`; `None` once taken or when the
    /// piece is not tracked (verified, or restored from a dump).
    pub fn take_last_wait(&mut self, index: u32, in_piece_offset: u32) -> Option<LastWait> {
        self.piece_mut(index)
            .and_then(|b| b.take_last_wait(in_piece_offset))
    }

    pub fn get_block_status(&self, req: &Request) -> Option<&BlockStatus> {
        if !self.check_block_validity(req) {
            return None;
        }
        self.piece(req.index)
            .map(|b| &b.block_map[req.begin as usize / BLOCK_SIZE])
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

    /// Pick up to `n` blocks from `peer`.
    ///
    /// Returns:
    /// - The picked block requests.
    /// - The actual number of blocks picked.
    ///
    /// Removed requests are added to `revoked`, grouped by peer and carrying
    /// the reason for the connection layer.
    pub fn pick_blocks(
        &mut self,
        peer: &PeerAddr,
        n: usize,
        mut n_in_flight: usize,
        avg_speed: f32,
        rtt: Option<time::Duration>,
        revoked: &mut HashMap<PeerAddr, Vec<Revoke>>,
        n_cache_vacant: usize,
        #[cfg(feature = "blocktrace")] bt_torrent_id: Option<i64>,
        #[cfg(feature = "blocktrace")] bt_peer_id: Option<i64>,
    ) -> (BlockRequests, usize) {
        if self.prev_time_check.elapsed() >= time::Duration::from_secs(1) {
            self.revoke_unrespond(revoked);
            self.prev_time_check = tokio::time::Instant::now();
        }
        // TODO: Reuse vectors to reduce allocations on the pick path.
        let mut ret = Vec::new();
        let mut remain = n;
        if n == 0 || self.piece_picker.peer_detail(peer).is_none_or(|p| p.choke) {
            return (
                BlockRequests {
                    piece_size: self.piece_size as u32,
                    range: ret,
                },
                0,
            );
        }
        let rush = n_cache_vacant < POOL_SIZE / 4;
        let slow = self.slow_peers.contains(peer);
        if !self.update_endgame() {
            let option = RepickOption {
                // TODO: Choose a proper rush repick limit instead of the fixed 7.
                repick_limit: if rush { 7 } else { 1 },
                endgame: false,
            };
            let order = self.normal_piece_order(rush, slow);
            self.pick_blocks_within(
                peer,
                &order,
                &mut remain,
                &mut n_in_flight,
                avg_speed,
                rtt,
                option,
                &mut ret,
                #[cfg(feature = "blocktrace")]
                bt_torrent_id,
                #[cfg(feature = "blocktrace")]
                bt_peer_id,
            );

            // Try takeover slow blocks before opening fresh pieces.
            if !slow && remain > 0 && n_cache_vacant > 0 && !self.update_endgame() {
                self.pick_slow_blocks(
                    peer,
                    &mut remain,
                    &mut n_in_flight,
                    avg_speed,
                    rtt,
                    &mut ret,
                    revoked,
                    #[cfg(feature = "blocktrace")]
                    bt_torrent_id,
                    #[cfg(feature = "blocktrace")]
                    bt_peer_id,
                );
            }

            // Existing pieces always precede fresh pieces, including in the slow pool.
            while remain > 0 && n_cache_vacant > 0 && !self.update_endgame() {
                let mode = if slow {
                    PickMode::ForSlowPeer
                } else {
                    PickMode::Normal
                };
                let Some(index) = self.piece_picker.pick_next(peer, mode) else {
                    break;
                };
                let blocks = self.piece_block_of(index);
                if slow {
                    self.slow_pieces.insert(index, blocks);
                } else {
                    self.requesting.insert(index, blocks);
                }

                self.pick_blocks_within(
                    peer,
                    &[index],
                    &mut remain,
                    &mut n_in_flight,
                    avg_speed,
                    rtt,
                    option,
                    &mut ret,
                    #[cfg(feature = "blocktrace")]
                    bt_torrent_id,
                    #[cfg(feature = "blocktrace")]
                    bt_peer_id,
                );
            }
        }

        // we may not in endgame before, but entering endgame now
        if remain > 0 && self.update_endgame() {
            self.pick_endgame(
                peer,
                &mut remain,
                &mut n_in_flight,
                avg_speed,
                rtt,
                &mut ret,
                #[cfg(feature = "blocktrace")]
                bt_torrent_id,
                #[cfg(feature = "blocktrace")]
                bt_peer_id,
            );
        }
        (
            BlockRequests {
                piece_size: self.piece_size as u32,
                range: ret,
            },
            n - remain,
        )
    }

    fn pick_slow_blocks(
        &mut self,
        peer: &PeerAddr,
        remain: &mut usize,
        n_in_flight: &mut usize,
        avg_speed: f32,
        rtt: Option<time::Duration>,
        ret: &mut Vec<BlockRange>,
        revoked: &mut HashMap<PeerAddr, Vec<Revoke>>,
        #[cfg(feature = "blocktrace")] bt_torrent_id: Option<i64>,
        #[cfg(feature = "blocktrace")] bt_peer_id: Option<i64>,
    ) {
        let now = tokio::time::Instant::now();
        let (requesting, receiving) = (&self.requesting, &self.receiving);
        let peer_state = self.piece_picker.peer_detail(peer).unwrap();

        // due is the blocks we have and can pick now
        let pickable: Vec<_> = self
            .slow_blocks
            .iter(false)
            .take_while(|(_, deadline)| **deadline <= now)
            .filter_map(|((idx, blk), _)| peer_state.have(*idx).then_some((*idx, *blk)))
            .filter(|(idx, _)| requesting.contains_key(&idx) || receiving.contains_key(&idx))
            .collect();

        for (index, block) in pickable {
            if *remain == 0 {
                break;
            }

            let (piece, accounting, block_occupy) = self.piece_state_mut(index).unwrap();
            if let BlockStatus::Requested { requested, .. } = &piece.block_map[block]
                && !requested.contains_key(peer)
            {
                let detail = PickedDetail::new(now, *n_in_flight, avg_speed, rtt, false);
                let sub = block / (SUB_PIECE_SIZE as usize / BLOCK_SIZE);
                PieceBlocks::settle_sub(&mut piece.subs, &piece.block_map, sub, accounting, now);

                let req = piece.request_at(block);
                let BlockStatus::Requested {
                    requested,
                    revoked: revoked_reqs,
                } = &mut piece.block_map[block]
                else {
                    unreachable!("slow takeover requires outstanding requests");
                };

                for (p, detail) in requested.drain() {
                    revoked_reqs.insert(p, detail);
                    revoked
                        .entry(p)
                        .or_default()
                        .push(Revoke::SlowTakeover(req));
                }

                #[cfg(feature = "blocktrace")]
                if let (Some(tid), Some(pid)) = (bt_torrent_id, bt_peer_id) {
                    let mut ev = blocktrace::BlockEvent::at(
                        tid,
                        pid,
                        index as i64,
                        block as i64,
                        blocktrace::EventKind::Repick,
                    );
                    ev.reason = Some(blocktrace::RepickReason::SlowTakeover);
                    ev.exp_us = Some(detail.expected_response_time.as_micros() as i64);
                    ev.n_inflight = Some(detail.n_in_flight_when_picked as i64);
                    blocktrace::record_block_event(ev);
                }

                piece
                    .repick_deadline
                    .insert(block, detail.repick_eligible_at());
                revoked_reqs.remove(peer);
                block_occupy.insert(*peer, (index, block), now);
                requested.insert(*peer, detail);
                self.slow_blocks.remove(&(index, block));
                ret.push(BlockRange { from: req, to: req });
                *remain -= 1;
                *n_in_flight += 1;

                #[cfg(feature = "metrics")]
                metrics::record_requests(&self.torrent_hex, "slow_takeover", 0, 1);
            }
        }
    }

    /// Pick blocks only from the pieces listed in `indices`, in the supplied order.
    /// The pieces in `indices` should be already in requesting/receiving/slow_pieces,
    /// otherwise no blocks are picked
    fn pick_blocks_within(
        &mut self,
        peer: &PeerAddr,
        indices: &[PieceIndex],
        remain: &mut usize,
        n_in_flight: &mut usize,
        avg_speed: f32,
        rtt: Option<time::Duration>,
        option: RepickOption,
        ret: &mut Vec<BlockRange>,
        #[cfg(feature = "blocktrace")] bt_torrent_id: Option<i64>,
        #[cfg(feature = "blocktrace")] bt_peer_id: Option<i64>,
    ) {
        #[cfg(feature = "metrics")]
        let torrent_hex = self.torrent_hex.clone();
        let slow = self.slow_peers.contains(peer);

        for &index in indices {
            if *remain == 0 {
                break;
            }

            let have_piece = self
                .piece_picker
                .peer_detail(peer)
                .is_some_and(|p| p.have(index));
            let piece = self
                .requesting
                .get_mut(&index)
                .or_else(|| self.receiving.get_mut(&index))
                .or_else(|| self.slow_pieces.get_mut(&index));

            if have_piece && let Some(blocks) = piece {
                let slowblocks = &mut self.slow_blocks;
                let picked = blocks.pick(
                    *peer,
                    *remain,
                    n_in_flight,
                    avg_speed,
                    rtt,
                    slow,
                    option,
                    ret,
                    &mut self.accounting,
                    &mut self.block_occupy,
                    |block, picked_at| {
                        let key = (index, block);
                        if slow && slowblocks.contains(&key) {
                            // Endgame lets a slow peer repick a fast-pool block.
                            slowblocks.insert(key, picked_at + SLOW_TAKEOVER_DELAY);
                        } else {
                            slowblocks.remove(&key);
                        }
                    },
                    #[cfg(feature = "metrics")]
                    &torrent_hex,
                    #[cfg(feature = "blocktrace")]
                    bt_torrent_id,
                    #[cfg(feature = "blocktrace")]
                    bt_peer_id,
                );
                *remain -= picked;
                self.update_piece_state_and_order(index);
            }
        }
    }

    fn pick_endgame(
        &mut self,
        peer: &PeerAddr,
        remain: &mut usize,
        n_in_flight: &mut usize,
        avg_speed: f32,
        rtt: Option<time::Duration>,
        ret: &mut Vec<BlockRange>,
        #[cfg(feature = "blocktrace")] bt_torrent_id: Option<i64>,
        #[cfg(feature = "blocktrace")] bt_peer_id: Option<i64>,
    ) {
        // Endgame uses exactly one candidate set for every peer, regardless of class.
        // TODO: Verify that endgame candidate sets stay small; computing `from`
        // currently scans every tracked block on each pick.
        let from = self
            .pieces()
            .flat_map(|(_, p)| &p.block_map)
            .filter_map(|b| match b {
                BlockStatus::NotRequested { .. } => Some(0),
                BlockStatus::Requested { requested, .. } => Some(requested.len()),
                BlockStatus::Received => None,
            })
            .min();
        if let Some(from) = from {
            // TODO: Set a dynamic duplicate-request upper limit and avoid repeated
            // scans when this peer has already been requested for every candidate.
            let order = self.endgame_piece_order();
            for limit in from..=from.saturating_add(1) {
                self.pick_blocks_within(
                    peer,
                    &order,
                    remain,
                    n_in_flight,
                    avg_speed,
                    rtt,
                    RepickOption {
                        repick_limit: limit,
                        endgame: true,
                    },
                    ret,
                    #[cfg(feature = "blocktrace")]
                    bt_torrent_id,
                    #[cfg(feature = "blocktrace")]
                    bt_peer_id,
                );
            }
        }
    }

    /// Run due 15-second maintenance before picking. Peer statistics are collected
    /// only when due; each entry contains a connection's identity and received bytes.
    pub(crate) fn tick(
        &mut self,
        peers: impl Iterator<Item = (PeerAddr, u64)>,
        #[cfg(feature = "blocktrace")] peer_trace_id: impl Fn(&PeerAddr) -> Option<i64>,
    ) {
        let now = tokio::time::Instant::now();
        if now.duration_since(self.prev_tick) < time::Duration::from_secs(15) {
            return;
        }
        self.rotate_slot_windows(now);
        let peers: Vec<_> = peers.collect();
        self.update_peer_classes(
            &peers,
            #[cfg(feature = "blocktrace")]
            peer_trace_id,
        );
        self.prev_tick = now;
    }

    /// Advance windows only from the periodic picker tick.
    fn rotate_slot_windows(&mut self, now: tokio::time::Instant) {
        self.settle_all_piece(now);
        for slot_time in self.accounting.peers.values_mut() {
            slot_time.rotate(0.5);
        }
    }

    /// Classify live connections as fast or slow using their current aged ST.
    ///
    /// Each entry in `peers` is `(peer, received_bytes)`:
    /// - `peer` is a `PeerAddr` containing the socket address and connection ID.
    /// - `received_bytes` is the cumulative number of bytes received on that
    ///   connection, including duplicate and late responses (`PeerConn::stat.total_recv`).
    fn update_peer_classes(
        &mut self,
        peers: &[(PeerAddr, u64)],
        #[cfg(feature = "blocktrace")] peer_trace_id: impl Fn(&PeerAddr) -> Option<i64>,
    ) {
        let now = tokio::time::Instant::now();
        self.settle_all_piece(now);

        let samples: Vec<_> = peers
            .iter()
            .map(|(peer, received_bytes)| {
                (*peer, self.peer_slot_cost(peer).blocker, *received_bytes)
            })
            .collect();
        let slow = super::peer_classification::classify_peers(&samples);

        #[cfg(feature = "blocktrace")]
        let ts_us = blocktrace::now_us();
        for (addr, _) in peers {
            if slow.contains(addr) != self.slow_peers.contains(addr) {
                info!(peer = ?addr, slow = slow.contains(addr), "peer picker class changed");
                #[cfg(feature = "blocktrace")]
                if let Some(peer_id) = peer_trace_id(addr) {
                    blocktrace::record_peer_state(blocktrace::PeerStateEvent {
                        peer_id,
                        ts_us,
                        kind: if slow.contains(addr) {
                            blocktrace::PeerStateKind::FastToSlow
                        } else {
                            blocktrace::PeerStateKind::SlowToFast
                        },
                        mode: None,
                    });
                }
            }
        }

        let old_slow = std::mem::replace(&mut self.slow_peers, slow);
        let live: Vec<_> = peers.iter().map(|(addr, _)| *addr).collect();
        self.reconcile_piece_pools(&live);

        // Enroll outstanding fast-pool blocks held by newly demoted peers.
        let mut demoted = Vec::new();
        for peer in self.slow_peers.difference(&old_slow) {
            if let Some(requests) = self.block_occupy.by_peer.get(peer) {
                for (&(index, block), _) in requests.iter(false) {
                    if let BlockStatus::Requested { requested, .. } =
                        &self.piece(index).unwrap().block_map[block]
                        && requested.contains_key(peer)
                    {
                        demoted.push((index, block));
                    }
                }
            }
        }
        for (index, block) in demoted {
            self.update_slow_blocks(index, Some(block));
        }

        self.update_endgame();
        info!(
            fast = peers.len() - self.slow_peers.len(),
            slow = self.slow_peers.len(),
            slow_pieces = self.slow_pieces.len(),
            "peer picker classes refreshed"
        );
    }

    /// Move pieces between fast pool and slow pool
    fn reconcile_piece_pools(&mut self, peers: &[PeerAddr]) {
        let mut to_fast = Vec::new();
        let mut to_slow = Vec::new();
        let pieces: Vec<_> = self
            .pieces()
            .filter_map(|(index, p)| (!p.is_all_received()).then_some(*index))
            .collect();

        'piece: for index in pieces {
            let in_slow = self.slow_pieces.contains_key(&index);
            if in_slow && self.slow_peers.is_empty() {
                to_fast.push(index);
                continue;
            }

            let mut fast_peers_have = false;
            let mut slow_peers_have = false;
            for peer in peers {
                let have_piece = self
                    .piece_picker
                    .peer_detail(peer)
                    .is_some_and(|p| p.have(index));
                if have_piece {
                    let is_slow_peer = self.slow_peers.contains(peer);
                    slow_peers_have |= is_slow_peer;
                    fast_peers_have |= !is_slow_peer;
                }
                if fast_peers_have && slow_peers_have {
                    continue 'piece;
                }
            }
            if in_slow && !slow_peers_have {
                to_fast.push(index);
            } else if !in_slow && !fast_peers_have && slow_peers_have {
                to_slow.push(index);
            }
        }

        for index in to_fast {
            let blocks = self.slow_pieces.remove(&index).unwrap();
            for (block, status) in blocks.block_map.iter().enumerate() {
                if let Some(deadline) = takeover_deadline_of(&self.slow_peers, status) {
                    self.slow_blocks.insert((index, block), deadline);
                }
            }

            // Just insert it to receiving, update will move it to correct place
            self.receiving.insert(index, blocks);
            self.update_piece_state_and_order(index);
            info!(piece = index, "piece returned to fast pool: no slow source");
        }
        for index in to_slow {
            // all this piece is moving to slow pool, fast peers should not
            // pick it anymore, remove it in slow_blocks tree
            self.remove_from_slow_blocks(index, None);
            let p = self
                .requesting
                .remove(&index)
                .or_else(|| self.receiving.remove(&index))
                .unwrap();
            self.slow_pieces.insert(index, p);
            self.update_piece_state_and_order(index);
            info!(piece = index, "piece moved to slow pool: only slow sources");
        }
    }

    /// The order of pieces to picking blocks from.
    /// In normal mode, slow peers only use slow piece pool, fast peers
    /// only use fast piece pool.
    /// In rush mode, fast peers can takeover slow blocks in slow pool
    fn normal_piece_order(&self, rush: bool, slow: bool) -> Vec<PieceIndex> {
        if slow {
            return self
                .slow_order
                .iter(false)
                .map(|(index, _)| *index)
                .collect();
        }

        let mut order = if rush {
            self.merge_receiving_and_slow_by_order()
        } else {
            Vec::new()
        };
        order.extend(self.requesting_order.iter(false).map(|(index, _)| *index));
        order
    }

    /// Endgame has no fast requesting pieces and uses both pools for every peer.
    fn endgame_piece_order(&self) -> Vec<PieceIndex> {
        self.merge_receiving_and_slow_by_order()
    }

    /// Merge the existing (unreceived count, piece index) orders in linear time.
    fn merge_receiving_and_slow_by_order(&self) -> Vec<PieceIndex> {
        let mut receiving = self
            .receiving_order
            .iter(false)
            .filter(|(_, count)| **count > 0)
            .map(|(index, count)| (*count, *index))
            .peekable();
        let mut slow = self
            .slow_order
            .iter(false)
            .map(|(index, count)| (*count, *index))
            .peekable();
        let mut order = Vec::with_capacity(self.receiving.len() + self.slow_pieces.len());
        while let (Some(receiving_key), Some(slow_key)) = (receiving.peek(), slow.peek()) {
            let next = if receiving_key <= slow_key {
                receiving.next()
            } else {
                slow.next()
            };
            order.push(next.unwrap().1);
        }

        // one iterator ends, append remaining
        order.extend(receiving.map(|(_, index)| index));
        order.extend(slow.map(|(_, index)| index));
        order
    }

    /// Call when some block request is rejected, and request for that block
    /// should be send to other peers again.
    pub fn peer_reject_block(&mut self, peer: &PeerAddr, req: Request) {
        if !self.check_block_validity(&req) {
            return;
        }
        let block = req.begin as usize / BLOCK_SIZE;
        let mut empty = false;
        if let Some((b, accounting, block_occupy)) = self.piece_state_mut(req.index) {
            b.reject(peer, req, accounting, block_occupy);
            empty = b.is_all_not_requested();
        }

        if empty {
            self.remove_piece(req.index);
            self.piece_picker.set_have(req.index, false);
        }

        self.update_slow_blocks(req.index, Some(block));
        self.update_piece_state_and_order(req.index);
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
        // `begin` is peer-controlled: an out-of-piece offset would index past
        // `block_map` in `want_block` / `receive` / `revoke`.
        if req.begin as usize >= self.piece_size(index) {
            info!("invalid req begin {} out of piece {index}", req.begin);
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

        let index = ji.index();
        self.remove_from_slow_blocks(index, Some(req.begin as usize / BLOCK_SIZE));
        let result = if let Some((b, accounting, block_occupy)) = self.piece_state_mut(index) {
            let cancel = b.receive(req, accounting, block_occupy);
            (
                PieceComplete {
                    piece: b.is_all_received(),
                    sub_piece: b.is_sub_all_received(req.begin),
                },
                cancel,
            )
        } else if self.piece_picker.selected(index) && !self.piece_picker.have(index) {
            // Late arrivals can reopen a piece after all its requests were revoked.
            let mut b = self.piece_block_of(index);
            let cancel = b.receive(req, &mut self.accounting, &mut self.block_occupy);
            let complete = PieceComplete {
                piece: b.is_all_received(),
                sub_piece: b.is_sub_all_received(req.begin),
            };
            self.requesting.insert(index, b);
            self.piece_picker.set_have(index, true);
            (complete, cancel)
        } else {
            // blocks we didn't select or already have
            (
                PieceComplete {
                    piece: false,
                    sub_piece: false,
                },
                vec![],
            )
        };
        self.update_piece_state_and_order(index);
        result
    }

    /// Call when some piece is verified (`success = true`) or invalidated
    /// (`success = false`). Reconciles per-file completion against the piece's
    /// actual verified state so the file tracker is never double-counted, and
    /// returns the indices of files that just became complete.
    pub fn piece_verified(&mut self, index: u32, success: bool) -> Vec<usize> {
        let had = self.have(index);
        self.remove_piece(index);
        self.piece_picker.set_have(index, success);
        self.update_piece_state_and_order(index);
        match (had, success) {
            // genuine transitions drive the tracker exactly once
            (false, true) => self.file_tracker.piece_verified(index, true),
            (true, false) => self.file_tracker.piece_verified(index, false),
            // no real change: don't touch the tracker
            _ => vec![],
        }
    }

    /// Indices of files not yet fully downloaded.
    pub fn incomplete_files(&self) -> Vec<usize> {
        self.file_tracker.incomplete_files()
    }

    /// Mark blocks as `NotRequested` if they are `Requested` and did not respond
    /// longer than timeout
    fn revoke_unrespond(&mut self, revoked: &mut HashMap<PeerAddr, Vec<Revoke>>) {
        let now = tokio::time::Instant::now();

        // Both active requests and revoked history expire in pick-time order.
        let mut expired = Vec::new();
        self.block_occupy.by_peer.retain(|peer, requests| {
            while requests
                .first()
                .is_some_and(|(_, picked_at)| now.duration_since(*picked_at) > NO_RESPONSE_TIMEOUT)
            {
                let (key, _) = requests.pop_first().unwrap();
                expired.push((*peer, key));
            }
            !requests.is_empty()
        });

        let mut pieces = HashSet::new();
        for (peer, (index, block)) in expired {
            let (piece, accounting, _) = self.piece_state_mut(index).unwrap();
            if let BlockStatus::Requested { requested, .. } = &piece.block_map[block]
                && requested.contains_key(&peer)
            {
                let req = piece.request_at(block);
                piece.revoke(&peer, req, accounting);
                revoked.entry(peer).or_default().push(Revoke::Timeout(req));
            }

            // Discard expired history, including the request just timed out.
            match &mut piece.block_map[block] {
                BlockStatus::Requested { revoked, .. } | BlockStatus::NotRequested { revoked } => {
                    revoked.remove(&peer);
                }
                BlockStatus::Received => {}
            }

            self.update_slow_blocks(index, Some(block));
            pieces.insert(index);
        }

        for index in pieces {
            let empty = self.piece(index).is_some_and(|piece| {
                piece.is_all_not_requested() && piece.block_map.iter().all(|status| {
                    matches!(status, BlockStatus::NotRequested { revoked } if revoked.is_empty())
                })
            });
            if empty {
                self.remove_piece(index);
                self.piece_picker.set_have(index, false);
            } else {
                self.update_piece_state_and_order(index);
            }
        }
    }

    fn update_endgame(&mut self) -> bool {
        let old = self.endgame;
        // Fast pieces with unrequested blocks live in requesting; receiving
        // already guarantees all blocks are requested or received.
        self.endgame = self.requesting.is_empty()
            && self.piece_picker.is_finished()
            && self
                .slow_pieces
                .values()
                .all(|p| p.requested_or_received_count == p.block_map.len());
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

        self.piece(index).is_none_or(|b| {
            !matches!(
                b.block_map[req.begin as usize / BLOCK_SIZE],
                BlockStatus::Received
            )
        })
    }

    /// returns the number of peers that have this piece.
    pub fn piece_availability(&self, index: u32) -> usize {
        self.piece_picker.piece_availability(index)
    }

    /// Are all selected pieces downloaded and verified?
    pub fn is_finished(&mut self) -> bool {
        self.receiving.is_empty()
            && self.requesting.is_empty()
            && self.slow_pieces.is_empty()
            && self.piece_picker.is_finished()
    }

    /// dump current status
    pub fn dump(&mut self) -> BlockPickerDump {
        let mut requesting = self.requesting.clone();
        let mut receiving = self.receiving.clone();
        for (&index, p) in &self.slow_pieces {
            if p.requested_or_received_count < p.block_map.len() {
                requesting.insert(index, p.clone());
            } else {
                receiving.insert(index, p.clone());
            }
        }

        let clear_requests =
            |(index, piece)| (index, PieceBlocks::from(PieceBlockDump::from(piece)));
        BlockPickerDump {
            receiving: receiving.into_iter().map(clear_requests).collect(),
            requesting: requesting.into_iter().map(clear_requests).collect(),
            piece_map: self.piece_picker.dump(),
            no_response_timeout: self.no_response_timeout,
        }
    }

    /// load progress
    pub fn load_progress(&mut self, dump: BlockPickerDump) {
        self.settle_all_piece(tokio::time::Instant::now());
        self.receiving = dump.receiving;
        self.requesting = dump.requesting;
        self.slow_pieces.clear();
        self.slow_peers.clear();
        self.slow_blocks = IndexTree::new();
        self.block_occupy = BlockOccupy::default();
        self.piece_picker.load(dump.piece_map);
        self.no_response_timeout = dump.no_response_timeout;

        // After deserialization, every `Requested` block was normalized to
        // `NotRequested` (see `PieceBlockDump`), so a piece kept in `receiving`
        // may no longer be fully received. Move any not-all-received piece back
        // to `requesting` so the pick loop can reach it.
        self.requesting
            .extend(self.receiving.extract_if(.., |_, p| !p.is_all_received()));

        // Pieces with zero received blocks have no progress at all: drop them
        // from `requesting` and clear their have-bit so they are picked again
        // from scratch as peers reconnect.
        for (idx, _) in self
            .requesting
            .extract_if(.., |_, p| p.is_all_not_requested())
        {
            self.piece_picker.set_have(idx, false);
        }

        let now = tokio::time::Instant::now();
        for p in self
            .requesting
            .values_mut()
            .chain(self.receiving.values_mut())
        {
            p.restart_slot_clocks(now);
        }

        // Rebuild the file tracker from the restored have-state; it is derived
        // state and is not part of the dump.
        self.file_tracker.reset();
        for i in 0..self.n as u32 {
            if self.have(i) {
                self.file_tracker.piece_verified(i, true);
            }
        }
        self.rebuild_piece_order();
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
        // `piece_picker.have` also marks pieces that are currently being
        // downloaded so they are not picked by another peer.  Only expose
        // fully verified pieces to peers in our bitfield.
        let mut state = BitField::with_bit_len(self.n);
        for i in 0..(self.n as u32) {
            state.set(i, self.have(i as u32));
        }
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

    fn revoke_peer_requests(&mut self, peer: &PeerAddr) {
        let Some(requests) = self.block_occupy.by_peer.get(peer) else {
            return;
        };
        let blocks: Vec<_> = requests.iter(false).map(|(key, _)| *key).collect();

        let mut affected_pieces = HashSet::new();
        for (index, block) in blocks {
            let (piece, accounting, _) = self.piece_state_mut(index).unwrap();
            if let BlockStatus::Requested { requested, .. } = &piece.block_map[block]
                && requested.contains_key(peer)
            {
                let req = piece.request_at(block);
                piece.revoke(peer, req, accounting);
                self.update_slow_blocks(index, Some(block));

                affected_pieces.insert(index);
            }
        }

        for index in affected_pieces {
            let empty = self
                .piece(index)
                .is_some_and(PieceBlocks::is_all_not_requested);
            if empty {
                self.remove_piece(index);
                self.piece_picker.set_have(index, false);
            } else {
                self.update_piece_state_and_order(index);
            }
        }
    }

    pub fn peer_leave(&mut self, addr: &PeerAddr) {
        self.revoke_peer_requests(addr);
        self.piece_picker.peer_leave(addr);
        self.slow_peers.remove(addr);
        self.accounting.peers.remove(addr);
    }

    /// True only once every block has arrived, independent of pool ownership.
    pub fn is_piece_wait_check(&self, index: u32) -> bool {
        self.piece(index).is_some_and(|b| b.is_all_received())
    }

    /// Unverified pieces whose first sub-piece is received, for restoring hashers.
    pub fn unverified_pieces_whose_head_received(&self) -> impl Iterator<Item = u32> + '_ {
        self.pieces().filter_map(|(&index, blocks)| {
            (self.selected(index) && blocks.is_sub_all_received(0)).then_some(index)
        })
    }

    pub fn peer_choke(&mut self, peer: &PeerAddr) {
        self.revoke_peer_requests(peer);
        self.piece_picker.peer_choke(peer);
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
            // Tracked pieces use have as an occupancy bit, not verified progress.
            if self.piece(index).is_some() {
                self.piece_picker.set_have(index, false);
            }
            self.remove_piece(index);
        }
        self.update_piece_state_and_order(index);
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
            && !self.slow_pieces.contains_key(&index)
    }

    /// returns if we have this sub-piece
    pub fn have_sub(&self, ji: JointIndex) -> bool {
        let index = ji.index();
        if self.have(index) {
            return true;
        }
        self.piece(index)
            .is_some_and(|b| b.is_sub_all_received(ji.in_piece_offset() as u32))
    }

    /// Blocks of this sub-piece already received
    pub fn sub_received_count(&self, ji: JointIndex) -> usize {
        let index = ji.index();
        let sub = ji.sub_index() as usize;
        if self.have(index) {
            let (n_blocks, _) = self.n_blocks_and_last_block_size(index);
            let blocks_per_sub = SUB_PIECE_SIZE as usize / BLOCK_SIZE;
            let from = sub * blocks_per_sub;
            let to = ((sub + 1) * blocks_per_sub).min(n_blocks);
            return to - from;
        }
        self.piece(index).map_or(0, |b| b.subs[sub].received)
    }

    /// set we have this piece
    pub fn set_have(&mut self, index: u32, have: bool) {
        self.piece_picker.set_have(index, have);
        if have {
            self.remove_piece(index);
        }
        self.update_piece_state_and_order(index);
    }
}

#[cfg(test)]
mod test {
    use crate::picker::BitField;

    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    const PEER1: PeerAddr = PeerAddr(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1),
        1,
    );
    const PEER2: PeerAddr = PeerAddr(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2),
        2,
    );
    const PEER3: PeerAddr = PeerAddr(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3),
        3,
    );

    fn index_fixture_requests(block_occupy: &mut BlockOccupy, piece: &PieceBlocks) {
        for (block, status) in piece.block_map.iter().enumerate() {
            if let BlockStatus::Requested { requested, .. } = status {
                for (peer, detail) in requested {
                    block_occupy.insert(*peer, (piece.piece_index, block), detail.pick_time);
                }
            }
        }
    }

    fn picked_detail_at(
        pick_time: tokio::time::Instant,
        n_in_flight_when_picked: usize,
    ) -> PickedDetail {
        PickedDetail::new(
            pick_time,
            n_in_flight_when_picked,
            BLOCK_SIZE as f32,
            Some(time::Duration::ZERO),
            false,
        )
    }

    #[test]
    fn test_block_pieces() {
        let mut b = PieceBlocks::from_block_map(
            0,
            4133,
            vec![
                BlockStatus::NotRequested {
                    revoked: HashMap::new()
                };
                50
            ],
        );

        {
            let mut picked = Vec::new();
            let count = b.pick(
                PEER1,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                Some(time::Duration::ZERO),
                false,
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                },
                &mut picked,
                &mut Accounting::default(),
                &mut BlockOccupy::default(),
                |_, _| {},
                #[cfg(feature = "metrics")]
                "test",
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
            let exp = vec![BlockRange {
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
            }];
            assert_eq!(picked, exp);
            assert_eq!(count, 30);
            assert_eq!(b.all_request_or_received_before, 30);
            assert_eq!(b.requested_or_received_count, 30);
        }
        {
            let mut picked = Vec::new();
            let count = b.pick(
                PEER1,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                Some(time::Duration::ZERO),
                false,
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                },
                &mut picked,
                &mut Accounting::default(),
                &mut BlockOccupy::default(),
                |_, _| {},
                #[cfg(feature = "metrics")]
                "test",
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
            let exp = vec![BlockRange {
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
            }];
            assert_eq!(picked, exp);
            assert_eq!(count, 20);
            assert_eq!(b.all_request_or_received_before, 50);
            assert_eq!(b.requested_or_received_count, 50);
            assert!(b.is_all_requested_or_received());
            assert!(!b.is_all_received());
        }
        {
            // test receive
            for i in 0..10 {
                b.receive(
                    Request {
                        index: 0,
                        begin: i * 16384,
                        len: 16384,
                    },
                    &mut Accounting::default(),
                    &mut BlockOccupy::default(),
                );
            }
            b.receive(
                Request {
                    index: 0,
                    begin: 15 * 16384,
                    len: 16384,
                },
                &mut Accounting::default(),
                &mut BlockOccupy::default(),
            );
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
                &mut Accounting::default(),
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
                &mut Accounting::default(),
            );
            assert!(!b.is_all_requested_or_received());
            assert!(!b.is_all_received());
            assert_eq!(b.all_request_or_received_before, 17);
            assert_eq!(b.received_count, 11);
            assert_eq!(b.requested_or_received_count, 49);
        }
        {
            let mut revoked = HashMap::new();
            b.revoke_all_requested_if(|_, _| true, &mut revoked, &mut Accounting::default());
            assert!(!b.is_all_requested_or_received());
            assert_eq!(b.all_request_or_received_before, 10);
            assert_eq!(b.received_count, 11);
            assert_eq!(b.requested_or_received_count, 11);
        }
    }

    #[test]
    fn test_block_pieces_non_consecutive() {
        let mut b = PieceBlocks::from_block_map(
            0,
            4133,
            vec![
                BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                },
                BlockStatus::Requested {
                    requested: HashMap::from([(
                        PEER1,
                        picked_detail_at(tokio::time::Instant::now(), 0),
                    )]),
                    revoked: HashMap::new(),
                },
                BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                },
            ],
        );
        {
            // A discontinuity (block 1 already requested) no longer ends the
            // pick early: a single call now walks the whole piece and returns
            // every contiguous run, so blocks 0 and 2 are both picked here.
            let mut picked = Vec::new();
            let count = b.pick(
                PEER1,
                30,
                &mut 0,
                BLOCK_SIZE as f32,
                Some(time::Duration::ZERO),
                false,
                RepickOption {
                    repick_limit: 1,
                    endgame: false,
                },
                &mut picked,
                &mut Accounting::default(),
                &mut BlockOccupy::default(),
                |_, _| {},
                #[cfg(feature = "metrics")]
                "test",
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
            let exp = vec![
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
                BlockRange {
                    from: Request {
                        index: 0,
                        begin: 2 * 16384,
                        len: 4133,
                    },
                    to: Request {
                        index: 0,
                        begin: 2 * 16384,
                        len: 4133,
                    },
                },
            ];
            assert_eq!(picked, exp);
            assert_eq!(count, 2);
            assert_eq!(b.all_request_or_received_before, 3);
            assert_eq!(b.requested_or_received_count, 3);
        }
    }

    /// Peer-controlled `begin` outside the piece must be rejected instead of
    /// indexing past `block_map`.
    #[test]
    fn test_out_of_range_begin_is_ignored() {
        use crate::picker::RarestPicker;
        const PIECE_SIZE: usize = 16384 * 10;
        let p = Box::new(RarestPicker::new(PIECE_SIZE as u64, PIECE_SIZE));
        let mut b = BlockPicker::new_single_file(
            PIECE_SIZE as u64,
            PIECE_SIZE,
            p,
            time::Duration::from_secs(10),
        );
        b.peer_add(PEER1, PieceState::HaveAll);
        b.select(0, true);
        b.piece_picker.set_have(0, true);
        let pb = b.piece_block_of(0);
        b.requesting.insert(0, pb);

        let req = Request {
            index: 0,
            begin: PIECE_SIZE as u32,
            len: BLOCK_SIZE as u32,
        };
        assert!(b.get_block_status(&req).is_none());
        assert!(!b.want_block(req));
        let (complete, peers) = b.receive_block(req);
        assert!(!complete.piece && !complete.sub_piece);
        assert!(peers.is_empty());
        // A REJECT for the same bogus block must not panic either.
        b.peer_reject_block(&PEER1, req);
    }

    #[test]
    fn test_block_picker_pick_select() {
        use crate::picker::RarestPicker;
        const PIECE_SIZE: usize = 16384 * 10;
        const TOTAL_SIZE: usize = 16384 * 10 * 10 + 1500;
        let p = Box::new(RarestPicker::new(TOTAL_SIZE as u64, PIECE_SIZE));
        let mut b = BlockPicker::new_single_file(
            TOTAL_SIZE as u64,
            PIECE_SIZE,
            p,
            time::Duration::from_secs(10),
        );
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
                Some(time::Duration::ZERO),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
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
                Some(time::Duration::ZERO),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
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
                Some(time::Duration::ZERO),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
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
        let mut b = PieceBlocks::from_block_map(
            0,
            BLOCK_SIZE,
            vec![BlockStatus::Requested {
                requested: HashMap::from([(
                    PEER1,
                    PickedDetail::new(
                        tokio::time::Instant::now(),
                        4,
                        1024.0,
                        Some(time::Duration::ZERO),
                        false,
                    ),
                )]),
                revoked: HashMap::new(),
            }],
        );

        let mut picked = Vec::new();
        let count = b.pick(
            PEER2,
            1,
            &mut 0,
            BLOCK_SIZE as f32 * 64.0,
            Some(time::Duration::ZERO),
            false,
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
            &mut picked,
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
            |_, _| {},
            #[cfg(feature = "metrics")]
            "test",
            #[cfg(feature = "blocktrace")]
            None,
            #[cfg(feature = "blocktrace")]
            None,
        );

        assert_eq!(count, 1);
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
        let mut b = PieceBlocks::from_block_map(
            0,
            BLOCK_SIZE,
            vec![BlockStatus::Requested {
                requested: HashMap::from([(
                    PEER1,
                    PickedDetail::new(
                        tokio::time::Instant::now(),
                        0,
                        4096.0,
                        Some(time::Duration::from_secs(10)),
                        false,
                    ),
                )]),
                revoked: HashMap::new(),
            }],
        );

        let mut picked = Vec::new();
        let count = b.pick(
            PEER2,
            1,
            &mut 0,
            BLOCK_SIZE as f32,
            Some(time::Duration::ZERO),
            false,
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
            &mut picked,
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
            |_, _| {},
            #[cfg(feature = "metrics")]
            "test",
            #[cfg(feature = "blocktrace")]
            None,
            #[cfg(feature = "blocktrace")]
            None,
        );

        assert_eq!(count, 1);
        match &b.block_map[0] {
            BlockStatus::Requested { requested, .. } => {
                assert!(requested.contains_key(&PEER1));
                assert!(requested.contains_key(&PEER2));
            }
            _ => panic!("block should stay requested"),
        }
    }

    #[test]
    fn test_pick_budget_zero_is_noop() {
        let before = PieceBlocks::from_block_map(
            0,
            BLOCK_SIZE,
            vec![
                BlockStatus::Requested {
                    requested: HashMap::from([(
                        PEER1,
                        picked_detail_at(
                            tokio::time::Instant::now() - time::Duration::from_secs(6),
                            0
                        ),
                    )]),
                    revoked: HashMap::new(),
                };
                2
            ],
        );
        for option in [
            RepickOption {
                repick_limit: 1,
                endgame: false,
            },
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
            RepickOption {
                repick_limit: 1,
                endgame: true,
            },
        ] {
            let mut blocks = before.clone();
            let mut inflight = 3;
            let mut picked = Vec::new();
            let count = blocks.pick(
                PEER2,
                0,
                &mut inflight,
                BLOCK_SIZE as f32,
                Some(time::Duration::ZERO),
                false,
                option,
                &mut picked,
                &mut Accounting::default(),
                &mut BlockOccupy::default(),
                |_, _| {},
                #[cfg(feature = "metrics")]
                "test",
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
            assert_eq!(count, 0, "zero budget must not repick in {option:?}");
            assert!(
                picked.is_empty(),
                "zero budget must not produce ranges in {option:?}"
            );
            assert_eq!(inflight, 3);
            assert_eq!(blocks, before, "zero budget must not change picker state");
        }
    }

    #[test]
    fn test_pick_budget_bounds_requests_in_rush_and_endgame() {
        // rush_mode kicks in when vacant < POOL_SIZE/4; 0 is strict rush, POOL_SIZE/8
        // is non-strict rush, POOL_SIZE is non-rush (endgame handles that case).
        for (endgame, requesting, vacant) in [
            (false, false, 0),
            (false, false, POOL_SIZE / 8),
            (false, true, POOL_SIZE / 8),
            (true, false, POOL_SIZE),
        ] {
            let (mut picker, peers) = make_block_picker_with_state(3, 4 * BLOCK_SIZE, 2, 0, 0);
            // Endgame must allow repicking even fresh requests; rush needs expired ones.
            let picked_at = if endgame {
                tokio::time::Instant::now()
            } else {
                tokio::time::Instant::now() - time::Duration::from_secs(6)
            };
            let mut map = vec![
                BlockStatus::Requested {
                    requested: HashMap::from([(peers[0], picked_detail_at(picked_at, 0))]),
                    revoked: HashMap::new(),
                };
                4
            ];
            if requesting {
                map[3] = BlockStatus::NotRequested {
                    revoked: HashMap::new(),
                };
            }
            let blocks = PieceBlocks::from_block_map(0, BLOCK_SIZE, map);
            picker.piece_picker.set_have(0, true);
            if requesting {
                picker.requesting.insert(0, blocks);
            } else {
                picker.receiving.insert(0, blocks);
            }
            picker.update_piece_state_and_order(0);
            if endgame {
                picker.piece_picker.set_have(1, true);
                picker.piece_picker.set_have(2, true);
            }

            let (requests, picked) = picker.pick_blocks(
                &peers[1],
                1,
                0,
                BLOCK_SIZE as f32,
                Some(time::Duration::ZERO),
                &mut HashMap::new(),
                vacant,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
            assert_eq!(
                picked, 1,
                "endgame={endgame}, requesting={requesting}, vacant={vacant}"
            );
            assert_eq!(
                requests
                    .range
                    .iter()
                    .flat_map(|r| r.iter(requests.piece_size))
                    .count(),
                1
            );
            let registered = picker.requesting.values().chain(picker.receiving.values())
                .flat_map(|p| &p.block_map)
                .filter(|b| matches!(b, BlockStatus::Requested { requested, .. } if requested.contains_key(&peers[1])))
                .count();
            assert_eq!(
                registered, 1,
                "must not register requests beyond the budget"
            );
            assert!(!picker.requesting.contains_key(&1));
            assert!(!picker.requesting.contains_key(&2));
        }
    }

    #[test]
    fn test_stale_request_triggers_repick() {
        let mut b = PieceBlocks::from_block_map(
            0,
            BLOCK_SIZE,
            vec![BlockStatus::Requested {
                requested: HashMap::from([(
                    PEER1,
                    picked_detail_at(
                        tokio::time::Instant::now() - time::Duration::from_secs(6),
                        0,
                    ),
                )]),
                revoked: HashMap::new(),
            }],
        );

        let mut picked = Vec::new();
        let count = b.pick(
            PEER2,
            1,
            &mut 0,
            BLOCK_SIZE as f32,
            Some(time::Duration::ZERO),
            false,
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
            &mut picked,
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
            |_, _| {},
            #[cfg(feature = "metrics")]
            "test",
            #[cfg(feature = "blocktrace")]
            None,
            #[cfg(feature = "blocktrace")]
            None,
        );

        assert_eq!(count, 1);
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
        let mut b = PieceBlocks::from_block_map(
            0,
            BLOCK_SIZE,
            vec![BlockStatus::Requested {
                requested: HashMap::from([(
                    PEER1,
                    PickedDetail::new(
                        tokio::time::Instant::now(),
                        0,
                        BLOCK_SIZE as f32,
                        Some(time::Duration::ZERO),
                        false,
                    ),
                )]),
                revoked: HashMap::new(),
            }],
        );

        let mut picked = Vec::new();
        let count = b.pick(
            PEER2,
            1,
            &mut 0,
            BLOCK_SIZE as f32,
            Some(time::Duration::ZERO),
            false,
            RepickOption {
                repick_limit: 7,
                endgame: false,
            },
            &mut picked,
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
            |_, _| {},
            #[cfg(feature = "metrics")]
            "test",
            #[cfg(feature = "blocktrace")]
            None,
            #[cfg(feature = "blocktrace")]
            None,
        );

        assert_eq!(count, 0);
        assert!(picked.is_empty());
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

    fn assert_cost(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-8, "{actual} != {expected}");
    }

    #[tokio::test(start_paused = true)]
    async fn slot_accounting_starts_at_first_request_without_cache_events() {
        let mut a = Accounting::default();
        let mut pb = blocks(vec![not_requested_block(), not_requested_block()]);
        pb.pick(
            PEER1,
            2,
            &mut 0,
            BLOCK_SIZE as f32,
            Some(time::Duration::ZERO),
            false,
            RepickOption {
                repick_limit: 1,
                endgame: false,
            },
            &mut Vec::new(),
            &mut a,
            &mut BlockOccupy::default(),
            |_, _| {},
            #[cfg(feature = "metrics")]
            "test",
            #[cfg(feature = "blocktrace")]
            None,
            #[cfg(feature = "blocktrace")]
            None,
        );
        tokio::time::advance(time::Duration::from_secs(7)).await;
        pb.settle_account(&mut a, tokio::time::Instant::now());
        assert_cost(a.peers[&PEER1].current_cost().0, 7.0);
        pb.receive(block_req(0), &mut a, &mut BlockOccupy::default());
        tokio::time::advance(time::Duration::from_secs(2)).await;
        pb.receive(block_req(1), &mut a, &mut BlockOccupy::default());
        tokio::time::advance(time::Duration::from_secs(9)).await;
        pb.settle_account(&mut a, tokio::time::Instant::now());
        assert_cost(a.peers[&PEER1].current_cost().0, 9.0);
        assert!(pb.subs[0].last_accounted_at.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn slot_accounting_four_remaining_blocks_match_manual_shares() {
        let p4 = PeerAddr(PEER3.0, 4);
        let now = tokio::time::Instant::now();
        // Block 0 starts the interval; the remaining four follow the worked example.
        let mut pb = blocks(vec![
            not_requested_block(),
            requested_block(&[(PEER1, now)], &[]),
            requested_block(&[(PEER2, now)], &[]),
            not_requested_block(),
            not_requested_block(),
        ]);
        let mut a = Accounting::default();
        pb.receive(block_req(0), &mut a, &mut BlockOccupy::default());
        tokio::time::advance(time::Duration::from_secs(3)).await;
        pb.receive(block_req(2), &mut a, &mut BlockOccupy::default());
        let mut picked = Vec::new();
        for peer in [PEER3, p4] {
            tokio::time::advance(time::Duration::from_secs(3)).await;
            assert_eq!(
                pb.pick(
                    peer,
                    1,
                    &mut 0,
                    BLOCK_SIZE as f32,
                    Some(time::Duration::ZERO),
                    false,
                    RepickOption {
                        repick_limit: 1,
                        endgame: false
                    },
                    &mut picked,
                    &mut a,
                    &mut BlockOccupy::default(),
                    |_, _| {},
                    #[cfg(feature = "metrics")]
                    "test",
                    #[cfg(feature = "blocktrace")]
                    None,
                    #[cfg(feature = "blocktrace")]
                    None,
                ),
                1
            );
        }
        for block in [1, 3, 4] {
            tokio::time::advance(time::Duration::from_secs(3)).await;
            pb.receive(block_req(block), &mut a, &mut BlockOccupy::default());
        }
        pb.settle_account(&mut a, tokio::time::Instant::now());
        for (p, st, fair) in [
            (PEER1, 3.75, 2.4),
            (PEER2, 0.75, 0.6),
            (PEER3, 3.5, 1.8),
            (p4, 5.5, 1.8),
        ] {
            assert_cost(a.peers[&p].current_cost().0, st);
            assert_cost(a.peers[&p].current_cost().1, fair);
        }
        assert_cost(a.scheduler_blocker_st, 4.5);
        assert_cost(a.scheduler_fair_st, 11.4);
    }

    #[tokio::test(start_paused = true)]
    async fn slot_accounting_repick_revoke_and_duplicate_do_not_reassign_history() {
        let mut pb = plain_blocks(2);
        let mut a = Accounting::default();
        pb.receive(block_req(0), &mut a, &mut BlockOccupy::default());
        let mut picked = Vec::new();
        for (seconds, peer) in [(2, PEER1), (4, PEER2)] {
            tokio::time::advance(time::Duration::from_secs(seconds)).await;
            assert_eq!(
                pb.pick(
                    peer,
                    1,
                    &mut 0,
                    BLOCK_SIZE as f32,
                    Some(time::Duration::ZERO),
                    false,
                    RepickOption {
                        repick_limit: 1,
                        endgame: true
                    },
                    &mut picked,
                    &mut a,
                    &mut BlockOccupy::default(),
                    |_, _| {},
                    #[cfg(feature = "metrics")]
                    "test",
                    #[cfg(feature = "blocktrace")]
                    None,
                    #[cfg(feature = "blocktrace")]
                    None,
                ),
                1
            );
        }
        tokio::time::advance(time::Duration::from_secs(2)).await;
        pb.revoke(&PEER1, block_req(1), &mut a);
        tokio::time::advance(time::Duration::from_secs(3)).await;
        pb.receive(block_req(1), &mut a, &mut BlockOccupy::default());
        tokio::time::advance(time::Duration::from_secs(5)).await;
        pb.receive(block_req(1), &mut a, &mut BlockOccupy::default());
        pb.settle_account(&mut a, tokio::time::Instant::now());
        assert_cost(a.peers[&PEER1].current_cost().0, 5.0);
        assert_cost(a.peers[&PEER2].current_cost().0, 4.0);
    }

    #[tokio::test(start_paused = true)]
    async fn slot_accounting_local_changes_settle_only_the_affected_subpiece() {
        for action in 0..4 {
            let now = tokio::time::Instant::now();
            let mut pb = blocks(vec![requested_block(&[(PEER1, now)], &[]); 64]);
            let mut a = Accounting::default();
            pb.receive(block_req(0), &mut a, &mut BlockOccupy::default());
            pb.receive(block_req(32), &mut a, &mut BlockOccupy::default());
            tokio::time::advance(time::Duration::from_secs(2)).await;
            match action {
                0 => {
                    pb.receive(block_req(1), &mut a, &mut BlockOccupy::default());
                }
                1 => pb.revoke(&PEER1, block_req(1), &mut a),
                _ => {
                    let picked = pb.pick(
                        if action == 2 { PEER1 } else { PEER2 },
                        2,
                        &mut 0,
                        BLOCK_SIZE as f32,
                        Some(time::Duration::ZERO),
                        false,
                        RepickOption {
                            repick_limit: 2,
                            endgame: true,
                        },
                        &mut Vec::new(),
                        &mut a,
                        &mut BlockOccupy::default(),
                        |_, _| {},
                        #[cfg(feature = "metrics")]
                        "test",
                        #[cfg(feature = "blocktrace")]
                        None,
                        #[cfg(feature = "blocktrace")]
                        None,
                    );
                    assert_eq!(picked, if action == 2 { 0 } else { 2 });
                }
            }
            // The other sub-piece remains deferred; a no-op pick settles nothing.
            assert_cost(
                a.peers.values().map(|cost| cost.current_cost().0).sum(),
                if action == 2 { 0.0 } else { 2.0 },
            );
            tokio::time::advance(time::Duration::from_secs(2)).await;
            pb.settle_account(&mut a, tokio::time::Instant::now());
            let transferred = if action == 1 || action == 3 {
                2.0 / 31.0
            } else {
                0.0
            };
            assert_cost(a.peers[&PEER1].current_cost().0, 8.0 - transferred);
            if action == 3 {
                assert_cost(a.peers[&PEER2].current_cost().0, transferred);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slot_accounting_subpieces_start_and_end_independently() {
        let t = tokio::time::Instant::now();
        let mut pb = blocks(vec![requested_block(&[(PEER1, t)], &[]); 65]);
        let mut a = Accounting::default();
        pb.receive(block_req(0), &mut a, &mut BlockOccupy::default());
        tokio::time::advance(time::Duration::from_secs(2)).await;
        pb.receive(block_req(32), &mut a, &mut BlockOccupy::default());
        // The last sub has one block: first arrival also completes it, costing zero.
        pb.receive(block_req(64), &mut a, &mut BlockOccupy::default());
        tokio::time::advance(time::Duration::from_secs(3)).await;
        for b in 1..32 {
            pb.receive(block_req(b), &mut a, &mut BlockOccupy::default());
        }
        tokio::time::advance(time::Duration::from_secs(4)).await;
        for b in 33..64 {
            pb.receive(block_req(b), &mut a, &mut BlockOccupy::default());
        }
        pb.settle_account(&mut a, tokio::time::Instant::now());
        assert_cost(a.peers[&PEER1].current_cost().0, 12.0);
    }

    #[tokio::test(start_paused = true)]
    async fn slot_accounting_piece_removal_settles_and_stops_clocks() {
        for action in 0..4 {
            let (mut bp, _) = make_block_picker_with_state(1, 32768, 0, 0, 0);
            let empty = bp.dump();
            bp.select(0, true);
            let t = tokio::time::Instant::now();
            let mut pb = blocks(vec![
                not_requested_block(),
                requested_block(&[(PEER1, t)], &[]),
            ]);
            pb.receive(block_req(0), &mut bp.accounting, &mut bp.block_occupy);
            index_fixture_requests(&mut bp.block_occupy, &pb);
            bp.requesting.insert(0, pb);
            tokio::time::advance(time::Duration::from_secs(2)).await;
            match action {
                0 => bp.select(0, false),
                1 => bp.set_have(0, true),
                2 => bp.load_progress(empty),
                _ => {
                    bp.piece_verified(0, false);
                }
            }
            tokio::time::advance(time::Duration::from_secs(3)).await;
            bp.settle_all_piece(tokio::time::Instant::now());
            assert_cost(bp.accounting.peers[&PEER1].current_cost().0, 2.0);
            assert_cost(bp.accounting.scheduler_blocker_st, 0.0);
            assert_cost(bp.accounting.scheduler_fair_st, 1.0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slot_accounting_timeout_changes_only_future_shares() {
        let (mut bp, _) = make_block_picker_with_state(1, 49152, 0, 0, 0);
        let t = tokio::time::Instant::now();
        let mut pb = blocks(vec![
            not_requested_block(),
            requested_block(&[(PEER1, t)], &[]),
            not_requested_block(),
        ]);
        pb.receive(block_req(0), &mut bp.accounting, &mut bp.block_occupy);
        tokio::time::advance(time::Duration::from_secs(50)).await;
        assert_eq!(
            pb.pick(
                PEER2,
                1,
                &mut 0,
                BLOCK_SIZE as f32,
                Some(time::Duration::ZERO),
                false,
                RepickOption {
                    repick_limit: 1,
                    endgame: false
                },
                &mut Vec::new(),
                &mut bp.accounting,
                &mut bp.block_occupy,
                |_, _| {},
                #[cfg(feature = "metrics")]
                "test",
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            ),
            1
        );
        index_fixture_requests(&mut bp.block_occupy, &pb);
        bp.receiving.insert(0, pb);
        tokio::time::advance(time::Duration::from_secs(41)).await;
        let mut revoked = HashMap::new();
        bp.revoke_unrespond(&mut revoked);
        assert_eq!(revoked[&PEER1], vec![Revoke::Timeout(block_req(1))]);
        assert!(!revoked.contains_key(&PEER2));
        tokio::time::advance(time::Duration::from_secs(2)).await;
        bp.settle_all_piece(tokio::time::Instant::now());
        assert_cost(bp.accounting.peers[&PEER1].current_cost().0, 45.5);
        assert_cost(bp.accounting.peers[&PEER2].current_cost().0, 21.5);
    }

    #[tokio::test(start_paused = true)]
    async fn slot_accounting_disconnect_cleans_peer_and_reconnect_is_separate() {
        let (mut bp, _) = make_block_picker_with_state(1, 32768, 0, 0, 0);
        bp.peer_add(PEER1, PieceState::HaveAll);
        let t = tokio::time::Instant::now();
        let mut pb = blocks(vec![
            not_requested_block(),
            requested_block(&[(PEER1, t)], &[]),
        ]);
        pb.receive(block_req(0), &mut bp.accounting, &mut bp.block_occupy);
        index_fixture_requests(&mut bp.block_occupy, &pb);
        bp.requesting.insert(0, pb);
        tokio::time::advance(time::Duration::from_secs(2)).await;
        bp.peer_leave(&PEER1);
        assert!(!bp.accounting.peers.contains_key(&PEER1));
        assert_eq!(bp.peer_slot_cost(&PEER1), PeerCost::default());
        tokio::time::advance(time::Duration::from_secs(3)).await;
        let new = PeerAddr(PEER1.0, 999);
        let pb = bp.requesting.get_mut(&0).unwrap();
        let mut picked = Vec::new();
        assert_eq!(
            pb.pick(
                new,
                1,
                &mut 0,
                BLOCK_SIZE as f32,
                Some(time::Duration::ZERO),
                false,
                RepickOption {
                    repick_limit: 1,
                    endgame: false
                },
                &mut picked,
                &mut bp.accounting,
                &mut bp.block_occupy,
                |_, _| {},
                #[cfg(feature = "metrics")]
                "test",
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            ),
            1
        );
        tokio::time::advance(time::Duration::from_secs(4)).await;
        bp.settle_all_piece(tokio::time::Instant::now());
        assert!(!bp.accounting.peers.contains_key(&PEER1));
        assert_eq!(bp.accounting.peers.len(), 1);
        assert_cost(bp.accounting.peers[&new].current_cost().0, 4.0);
        assert_cost(bp.accounting.scheduler_blocker_st, 3.0);
        assert_cost(bp.accounting.scheduler_fair_st, 6.0);
    }

    // ---- lift: sub-piece last wait ----

    /// Piece declared from an explicit per-block map; the derived cursors
    /// (counts, subs, repick deadlines) are rebuilt from it.
    fn blocks(block_map: Vec<BlockStatus>) -> PieceBlocks {
        PieceBlocks::from_block_map(0, BLOCK_SIZE, block_map)
    }

    fn not_requested_block() -> BlockStatus {
        BlockStatus::NotRequested {
            revoked: HashMap::new(),
        }
    }

    /// Block requested by `reqs` `(peer, pick_time)`, already given up by
    /// `revoked` `(peer, pick_time)`.
    fn requested_block(
        reqs: &[(PeerAddr, tokio::time::Instant)],
        revoked: &[(PeerAddr, tokio::time::Instant)],
    ) -> BlockStatus {
        BlockStatus::Requested {
            requested: reqs
                .iter()
                .map(|(p, t)| (*p, picked_detail_at(*t, 0)))
                .collect(),
            revoked: revoked
                .iter()
                .map(|(p, t)| (*p, picked_detail_at(*t, 0)))
                .collect(),
        }
    }

    /// All-`NotRequested` piece of `n` blocks.
    fn plain_blocks(n: usize) -> PieceBlocks {
        blocks(vec![not_requested_block(); n])
    }

    /// A pick far enough in the past that it never bounds a test's wait.
    fn picked_long_ago() -> tokio::time::Instant {
        tokio::time::Instant::now() - time::Duration::from_secs(1)
    }

    fn block_req(i: usize) -> Request {
        Request {
            index: 0,
            begin: (i * BLOCK_SIZE) as u32,
            len: BLOCK_SIZE as u32,
        }
    }

    /// Mirrors the caller: only a completed sub-piece settles a last wait.
    fn take_completed_last_wait(pb: &mut PieceBlocks, i: usize) -> Option<LastWait> {
        if !pb.is_sub_all_received(block_req(i).begin) {
            return None;
        }
        pb.take_last_wait(block_req(i).begin)
    }

    /// The last wait is the gap between the last two arrivals, and the `blocker` is the
    /// earliest still-outstanding requester of the last-arriving block — not of
    /// the highest-indexed one.
    #[tokio::test(start_paused = true)]
    async fn test_last_wait_measures_last_two_arrivals() {
        let t0 = picked_long_ago();
        let mut pb = blocks(vec![
            requested_block(&[(PEER3, t0)], &[]),
            requested_block(&[(PEER2, t0)], &[]),
            requested_block(&[(PEER1, t0)], &[]),
        ]);
        // block 1 arrives last, so it is the gating block
        pb.receive(
            block_req(0),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );
        pb.receive(
            block_req(2),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );
        tokio::time::advance(time::Duration::from_millis(120)).await;
        pb.receive(
            block_req(1),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );

        let g = take_completed_last_wait(&mut pb, 1).expect("sub is complete");
        assert!(g.wait >= time::Duration::from_millis(120), "{:?}", g.wait);
        assert!(g.wait < time::Duration::from_secs(1), "{:?}", g.wait);
        assert_eq!(
            g.blocker,
            Some(PEER2),
            "blocker must be the last-arriving block's requester"
        );
    }

    /// A duplicate must not refresh the arrival pair nor inflate the sub count:
    /// only a block's first arrival bounds the sub-piece's wait.
    #[tokio::test(start_paused = true)]
    async fn test_duplicate_does_not_move_last_wait() {
        let t0 = picked_long_ago();
        let mut pb = blocks(vec![
            requested_block(&[(PEER1, t0)], &[]),
            requested_block(&[(PEER1, t0)], &[]),
        ]);

        pb.receive(
            block_req(0),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );
        let first_arrival = pb.subs[0].last.expect("first arrival recorded");
        tokio::time::advance(time::Duration::from_millis(60)).await;
        assert!(
            pb.receive(
                block_req(0),
                &mut Accounting::default(),
                &mut BlockOccupy::default()
            )
            .is_empty(),
            "duplicate is a no-op"
        );
        assert_eq!(
            pb.subs[0].last,
            Some(first_arrival),
            "duplicate moved `last`"
        );
        assert_eq!(pb.subs[0].received, 1, "duplicate inflated the sub count");

        tokio::time::advance(time::Duration::from_millis(60)).await;
        pb.receive(
            block_req(1),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );
        let g = take_completed_last_wait(&mut pb, 1).expect("sub is complete");
        assert!(g.wait >= time::Duration::from_millis(120), "{:?}", g.wait);
    }

    /// A peer that only picks up a block after its previous holder gave up is
    /// charged from its own pick, not from the previous arrival: the gap it did
    /// not cause is nobody's.
    #[tokio::test(start_paused = true)]
    async fn test_rescuer_is_not_charged_the_gap_before_its_pick() {
        // PEER1 holds the last block for the whole 500 ms gap and is revoked;
        // PEER2 picks it up 50 ms before delivering it.
        let mut pb = blocks(vec![
            requested_block(&[(PEER1, picked_long_ago())], &[]),
            not_requested_block(),
        ]);

        pb.receive(
            block_req(0),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );
        tokio::time::advance(time::Duration::from_millis(500)).await;
        // the take-over: PEER2 picks the block up, PEER1 is out
        pb.block_map[1] = requested_block(
            &[(
                PEER2,
                tokio::time::Instant::now() - time::Duration::from_millis(50),
            )],
            &[(PEER1, picked_long_ago())],
        );
        pb.receive(
            block_req(1),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );

        let g = take_completed_last_wait(&mut pb, 1).expect("sub is complete");
        assert_eq!(g.blocker, Some(PEER2));
        assert!(g.wait >= time::Duration::from_millis(50), "{:?}", g.wait);
        assert!(
            g.wait < time::Duration::from_millis(400),
            "the gap before the pick must not be charged: {:?}",
            g.wait
        );
    }

    /// `take_last_wait` settles a sub-piece once: the second call is `None`, and an
    /// incomplete sub-piece has no last wait.
    #[test]
    fn test_take_last_wait_is_one_shot() {
        let t0 = tokio::time::Instant::now();
        let mut pb = blocks(vec![
            requested_block(&[(PEER1, t0)], &[]),
            requested_block(&[(PEER1, t0)], &[]),
        ]);

        pb.receive(
            block_req(0),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );
        assert!(
            take_completed_last_wait(&mut pb, 0).is_none(),
            "incomplete sub has no last wait"
        );

        pb.receive(
            block_req(1),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );
        assert!(take_completed_last_wait(&mut pb, 1).is_some());
        assert!(
            take_completed_last_wait(&mut pb, 1).is_none(),
            "second take must be None"
        );
    }

    /// One last wait per sub-piece: a 128-block piece has four, settled by blocks
    /// 31/63/95/127; a 16-block piece has a single sub-piece.
    #[test]
    fn test_last_wait_per_sub_piece() {
        let t0 = tokio::time::Instant::now();
        let mut pb = blocks(vec![requested_block(&[(PEER1, t0)], &[]); 128]);
        assert_eq!(pb.subs.len(), 4);

        let mut settled = Vec::new();
        for i in 0..128 {
            pb.receive(
                block_req(i),
                &mut Accounting::default(),
                &mut BlockOccupy::default(),
            );
            if let Some(g) = take_completed_last_wait(&mut pb, i) {
                settled.push((i, g));
            }
        }
        assert_eq!(
            settled.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            vec![31, 63, 95, 127],
            "each last wait settles on its sub-piece's last block"
        );
        assert!(settled.iter().all(|(_, g)| g.blocker == Some(PEER1)));

        assert_eq!(plain_blocks(16).subs.len(), 1);
    }

    /// A 128-block piece rebuilds four sub-pieces (the old `div_ceil(sub_sz)`
    /// produced one and panicked on `subs[i / blocks_per_sub]`), and a restored
    /// dump carries counts but no arrival instants, so it yields no last wait.
    #[test]
    fn test_subs_survive_dump_restore() {
        let t0 = tokio::time::Instant::now();
        let mut pb = blocks(vec![requested_block(&[(PEER1, t0)], &[]); 128]);
        pb.receive(
            block_req(3),
            &mut Accounting::default(),
            &mut BlockOccupy::default(),
        );
        assert_eq!(pb.subs.len(), 4);
        assert_eq!(pb.subs[0].received, 1);

        let dump: PieceBlockDump = pb.clone().into();
        assert_eq!(dump.sub_receive_count, vec![1, 0, 0, 0]);

        let mut restored = PieceBlocks::from(dump);
        assert_eq!(restored.subs.len(), 4);
        assert_eq!(restored.subs[0].received, 1);
        assert!(
            restored.take_last_wait(block_req(3).begin).is_none(),
            "restored pieces have no arrival instants"
        );
    }

    /// `BlockPicker::take_last_wait` finds the piece in `requesting` or `receiving`,
    /// and is a no-op for a piece it does not track.
    #[test]
    fn test_block_picker_take_last_wait_lookup() {
        let (mut bp, peers) = make_block_picker_with_state(1, SUB_PIECE_SIZE as usize, 1, 1, 0);
        let peer = peers[0];
        let n_blocks = (SUB_PIECE_SIZE as usize) / BLOCK_SIZE;
        for i in 0..n_blocks {
            let req = block_req(i);
            let (complete, _) = bp.receive_block(req);
            if i + 1 < n_blocks {
                assert!(!complete.sub_piece);
            } else {
                assert!(complete.sub_piece);
                let g = bp
                    .take_last_wait(0, req.begin)
                    .expect("last wait after the last block");
                assert_eq!(g.blocker, Some(peer));
                assert!(bp.take_last_wait(0, req.begin).is_none(), "one-shot");
            }
        }
        assert!(
            bp.take_last_wait(1, 0).is_none(),
            "untracked piece has no last wait"
        );
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
        let p = Box::new(RarestPicker::new(total_size as u64, piece_size));
        let mut bp = BlockPicker::new_single_file(
            total_size as u64,
            piece_size,
            p,
            time::Duration::from_secs(10),
        );

        let mut peers = Vec::new();
        for i in 0..n_peers {
            let addr = PeerAddr(
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(10, 0, (i / 256) as u8, (i % 256) as u8)),
                    (6881 + i) as u16,
                ),
                i as u64,
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
                Some(time::Duration::from_millis(100)),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
        }

        // Move some to receiving state
        for i in 0..n_receiving.min(n_requesting) {
            let idx = i as u32;
            if let Some(blocks) = bp.requesting.remove(&idx) {
                bp.receiving.insert(idx, blocks);
            }
        }

        bp.rebuild_piece_order();
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

        let start = tokio::time::Instant::now();
        for i in 0..ITERATIONS {
            let peer = &peers[i % N_PEERS];
            let mut revoked = HashMap::new();
            let _ = bp.pick_blocks(
                peer,
                0, // ticker only: n=0
                100,
                BLOCK_SIZE as f32,
                Some(time::Duration::from_millis(100)),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
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
            Some(time::Duration::from_millis(100)),
            &mut revoked,
            POOL_SIZE,
            #[cfg(feature = "blocktrace")]
            None,
            #[cfg(feature = "blocktrace")]
            None,
        );
        eprintln!(
            "perf_pick_blocks_with_picks setup: {} requesting, {} receiving",
            bp.requesting.len(),
            bp.receiving.len()
        );

        // Measure n=0 path (ticker only, no actual picks)
        let start = tokio::time::Instant::now();
        for i in 0..ITERATIONS {
            let peer = &peers[i % N_PEERS];
            let mut revoked = HashMap::new();
            let _ = bp.pick_blocks(
                peer,
                0,
                N_PREPICK,
                BLOCK_SIZE as f32,
                Some(time::Duration::from_millis(100)),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
        }
        let elapsed_n0 = start.elapsed();
        let per_call_n0 = elapsed_n0 / ITERATIONS as u32;

        // Measure n=1 path (pick 1 block per call, also receive blocks to keep state realistic)
        let start = tokio::time::Instant::now();
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
                Some(time::Duration::from_millis(100)),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
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
                Some(time::Duration::from_millis(100)),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
        }

        // Now benchmark receiving blocks
        let n_blocks_per_piece = PIECE_SIZE / BLOCK_SIZE;
        let total_blocks = N_PIECES * n_blocks_per_piece;
        let start = tokio::time::Instant::now();
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
                Some(time::Duration::from_millis(100)),
                &mut revoked,
                POOL_SIZE,
                #[cfg(feature = "blocktrace")]
                None,
                #[cfg(feature = "blocktrace")]
                None,
            );
        }
        eprintln!(
            "revoke_unrespond setup: {} requesting, {} receiving",
            bp.requesting.len(),
            bp.receiving.len()
        );

        let start = tokio::time::Instant::now();
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
                    subs: vec![SubPieceState::new()],
                    repick_deadline: IndexTree::new(),
                },
            );
        }

        let start = tokio::time::Instant::now();
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

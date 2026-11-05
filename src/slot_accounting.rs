//! Picker-owned, aged peer costs for sub-piece request-to-completion time.
//! Both metrics share one timing boundary per SubPieceState. Accounting owns
//! the peer windows and cumulative scheduler shares.

use crate::conn::PeerAddr;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub(crate) struct Accounting {
    /// Cumulative missing-block shares without a requester, in sub-piece-seconds.
    pub scheduler_blocker_st: f64,
    /// Cumulative fixed-block shares without a requester, including received blocks.
    pub scheduler_fair_st: f64,
    pub peers: HashMap<PeerAddr, SlotTime>,
}

impl Accounting {
    pub fn accept_block(&mut self, peer: PeerAddr) {
        self.peers.entry(peer).or_default().accept_block();
    }

    pub fn add_cost(&mut self, peer: PeerAddr, share: f64, fair_share: f64) {
        self.peers
            .entry(peer)
            .or_default()
            .add_cost(share, fair_share);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct PeerCost {
    pub blocker: Option<f64>,
    pub fair: Option<f64>,
}

/// Aging counters for one slot-time metric.
#[derive(Debug, Clone, Copy, Default)]
struct Window {
    old_time: f64,
    old_blocks: f64,
    new_time: f64,
    new_blocks: f64,
}

impl Window {
    pub fn add_time(&mut self, seconds: f64) {
        self.new_time += seconds;
    }

    pub fn accept_block(&mut self) {
        self.new_blocks += 1.0;
    }

    /// The picker supplies one decay factor for every peer at the same boundary.
    pub fn rotate(&mut self, decay: f64) {
        self.old_time = self.old_time * decay + self.new_time;
        self.old_blocks = self.old_blocks * decay + self.new_blocks;
        self.new_time = 0.0;
        self.new_blocks = 0.0;
    }

    /// Reads include the open window and never trigger rotation.
    pub fn per_block(&self) -> Option<f64> {
        let blocks = self.old_blocks + self.new_blocks;
        let seconds = self.old_time + self.new_time;
        if blocks > 0.0 {
            Some(seconds / blocks)
        } else if seconds > 0.0 {
            Some(f64::INFINITY)
        } else {
            None
        }
    }
}

/// The two slot-time metrics for one connection; window details stay private.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SlotTime {
    st: Window,
    fair: Window,
}

impl SlotTime {
    pub fn add_cost(&mut self, direct: f64, fair: f64) {
        self.st.add_time(direct);
        self.fair.add_time(fair);
    }

    pub fn accept_block(&mut self) {
        self.st.accept_block();
        self.fair.accept_block();
    }

    pub fn rotate(&mut self, decay: f64) {
        self.st.rotate(decay);
        self.fair.rotate(decay);
    }

    pub fn per_block(&self) -> PeerCost {
        PeerCost {
            blocker: self.st.per_block(),
            fair: self.fair.per_block(),
        }
    }

    /// Inspect the actual window cost for attribution tests; retains no history.
    #[cfg(test)]
    pub fn current_cost(&self) -> (f64, f64) {
        (
            self.st.old_time + self.st.new_time,
            self.fair.old_time + self.fair.new_time,
        )
    }
}

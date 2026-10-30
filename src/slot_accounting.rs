//! Picker-owned sub-piece fill-time totals. Timing boundaries live in SubPieceState;
//! there are no cache callbacks, duplicated block maps, slot registries, or locks.

use crate::conn::PeerAddr;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PeerCost {
    /// Cumulative share of first-block-to-completion waiting time, in sub-piece-seconds.
    pub direct: f64,
}

/// Owned exclusively by BlockPicker. Mutators receive a mutable reference to
/// accumulate against the live block_map before changing request ownership.
#[derive(Debug, Default)]
pub(crate) struct Accounting {
    pub peers: HashMap<PeerAddr, PeerCost>,
    /// Preserve historical totals without retaining disconnected connection keys.
    retired_direct: f64,
    /// Shares of missing blocks without a current requester.
    pub scheduler: f64,
    /// Sum of all active sub-piece intervals, including scheduler shares.
    pub total: f64,
}

/// Scalar test snapshot. Production tracing only exports seconds per block.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Snapshot {
    /// Sum of historical peer costs, including disconnected connections.
    pub direct: f64,
    pub scheduler: f64,
    pub total: f64,
}

#[cfg(test)]
impl Snapshot {
    pub fn residual(&self) -> f64 {
        self.total - self.scheduler - self.direct
    }
}

impl Accounting {
    pub fn remove_peer(&mut self, peer: &PeerAddr) {
        if let Some(cost) = self.peers.remove(peer) {
            self.retired_direct += cost.direct;
        }
    }

    /// The picker must first accumulate unfinished intervals to the sample time.
    #[cfg(test)]
    pub fn snapshot(&self) -> Snapshot {
        let snapshot = Snapshot {
            direct: self.retired_direct + self.peers.values().map(|p| p.direct).sum::<f64>(),
            scheduler: self.scheduler,
            total: self.total,
        };
        debug_assert!(snapshot.residual().abs() < 1e-6 + snapshot.total * 1e-9);
        snapshot
    }
}

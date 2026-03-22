use std::collections::HashSet;

use tokio::time;

use crate::protocol::Request;

/// modes for bandwidth control
/// we want a balanced max-bandwidth and min rtt
#[derive(Debug)]
pub enum BandwidthMode {
    Startup {
        min_rtt: time::Duration,
        since_min_rtt: time::Instant,
        cwnd_since: time::Instant,
        cwnd: usize,
        max_bw: f32,
        limit_count: u32,
    },

    /// adaptively increase requests
    ProbeBW {
        since_cycle: time::Instant,
        since_min_rtt: time::Instant,
        min_rtt: time::Duration,

        // how many consecutive response which have rtt greater than (mean + sigma)
        slow_count: u32,
        slow_down_to: usize,

        // timestamp of last piece receive
        last_piece_time: time::Instant,
        // slow_down_to: usize,
        cycle_index: usize,
    },

    Choked,

    /// slow down to this in-flight
    SlowDown {
        // TODO: extract common fields out of enum
        since_min_rtt: time::Instant,

        // timestamp of last piece receive
        last_piece_time: time::Instant,
        min_rtt: time::Duration,

        // slow down to target inflight
        inflight_target: usize,
    },

    /// probe rtt
    ProbeRTT {
        // how many pieces received since probe mode start
        cnt: usize,

        since_min_rtt: time::Instant,
        min_rtt: time::Duration,

        since: time::Instant,

        // timestamp of last piece receive
        last_piece_time: time::Instant,

        inflight_target: usize,
        from_clear: bool,

        normal_count: u32,
        slow_count: u32,
    },
}

impl BandwidthMode {
    pub fn new_choked() -> Self {
        BandwidthMode::Choked
    }

    pub fn new_auto() -> Self {
        BandwidthMode::Startup {
            min_rtt: time::Duration::from_secs(10),
            cwnd_since: time::Instant::now(),
            since_min_rtt: time::Instant::now(),
            cwnd: 4,
            max_bw: 0.0,
            limit_count: 0,
        }
    }
}

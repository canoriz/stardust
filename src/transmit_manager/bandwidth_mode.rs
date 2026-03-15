use std::collections::HashSet;

use tokio::time;

use crate::protocol::Request;

/// modes for bandwidth control
/// we want a balanced max-bandwidth and min rtt
#[derive(Debug)]
pub enum BandwidthMode {
    /// adaptively increase requests
    Auto {
        since: time::Instant,
        min_rtt: time::Duration,

        // how many consecutive response which have rtt greater than (mean + sigma)
        slow_count: u32,
        slow_down_to: usize,

        // timestamp of last piece receive
        last_piece_time: time::Instant,
        // slow_down_to: usize,
    },

    Choked,

    /// slow down to this in-flight
    SlowDown {
        since_auto: time::Instant,

        // timestamp of last piece receive
        last_piece_time: time::Instant,
        min_rtt: time::Duration,

        // slow down to target inflight
        inflight_target: usize,

        // if rtt is smaller
        faster: bool,
    },

    /// probe rtt
    ProbeRTT {
        since_auto: time::Instant,

        // timestamp of last piece receive
        last_piece_time: time::Instant,

        min_rtt: time::Duration,

        to_receive: HashSet<Request>,
        n_to_probe: usize,
        inflight_target: usize,
        from_clear: bool,
    },
}

impl BandwidthMode {
    pub fn new_auto() -> Self {
        BandwidthMode::Auto {
            since: time::Instant::now(),
            min_rtt: time::Duration::from_secs(10),

            slow_count: 0,
            slow_down_to: 0,
            last_piece_time: time::Instant::now(),
        }
    }
}

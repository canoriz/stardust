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
        // TODO: need this anymore if we have last_piece_time?
        expire: time::Instant,

        rtt_before: time::Duration,

        // if rtt is smaller
        faster: bool,
    },

    /// probe rtt
    ProbeRTT {
        since_auto: time::Instant,

        // timestamp of last piece receive
        last_piece_time: time::Instant,

        min_rtt: time::Duration,

        // TODO: need this anymore if we have last_piece_time?
        expire: time::Instant,

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
            min_rtt: time::Duration::MAX.mul_f32(0.5),
        }
    }
}

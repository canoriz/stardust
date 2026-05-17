use tokio::time;

/// modes for bandwidth control
/// we want a balanced max-bandwidth and min rtt
#[derive(Debug)]
pub enum BandwidthMode {
    Startup {
        cwnd_since: time::Instant,
        cwnd: usize,
        max_bw: f32,
        limit_count: u32,
    },

    /// adaptively increase requests
    ProbeBW {
        since_cycle: time::Instant,

        // how many consecutive response which have rtt greater than (mean + sigma)
        slow_count: u32,
        // degrees of freedom for one-tail t critical value in current ProbeBW phase
        probe_df: usize,
        slow_down_to: usize,

        // timestamp of last piece receive
        last_piece_time: time::Instant,
        // slow_down_to: usize,
        cycle_index: usize,

        capacity: usize,
    },

    Choked,

    /// slow down to this in-flight
    SlowDown {
        // timestamp of last piece receive
        last_piece_time: time::Instant,

        // slow down to target inflight
        inflight_target: usize,
    },

    /// probe rtt
    ProbeRTT {
        // how many pieces received since probe mode start
        cnt: usize,

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
    pub const PACING: [u32; 8] = [5, 3, 4, 4, 4, 4, 4, 4];
    pub const MIN_PROBE_BW_CAPACITY: usize = 4;

    // gain: increase final result by (gain/4)
    pub fn compute_probe_bw_capacity(
        rtt: time::Duration,
        max_bw: f32,
        cycle_index: usize,
        gain: usize, //
    ) -> usize {
        let base = (rtt.as_secs_f32() * max_bw.max(0.0)) as usize;
        let capacity = base * Self::PACING[cycle_index] as usize * gain / 4 / 4 / 16384;
        capacity.max(Self::MIN_PROBE_BW_CAPACITY)
    }

    pub fn new_choked() -> Self {
        BandwidthMode::Choked
    }

    pub fn new_auto() -> Self {
        BandwidthMode::Startup {
            cwnd_since: time::Instant::now(),
            cwnd: 4,
            max_bw: 0.0,
            limit_count: 0,
        }
    }
}

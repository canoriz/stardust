use tokio::time;
use tracing::info;

#[cfg(feature = "blocktrace")]
use crate::blocktrace;
use crate::conn::{INIT_RTT_STARTUP, Inflight, PeerAddr, conn::Bandwidth};

// fixed wall-clock interval (BBR RTpropFilterLen), never scaled by min_rtt:
// a queue-inflated min_rtt would push this out of reach and lock in the bloat.
const PROBE_RTT_INTERVAL: time::Duration = time::Duration::from_secs(10);
// Transform to slowdown mode if avg rtt is larger than min rtt + threshold
const AVG_RTT_THRESHOLD: time::Duration = time::Duration::from_millis(2000);
const MIN_IN_FLIGHT: usize = 5;
const TEN_SECS: time::Duration = time::Duration::from_secs(10);
const RTT_DELTA: time::Duration = time::Duration::from_millis(500);
const STARTUP_CWND_GAIN: f64 = 2.89;
const STARTUP_FULL_BW_GAIN: f32 = 1.25;
const STARTUP_FULL_BW_ROUNDS: u32 = 2;

fn compute_probe_bdp_rtt(min_rtt: time::Duration) -> time::Duration {
    min_rtt.max(time::Duration::from_millis(50))
}

fn bw_look_back_window(probe_bdp_rtt: time::Duration) -> time::Duration {
    (10 * probe_bdp_rtt).max(time::Duration::from_millis(1500))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BandwidthModeKind {
    Choked,
    Startup,
    ProbeBW,
    SlowDown,
    ProbeRTT,
}

/// modes for bandwidth control
/// we want a balanced max-bandwidth and min rtt
#[derive(Debug)]
pub struct BandwidthMode {
    min_rtt: time::Duration,
    min_rtt_at: time::Instant,

    // timestamp of last piece receive
    last_piece_time: time::Instant,

    phase: BandwidthPhase,
}

#[derive(Debug)]
enum BandwidthPhase {
    Choked,

    Startup {
        // Last delivery round considered for the bandwidth plateau check.
        last_round: u64,
        // In-flight request limit, grown on each matched block response.
        cwnd: usize,
        // Plateau baseline; updated only after a significant bandwidth gain.
        full_bw: f32,
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

        // slow_down_to: usize,
        cycle_index: usize,

        capacity: usize,
    },

    /// slow down to this in-flight
    SlowDown {
        // n received since last pick
        n_recv: u32,

        // timestamp of last pick
        // if we don't pick for a long time
        // peer may choke us
        last_pick_time: time::Instant,

        // slow down to target inflight
        inflight_target: usize,
    },

    /// probe rtt
    ProbeRTT {
        // how many pieces received since probe mode start
        cnt: usize,

        since: time::Instant,

        inflight_target: usize,
        from_clear: bool,

        normal_count: u32,
        slow_count: u32,
    },
}

impl BandwidthMode {
    pub const PACING: [u32; 8] = [5, 3, 4, 4, 4, 4, 4, 4];
    pub const MIN_PROBE_BW_CAPACITY: usize = 4;

    /// Probe around the measured inflight at which the BtlBw peak appeared.
    /// The eight gains are 1.25, 0.75, then six 1.0 cycles.
    pub fn compute_probe_bw_capacity_from_inflight(
        max_bw_inflight: usize,
        cycle_index: usize,
    ) -> usize {
        max_bw_inflight.max(Self::MIN_PROBE_BW_CAPACITY) * Self::PACING[cycle_index] as usize / 4
    }

    #[inline]
    fn rtt_base(min_rtt: time::Duration, avg_rtt: time::Duration) -> time::Duration {
        (min_rtt + RTT_DELTA).min(avg_rtt)
    }

    // gain: increase final result by (gain/4)
    pub fn compute_probe_bw_capacity(
        min_rtt: time::Duration,
        avg_rtt: time::Duration,
        max_bw: f32,
        cycle_index: usize,
        gain: usize,
    ) -> usize {
        let base = (Self::rtt_base(min_rtt, avg_rtt).as_secs_f32() * max_bw.max(0.0)) as usize;
        let capacity = base * Self::PACING[cycle_index] as usize * gain / 4 / 4 / 16384;
        capacity.max(Self::MIN_PROBE_BW_CAPACITY)
    }

    pub fn new(kind: BandwidthModeKind) -> Self {
        let now = time::Instant::now();
        let phase = match kind {
            BandwidthModeKind::Choked => BandwidthPhase::Choked,
            BandwidthModeKind::Startup => BandwidthPhase::Startup {
                last_round: 0,
                cwnd: 4,
                full_bw: 0.0,
                limit_count: 0,
            },
            BandwidthModeKind::ProbeBW => BandwidthPhase::ProbeBW {
                since_cycle: now,
                slow_count: 0,
                probe_df: 1,
                slow_down_to: 0,
                cycle_index: 0,
                capacity: Self::MIN_PROBE_BW_CAPACITY,
            },
            BandwidthModeKind::SlowDown => BandwidthPhase::SlowDown {
                n_recv: 0,
                last_pick_time: now,
                inflight_target: 0,
            },
            BandwidthModeKind::ProbeRTT => BandwidthPhase::ProbeRTT {
                cnt: 0,
                since: now,
                inflight_target: 4,
                from_clear: true,
                normal_count: 0,
                slow_count: 0,
            },
        };
        Self {
            min_rtt: if kind == BandwidthModeKind::Choked {
                time::Duration::from_millis(100)
            } else {
                INIT_RTT_STARTUP
            },
            min_rtt_at: now,
            last_piece_time: now,
            phase,
        }
    }

    pub fn mode(&self) -> BandwidthModeKind {
        match &self.phase {
            BandwidthPhase::Choked => BandwidthModeKind::Choked,
            BandwidthPhase::Startup { .. } => BandwidthModeKind::Startup,
            BandwidthPhase::ProbeBW { .. } => BandwidthModeKind::ProbeBW,
            BandwidthPhase::SlowDown { .. } => BandwidthModeKind::SlowDown,
            BandwidthPhase::ProbeRTT { .. } => BandwidthModeKind::ProbeRTT,
        }
    }

    fn startup_cwnd_limit(inflight: &Inflight, avg_rtt: time::Duration) -> usize {
        inflight
            .min_rtt()
            .map_or(4, |(min_rtt, _)| {
                let rtt_base = Self::rtt_base(min_rtt, avg_rtt);
                (STARTUP_CWND_GAIN * f64::from(inflight.max_bw()) * rtt_base.as_secs_f64()
                    / 16384.0)
                    .ceil() as usize
            })
            .max(4)
    }
    pub fn receive(
        &mut self,
        rtt: time::Duration,
        avg_rtt: time::Duration,
        acknowledged_bytes: usize,
        inflight: &Inflight,
    ) {
        let now = time::Instant::now();
        self.last_piece_time = now;

        if matches!(self.phase, BandwidthPhase::Choked) {
            return;
        }

        if rtt < self.min_rtt {
            self.min_rtt = rtt;
            self.min_rtt_at = now;
        }

        match &mut self.phase {
            BandwidthPhase::Startup { cwnd, .. } => {
                *cwnd = cwnd
                    .saturating_add(acknowledged_bytes.div_ceil(16384))
                    .min(Self::startup_cwnd_limit(inflight, avg_rtt));
            }
            BandwidthPhase::ProbeRTT { cnt, .. } => {
                *cnt += 1;
            }
            _ => {}
        };
    }

    fn should_slowdown_to(
        min_rtt: time::Duration,
        min_rtt_at: time::Instant,
        avg_rtt: time::Duration,
        max_bw: f32,
        max_in_flight: usize,
        n_received: usize,
    ) -> Option<usize> {
        if min_rtt_at.elapsed() > PROBE_RTT_INTERVAL {
            Some(0)
        } else if n_received > 0 && avg_rtt > min_rtt + AVG_RTT_THRESHOLD {
            Some(Self::compute_probe_bw_capacity(min_rtt, avg_rtt, max_bw, 2, 4).min(max_in_flight))
        } else {
            None
        }
    }

    pub fn on_picked(&mut self) {
        let now = time::Instant::now();
        match &mut self.phase {
            BandwidthPhase::SlowDown {
                last_pick_time,
                n_recv,
                ..
            } => {
                *last_pick_time = now;
                *n_recv = 0;
            }
            _ => {}
        }
    }

    pub fn on_blocks_received(
        &mut self,
        peer: PeerAddr,
        inflight: &mut Inflight,
        bw: &mut Bandwidth,
        max_in_flight: usize,
        n_received: usize,
        #[cfg(feature = "blocktrace")] need_sample: &mut bool,
    ) {
        let now = time::Instant::now();
        let n_req_in_flight = inflight.inflight(bw.get_rtt_4var());
        let probe_bw_cycle = self.probe_bw_cycle();
        let look_back_duration = bw_look_back_window(probe_bw_cycle);
        let avg_bw_10s = bw.count_avg_bw_in(TEN_SECS);
        let avg_bw = bw.count_avg_bw_in(look_back_duration);
        let avg_rtt = bw.get_rtt();
        let max_bw = inflight.max_bw();

        let Self {
            min_rtt,
            min_rtt_at,
            last_piece_time,
            phase,
        } = self;
        match phase {
            BandwidthPhase::Startup {
                cwnd,
                full_bw,
                last_round,
                limit_count,
            } => {
                let round = inflight.round_count();
                if let Some(n) = Self::should_slowdown_to(
                    *min_rtt,
                    *min_rtt_at,
                    avg_rtt,
                    max_bw,
                    max_in_flight,
                    n_received,
                ) {
                    *last_piece_time = now;
                    *phase = BandwidthPhase::SlowDown {
                        n_recv: 0,
                        // TODO: should be a real last pick time
                        last_pick_time: now,
                        inflight_target: n,
                    };
                    inflight.set_app_limited();
                } else if round > *last_round {
                    let max_bw_in_rtt = inflight.max_bw();
                    let app_limited_at_round_start = inflight.app_limited_at_round_start();
                    info!(
                        "{peer:?} in Startup mode, cwnd {}, inflight {} full bw {}, avg-bw {} avg_bw_10s {} new max bw {}, limit count {} app limited at round start {} current_app_limited {}",
                        *cwnd,
                        n_req_in_flight,
                        *full_bw,
                        avg_bw,
                        avg_bw_10s,
                        max_bw_in_rtt,
                        *limit_count,
                        app_limited_at_round_start,
                        inflight.is_app_limited(),
                    );
                    info!("startup round {} -> {}", *last_round, round);
                    *last_round = round;

                    if !app_limited_at_round_start {
                        if max_bw_in_rtt < *full_bw * STARTUP_FULL_BW_GAIN {
                            *limit_count += 1;
                            info!(
                                "{peer:?} Startup stagnation {}, round {}, max bw in rtt {}, full bw baseline {}",
                                *limit_count, round, max_bw_in_rtt, full_bw,
                            );
                        } else {
                            *full_bw = max_bw_in_rtt;
                            *limit_count = 0;
                        }

                        if *limit_count >= STARTUP_FULL_BW_ROUNDS {
                            let optimum =
                                (max_bw_in_rtt * (*min_rtt).as_secs_f32() / 16384.0) as usize;
                            *last_piece_time = now;
                            *phase = BandwidthPhase::SlowDown {
                                n_recv: 0,
                                // TODO: should be a real last pick time
                                last_pick_time: now,
                                inflight_target: optimum / 2,
                            };
                            inflight.set_app_limited();

                            #[cfg(feature = "blocktrace")]
                            {
                                *need_sample = true;
                            }
                        }
                    }
                }
            }
            BandwidthPhase::ProbeBW {
                since_cycle,
                slow_down_to,
                probe_df,
                cycle_index,
                capacity,
                ..
            } => {
                if min_rtt_at.elapsed() > PROBE_RTT_INTERVAL {
                    info!("{peer:?} change from ProbeBW to SlowDown(0)");
                    *phase = BandwidthPhase::SlowDown {
                        n_recv: 0,
                        // TODO: should be a real last pick time
                        last_pick_time: now,
                        inflight_target: 0,
                    };
                    inflight.set_app_limited();

                    #[cfg(feature = "blocktrace")]
                    {
                        *need_sample = true;
                    }
                } else if since_cycle.elapsed() > probe_bw_cycle {
                    *cycle_index = (*cycle_index + 1) & 0x7;
                    *since_cycle = now;

                    *capacity = BandwidthMode::compute_probe_bw_capacity_from_inflight(
                        inflight.max_bw_inflight(),
                        *cycle_index,
                    )
                    .min(max_in_flight);
                    info!(
                        "{peer:?} ProbeBW cycle update {}, probe df {}, max bw {}, avg bw {}, avg_bw_10s {}, min rtt {:?}, probe rtt {:?} capacity {}",
                        *cycle_index,
                        *probe_df,
                        max_bw,
                        avg_bw,
                        avg_bw_10s,
                        *min_rtt,
                        probe_bw_cycle,
                        *capacity,
                    );

                    // only check rtt at cycle change, so a complete cycle is sent
                    // we can probe a larger bandwidth
                    // if slowdown happens at any time, we may not be able to probe
                    // a higher bandwidth.
                    if let Some(n) = Self::should_slowdown_to(
                        *min_rtt,
                        *min_rtt_at,
                        avg_rtt,
                        max_bw,
                        max_in_flight,
                        n_received,
                    ) {
                        info!("{peer:?} change from ProbeBW to SlowDown({n})");
                        *phase = BandwidthPhase::SlowDown {
                            n_recv: 0,
                            // TODO: should be a real last pick time
                            last_pick_time: now,
                            inflight_target: n,
                        };
                        inflight.set_app_limited();

                        #[cfg(feature = "blocktrace")]
                        {
                            *need_sample = true;
                        }
                    }
                }
            }
            BandwidthPhase::SlowDown {
                n_recv,
                inflight_target,
                ..
            } => {
                if n_req_in_flight <= *inflight_target {
                    if *inflight_target > 0 {
                        let cap = BandwidthMode::compute_probe_bw_capacity_from_inflight(
                            inflight.max_bw_inflight(),
                            2,
                        )
                        .min(max_in_flight);
                        // info!(
                        //     "{peer:?} change from Slowdown to ProbeBW mode, because inflight target {inflight_target} > 0"
                        // );
                        *phase = BandwidthPhase::ProbeBW {
                            slow_count: 0,
                            probe_df: 1,
                            slow_down_to: 0,
                            cycle_index: 0,
                            since_cycle: now,
                            capacity: cap,
                        };

                        #[cfg(feature = "blocktrace")]
                        {
                            *need_sample = true;
                        }
                    } else {
                        info!("{peer:?} change from Slowdown to ProbeRTT mode");
                        bw.reset_var();
                        let (inflight_min_rtt, inflight_min_rtt_at) = inflight.min_rtt().unzip();
                        *min_rtt = inflight_min_rtt.unwrap_or(INIT_RTT_STARTUP);
                        *min_rtt_at = inflight_min_rtt_at.unwrap_or(now);
                        *phase = BandwidthPhase::ProbeRTT {
                            slow_count: 0,
                            since: now,
                            cnt: 0,
                            inflight_target: 4,
                            from_clear: *inflight_target == 0,
                            normal_count: 0,
                        };
                        inflight.set_app_limited();

                        #[cfg(feature = "blocktrace")]
                        {
                            *need_sample = true;
                        }
                    }
                } else {
                    *n_recv += n_received as u32;
                    *last_piece_time = now;
                }
            }
            BandwidthPhase::ProbeRTT {
                since,
                normal_count,
                from_clear,
                cnt,
                ..
            } => {
                info!(
                    "{peer:?} in ProbeRTT mode cnt {} normal count {} min rtt {:?} avg bw {} avg_bw_10s {} req in flight {}",
                    cnt, normal_count, *min_rtt, avg_bw, avg_bw_10s, n_req_in_flight
                );
                if *cnt > 4 || since.elapsed() > time::Duration::from_secs(1) {
                    let (inflight_min_rtt, inflight_min_rtt_at) = inflight.min_rtt().unzip();
                    *min_rtt = inflight_min_rtt.unwrap_or(INIT_RTT_STARTUP);
                    *min_rtt_at = inflight_min_rtt_at.unwrap_or(now);
                    let cap = BandwidthMode::compute_probe_bw_capacity_from_inflight(
                        inflight.max_bw_inflight(),
                        2,
                    )
                    .min(max_in_flight);
                    // info!("{peer:?} change from ProbeRTT to ProbeBW mode");
                    *phase = BandwidthPhase::ProbeBW {
                        slow_count: 0,
                        since_cycle: now,
                        probe_df: 1,
                        slow_down_to: 0,
                        cycle_index: 0,
                        capacity: cap,
                    };

                    #[cfg(feature = "blocktrace")]
                    {
                        *need_sample = true;
                    }
                }
            }
            BandwidthPhase::Choked => {}
        };
    }

    pub fn fill_capacity(
        &mut self,
        peer: PeerAddr,
        inflight: &Inflight,
        bw: &Bandwidth,
        max_in_flight: usize,
    ) -> usize {
        let n_req_in_flight = inflight.inflight(bw.get_rtt_4var());
        let max_bw = inflight.max_bw();

        let probe_bw_cycle = self.probe_bw_cycle();
        let look_back_duration = bw_look_back_window(probe_bw_cycle);
        let avg_bw_10s = bw.count_avg_bw_in(TEN_SECS);
        let avg_bw = bw.count_avg_bw_in(look_back_duration);
        let avg_rtt = bw.get_rtt();

        let Self { min_rtt, phase, .. } = self;
        match phase {
            BandwidthPhase::Startup { cwnd, .. } => (*cwnd)
                .min(Self::startup_cwnd_limit(inflight, bw.get_rtt()))
                .min(max_in_flight)
                .saturating_sub(n_req_in_flight),
            BandwidthPhase::ProbeBW {
                cycle_index,
                capacity,
                ..
            } => {
                // Only can pick more blocks if we received some or no requests in flight.
                // For peers with small bandwidth, we don't request too much from them
                // to avoid mark these blocks as in-flight and not requesting from other peers.
                // preventing accumulating too much partial downloaded pieces.

                let limit = BandwidthMode::compute_probe_bw_capacity_from_inflight(
                    inflight.max_bw_inflight(),
                    *cycle_index,
                )
                .max(Self::compute_probe_bw_capacity(
                    *min_rtt,
                    avg_rtt,
                    max_bw,
                    *cycle_index,
                    4,
                ))
                .min(max_in_flight)
                .max(MIN_IN_FLIGHT);

                info!(
                    "{peer:?} ProbeBW mode max bw {}, avg bw {}, avg_bw_10s {}, limit {}",
                    max_bw, avg_bw, avg_bw_10s, limit
                );

                let n_to_pick = limit.saturating_sub(n_req_in_flight);
                info!(
                    "{peer:?} ProbeBW mode cycle {} capacity {} min rtt {:?} probe rtt {:?} avg bw {} avg_bw_10s {} max_bw {} req in flight {}",
                    cycle_index,
                    capacity,
                    *min_rtt,
                    probe_bw_cycle,
                    avg_bw,
                    avg_bw_10s,
                    max_bw,
                    n_req_in_flight
                );
                n_to_pick
            }
            BandwidthPhase::SlowDown {
                last_pick_time,
                n_recv,
                inflight_target,
            } => {
                info!(
                    "{peer:?} slow down mode {} to recv",
                    n_req_in_flight.saturating_sub(*inflight_target)
                );
                info!(
                    "{peer:?} Slowdown mode min rtt {:?} avg bw {} avg_bw_10s {} req in flight {}",
                    *min_rtt, avg_bw, avg_bw_10s, n_req_in_flight
                );
                // Keep the occasional probe while draining toward the
                // slowdown target, but never exceed the peer/request cap.
                if *n_recv > 1
                    && max_in_flight > n_req_in_flight
                    && last_pick_time.elapsed() > time::Duration::from_secs(2)
                {
                    1
                } else {
                    0
                }
            }
            BandwidthPhase::ProbeRTT {
                inflight_target, ..
            } => {
                info!(
                    "{peer:?} ProbeRTT mode min rtt {:?} avg bw {} avg_bw_10s {} inflight_target {} req in flight {}",
                    *min_rtt, avg_bw, avg_bw_10s, inflight_target, n_req_in_flight
                );
                inflight_target.saturating_sub(n_req_in_flight)
            }
            BandwidthPhase::Choked => 0,
        }
    }

    pub fn min_rtt(&self) -> time::Duration {
        self.min_rtt
    }

    fn probe_bw_cycle(&self) -> time::Duration {
        compute_probe_bdp_rtt(self.min_rtt())
    }

    pub fn look_back_window(&self) -> time::Duration {
        bw_look_back_window(self.probe_bw_cycle())
    }
}

#[cfg(feature = "blocktrace")]
#[inline]
pub fn bt_bw_mode(m: &BandwidthMode) -> blocktrace::BwMode {
    match m.mode() {
        BandwidthModeKind::Startup => blocktrace::BwMode::Startup,
        BandwidthModeKind::ProbeBW => blocktrace::BwMode::ProbeBw,
        BandwidthModeKind::SlowDown => blocktrace::BwMode::SlowDown,
        BandwidthModeKind::ProbeRTT => blocktrace::BwMode::ProbeRtt,
        BandwidthModeKind::Choked => blocktrace::BwMode::Choked,
    }
}

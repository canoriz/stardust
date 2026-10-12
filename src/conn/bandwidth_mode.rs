use std::net::SocketAddr;

use tokio::time;
use tracing::info;

#[cfg(feature = "blocktrace")]
use crate::blocktrace;
use crate::conn::{INIT_RTT_STARTUP, Inflight, conn::Bandwidth};

// fixed wall-clock interval (BBR RTpropFilterLen), never scaled by min_rtt:
// a queue-inflated min_rtt would push this out of reach and lock in the bloat.
const PROBE_RTT_INTERVAL: time::Duration = time::Duration::from_secs(10);
const MIN_IN_FLIGHT: usize = 5;
const TEN_SECS: time::Duration = time::Duration::from_secs(10);

fn compute_probe_bdp_rtt(min_rtt: time::Duration) -> time::Duration {
    min_rtt + time::Duration::from_millis(50)
}

fn bw_look_back_window(probe_bdp_rtt: time::Duration) -> time::Duration {
    (10 * probe_bdp_rtt).max(time::Duration::from_millis(1500))
}

/// modes for bandwidth control
/// we want a balanced max-bandwidth and min rtt
#[derive(Debug)]
pub enum BandwidthMode {
    Choked,

    Startup {
        // BBR round at which cwnd was last doubled; grow once per round (≈ per RTT)
        last_round: u64,
        cwnd: usize,
        max_bw: f32,
        limit_count: u32,

        min_rtt: time::Duration,
        min_rtt_at: time::Instant,
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

        min_rtt: time::Duration,
        min_rtt_at: time::Instant,
    },

    /// slow down to this in-flight
    SlowDown {
        // timestamp of last piece receive
        last_piece_time: time::Instant,

        // slow down to target inflight
        inflight_target: usize,

        min_rtt: time::Duration,
        min_rtt_at: time::Instant,
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

        min_rtt: time::Duration,
        min_rtt_at: time::Instant,
    },
}

impl BandwidthMode {
    pub const PACING: [u32; 8] = [5, 3, 4, 4, 4, 4, 4, 4];
    pub const MIN_PROBE_BW_CAPACITY: usize = 4;

    // gain: increase final result by (gain/4)
    pub fn compute_probe_bw_capacity(
        min_rtt: time::Duration,
        avg_rtt: time::Duration,
        max_bw: f32,
        cycle_index: usize,
        gain: usize,
    ) -> usize {
        let base = ((min_rtt + time::Duration::from_millis(300))
            .min(avg_rtt)
            .as_secs_f32()
            * max_bw.max(0.0)) as usize;
        let capacity = base * Self::PACING[cycle_index] as usize * gain / 4 / 4 / 16384;
        capacity.max(Self::MIN_PROBE_BW_CAPACITY)
    }

    pub fn new_choked() -> Self {
        BandwidthMode::Choked
    }

    pub fn new_auto() -> Self {
        BandwidthMode::Startup {
            last_round: 0,
            cwnd: 4,
            max_bw: 0.0,
            limit_count: 0,
            min_rtt: INIT_RTT_STARTUP,
            min_rtt_at: time::Instant::now(),
        }
    }

    pub fn receive(&mut self, rtt: time::Duration) {
        if matches!(self, BandwidthMode::Choked) {
            return;
        }

        let now = time::Instant::now();
        let (min_rtt, min_rtt_at) = self.min_rtt_mut();
        if rtt < *min_rtt {
            *min_rtt = rtt;
            *min_rtt_at = now;
        }

        match self {
            BandwidthMode::Startup { .. } => {}
            BandwidthMode::ProbeBW {
                last_piece_time,
                slow_count,
                probe_df,
                ..
            } => {
                *last_piece_time = now;
            }
            BandwidthMode::SlowDown {
                last_piece_time, ..
            } => {
                *last_piece_time = now;
            }
            BandwidthMode::ProbeRTT {
                cnt,
                last_piece_time,
                ..
            } => {
                *cnt += 1;
                *last_piece_time = now;
            }
            BandwidthMode::Choked => {}
        };
    }

    pub fn on_blocks_received(
        &mut self,
        peer: SocketAddr,
        inflight: &Inflight,
        bw: &mut Bandwidth,
        app_limited: &mut bool,
        max_in_flight: usize,
        #[cfg(feature = "blocktrace")] need_sample: &mut bool,
    ) {
        let rtt_for_capacity = bw.get_rtt() + bw.get_var();
        let now = time::Instant::now();
        let n_req_in_flight = inflight.inflight(bw.get_rtt_4var());
        let probe_bw_cycle = self.probe_bw_cycle();
        let look_back_duration = bw_look_back_window(probe_bw_cycle);
        let avg_bw_10s = bw.count_avg_bw_in(TEN_SECS);
        let avg_bw = bw.count_avg_bw_in(look_back_duration);

        match self {
            BandwidthMode::Startup {
                cwnd,
                max_bw,
                last_round,
                limit_count,
                min_rtt,
                min_rtt_at,
            } => {
                const NO_MORE_GAIN_LIMIT: u32 = 2;
                // BBR: double cwnd once per round (≈ per RTT), ack-clocked, not wall-clock
                let round = inflight.round_count();
                if min_rtt_at.elapsed() > PROBE_RTT_INTERVAL {
                    *self = BandwidthMode::SlowDown {
                        last_piece_time: now,
                        inflight_target: 0,
                        min_rtt: *min_rtt,
                        min_rtt_at: *min_rtt_at,
                    }
                } else if round > *last_round {
                    let max_bw_in_rtt = inflight.max_bw();
                    info!(
                        "{peer} in Startup mode, cwnd {}, inflight {} prev max bw {}, avg-bw {} avg_bw_10s {} new max bw {}, limit count {} app limited {}",
                        *cwnd,
                        n_req_in_flight,
                        *max_bw,
                        avg_bw,
                        avg_bw_10s,
                        max_bw_in_rtt,
                        *limit_count,
                        *app_limited,
                    );
                    info!("startup round {} -> {}", *last_round, round);
                    *last_round = round;

                    if !*app_limited {
                        if max_bw_in_rtt <= *max_bw * 1.2 {
                            *limit_count += 1;
                            info!(
                                "{peer} Startup stagnation {}, round {}, max bw in rtt {}, current max bw {}",
                                *limit_count, round, max_bw_in_rtt, max_bw,
                            );
                        } else {
                            *limit_count = 0;
                        }

                        *max_bw = (*max_bw).max(max_bw_in_rtt);
                        *cwnd = (*cwnd * 2).min(max_in_flight).max(4);

                        if *limit_count >= NO_MORE_GAIN_LIMIT {
                            let optimum = (*max_bw * (*min_rtt).as_secs_f32() / 16384.0) as usize;
                            // info!(
                            //     "{peer} change from Startup to Slowdown mode, {} {:?} slow down to {optimum}",
                            //     *max_bw, conn.min_rtt_phase,
                            // );
                            *self = BandwidthMode::SlowDown {
                                last_piece_time: now,
                                inflight_target: optimum / 2,
                                min_rtt: *min_rtt,
                                min_rtt_at: *min_rtt_at,
                            };
                            *app_limited = true;

                            #[cfg(feature = "blocktrace")]
                            {
                                *need_sample = true;
                            }
                        }
                    }
                }
            }
            BandwidthMode::ProbeBW {
                since_cycle,
                last_piece_time,
                slow_down_to,
                slow_count,
                probe_df,
                cycle_index,
                capacity,
                min_rtt,
                min_rtt_at,
            } => {
                if since_cycle.elapsed() > probe_bw_cycle {
                    *cycle_index = (*cycle_index + 1) & 0x7;
                    *since_cycle = now;
                    let max_bw = inflight.max_bw();

                    *capacity = BandwidthMode::compute_probe_bw_capacity(
                        probe_bw_cycle,
                        rtt_for_capacity,
                        max_bw,
                        *cycle_index,
                        4, // 1.0
                    );
                    info!(
                        "{peer} ProbeBW cycle update {}, probe df {}, max bw {}, avg bw {}, avg_bw_10s {}, min rtt {:?}, probe rtt {:?} capacity {}",
                        *cycle_index,
                        *probe_df,
                        max_bw,
                        avg_bw,
                        avg_bw_10s,
                        *min_rtt,
                        probe_bw_cycle,
                        *capacity,
                    );
                }

                const SLOW_LIMIT: u32 = 2;
                if min_rtt_at.elapsed() > PROBE_RTT_INTERVAL {
                    info!(
                        "{peer} change from ProbeBW to SlowDown mode because no smaller rtt after {:?}",
                        PROBE_RTT_INTERVAL,
                    );
                    *self = BandwidthMode::SlowDown {
                        last_piece_time: *last_piece_time,
                        inflight_target: 0,
                        min_rtt: *min_rtt,
                        min_rtt_at: *min_rtt_at,
                    };
                    *app_limited = true;

                    #[cfg(feature = "blocktrace")]
                    {
                        *need_sample = true;
                    }
                    // } else if *slow_count >= SLOW_LIMIT {
                    //     info!("{peer} change from ProbeBW to Slowdown mode because {SLOW_LIMIT} times of slow rtt");
                    //     conn.bw_mode = BandwidthMode::SlowDown {
                    //         since_min_rtt,
                    //         last_piece_time: *last_piece_time,
                    //         min_rtt,
                    //         inflight_target: n_req_in_flight / 2,
                    //     };
                    // } else if last_piece_time.elapsed() > likely_recv_next_within {
                    //     // either it's a small probability event
                    //     // or the peer is slowing the send rate
                    //     info!(
                    //         "{peer} change from ProbeBW to Slowdown mode because no packet in {:?}",
                    //         likely_recv_next_within,
                    //     );
                    //     conn.bw_mode = BandwidthMode::SlowDown {
                    //         since_min_rtt,
                    //         last_piece_time: *last_piece_time,
                    //         min_rtt,
                    //         inflight_target: 0,
                    //     };
                }
            }
            BandwidthMode::SlowDown {
                last_piece_time,
                inflight_target,
                min_rtt,
                min_rtt_at,
            } => {
                if n_req_in_flight <= *inflight_target {
                    if *inflight_target > 0 {
                        let max_bw = inflight.max_bw();
                        let cap = BandwidthMode::compute_probe_bw_capacity(
                            probe_bw_cycle,
                            rtt_for_capacity,
                            max_bw,
                            2,
                            4,
                        )
                        .min(max_in_flight);
                        // info!(
                        //     "{peer} change from Slowdown to ProbeBW mode, because inflight target {inflight_target} > 0"
                        // );
                        *self = BandwidthMode::ProbeBW {
                            slow_count: 0,
                            probe_df: 1,
                            slow_down_to: 0,
                            last_piece_time: *last_piece_time,
                            cycle_index: 0,
                            since_cycle: now,
                            capacity: cap * 2,
                            min_rtt: *min_rtt,
                            min_rtt_at: *min_rtt_at,
                        };

                        #[cfg(feature = "blocktrace")]
                        {
                            *need_sample = true;
                        }
                    } else {
                        info!("{peer} change from Slowdown to ProbeRTT mode");
                        bw.reset_var();
                        let (inflight_min_rtt, inflight_min_rtt_at) = inflight.min_rtt().unzip();
                        *min_rtt = inflight_min_rtt.unwrap_or(INIT_RTT_STARTUP);
                        *min_rtt_at = inflight_min_rtt_at.unwrap_or(now);
                        *self = BandwidthMode::ProbeRTT {
                            since: now,
                            cnt: 0,
                            last_piece_time: *last_piece_time,
                            inflight_target: 4,
                            from_clear: *inflight_target == 0,
                            normal_count: 0,
                            slow_count: 0,
                            min_rtt: *min_rtt,
                            min_rtt_at: *min_rtt_at,
                        };

                        #[cfg(feature = "blocktrace")]
                        {
                            *need_sample = true;
                        }
                    }
                }
            }
            BandwidthMode::ProbeRTT {
                since,
                last_piece_time,
                normal_count,
                slow_count,
                from_clear,
                inflight_target,
                cnt,
                min_rtt,
                min_rtt_at,
            } => {
                info!(
                    "{peer} in ProbeRTT mode cnt {} normal count {} min rtt {:?} avg bw {} avg_bw_10s {} req in flight {}",
                    cnt, normal_count, *min_rtt, avg_bw, avg_bw_10s, n_req_in_flight
                );
                let max_bw = inflight.max_bw();

                if *cnt > 4 {
                    let (inflight_min_rtt, inflight_min_rtt_at) = inflight.min_rtt().unzip();
                    *min_rtt = inflight_min_rtt.unwrap_or(INIT_RTT_STARTUP);
                    *min_rtt_at = inflight_min_rtt_at.unwrap_or(now);
                    let cap = BandwidthMode::compute_probe_bw_capacity(
                        probe_bw_cycle,
                        rtt_for_capacity,
                        max_bw,
                        2,
                        4,
                    )
                    .min(max_in_flight);
                    // info!("{peer} change from ProbeRTT to ProbeBW mode");
                    *self = BandwidthMode::ProbeBW {
                        since_cycle: now,
                        slow_count: 0,
                        probe_df: 1,
                        slow_down_to: 0,
                        last_piece_time: *last_piece_time,
                        cycle_index: 0,
                        capacity: cap * 2,
                        min_rtt: *min_rtt,
                        min_rtt_at: *min_rtt_at,
                    };
                    *app_limited = false;

                    #[cfg(feature = "blocktrace")]
                    {
                        *need_sample = true;
                    }
                }
            }
            BandwidthMode::Choked => {}
        };
    }

    pub fn fill_capacity(
        &mut self,
        peer: SocketAddr,
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

        match self {
            BandwidthMode::Startup { cwnd, .. } => cwnd.saturating_sub(n_req_in_flight),
            BandwidthMode::ProbeBW {
                cycle_index,
                capacity,
                min_rtt,
                ..
            } => {
                // Only can pick more blocks if we received some or no requests in flight.
                // For peers with small bandwidth, we don't request too much from them
                // to avoid mark these blocks as in-flight and not requesting from other peers.
                // preventing accumulating too much partial downloaded pieces.

                let rtt_for_capacity = bw.get_rtt() + bw.get_var();
                let limit = BandwidthMode::compute_probe_bw_capacity(
                    probe_bw_cycle,
                    rtt_for_capacity,
                    max_bw,
                    *cycle_index,
                    4,
                )
                .min(max_in_flight)
                .max(MIN_IN_FLIGHT);

                info!(
                    "{peer} ProbeBW mode max bw {}, avg bw {}, avg_bw_10s {}, limit {}",
                    max_bw, avg_bw, avg_bw_10s, limit
                );

                let n_to_pick = limit.saturating_sub(n_req_in_flight);
                info!(
                    "{peer} ProbeBW mode cycle {} capacity {} min rtt {:?} probe rtt {:?} avg bw {} avg_bw_10s {} max_bw {} req in flight {}",
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
            BandwidthMode::SlowDown {
                last_piece_time,
                inflight_target,
                min_rtt,
                ..
            } => {
                info!(
                    "{peer} slow down mode {} to recv",
                    n_req_in_flight.saturating_sub(*inflight_target)
                );
                info!(
                    "{peer} Slowdown mode min rtt {:?} avg bw {} avg_bw_10s {} req in flight {}",
                    *min_rtt, avg_bw, avg_bw_10s, n_req_in_flight
                );
                0
            }
            BandwidthMode::ProbeRTT {
                inflight_target,
                min_rtt,
                ..
            } => {
                info!(
                    "{peer} ProbeRTT mode min rtt {:?} avg bw {} avg_bw_10s {} inflight_target {} req in flight {}",
                    *min_rtt, avg_bw, avg_bw_10s, inflight_target, n_req_in_flight
                );
                inflight_target.saturating_sub(n_req_in_flight)
            }
            BandwidthMode::Choked => 0,
        }
    }

    pub fn min_rtt(&self) -> time::Duration {
        match self {
            BandwidthMode::Choked => time::Duration::from_millis(100),
            BandwidthMode::Startup { min_rtt, .. } => *min_rtt,
            BandwidthMode::ProbeBW {
                min_rtt,
                ..
            } => *min_rtt,
            BandwidthMode::SlowDown {
                min_rtt,
                ..
            } => *min_rtt,
            BandwidthMode::ProbeRTT {
                min_rtt,
                ..
            } => *min_rtt,
        }
    }

    fn min_rtt_mut(&mut self) -> (&mut time::Duration, &mut time::Instant) {
        match self {
            BandwidthMode::Choked => unreachable!(),
            BandwidthMode::Startup {
                min_rtt,
                min_rtt_at,
                ..
            } => (min_rtt, min_rtt_at),
            BandwidthMode::ProbeBW {
                min_rtt,
                min_rtt_at,
                ..
            } => (min_rtt, min_rtt_at),
            BandwidthMode::SlowDown {
                min_rtt,
                min_rtt_at,
                ..
            } => (min_rtt, min_rtt_at),
            BandwidthMode::ProbeRTT {
                min_rtt,
                min_rtt_at,
                ..
            } => (min_rtt, min_rtt_at),
        }
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
    match m {
        BandwidthMode::Startup { .. } => blocktrace::BwMode::Startup,
        BandwidthMode::ProbeBW { .. } => blocktrace::BwMode::ProbeBw,
        BandwidthMode::SlowDown { .. } => blocktrace::BwMode::SlowDown,
        BandwidthMode::ProbeRTT { .. } => blocktrace::BwMode::ProbeRtt,
        BandwidthMode::Choked => blocktrace::BwMode::Choked,
    }
}

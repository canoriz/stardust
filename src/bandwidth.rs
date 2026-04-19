use tokio::time::{Duration, Instant};

mod regression;
mod rtt;
pub use regression::SlidingWindowRegression;
pub use rtt::{ALPHA, BETA, RTT};
use tracing::{info, instrument, trace};

#[derive(Debug, Clone)]
pub(crate) struct Bandwidth<const SLOT_SIZE: usize> {
    circular: [Period; SLOT_SIZE],
    head: usize,

    rtt: RTT,
}

#[derive(Debug, Copy, Clone)]
struct Period {
    /// packet number
    pkg_count: u32,

    /// bytes number
    bytes_count: usize,

    /// time since when
    since: Instant,

    /// slot duration for this period
    dur: Duration,

    rtt: RTT,
}

// impl Default for Period {
//     fn default() -> Self {
//         Self {
//             pkg_count: 0,
//             bytes_count: 0,
//             since: Instant::now(),
//             rtt: RTT::new(ALPHA, BETA),
//         }
//     }
// }

impl Period {
    /// new a Period with initial rtt and variation
    fn new() -> Self {
        let now = Instant::now();
        Self {
            pkg_count: 0,
            bytes_count: 0,
            since: now,
            // default slot duration kept as 500ms for initial periods
            dur: Duration::from_millis(500),
            rtt: RTT::new(ALPHA, BETA),
        }
    }

    fn add(&mut self, size: usize, n_packet: u32, rtt: Duration) {
        self.bytes_count += size;
        self.pkg_count += n_packet;
        self.rtt.add_rtt_sample(rtt);
    }
}

impl<const SLOT_SIZE: usize> Bandwidth<SLOT_SIZE> {
    pub fn new() -> Bandwidth<SLOT_SIZE> {
        Bandwidth {
            circular: [Period::new(); SLOT_SIZE],
            head: 0,

            rtt: RTT::new(ALPHA, BETA),
        }
    }

    /// add a rtt sample to rtt estimator
    /// this does not add a sample to bandwidth estimator, use add_sample for that
    pub fn add_rtt(&mut self, rtt: Duration) {
        self.rtt.add_rtt_sample(rtt);
    }

    /// updates bandwidth status
    /// how many new bytes received
    /// if given rtt, use this rtt
    /// if not given, will use an average rtt
    pub fn add_sample(
        &mut self,
        n_bytes: usize,
        rtt: Option<Duration>,
        n_in_flight: Option<usize>,
    ) {
        let before_rtt = self.circular[self.head].rtt.get_rtt();

        let cur_rtt = self.current_slot_duration();
        let elapsed = self.circular[self.head].since.elapsed();
        let pre_dur = self.circular[self.head].dur;

        let rtt_changed = elapsed > cur_rtt && elapsed < pre_dur;
        if elapsed > pre_dur || rtt_changed {
            if rtt_changed {
                // update old slot's duration
                self.circular[self.head].dur = elapsed;
            }

            // normal rotation: this slot has expired
            if self.head + 1 >= SLOT_SIZE {
                self.head = 0;
            } else {
                self.head += 1;
            }
            let mut p = Period::new();
            p.dur = cur_rtt;
            self.circular[self.head] = p;
        }

        let rtt = rtt.unwrap_or(before_rtt);
        self.circular[self.head].add(n_bytes, 1, rtt);
    }

    /// get average bandwidth in back_interval, and min rtt in that period
    pub fn count_avg_bw_in(&self, back_interval: Duration) -> f32 {
        // ensure we consider at least one slot duration
        let back = back_interval.max(self.current_slot_duration());

        let map_f = |begin: Instant, _end: Instant, p: &Period| (p.bytes_count, begin);

        let reduce_f = |acc: (usize, Instant), this: (usize, Instant)| -> (usize, Instant) {
            (acc.0 + this.0, acc.1.min(this.1))
        };
        self.reduce_periods_within_interval(back, map_f, reduce_f)
            .map_or(0.0, |(total_size, since)| {
                // Use at least `back` as denominator: if the oldest included period started
                // very recently (e.g. after a long gap), dividing by its tiny elapsed time
                // would produce a spurious spike.
                (total_size as f32) / since.elapsed().max(back).as_secs_f32()
            })
    }

    pub fn count_max_bw_and_min_rtt(&self, back_interval: Duration) -> (f32, Duration, Instant) {
        let map_f = |_begin: Instant, end: Instant, p: &Period| {
            let dt = end - p.since;
            let bw = (p.bytes_count as f32) / p.dur.as_secs_f32();
            trace!(
                "bytes_count {} pkt_count {} dt {dt:?} bw {bw}, since before {:?}",
                p.bytes_count,
                p.pkg_count,
                p.since.elapsed()
            );

            let (p_min, p_min_at) = p.rtt.get_min_rtt_with_timestamp();
            (bw, p_min, p_min_at)
        };

        let reduce_f = |acc: (f32, Duration, Instant), this: (f32, Duration, Instant)| {
            let (bw, p_min, p_min_at) = this;
            if p_min < acc.1 {
                (acc.0.max(bw), p_min, p_min_at)
            } else {
                (acc.0.max(bw), acc.1, acc.2)
            }
        };

        let (min_rtt, min_rtt_at) = self.rtt.get_min_rtt_with_timestamp();
        self.reduce_periods_within_interval(back_interval, map_f, reduce_f)
            .unwrap_or((0.0, min_rtt, min_rtt_at))
    }

    fn reduce_periods_within_interval<T, MF, RF>(
        &self,
        back_interval: Duration,
        mut map_f: MF,
        mut reduce_f: RF,
    ) -> Option<T>
    where
        MF: FnMut(Instant /* begin */, Instant /* end */, &Period) -> T,
        RF: FnMut(T, T) -> T,
    {
        let mut slot_id = self.head;

        let begin_time = Instant::now() - back_interval;
        let mut acc = {
            let slot = &self.circular[slot_id];

            let end_time = slot.since + slot.dur;
            if begin_time <= slot.since {
                map_f(slot.since, end_time, slot)
            } else if begin_time < end_time {
                map_f(begin_time, end_time, slot)
            } else {
                return None;
            }
        };

        loop {
            if slot_id == 0 {
                slot_id = SLOT_SIZE - 1;
            } else {
                slot_id -= 1;
            }

            // have come all the way around
            if slot_id == self.head {
                break;
            }
            let slot = &self.circular[slot_id];

            let end_time = slot.since + slot.dur;
            if begin_time <= slot.since {
                let t = map_f(slot.since, end_time, slot);
                // querying range covers entire slot
                acc = reduce_f(acc, t);
            } else if begin_time < end_time {
                // querying range covers part of this slot's time range
                let t = map_f(begin_time, end_time, slot);
                acc = reduce_f(acc, t);
                break;
            }
        }

        Some(acc)
    }

    /// returns how many bytes received in back_interval, and min RTT
    pub fn count_bytes_within_period(&self, back_interval: Duration) -> (usize, Duration) {
        let map_f = |begin: Instant, end: Instant, p: &Period| {
            let ratio = if begin <= p.since {
                1.0
            } else {
                (end - begin).div_duration_f32(end - p.since)
            };
            let (min_rtt, _) = p.rtt.get_min_rtt_with_timestamp();
            ((ratio * (p.bytes_count as f32)) as usize, min_rtt)
        };

        let reduce_f = |acc: (usize, Duration), this: (usize, Duration)| -> (usize, Duration) {
            let (n, min_rtt) = this;
            (acc.0 + n, acc.1.min(min_rtt))
        };
        self.reduce_periods_within_interval(back_interval, map_f, reduce_f)
            .unwrap_or((0, Duration::from_secs(2)))
    }

    pub fn get_rtt(&self) -> Duration {
        self.rtt.get_rtt()
    }

    pub fn get_rtt_count(&self) -> usize {
        self.rtt.get_count()
    }

    pub fn reset_var(&mut self) {
        self.rtt.reset_var();
    }

    pub fn get_var(&self) -> Duration {
        self.rtt.get_variation()
    }

    pub fn get_rtt_4var(&self) -> Duration {
        self.get_var() * 4 + self.get_rtt()
    }

    /// compute current slot duration based on RTT estimator with clamping
    fn current_slot_duration(&self) -> Duration {
        let est = self.rtt.get_rtt();
        let min = Duration::from_millis(50);
        let max = Duration::from_millis(500);
        est.min(max).max(min)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::advance;

    #[test]
    fn test_single_slots() {
        let mut bw = Bandwidth::<8>::new();
        bw.add_sample(5, None, None);
        bw.add_sample(9, None, None);
        assert_eq!(
            bw.count_bytes_within_period(Duration::from_millis(10)).0,
            14
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_multi_slots() {
        let mut bw = Bandwidth::<8>::new();
        bw.add_sample(10, Some(Duration::from_millis(40)), None);
        bw.add_sample(5, Some(Duration::from_millis(40)), None);
        advance(Duration::from_millis(50)).await;
        bw.add_sample(10, Some(Duration::from_millis(40)), None);
        advance(Duration::from_millis(30)).await;
        assert_eq!(
            bw.count_bytes_within_period(Duration::from_millis(1000)).0,
            25
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_circle() {
        let mut bw = Bandwidth::<4>::new();
        for _ in 0..100 {
            bw.add_sample(10, Some(Duration::from_millis(40)), None);
        }
        advance(Duration::from_millis(5000)).await;

        bw.add_sample(10, Some(Duration::from_millis(40)), None); // slot 0
        bw.add_sample(5, Some(Duration::from_millis(40)), None); // slot 0
        advance(Duration::from_millis(600)).await;
        bw.add_sample(10, Some(Duration::from_millis(40)), None); // slot 1
        advance(Duration::from_millis(600)).await;
        bw.add_sample(20, Some(Duration::from_millis(40)), None); // slot 2
        advance(Duration::from_millis(600)).await;
        bw.add_sample(30, Some(Duration::from_millis(40)), None); // slot 3
        advance(Duration::from_millis(600)).await;
        bw.add_sample(30, Some(Duration::from_millis(40)), None); // slot 0
        assert_eq!(
            bw.count_bytes_within_period(Duration::from_millis(1550)).0,
            85
        );
    }

    #[test]
    fn perf_add_sample_and_query() {
        // Benchmark: add_sample + count_avg_bw_in + count_bytes_within_period + get_rtt
        // This simulates what handle_blocks_received does on every call.
        const ITERATIONS: usize = 10_000;

        let mut bw = Bandwidth::<50>::new();
        // Warm up with some data
        for _ in 0..100 {
            bw.add_sample(16384, Some(Duration::from_millis(300)), Some(100));
        }

        let start = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            bw.add_sample(16384, Some(Duration::from_millis(300)), Some(100));
            let _ = std::hint::black_box(bw.count_avg_bw_in(Duration::from_millis(1500)));
            let _ = std::hint::black_box(bw.count_bytes_within_period(Duration::from_secs(10)));
            let _ = std::hint::black_box(bw.get_rtt());
            let _ = std::hint::black_box(bw.get_rtt_4var());
            let _ = std::hint::black_box(bw.count_max_bw_and_min_rtt(Duration::from_millis(1500)));
        }
        let elapsed = start.elapsed();
        let per_iter = elapsed / ITERATIONS as u32;

        eprintln!(
            "perf_add_sample_and_query: {} iterations, total {:?}, per iter {:?}",
            ITERATIONS, elapsed, per_iter
        );
        assert!(
            per_iter < std::time::Duration::from_micros(50),
            "bandwidth add_sample+query too slow: {:?}/iter",
            per_iter
        );
    }
}

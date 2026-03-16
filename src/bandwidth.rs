use tokio::time::{Duration, Instant};

mod regression;
mod rtt;
pub use regression::SlidingWindowRegression;
pub use rtt::{ALPHA, BETA, RTT};
use tracing::{info, trace};

#[derive(Debug, Clone)]
pub(crate) struct Bandwidth<const SLOT_SIZE: usize> {
    circular: [Period; SLOT_SIZE],
    head: usize,

    rtt: RTT,
    tendency: SlidingWindowRegression,
}

#[derive(Debug, Copy, Clone)]
struct Period {
    /// packet number
    pkg_count: u32,

    /// bytes number
    bytes_count: usize,

    /// time since when
    since: Instant,

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
    const SPLIT_DURATION: Duration = Duration::from_millis(500);

    pub fn new() -> Bandwidth<SLOT_SIZE> {
        Bandwidth {
            circular: [Period::new(); SLOT_SIZE],
            head: 0,

            tendency: SlidingWindowRegression::new(10),
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

        // alloc a new slot if time of rtt has passed
        if self.circular[self.head].since.elapsed() > Self::SPLIT_DURATION {
            if self.head + 1 >= SLOT_SIZE {
                self.head = 0;
            } else {
                self.head += 1;
            }
            self.circular[self.head] = Period::new();
        }

        info!("rtt: {rtt:?}");
        let rtt = rtt.unwrap_or(before_rtt);
        let n_in_flight = n_in_flight.unwrap_or(2);

        self.circular[self.head].add(n_bytes, 1, rtt);
        info!(
            "tendency add sample {rtt:?}, period: {:?}, elapsed {:?}",
            self.circular[self.head],
            self.circular[self.head].since.elapsed()
        );
        self.tendency.add(n_in_flight as f64, rtt.as_secs_f64());
    }

    pub fn get_rtt_slope_and_correlation(&self) -> (f64, f64) {
        self.tendency.get_results()
    }

    pub fn shrink_reset_slope(&mut self, n: usize) {
        self.tendency.shrink_to(n);
    }

    pub fn get_rtt_n_points(&self) -> usize {
        self.tendency.n_points()
    }

    pub fn get_optimum_in_flight(&self) -> usize {
        // try to get the max in flight that
        // rtt < 1.2 * min_rtt
        let min_rtt = self
            .tendency
            .datapoints()
            .iter()
            .map(|(_, rtt)| *rtt)
            .fold(f64::MAX / 2.0, |a, x| a.min(x));
        self.tendency
            .datapoints()
            .iter()
            .filter_map(|(n, rtt)| (*rtt < min_rtt * 1.2).then_some(*n))
            .fold(0f64, |a, n| {
                if n > a {
                    dbg!(n);
                }
                n.max(a)
            }) as usize
    }

    pub fn count_max_bw_and_min_rtt(&self, back_interval: Duration) -> (f32, Duration) {
        let f = |acc: (f32, Duration), _begin: Instant, end: Instant, p: &Period| {
            let dt = end - p.since;
            if dt >= Self::SPLIT_DURATION {
                let bw = (p.bytes_count as f32) / Self::SPLIT_DURATION.as_secs_f32();
                trace!(
                    "bytes_count {} dt {dt:?} bw {bw}, since before {:?}",
                    p.bytes_count,
                    p.since.elapsed()
                );
                // only count slots that dt are large enough slots to avoid division
                // by near-zero duration and resulting large bandwidth
                (acc.0.max(bw), acc.1.min(p.rtt.get_min_rtt()))
            } else {
                (acc.0, acc.1.min(p.rtt.get_min_rtt()))
            }
        };
        self.fold_periods_within_interval(back_interval, (0.0, Duration::MAX / 30), f)
    }

    fn fold_periods_within_interval<T, F>(&self, back_interval: Duration, init: T, mut f: F) -> T
    where
        F: FnMut(T, Instant /* begin */, Instant /* end */, &Period) -> T,
    {
        let mut slot_id = self.head;

        let mut end_time = Instant::now();
        let now = Instant::now();
        let begin_time = now - back_interval;
        let mut t = init;
        loop {
            let slot = &self.circular[slot_id];

            if begin_time <= slot.since {
                // querying range covers entire slot
                if slot.pkg_count > 0 {
                    t = f(t, slot.since, end_time, slot);
                }
            } else if begin_time < end_time {
                // querying range covers part of this slot's time range
                if slot.pkg_count > 0 {
                    t = f(t, begin_time, end_time, slot);
                }
                break;
            }

            end_time = slot.since;
            if slot_id == 0 {
                slot_id = SLOT_SIZE - 1;
            } else {
                slot_id -= 1;
            }

            // have come all the way around
            if slot_id == self.head {
                break;
            }
        }

        t
    }

    /// returns how many bytes received in back_interval, and average RTT
    pub fn count_bytes_within_period(&self, back_interval: Duration) -> (usize, Duration) {
        let f = |acc: (usize, Duration), begin: Instant, end: Instant, p: &Period| {
            let ratio = if begin <= p.since {
                1.0
            } else {
                (end - begin).div_duration_f32(end - p.since)
            };

            (
                acc.0 + (ratio * (p.bytes_count as f32)) as usize,
                acc.1.min(p.rtt.get_min_rtt()),
            )
        };
        self.fold_periods_within_interval(back_interval, (0, Duration::MAX), f)
    }

    pub fn get_rtt(&self) -> Duration {
        self.rtt.get_rtt()
    }

    pub fn get_var(&self) -> Duration {
        self.rtt.get_variation()
    }

    pub fn get_rtt_4var(&self) -> Duration {
        self.get_var() * 4 + self.get_rtt()
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
        advance(Duration::from_millis(500)).await;
        bw.add_sample(10, Some(Duration::from_millis(40)), None); // slot 1
        advance(Duration::from_millis(500)).await;
        bw.add_sample(20, Some(Duration::from_millis(40)), None); // slot 2
        advance(Duration::from_millis(500)).await;
        bw.add_sample(30, Some(Duration::from_millis(40)), None); // slot 3
        advance(Duration::from_millis(500)).await;
        bw.add_sample(30, Some(Duration::from_millis(40)), None); // slot 0
        assert_eq!(
            bw.count_bytes_within_period(Duration::from_millis(1250)).0,
            85
        );
    }
}

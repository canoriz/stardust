use tokio::time::{Duration, Instant};

mod rtt;
pub use rtt::{ALPHA, BETA, RTT};
use tracing::info;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Bandwidth<const SLOT_SIZE: usize> {
    circular: [Period; SLOT_SIZE],
    head: usize,
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
    fn new(init_rtt: Duration, init_var: Duration) -> Self {
        Self {
            pkg_count: 0,
            bytes_count: 0,
            since: Instant::now(),
            rtt: RTT::new(ALPHA, BETA, init_rtt, init_var),
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
            circular: [Period::new(Duration::from_secs(1), Duration::from_secs(0)); SLOT_SIZE],
            head: 0,
        }
    }

    /// updates bandwidth status
    /// how many new bytes received
    /// if given rtt, use this rtt
    /// if not given, will use an average rtt
    pub fn add_sample(&mut self, n_bytes: usize, rtt: Option<Duration>) {
        let before_rtt = self.circular[self.head].rtt.get_rtt();
        let before_var = self.circular[self.head].rtt.get_variation();
        let split_rtt = before_rtt.max(Duration::from_millis(100));

        // alloc a new slot if time of rtt has passed
        if self.circular[self.head].since.elapsed() > split_rtt {
            if self.head + 1 >= SLOT_SIZE {
                self.head = 0;
            } else {
                self.head += 1;
            }
            self.circular[self.head] = Period::new(before_rtt, before_var);
        }

        self.circular[self.head].add(n_bytes, 1, rtt.unwrap_or(before_rtt));
    }

    pub fn count_max_bw_and_min_rtt(&self, back_interval: Duration) -> (f32, Duration) {
        let f = |acc: (f32, Duration), _begin: Instant, end: Instant, p: &Period| {
            let dt = end - p.since;
            let bw = (p.bytes_count as f32) / dt.as_secs_f32();
            (acc.0.max(bw), acc.1.min(p.rtt.get_min_rtt()))
        };
        self.fold_periods_within_interval(back_interval, (0.0, Duration::MAX), f)
    }

    fn fold_periods_within_interval<T, F>(&self, back_interval: Duration, init: T, mut f: F) -> T
    where
        F: FnMut(T, Instant /* begin */, Instant /* end */, &Period) -> T,
    {
        let mut slot_id = self.head;

        let mut end_time = Instant::now();
        let mut min_rtt = Duration::MAX;
        let now = Instant::now();
        let begin_time = now - back_interval;
        let mut t = init;
        loop {
            let slot = &self.circular[slot_id];

            min_rtt = min_rtt.min(slot.rtt.get_min_rtt());
            if begin_time <= slot.since {
                // querying range covers entire slot
                t = f(t, slot.since, end_time, slot);
            } else if begin_time < end_time {
                // querying range covers part of this slot's time range
                t = f(t, begin_time, end_time, slot);
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

    pub fn get_rtt_4var(&self) -> Duration {
        let f = |acc: (u32, Duration), _begin: Instant, _: Instant, p: &Period| {
            let n = acc.0;
            info!(
                "rtt: {:?} var: {:?}",
                p.rtt.get_rtt(),
                p.rtt.get_variation()
            );

            if p.bytes_count > 0 {
                (
                    n + 1,
                    acc.1.mul_f32(((n - 1) as f32) / (n as f32))
                        + (p.rtt.get_rtt() + 4 * p.rtt.get_variation()).mul_f32(1.0 / (n as f32)),
                )
            } else {
                acc
            }
        };
        self.fold_periods_within_interval(Duration::from_secs(10), (1, Duration::from_secs(1)), f)
            .1
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::advance;

    #[test]
    fn test_single_slots() {
        let mut bw = Bandwidth::<8>::new();
        bw.add_sample(5, None);
        bw.add_sample(9, None);
        assert_eq!(
            bw.count_bytes_within_period(Duration::from_millis(10)).0,
            14
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_multi_slots() {
        let mut bw = Bandwidth::<8>::new();
        bw.add_sample(10, Some(Duration::from_millis(40)));
        bw.add_sample(5, Some(Duration::from_millis(40)));
        advance(Duration::from_millis(50)).await;
        bw.add_sample(10, Some(Duration::from_millis(40)));
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
            bw.add_sample(10, Some(Duration::from_millis(40)));
        }
        advance(Duration::from_millis(5000)).await;

        bw.add_sample(10, Some(Duration::from_millis(40))); // slot 0
        bw.add_sample(5, Some(Duration::from_millis(40))); // slot 0
        advance(Duration::from_millis(500)).await;
        bw.add_sample(10, Some(Duration::from_millis(40))); // slot 1
        advance(Duration::from_millis(500)).await;
        bw.add_sample(20, Some(Duration::from_millis(40))); // slot 2
        advance(Duration::from_millis(500)).await;
        bw.add_sample(30, Some(Duration::from_millis(40))); // slot 3
        advance(Duration::from_millis(500)).await;
        bw.add_sample(30, Some(Duration::from_millis(40))); // slot 0
        assert_eq!(
            bw.count_bytes_within_period(Duration::from_millis(1250)).0,
            85
        );
    }
}

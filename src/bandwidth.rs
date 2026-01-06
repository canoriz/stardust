use std::time::{self, Duration, Instant};

mod rtt;
pub use rtt::{ALPHA, BETA, RTT};

#[derive(Debug, Copy, Clone)]
pub(crate) struct Bandwidth<const SLOT_SIZE: usize> {
    circular: [Period; SLOT_SIZE],
    interval: Duration,
    head: usize,
}

#[derive(Debug, Copy, Clone)]
struct Period {
    /// packet number
    count: u32,

    /// bytes number
    val: usize,

    /// time since when
    time: Instant,

    /// average response time
    avg_rtt: time::Duration,
}

impl Default for Period {
    fn default() -> Self {
        Self {
            count: 0,
            val: 0,
            time: Instant::now(),
            avg_rtt: time::Duration::from_secs(0),
        }
    }
}

impl Period {
    fn add(&mut self, size: usize, n_packet: u32, rtt: Duration) {
        self.val += size;
        let total_time = self.avg_rtt * self.count + rtt * n_packet;
        self.count += n_packet;
        self.avg_rtt = total_time / self.count;
    }
}

impl<const SLOT_SIZE: usize> Bandwidth<SLOT_SIZE> {
    pub fn new(interval: time::Duration) -> Bandwidth<SLOT_SIZE> {
        Bandwidth {
            circular: [Period::default(); SLOT_SIZE],
            interval,
            head: 0,
        }
    }

    /// updates bandwidth status
    /// how many new bytes received
    pub fn add(&mut self, size: usize) {
        self.add_with_rtt(size, 1, Duration::from_secs(1));
    }

    /// how many new bytes received within a round trip time of rtt
    pub fn add_with_rtt(&mut self, size: usize, n: u32, rtt: Duration) {
        if self.circular[self.head].time.elapsed() > self.interval {
            if self.head + 1 >= SLOT_SIZE {
                self.head = 0;
            } else {
                self.head += 1;
            }
            self.circular[self.head] = Period::default();
        }
        self.circular[self.head].add(size, n, rtt);
    }

    pub fn count(&self, back_interval: Duration) -> usize {
        self.count_within_period(back_interval).0
    }

    /// returns how many bytes received in back_interval, and average RTT
    pub fn count_within_period(&self, back_interval: Duration) -> (usize, Duration) {
        let mut total = 0usize;
        let mut slot_id = self.head;
        let mut total_dur = Duration::from_secs(0);
        loop {
            let slot = &self.circular[slot_id];

            let now = Instant::now();
            let time_elapsed = slot.time.elapsed();
            if time_elapsed <= back_interval {
                // querying range covers entire slot
                total += slot.val;
                total_dur += slot.avg_rtt * slot.count;
            } else if now.duration_since(slot.time + self.interval) <= back_interval {
                // querying range covers part of this slot's time range
                let ratio = 1.0
                    - (now - back_interval)
                        .duration_since(slot.time)
                        .div_duration_f32(self.interval);

                total += (ratio * (slot.val as f32)) as usize;
                let slot_cnt = ratio * slot.count as f32;
                total_dur += slot.avg_rtt.mul_f32(slot_cnt);
                break;
            }

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

        let avg_rtt = if total == 0 {
            Duration::from_secs(1)
        } else {
            total_dur / total as u32
        };
        (total, avg_rtt)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_single_slots() {
        let mut bw = Bandwidth::<8>::new(Duration::from_millis(4));
        bw.add(5);
        bw.add(9);
        assert_eq!(bw.count(Duration::from_millis(10)), 14);
    }

    #[test]
    fn test_multi_slots() {
        let mut bw = Bandwidth::<8>::new(Duration::from_millis(4));
        bw.add(10);
        bw.add(5);
        sleep(Duration::from_millis(5));
        bw.add(10);
        sleep(Duration::from_millis(3));
        assert_eq!(bw.count(Duration::from_millis(100)), 25);
    }

    #[test]
    fn test_circle() {
        let mut bw = Bandwidth::<4>::new(Duration::from_millis(4));

        bw.add(10); // slot 0
        bw.add(5);
        sleep(Duration::from_millis(5));
        bw.add(10); // slot 1
        sleep(Duration::from_millis(5));
        bw.add(20); // slot 2
        sleep(Duration::from_millis(5));
        bw.add(30); // slot 3
        sleep(Duration::from_millis(5));
        bw.add(10); // slot 0
        assert_eq!(bw.count(Duration::from_millis(100)), 70);
    }
}

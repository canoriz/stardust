use tokio::time::{Duration, Instant};

pub const ALPHA: f32 = 0.125;
pub const BETA: f32 = 0.25;

#[derive(Copy, Clone, Debug)]
pub struct RTT {
    /// total sample count
    count: usize,

    /// smoothed average RTT
    smooth_rtt: Duration,

    /// average rtt variation
    rtt_var: Duration,

    /// alpha
    a: f32,

    /// beta
    b: f32,

    /// historical minimum rtt
    min_rtt: Duration,

    /// timestamp when historical minimum rtt is observed
    min_rtt_at: Instant,
}

impl RTT {
    pub fn new(alpha: f32, beta: f32) -> Self {
        let now = Instant::now();
        Self {
            count: 0,
            smooth_rtt: Duration::from_secs(6),
            rtt_var: Duration::from_secs(0),
            a: alpha,
            b: beta,
            min_rtt: Duration::from_secs(6),
            min_rtt_at: now,
        }
    }

    /// get smoothed average RTT
    pub fn get_rtt(&self) -> Duration {
        self.smooth_rtt
    }

    /// get historical min rtt with timestamp when it was observed
    pub fn get_min_rtt_with_timestamp(&self) -> (Duration, Instant) {
        (self.min_rtt, self.min_rtt_at)
    }

    /// get variation of RTT
    pub fn get_variation(&self) -> Duration {
        self.rtt_var
    }

    pub fn get_count(&self) -> usize {
        self.count
    }

    pub fn reset(&mut self) {
        *self = Self::new(self.a, self.b);
    }

    /// add a new sample of RTT
    pub fn add_rtt_sample(&mut self, rtt: Duration) {
        if self.count == 0 || rtt < self.min_rtt {
            self.min_rtt = rtt;
            self.min_rtt_at = Instant::now();
        }
        if self.count as f32 * self.a >= 1.0 {
            self.rtt_var =
                self.rtt_var.mul_f32(1.0 - self.b) + self.smooth_rtt.abs_diff(rtt).mul_f32(self.b);
            self.smooth_rtt = self.smooth_rtt.mul_f32(1.0 - self.a) + rtt.mul_f32(self.a);
        } else {
            self.smooth_rtt =
                (self.smooth_rtt.mul_f32(self.count as f32) + rtt) / ((self.count + 1) as u32);
            self.rtt_var = (self.rtt_var.mul_f32(self.count as f32)
                + self.smooth_rtt.abs_diff(rtt))
                / ((self.count + 1) as u32);
        }
        self.count += 1;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_rtt_estimator() {
        let mut r = RTT::new(ALPHA, BETA);
        r.add_rtt_sample(Duration::from_secs(1));
        r.add_rtt_sample(Duration::from_secs(2));
        r.add_rtt_sample(Duration::from_secs(1));
        r.add_rtt_sample(Duration::from_secs(1));
        let rtt = r.get_rtt();
        println!("{rtt:?}");
        let var = r.get_variation();
        println!("{var:?}");
    }
}

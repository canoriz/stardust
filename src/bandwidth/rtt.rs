use tokio::time::Duration;

pub const ALPHA: f32 = 0.125;
pub const BETA: f32 = 0.25;

#[derive(Copy, Clone, Debug)]
pub struct RTT {
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
}

impl RTT {
    pub fn new(alpha: f32, beta: f32, init_rtt: Duration, init_var: Duration) -> Self {
        Self {
            // set start up rtt to 3 sec
            smooth_rtt: init_rtt,
            rtt_var: init_var,
            a: alpha,
            b: beta,
            min_rtt: Duration::from_secs(60),
        }
    }

    /// get smoothed average RTT
    pub fn get_rtt(&self) -> Duration {
        self.smooth_rtt.max(Duration::from_millis(10))
    }

    /// get historical min rtt
    pub fn get_min_rtt(&self) -> Duration {
        self.min_rtt
    }

    /// get variation of RTT
    pub fn get_variation(&self) -> Duration {
        self.rtt_var
    }

    /// add a new sample of RTT
    pub fn add_rtt_sample(&mut self, rtt: Duration) {
        self.rtt_var =
            self.rtt_var.mul_f32(1.0 - self.b) + self.smooth_rtt.abs_diff(rtt).mul_f32(self.b);
        self.smooth_rtt = self.smooth_rtt.mul_f32(1.0 - self.a) + rtt.mul_f32(self.a);
        self.min_rtt = self.min_rtt.min(rtt)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_rtt_estimator() {
        let mut r = RTT::new(ALPHA, BETA, Duration::from_secs(1), Duration::from_secs(1));
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

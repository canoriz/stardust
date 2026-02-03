use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct SlidingWindowRegression {
    window: VecDeque<(f64, f64)>,
    capacity: usize,
    iter_since_recalc: u64,

    // core stats
    n: usize,
    m_x: f64,
    m_y: f64,
    l_xx: f64,
    l_yy: f64,
    l_xy: f64,
}

impl SlidingWindowRegression {
    pub fn new(capacity: usize) -> Self {
        Self {
            window: VecDeque::with_capacity(capacity),
            capacity,
            iter_since_recalc: 0,
            n: 0,
            m_x: 0.0,
            m_y: 0.0,
            l_xx: 0.0,
            l_yy: 0.0,
            l_xy: 0.0,
        }
    }

    pub fn datapoints(&self) -> &VecDeque<(f64, f64)> {
        &self.window
    }

    /// add new sample, if exceeds capacity, remove old sample
    pub fn add(&mut self, x: f64, y: f64) {
        self.add_step(x, y);
        self.window.push_back((x, y));

        // re-calculate every 1000 times
        self.iter_since_recalc += 1;
        if self.iter_since_recalc >= 1000 {
            self.recalculate();
        }
    }

    /// add a sample
    fn add_step(&mut self, x: f64, y: f64) {
        self.n += 1;
        let dx = x - self.m_x;
        let dy = y - self.m_y;

        self.m_x += dx / (self.n as f64);
        self.m_y += dy / (self.n as f64);

        // (x - old_mean) * (x - new_mean)
        self.l_xx += dx * (x - self.m_x);
        self.l_yy += dy * (y - self.m_y);
        self.l_xy += dx * (y - self.m_y);
    }

    /// remove a sample
    fn remove_step(&mut self, x: f64, y: f64) {
        if self.n <= 1 {
            self.reset_stats();
            return;
        }
        let dx = x - self.m_x;
        let dy = y - self.m_y;

        self.n -= 1;
        self.m_x -= dx / (self.n as f64);
        self.m_y -= dy / (self.n as f64);

        self.l_xx -= dx * (x - self.m_x);
        self.l_yy -= dy * (y - self.m_y);
        self.l_xy -= dx * (y - self.m_y);
    }

    pub fn shrink_to(&mut self, n: usize) {
        while self.window.len() > n {
            if let Some((x, y)) = self.window.pop_front() {
                self.remove_step(x, y)
            }
        }
    }

    /// recalculate to avoid accumulated error
    fn recalculate(&mut self) {
        self.reset_stats();
        let samples: Vec<(f64, f64)> = self.window.iter().map(|(x, y)| (*x, *y)).collect();
        for (x, y) in samples {
            self.add_step(x, y);
        }
        self.iter_since_recalc = 0;
    }

    fn reset_stats(&mut self) {
        self.n = 0;
        self.m_x = 0.0;
        self.m_y = 0.0;
        self.l_xx = 0.0;
        self.l_yy = 0.0;
        self.l_xy = 0.0;
    }

    /// get slope and correlation coefficient
    pub fn get_results(&self) -> (f64, f64) {
        if self.n < 2 || self.l_xx <= 0.0 || self.l_yy <= 0.0 {
            return (0.0, 0.0);
        }

        let w = self.l_xy / self.l_xx;
        let r = self.l_xy / (self.l_xx * self.l_yy).sqrt();

        // 限制 r 在 [-1, 1] 范围内，防止浮点微小溢出
        (w, r.clamp(-1.0, 1.0))
    }

    /// get values based on all sample point
    pub fn fold<F, T>(&self, init: T, f: F) -> T
    where
        F: FnMut(T, &(f64, f64)) -> T,
    {
        self.window.iter().fold(init, f)
    }

    /// number of points in the window
    pub fn n_points(&self) -> usize {
        self.window.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 辅助函数：简单全量计算 r 和 w，用于校对
    fn direct_calculate(data: &VecDeque<(f64, f64)>) -> (f64, f64) {
        let n = data.len() as f64;
        if n < 2.0 {
            return (0.0, 0.0);
        }

        let m_x = data.iter().map(|p| p.0).sum::<f64>() / n;
        let m_y = data.iter().map(|p| p.1).sum::<f64>() / n;

        let mut l_xx = 0.0;
        let mut l_yy = 0.0;
        let mut l_xy = 0.0;

        for (x, y) in data {
            l_xx += (x - m_x) * (x - m_x);
            l_yy += (y - m_y) * (y - m_y);
            l_xy += (x - m_x) * (y - m_y);
        }

        let w = l_xy / l_xx;
        let r = l_xy / (l_xx * l_yy).sqrt();
        (w, r)
    }

    #[test]
    fn test_perfect_linear() {
        // 测试完全线性相关的情况: y = 2x + 10
        let mut reg = SlidingWindowRegression::new(10);
        for i in 0..10 {
            let x = i as f64;
            let y = 2.0 * x + 10.0;
            reg.add(x, y);
        }
        let (w, r) = reg.get_results();
        assert!((w - 2.0).abs() < 1e-10);
        assert!((r - 1.0).abs() < 1e-10); // 完全正相关
    }

    #[test]
    fn test_sliding_window_removal() {
        // 测试窗口滑动。容量为5，添加10个点，应只保留后5个
        let mut reg = SlidingWindowRegression::new(5);
        for i in 0..10 {
            reg.add(i as f64, (i * i) as f64); // y = x^2
        }

        assert_eq!(reg.window.len(), 5);

        // 验证递推结果是否与对当前窗口进行全量计算的结果一致
        let (w_rec, r_rec) = reg.get_results();
        let (w_dir, r_dir) = direct_calculate(&reg.window);

        assert!((w_rec - w_dir).abs() < 1e-10);
        assert!((r_rec - r_dir).abs() < 1e-10);
    }

    #[test]
    fn test_zero_variance() {
        // 测试 x 恒定（分母为0）的情况
        let mut reg = SlidingWindowRegression::new(10);
        for _ in 0..10 {
            reg.add(100.0, 20.0); // x 始终是 100
        }
        let (w, r) = reg.get_results();
        assert_eq!(w, 0.0);
        assert_eq!(r, 0.0);
    }

    #[test]
    fn test_recalculation_drift() {
        // 模拟一万次随机操作，强制触发多次 recalculate，检查是否发生崩坏
        let mut reg = SlidingWindowRegression::new(50);
        for i in 0..10000 {
            let x = i as f64 + rand::random::<f64>();
            let y = x * 0.5 + rand::random::<f64>();
            reg.add(x, y);
        }

        let (w_rec, r_rec) = reg.get_results();
        let (w_dir, r_dir) = direct_calculate(&reg.window);

        // 递推值与全量值在 10000 次后仍应高度一致
        assert!((w_rec - w_dir).abs() < 1e-9);
        assert!((r_rec - r_dir).abs() < 1e-9);
    }

    #[test]
    fn test_negative_correlation() {
        // 测试负相关: y = -5x
        let mut reg = SlidingWindowRegression::new(10);
        for i in 0..10 {
            reg.add(i as f64, -5.0 * i as f64);
        }
        let (w, r) = reg.get_results();
        assert!((w + 5.0).abs() < 1e-10);
        assert!((r + 1.0).abs() < 1e-10); // 完全负相关
    }
}

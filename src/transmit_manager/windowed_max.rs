//! Kathleen Nichols' windowed running min/max (Linux `lib/win_minmax.c` port):
//! O(1) tracking of the extremum value over a sliding window of `win` keys,
//! keeping only 3 candidate samples.
//!
//! Generic over the value type `T` and a comparator `C` (`&T, &T -> bool`,
//! normally `T`'s ordering). The window key is always `u64` (BtlBw: round count;
//! min RTT: wall-clock ms). `reset`/`subwin_update` only compare keys, so both
//! directions share them; only the three value comparisons flip.

use std::marker::PhantomData;

/// comparator: true if `new` should replace `cur` as the extremum
pub trait Compare<T> {
    fn better(new: &T, cur: &T) -> bool;
}

/// keeps the larger value
pub struct Max;
impl<T: PartialOrd> Compare<T> for Max {
    #[inline]
    fn better(new: &T, cur: &T) -> bool {
        new >= cur
    }
}

/// keeps the smaller value
pub struct Min;
impl<T: PartialOrd> Compare<T> for Min {
    #[inline]
    fn better(new: &T, cur: &T) -> bool {
        new <= cur
    }
}

/// one tracked candidate: window key `t` and measured value `v`
#[derive(Clone, Copy)]
struct Sample<T> {
    t: u64,
    v: T,
}

/// windowed running extremum over the last `win` keys
pub struct Window<T, C> {
    win: u64,
    s: [Sample<T>; 3],
    _c: PhantomData<C>,
}

pub type WindowMax<T> = Window<T, Max>;
pub type WindowMin<T> = Window<T, Min>;

impl<T: Copy, C: Compare<T>> Window<T, C> {
    /// `init` is the seed value, which any real sample must beat
    pub fn new(win: u64, init: T) -> Self {
        Self {
            win,
            s: [Sample { t: 0, v: init }; 3],
            _c: PhantomData,
        }
    }

    /// current window extremum (`s[0]` spans the whole window)
    pub fn get(&self) -> T {
        self.s[0].v
    }

    /// key of the current extremum sample
    pub fn key(&self) -> u64 {
        self.s[0].t
    }

    /// record sample `v` at key `t`, evicting values older than `win`; returns new extremum
    pub fn update(&mut self, t: u64, v: T) -> T {
        let val = Sample { t, v };
        if C::better(&val.v, &self.s[0].v) || t.saturating_sub(self.s[2].t) > self.win {
            return self.reset(val);
        }
        if C::better(&val.v, &self.s[1].v) {
            self.s[1] = val;
            self.s[2] = val;
        } else if C::better(&val.v, &self.s[2].v) {
            self.s[2] = val;
        }
        self.subwin_update(val)
    }

    /// new all-time extremum (or empty window): collapse all three slots onto it
    fn reset(&mut self, val: Sample<T>) -> T {
        self.s = [val; 3];
        self.s[0].v
    }

    /// age out `s[0]` when its sample leaves the window; spread `s[1]`/`s[2]`
    /// across the sub-windows after `win/4` and `win/2`. the shift runs at most
    /// twice: the top-level reset guard keeps `s[2]` in-window, so two promotions
    /// always land an in-window sample in `s[0]`.
    fn subwin_update(&mut self, val: Sample<T>) -> T {
        let dt = val.t.saturating_sub(self.s[0].t);
        if dt > self.win {
            self.s[0] = self.s[1];
            self.s[1] = self.s[2];
            self.s[2] = val;
            if val.t.saturating_sub(self.s[0].t) > self.win {
                self.s[0] = self.s[1];
                self.s[1] = self.s[2];
                self.s[2] = val;
            }
        } else if self.s[1].t == self.s[0].t && dt > self.win / 4 {
            self.s[1] = val;
            self.s[2] = val;
        } else if self.s[2].t == self.s[1].t && dt > self.win / 2 {
            self.s[2] = val;
        }
        self.s[0].v
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn max_new_peak_resets() {
        let mut w = WindowMax::<f32>::new(10, 0.0);
        assert_eq!(w.update(1, 5.0), 5.0);
        assert_eq!(w.update(2, 8.0), 8.0);
        // smaller samples do not lower the max while the peak is in window
        assert_eq!(w.update(3, 6.0), 8.0);
        assert_eq!(w.update(4, 3.0), 8.0);
    }

    #[test]
    fn max_peak_ages_out() {
        let mut w = WindowMax::<f32>::new(10, 0.0);
        w.update(1, 5.0);
        w.update(2, 8.0); // peak at round 2
        for t in 3..=5 {
            w.update(t, 4.0);
        }
        for t in 6..=12 {
            w.update(t, 2.0);
        }
        assert_eq!(w.get(), 8.0);
        // round 13: age(2) = 11 > win(10), peak evicted, s[1]=4 promotes
        assert_eq!(w.update(13, 2.0), 4.0);
    }

    #[test]
    fn min_trough_ages_out() {
        let mut w = WindowMin::<f32>::new(10, f32::INFINITY);
        assert_eq!(w.update(1, 5.0), 5.0);
        assert_eq!(w.update(2, 2.0), 2.0); // trough at round 2
        assert_eq!(w.update(3, 4.0), 2.0); // larger samples do not raise the min
        for t in 4..=12 {
            w.update(t, 4.0);
        }
        assert_eq!(w.get(), 2.0);
        // round 13: trough evicted, min rises to 4
        assert_eq!(w.update(13, 4.0), 4.0);
    }
}

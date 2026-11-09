use std::{cmp::Ordering, collections::HashMap};

use crate::{
    protocol::Request,
    util::{WindowMin, WindowTimeMax},
};
use tokio::time;
use tracing::info;

/// BtlBw filter window, in BBR rounds (≈ RTTs)
const BTLBW_WIN_ROUNDS: u64 = 16;

/// RTprop (min RTT) filter window, in wall-clock ms
const MIN_RTT_WIN_MS: u64 = 10_000;

/// Prefer higher delivery rates, then smaller inflight counts at equal rates.
#[derive(Clone, Copy, Debug)]
struct BtlBwSample {
    rate: f32,
    inflight: usize,
}

impl Ord for BtlBwSample {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rate
            .total_cmp(&other.rate)
            .then_with(|| other.inflight.cmp(&self.inflight))
    }
}

impl PartialOrd for BtlBwSample {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for BtlBwSample {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for BtlBwSample {}

struct FlightReq {
    // BBR packet.sent_time; also the request's send instant used for RTT
    sent_time: time::Instant,
    inflight_at_pick: u32,

    // BBR per-packet delivery-rate snapshot, captured at send time
    delivered_at_send: u64,
    delivered_time_at_send: time::Instant,
    first_sent_time_at_send: time::Instant,
    app_limited_at_send: bool,
}

/// one delivery-rate sample produced on ack (`delivery_rate` is bytes/sec over `interval`)
#[derive(Debug, Clone, Copy)]
pub struct RateSample {
    pub delivery_rate: f32,
    pub is_app_limited: bool,
    pub interval: time::Duration,
    pub delivered: u64,
}

/// outcome of acking one inflight request
#[derive(Debug, Clone, Copy)]
pub struct Received {
    pub rtt: time::Duration,
    pub inflight_at_pick: u32,
    pub rate: RateSample,
    pub canceled: bool,
}

/// outcome of acking one inflight request
#[derive(Debug, Clone, Copy)]
pub struct Rejected {
    pub rtt: time::Duration,
    pub inflight_at_pick: u32,
}

/// manages states of a peer's inflight requests
pub struct Inflight {
    requested: HashMap<Request, FlightReq>,
    // Total bytes of not responded requests
    inflight_bytes: u64,
    canceled: HashMap<Request, FlightReq>,

    // an optimistic estimation of inflight
    inflight: u32,

    // BBR delivery-rate accounting (cumulative, peer-scoped)
    delivered: u64,
    // Byte-delivery bookmark for the current application-limited episode.
    app_limited_until: Option<u64>,
    delivered_time: time::Instant,
    first_sent_time: time::Instant,

    // BBR round counting: a round ends when an ack whose send-time snapshot
    // crossed `next_round_delivered` arrives (≈ 1 RTT after that send)
    next_round_delivered: u64,
    round_count: u64,
    // Send-time app-limited flag of the response that started the latest round.
    app_limited_at_round_start: bool,

    // BtlBw: windowed max of delivery-rate samples, keyed by round
    // Delivery rate and inflight at pick age as one window sample.
    btlbw: WindowTimeMax<BtlBwSample>,

    // RTprop: windowed min of RTT, keyed by ms since `anchor`
    min_rtt: WindowMin<time::Duration>,
    anchor: time::Instant,

    // false until the first real RTT sample lands; while false, `min_rtt` only
    // holds its startup sentinel, so callers must treat the value as unknown
    has_rtt_sample: bool,

    timeout: time::Duration,
}

impl Inflight {
    pub fn new(timeout: time::Duration) -> Self {
        let now = time::Instant::now();
        Self {
            requested: HashMap::new(),
            inflight_bytes: 0,
            canceled: HashMap::new(),
            inflight: 0,
            delivered: 0,
            app_limited_until: None,
            delivered_time: now,
            first_sent_time: now,
            next_round_delivered: 0,
            round_count: 0,
            app_limited_at_round_start: false,
            btlbw: WindowTimeMax::new(
                (time::Duration::from_secs(10), BTLBW_WIN_ROUNDS),
                BtlBwSample {
                    rate: 0.0,
                    inflight: 0,
                },
            ),
            min_rtt: WindowMin::new(MIN_RTT_WIN_MS, time::Duration::from_secs(50)),
            anchor: now,
            has_rtt_sample: false,
            timeout,
        }
    }

    /// Mark a supply gap; recovery is determined by delivery, not the next fill.
    /// Only active requests contribute to the boundary, not canceled history.
    pub fn set_app_limited(&mut self) {
        self.app_limited_until = Some(self.delivered.saturating_add(self.inflight_bytes));
    }

    pub fn is_app_limited(&self) -> bool {
        self.app_limited_until.is_some()
    }

    fn clear_app_limited_if_delivered(&mut self) {
        if self
            .app_limited_until
            .is_some_and(|until| self.delivered > until)
        {
            self.app_limited_until = None;
        }
    }

    fn retire_app_limited_bytes(&mut self, bytes: u32) {
        if let Some(until) = self.app_limited_until.as_mut() {
            *until = until.saturating_sub(u64::from(bytes));
        }
        self.clear_app_limited_if_delivered();
    }

    pub fn request(&mut self, req: Request) {
        let now = time::Instant::now();
        let app_limited = self.is_app_limited();
        if self.inflight == 0 {
            // pipe empty: restart the delivery interval at this send
            self.first_sent_time = now;
            self.delivered_time = now;
        }
        if let Some(old_ts) = self.requested.insert(
            req,
            FlightReq {
                sent_time: now,
                inflight_at_pick: self.inflight,
                delivered_at_send: self.delivered,
                delivered_time_at_send: self.delivered_time,
                first_sent_time_at_send: self.first_sent_time,
                app_limited_at_send: app_limited,
            },
        ) {
            info!("re-request {req:?}, old request at {:?}", old_ts.sent_time);
        } else {
            self.inflight_bytes += req.len as u64;
        }
        self.inflight += 1;
    }

    fn remove_requested(&mut self, req: &Request) -> Option<FlightReq> {
        let fr = self.requested.remove(req)?;
        self.inflight_bytes -= req.len as u64;
        Some(fr)
    }

    /// A request is cancelled by us
    pub fn cancel(&mut self, req: Request) {
        if let Some(request_ts) = self.remove_requested(&req) {
            self.retire_app_limited_bytes(req.len);
            self.canceled.insert(req, request_ts);
        }
    }

    /// A request is rejected
    pub fn reject(&mut self, req: Request) -> Option<Rejected> {
        self.inflight = self.inflight.saturating_sub(1);
        let r = self.remove_requested(&req);
        let c = self.canceled.remove(&req);
        if r.is_some() {
            self.retire_app_limited_bytes(req.len);
        }
        r.or(c).map(|fr| Rejected {
            rtt: fr.sent_time.elapsed(),
            inflight_at_pick: fr.inflight_at_pick,
        })
    }

    /// A request is timeout
    pub fn timeout(&mut self, req: Request) {
        self.inflight = self.inflight.saturating_sub(1);
        let r = self.remove_requested(&req);
        self.canceled.remove(&req);
        if r.is_some() {
            self.retire_app_limited_bytes(req.len);
        }
    }

    pub fn receive(&mut self, req: Request) -> Option<Received> {
        // the bytes were delivered over the network regardless of our bookkeeping,
        // so always count them; only the rate sample needs a FlightReq snapshot
        self.delivered += req.len as u64;
        self.delivered_time = time::Instant::now();
        self.clear_app_limited_if_delivered();

        self.inflight = self.inflight.saturating_sub(1);
        let (fr, canceled) = if let Some(fr) = self.remove_requested(&req) {
            self.canceled.remove(&req);
            (fr, false)
        } else {
            (self.canceled.remove(&req)?, true)
        };

        // advance only on the ack of a later-sent packet; an out-of-order
        // (earlier-sent) ack must not pull first_sent_time backwards
        if fr.sent_time > self.first_sent_time {
            self.first_sent_time = fr.sent_time;
        }

        // BBR round: the acked packet was sent ~1 RTT ago; if its send-time
        // snapshot crossed the bookmark, a round has elapsed
        if fr.delivered_at_send >= self.next_round_delivered {
            self.next_round_delivered = self.delivered;
            self.round_count += 1;
            self.app_limited_at_round_start = fr.app_limited_at_send;
        }

        let rate = self.rate_sample(&fr);
        // app-limited samples may raise BtlBw but never lower it
        let max_bw = self.btlbw.get().rate;
        if !rate.is_app_limited || rate.delivery_rate >= max_bw {
            self.btlbw.update(
                (self.delivered_time, self.round_count),
                BtlBwSample {
                    rate: rate.delivery_rate,
                    inflight: fr.inflight_at_pick as usize,
                },
            );
        }

        // TODO: ideal app-limited recovery — on unchoke or a new `have` (peer was
        // choked / had no data, now does), we want to jump straight back to the
        // last known BtlBw and re-fill the pipe immediately.
        // Actual behavior: recovery is BBR-native, not instant. The gate above only
        // keeps BtlBw from being lowered while app-limited; it does not restore it.
        // A choke freezes round_count (no acks -> no window aging), so a short choke
        // preserves the peak. But during a long app-limited stretch that still trickles
        // acks, round_count keeps advancing and the 10-round BtlBw window can age the
        // peak out; the rate then only climbs back via the next ProbeBW re-probe,
        // not the moment data resumes.

        let rtt = fr.sent_time.elapsed();
        let ms = self.anchor.elapsed().as_millis() as u64;
        self.min_rtt.update(ms, rtt);
        self.has_rtt_sample = true;

        Some(Received {
            rtt,
            inflight_at_pick: fr.inflight_at_pick,
            rate,
            canceled,
        })
    }

    /// current BtlBw estimate (windowed max delivery rate, bytes/sec)
    pub fn max_bw(&self) -> f32 {
        self.btlbw.get().rate
    }

    /// in-flight request count captured when the current max bandwidth appeared
    pub fn max_bw_inflight(&self) -> usize {
        self.btlbw.get().inflight
    }

    /// BBR round counter; advances ~once per RTT as acks cross the round bookmark
    pub fn round_count(&self) -> u64 {
        self.round_count
    }

    /// Whether the request whose response started the latest round was
    /// app-limited when sent; this is not an OR over the whole round.
    pub fn app_limited_at_round_start(&self) -> bool {
        self.app_limited_at_round_start
    }

    /// current RTprop estimate (windowed min RTT) and the instant that sample was
    /// taken; `None` until a real sample lands (the window still holds only its sentinel)
    pub fn min_rtt(&self) -> Option<(time::Duration, time::Instant)> {
        if !self.has_rtt_sample {
            return None;
        }
        let v = self.min_rtt.get();
        Some((
            v,
            self.anchor + time::Duration::from_millis(self.min_rtt.key()),
        ))
    }

    /// compute the delivery-rate sample for a just-acked request (read-only)
    fn rate_sample(&self, fr: &FlightReq) -> RateSample {
        let send_elapsed = fr
            .sent_time
            .saturating_duration_since(fr.first_sent_time_at_send);
        let ack_elapsed = self
            .delivered_time
            .saturating_duration_since(fr.delivered_time_at_send);
        let interval = send_elapsed.max(ack_elapsed);
        let delivered = self.delivered - fr.delivered_at_send;
        let delivery_rate = if interval > time::Duration::ZERO {
            delivered as f32 / interval.as_secs_f32()
        } else {
            0.0
        };
        RateSample {
            delivery_rate,
            is_app_limited: fr.app_limited_at_send,
            interval,
            delivered,
        }
    }

    /// giving a conservative estimation of how many request are in flight
    pub fn inflight(&self, rtt: time::Duration) -> usize {
        // TODO: remove canceled requests after some rtt_mean + 4sigma
        self.requested.len()
            + self
                .canceled
                .iter()
                .filter(|&(_, v)| {
                    v.sent_time.elapsed() < rtt.mul_f32(1.5).max(time::Duration::from_secs(20))
                })
                .count()
    }
}

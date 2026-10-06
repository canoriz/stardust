use std::collections::HashMap;

use super::windowed_max::{WindowMax, WindowMin};
use crate::protocol::Request;
use tokio::time;
use tracing::info;

/// BtlBw filter window, in BBR rounds (≈ RTTs)
const BTLBW_WIN_ROUNDS: u64 = 10;

/// RTprop (min RTT) filter window, in wall-clock ms
const MIN_RTT_WIN_MS: u64 = 10_000;

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
}

/// manages states of a peer's inflight requests
pub struct Inflight {
    requested: HashMap<Request, FlightReq>,
    canceled: HashMap<Request, FlightReq>,

    // an optimistic estimation of inflight
    inflight: u32,

    // BBR delivery-rate accounting (cumulative, peer-scoped)
    delivered: u64,
    delivered_time: time::Instant,
    first_sent_time: time::Instant,

    // BBR round counting: a round ends when an ack whose send-time snapshot
    // crossed `next_round_delivered` arrives (≈ 1 RTT after that send)
    next_round_delivered: u64,
    round_count: u64,

    // BtlBw: windowed max of delivery-rate samples, keyed by round
    btlbw: WindowMax<f32>,

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
            canceled: HashMap::new(),
            inflight: 0,
            delivered: 0,
            delivered_time: now,
            first_sent_time: now,
            next_round_delivered: 0,
            round_count: 0,
            btlbw: WindowMax::new(BTLBW_WIN_ROUNDS, 0.0),
            min_rtt: WindowMin::new(MIN_RTT_WIN_MS, time::Duration::from_secs(50)),
            anchor: now,
            has_rtt_sample: false,
            timeout,
        }
    }

    pub fn request(&mut self, req: Request, app_limited: bool) {
        let now = time::Instant::now();
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
        }
        self.inflight += 1;
    }

    /// A request is cancelled by us
    pub fn cancel(&mut self, req: Request) {
        if let Some(request_ts) = self.requested.remove(&req) {
            self.canceled.insert(req, request_ts);
        }
    }

    /// A request is rejected
    pub fn reject(&mut self, req: Request) {
        self.inflight = self.inflight.saturating_sub(1);
        self.requested.remove(&req);
        self.canceled.remove(&req);
    }

    /// A request is timeout
    pub fn timeout(&mut self, req: Request) {
        self.inflight = self.inflight.saturating_sub(1);
        self.requested.remove(&req);
        self.canceled.remove(&req);
    }

    pub fn receive(&mut self, req: Request) -> Option<Received> {
        // the bytes were delivered over the network regardless of our bookkeeping,
        // so always count them; only the rate sample needs a FlightReq snapshot
        self.delivered += req.len as u64;
        self.delivered_time = time::Instant::now();

        self.inflight = self.inflight.saturating_sub(1);
        let fr = if let Some(fr) = self.requested.remove(&req) {
            self.canceled.remove(&req);
            fr
        } else {
            self.canceled.remove(&req)?
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
        }

        let rate = self.rate_sample(&fr);
        // app-limited samples may raise BtlBw but never lower it
        if !rate.is_app_limited || rate.delivery_rate >= self.btlbw.get() {
            self.btlbw.update(self.round_count, rate.delivery_rate);
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
        })
    }

    /// current BtlBw estimate (windowed max delivery rate, bytes/sec)
    pub fn max_bw(&self) -> f32 {
        self.btlbw.get()
    }

    /// BBR round counter; advances ~once per RTT as acks cross the round bookmark
    pub fn round_count(&self) -> u64 {
        self.round_count
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

use std::collections::HashMap;

use crate::protocol::Request;
use tokio::time;
use tracing::info;

struct FlightReq {
    t: time::Instant,
    inflight_at_pick: u32,
}

/// manages states of a peer's inflight requests
pub struct Inflight {
    requested: HashMap<Request, FlightReq>,
    canceled: HashMap<Request, FlightReq>,

    // an optimistic estimation of inflight
    inflight: u32,

    timeout: time::Duration,
}

impl Inflight {
    pub fn new(timeout: time::Duration) -> Self {
        Self {
            requested: HashMap::new(),
            canceled: HashMap::new(),
            inflight: 0,
            timeout,
        }
    }

    pub fn request(&mut self, req: Request) {
        if let Some(old_ts) = self.requested.insert(
            req,
            FlightReq {
                t: time::Instant::now(),
                inflight_at_pick: self.inflight,
            },
        ) {
            info!("re-request {req:?}, old request at {:?}", old_ts.t);
        }
        self.inflight += 1;
    }

    /// A request is cancelled by us
    pub fn cancel(&mut self, req: Request) {
        if let Some(request_ts) = self.requested.remove(&req) {
            self.inflight.saturating_sub(1);
            self.canceled.insert(req, request_ts);
        }
    }

    /// A request is rejected
    pub fn reject(&mut self, req: Request) {
        if self.requested.remove(&req).is_some() {
            self.inflight -= 1;
        }
        self.canceled.remove(&req);
    }

    /// A request is timeout
    pub fn timeout(&mut self, req: Request) {
        if self.requested.remove(&req).is_some() {
            self.inflight -= 1;
        }
        self.canceled.remove(&req);
    }

    pub fn receive(&mut self, req: Request) -> Option<(time::Duration, u32)> {
        if let Some(fr) = self.requested.remove(&req) {
            // self.canceled.retain(|_, &mut v| v > ts);
            self.canceled.remove(&req);
            self.inflight.saturating_sub(1);
            Some((fr.t.elapsed(), fr.inflight_at_pick))
        } else {
            // we already decremented this req
            self.canceled
                .remove(&req)
                .map(|fr| (fr.t.elapsed(), fr.inflight_at_pick))
        }
    }

    /// giving a conservative estimation of how many request are in flight
    pub fn inflight(&self, rtt: time::Duration) -> usize {
        // TODO: remove canceled requests after some rtt_mean + 4sigma
        self.requested.len()
            + self
                .canceled
                .iter()
                .filter(|&(_, v)| v.t.elapsed() < rtt.mul_f32(1.5))
                .count()
    }
}

use std::collections::HashMap;

use tokio::time;
use tracing::info;

use crate::protocol::Request;

struct Info {
    ts: time::Instant,
    count: usize,
}

/// manages states of a peer's inflight requests
pub struct Inflight {
    requested: HashMap<Request, time::Instant>,
    canceled: HashMap<Request, time::Instant>,
    timeout: time::Duration,
}

impl Inflight {
    pub fn new(timeout: time::Duration) -> Self {
        Self {
            requested: HashMap::new(),
            canceled: HashMap::new(),
            timeout,
        }
    }

    pub fn request(&mut self, req: Request) {
        if let Some(old_ts) = self.requested.insert(req, time::Instant::now()) {
            info!("re-request {req:?}, old request at {old_ts:?}");
        }
    }

    /// A request is cancelled by us
    pub fn cancel(&mut self, req: Request) {
        if let Some(request_ts) = self.requested.remove(&req) {
            self.canceled.insert(req, request_ts);
        }
    }

    /// A request is rejected
    pub fn reject(&mut self, req: Request) {
        self.requested.remove(&req);
        self.canceled.remove(&req);
    }

    /// A request is timeout
    pub fn timeout(&mut self, req: Request) {
        self.requested.remove(&req);
        self.canceled.remove(&req);
    }

    pub fn receive(&mut self, req: Request) {
        self.canceled.remove(&req);
        if let Some(ts) = self.requested.remove(&req) {
            self.canceled.retain(|_, &mut v| v > ts);
        }
    }

    pub fn inflight(&self) -> usize {
        // TODO: remove canceled requests after some rtt_mean + 4sigma
        self.requested.len()
            + self
                .canceled
                .iter()
                .filter(|(_, &v)| v.elapsed() < time::Duration::from_secs(5))
                .count()
    }
}

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

    pub fn cancel(&mut self, req: Request, direct: bool) {
        if let Some(request_ts) = self.requested.remove(&req) {
            if !direct {
                self.canceled.insert(req, request_ts);
            }
        } else {
            info!("cancel a not requested {req:?}");
        }
    }

    pub fn receive(&mut self, req: Request) {
        self.requested.remove(&req);
        self.canceled.remove(&req);
    }

    pub fn inflight(&self) -> usize {
        self.requested.len()
    }
}

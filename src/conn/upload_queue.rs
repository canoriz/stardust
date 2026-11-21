use std::collections::VecDeque;

use tokio::time::Instant;

use crate::protocol::Request;

/// A peer request plus when the transmit manager received it; the difference to
/// the moment it is handed to the cache manager is that peer's upload RTT sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuedRequest {
    pub req: Request,
    pub recv_at: Instant,
}

/// Upload requests from one peer, held until the quota allows serving them.
#[derive(Debug, Default)]
pub struct UploadQueue {
    queue: VecDeque<QueuedRequest>,
    queued_bytes: u64,
    /// Granted but not yet spent; always below the head request's length
    /// while the queue is non-empty, and zero when it is empty.
    quota: u64,
}

impl UploadQueue {
    pub fn queued_bytes(&self) -> u64 {
        self.queued_bytes
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn push(&mut self, req: Request) {
        self.queued_bytes += req.len as u64;
        self.queue.push_back(QueuedRequest {
            req,
            recv_at: Instant::now(),
        });
    }

    /// Remove a queued request, e.g. on the peer's Cancel.
    pub fn remove(&mut self, req: &Request) -> bool {
        // TODO: optimize, don't iterate
        let Some(pos) = self.queue.iter().position(|q| q.req == *req) else {
            return false;
        };
        self.queue.remove(pos);
        self.queued_bytes -= req.len as u64;
        if self.queue.is_empty() {
            self.quota = 0;
        }
        true
    }

    pub fn add_quota(&mut self, bytes: u64) {
        self.quota += bytes;
    }

    /// Pop the oldest request if the quota covers it.
    pub fn pop_next_request(&mut self) -> Option<QueuedRequest> {
        let head = self.queue.front()?;
        if head.req.len as u64 > self.quota {
            return None;
        }
        let queued = self.queue.pop_front()?;
        self.quota -= queued.req.len as u64;
        self.queued_bytes -= queued.req.len as u64;
        if self.queue.is_empty() {
            self.quota = 0;
        }
        Some(queued)
    }

    /// Take every queued request and drop the quota.
    pub fn drain(&mut self) -> Vec<Request> {
        self.queued_bytes = 0;
        self.quota = 0;
        self.queue.drain(..).map(|q| q.req).collect()
    }
}

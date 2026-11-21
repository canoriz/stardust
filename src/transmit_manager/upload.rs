use super::*;
use rand::seq::IndexedRandom;

pub(super) const UPLOAD_SLOTS: usize = 4;
/// Upload rate limit of one torrent, bytes per second.
pub(super) const UPLOAD_RATE: u64 = 1024 * 1024;
/// Period of quota recompute and issue.
pub(super) const UPLOAD_TICK: time::Duration = time::Duration::from_millis(250);
pub(super) const RECHOKE_INTERVAL: time::Duration = time::Duration::from_secs(10);
const MAX_QUEUED_REQUESTS: usize = 256;

/// calculating how many upload quota a peer has by
/// `budget * (queued / total)`;
/// `total` is the sum of length of all peers' queued requests and must not be zero.
fn alloc_quota(budget: u64, queued: u64, total: u64) -> u64 {
    if total == 0 {
        0
    } else {
        (budget as u128 * queued as u128 / total as u128) as u64
    }
}

impl TransmitWorker {
    #[cfg(feature = "blocktrace")]
    pub(super) fn trace_upload_request(
        &self,
        peer: PeerAddr,
        request: Request,
        kind: blocktrace::EventKind,
        rtt: Option<time::Duration>,
    ) {
        let Some(conn) = self.connected_peers.get(&peer) else {
            return;
        };
        if let Some(mut ev) = conn.block_event(self.blocktrace_torrent_id, request, kind) {
            ev.bytes = Some(request.len as i64);
            ev.rtt_us = rtt.map(|d| d.as_micros() as i64);
            blocktrace::record_block_event(ev);
        }
    }

    /// Queue a request from an upload slot peer; false if it is not a slot or its queue is full.
    pub(super) fn enqueue_upload_requests(
        &mut self,
        peer: PeerAddr,
        request: &[Request],
        recv_at: time::Instant,
    ) -> bool {
        let Some(pc) = self.connected_peers.get_mut(&peer) else {
            return false;
        };
        // TODO: align with reqq limit
        if !pc.upload_enabled() || pc.upload_reqs.len() >= MAX_QUEUED_REQUESTS {
            return false;
        }
        for req in request {
            pc.upload_reqs.push(*req, recv_at);
        }
        true
    }

    /// Approve pending upload block requests according to available quota
    pub(super) fn approve_upload(&mut self, peer: PeerAddr) {
        let Some(conn) = self.connected_peers.get_mut(&peer) else {
            return;
        };
        if !conn.upload_enabled() {
            return;
        }

        let reqs: Vec<_> = std::iter::from_fn(|| conn.upload_reqs.pop_next_request()).collect();
        if reqs.is_empty() {
            return;
        }
        for queued in reqs {
            let request = queued.req;
            let ji = JointIndex::from(request);
            // Upload RTT ends at cache handoff, excluding cache reads and wire IO.
            let rtt = queued.recv_at.elapsed();
            self.request_piecebuf(ji, false);
            let conn = self.connected_peers.get_mut(&peer).expect("connected peer");
            conn.upload_bw
                .add_sample(request.len as usize, Some(rtt), None);
            conn.upload_bw.add_rtt(rtt);
            self.waiting_for_piecebuf
                .get_mut(&ji)
                .expect("just requested")
                .reads
                .push(PendingBlockRead { peer, request });
            #[cfg(feature = "blocktrace")]
            self.trace_upload_request(peer, request, blocktrace::EventKind::ApproveReq, Some(rtt));
        }
    }

    /// Split this tick's budget among slot peers by queued bytes and release what it covers.
    pub(super) fn upload_tick(&mut self) {
        if !self.upload_enabled() {
            return;
        }
        let total: u64 = self
            .connected_peers
            .values()
            .filter(|pc| pc.upload_enabled())
            .map(|pc| pc.upload_reqs.queued_bytes())
            .sum();
        if total == 0 {
            return;
        }

        let total_quota = UPLOAD_RATE * UPLOAD_TICK.as_millis() as u64 / 1000;
        let mut upload_to_peers: Vec<_> = self
            .connected_peers
            .iter_mut()
            .filter(|(_, pc)| pc.upload_enabled())
            .collect();

        // TODO: FIXME: this gives priority to peers who queued a lot requests
        for (_, conn) in &mut upload_to_peers {
            conn.upload_reqs.add_quota(alloc_quota(
                total_quota,
                conn.upload_reqs.queued_bytes(),
                total,
            ));
        }

        let peers: Vec<_> = upload_to_peers.into_iter().map(|(p, _)| *p).collect();
        for p in peers {
            self.approve_upload(p);
        }
    }

    /// Randomly pick up to `UPLOAD_SLOTS` of the peers interested in us as upload slots.
    pub(super) fn rechoke(&mut self) {
        if !self.upload_enabled() {
            return;
        }
        let all_peers: Vec<PeerAddr> = self
            .connected_peers
            .iter()
            .filter(|(_, pc)| pc.state.peer_interest_status == Some(true))
            .map(|(addr, _)| *addr)
            .collect();
        let new_selected: HashSet<PeerAddr> = all_peers
            .choose_multiple(&mut rand::rng(), UPLOAD_SLOTS)
            .copied()
            .collect();
        let dropped: Vec<PeerAddr> = self
            .connected_peers
            .iter()
            .filter(|(addr, pc)| pc.upload_enabled() && !new_selected.contains(addr))
            .map(|(addr, _)| *addr)
            .collect();
        for peer in dropped {
            self.choke_peer(peer);
        }
        for peer in new_selected {
            if let Some(pc) = self.connected_peers.get_mut(&peer)
                && !pc.upload_enabled()
            {
                pc.state.our_choke_status = Some(false);
                pc.conn.send_stream_cmd(ConnMsg::Unchoke);
                #[cfg(feature = "blocktrace")]
                pc.trace_choke_state(blocktrace::PeerStateKind::OurUnchoke);
            }
        }
    }

    fn choke_peer(&mut self, peer: PeerAddr) {
        self.reject_pending_upload(peer);
        self.reject_approved_upload(peer);
        let Some(pc) = self.connected_peers.get_mut(&peer) else {
            return;
        };
        pc.state.our_choke_status = Some(true);
        pc.conn.send_stream_cmd(ConnMsg::Choke);
        #[cfg(feature = "blocktrace")]
        pc.trace_choke_state(blocktrace::PeerStateKind::OurChoke);
    }

    /// Drop and reject all not yet approved requests from `peer`
    fn reject_pending_upload(&mut self, peer: PeerAddr) {
        let Some(pc) = self.connected_peers.get_mut(&peer) else {
            return;
        };
        for request in pc.upload_reqs.drain() {
            // TODO: optimize, don't lookup map every time
            self.maybe_reject_piece_request(peer, request);
        }
    }

    /// Drop and reject all approved but not sent requests from `peer`
    fn reject_approved_upload(&mut self, peer: PeerAddr) {
        let mut rejected = Vec::new();
        for pending in self.waiting_for_piecebuf.values_mut() {
            pending.reads.retain(|r| {
                if r.peer == peer {
                    rejected.push(r.request);
                    false
                } else {
                    true
                }
            });
        }
        for request in rejected {
            // TODO: optimize, don't lookup map every time
            self.maybe_reject_piece_request(peer, request);
        }
    }

    /// Reject all peers' unfinished(pending + approved) upload requests
    pub fn reject_all_upload_req(&mut self) {
        let peers: Vec<PeerAddr> = self.connected_peers.keys().copied().collect();
        for peer in peers {
            // TODO: optimize, don't lookup map every time
            self.reject_pending_upload(peer);
        }

        let reads: Vec<_> = self
            .waiting_for_piecebuf
            .values_mut()
            .flat_map(|pending| std::mem::take(&mut pending.reads))
            .collect();
        for PendingBlockRead { peer, request } in reads {
            self.maybe_reject_piece_request(peer, request);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::UploadQueue;

    fn req(len: u32) -> Request {
        Request {
            index: 0,
            begin: 0,
            len,
        }
    }

    #[test]
    fn split_budget_is_proportional() {
        assert_eq!(alloc_quota(100, 300, 400), 75);
        assert_eq!(alloc_quota(100, 0, 400), 0);
        assert_eq!(alloc_quota(u64::MAX, u64::MAX, u64::MAX), u64::MAX);
    }

    #[test]
    fn queue_approve_only_what_quota_covers_and_keeps_remainder() {
        let mut q = UploadQueue::default();
        q.push(req(10), time::Instant::now());
        q.push(req(10), time::Instant::now());
        q.add_quota(15);
        assert!(UploadQueue::pop_next_request(&mut q).is_some());
        assert!(UploadQueue::pop_next_request(&mut q).is_none());
        q.add_quota(5);
        assert!(UploadQueue::pop_next_request(&mut q).is_some());
        assert_eq!(q.queued_bytes(), 0);
    }
}

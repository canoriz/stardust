use std::{collections::HashMap, net::SocketAddr};

use tokio::time;
use tracing::{debug, trace};

#[cfg(feature = "metrics")]
use crate::metrics;

use crate::{
    api::InfoHash,
    conn::{BandwidthMode, Inflight, PeerAddr, inflight::Rejected},
    connection_manager::{ConnectionManagerHandle, CtrlOfSend},
    picker::{BlockRequests, PieceState},
    protocol::{PexFlag, Request},
};

pub type ChokeStatus = Option<bool>;
pub type InterestStatus = Option<bool>;
pub const CHOKE_STATUS_UNKNOWN: ChokeStatus = None;
pub const INTEREST_STATUS_UNKNOWN: InterestStatus = None;

pub const INIT_RTT_STARTUP: time::Duration = time::Duration::from_secs(50);

#[derive(Debug, PartialEq, Eq)]
pub struct PeerStatus {
    pub our_choke_status: ChokeStatus,
    pub our_interest_status: InterestStatus,
    pub peer_choke_status: ChokeStatus,
    pub peer_interest_status: InterestStatus,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
/// Stats, all in bytes
pub struct Stat {
    pub total_recv: u64,
    pub total_req: u64,
    pub total_reject: u64,
    pub total_cancel: u64,
    pub total_sent: u64,
    pub total_discard: u64,
    pub total_timeout: u64,
    pub last_recv: time::Instant,
    pub last_req: time::Instant,

    /// Creation time of this connection
    pub create_time: time::Instant,
}

impl Default for Stat {
    fn default() -> Self {
        Self {
            total_recv: Default::default(),
            total_req: Default::default(),
            total_sent: Default::default(),
            total_reject: Default::default(),
            total_cancel: Default::default(),
            total_discard: Default::default(),
            total_timeout: Default::default(),
            last_recv: time::Instant::now(),
            last_req: time::Instant::now(),

            create_time: time::Instant::now(),
        }
    }
}

pub type Bandwidth = crate::bandwidth::Bandwidth<50>;

pub type Version = u64;

pub struct PeerConn {
    pub conn: ConnectionManagerHandle,
    /// PEX state: peers we have already advertised to this peer.
    pub pex_known_peers: HashMap<SocketAddr, Option<PexFlag>>,
    pub state: PeerStatus,
    pub bitmap: Option<PieceState>,
    pub bw: Bandwidth,

    pub inflight: Inflight,
    pub bw_mode: BandwidthMode,

    pub stat: Stat,

    pub app_limited: bool,

    #[cfg(feature = "metrics")]
    pub metrics: metrics::PeerMetrics,

    /// blocktrace `peer.id` FK, resolved at connect (`None` if the sink is off).
    #[cfg(feature = "blocktrace")]
    pub blocktrace_peer_id: Option<i64>,

    /// The peer's self-reported BT id, used to ban it across reconnects.
    pub peer_bt_id: InfoHash,

    /// Id of this connection, not of the peer address. A later connection to the
    /// same address gets a new one, so a stale `PeerLeave` can be told apart.
    pub conn_id: Version,
}

impl PeerConn {
    pub fn new(
        cm: ConnectionManagerHandle,
        conn_id: Version,
        peer_bt_id: InfoHash,
        #[cfg(feature = "metrics")] info_hash: InfoHash,
        #[cfg(feature = "metrics")] peer_addr: SocketAddr,
        #[cfg(feature = "blocktrace")] blocktrace_peer_id: Option<i64>,
    ) -> Self {
        PeerConn {
            conn: cm,
            pex_known_peers: HashMap::new(),
            state: PeerStatus {
                our_choke_status: CHOKE_STATUS_UNKNOWN,
                our_interest_status: INTEREST_STATUS_UNKNOWN,
                peer_choke_status: CHOKE_STATUS_UNKNOWN,
                peer_interest_status: INTEREST_STATUS_UNKNOWN,
            },
            bitmap: None,
            bw: Bandwidth::new(),
            bw_mode: BandwidthMode::new_choked(),
            inflight: Inflight::new(time::Duration::from_secs(90)),
            stat: Default::default(),
            app_limited: false,
            peer_bt_id,

            #[cfg(feature = "metrics")]
            metrics: metrics::PeerMetrics::new(&hex::encode(info_hash), &peer_addr),

            #[cfg(feature = "blocktrace")]
            blocktrace_peer_id,
            conn_id,
        }
    }

    pub fn choke(&mut self) {
        self.state.peer_choke_status = Some(true);
        self.bw_mode = BandwidthMode::Choked;
    }

    pub fn unchoke(&mut self) {
        self.state.peer_choke_status = Some(false);
        self.app_limited = false;
        self.bw_mode = BandwidthMode::new_auto();
    }

    pub fn is_choked(&self) -> ChokeStatus {
        self.state.peer_choke_status
    }

    pub fn is_interested(&self) -> InterestStatus {
        self.state.peer_interest_status
    }

    pub fn adjust_app_limited(&mut self, underfill: bool) {
        if underfill {
            if !self.app_limited {
                self.app_limited = true;
            }
        } else if matches!(
            self.bw_mode,
            BandwidthMode::ProbeBW { .. } | BandwidthMode::Startup { .. }
        ) {
            // in slowdown mode or probeRTT mode, pipe is easily full, only reset app_limited if
            // if we are in startup|probeBW mode
            if self.app_limited {
                self.app_limited = false;
            }
        }
    }

    pub fn request_batch(&mut self, peer: PeerAddr, reqs: BlockRequests) {
        for rg in reqs.range.iter() {
            for req in rg.iter(reqs.piece_size) {
                self.request(req);
                debug!("{peer:?} pick block request {req:?}",);
            }
        }

        self.conn.send_stream_cmd(CtrlOfSend::RequestBlocks(reqs));
    }

    /// receives a block, returns rtt
    pub fn receive(&mut self, req: Request) -> Option<time::Duration> {
        self.stat.total_recv += req.len as u64;
        let Some(recv) = self.inflight.receive(req) else {
            self.bw.add_sample(req.len as usize, None, None);
            return None;
        };
        let rtt = recv.rtt;
        self.bw
            .add_sample(req.len as usize, Some(rtt), Some(recv.inflight_at_pick));

        self.bw.add_rtt(rtt);
        self.stat.last_recv = time::Instant::now();

        trace!(
            "delivery-rate sample: {:.0} B/s, delivered {} over {:?}, app_limited {}",
            recv.rate.delivery_rate,
            recv.rate.delivered,
            recv.rate.interval,
            recv.rate.is_app_limited
        );

        #[cfg(feature = "metrics")]
        self.metrics.record_response(rtt.as_secs_f64());

        self.bw_mode.receive(rtt);

        // let mean = self.bw.get_rtt();
        // let sigma = self.bw.get_var();
        Some(rtt)
    }

    pub fn request(&mut self, req: Request) {
        self.inflight.request(req, self.app_limited);
        self.stat.last_req = time::Instant::now();
        self.stat.total_req += req.len as u64;
    }

    #[inline]
    pub fn discard(&mut self, req: Request) {
        self.stat.total_discard += req.len as u64;
    }

    #[inline]
    pub fn request_timeout(&mut self, req: Request) {
        self.stat.total_timeout += req.len as u64;
        self.inflight.timeout(req)
    }

    #[inline]
    pub fn reject(&mut self, req: Request) -> Option<Rejected> {
        self.stat.total_reject += req.len as u64;
        self.inflight.reject(req)
    }

    pub fn cancel(&mut self, req: Request) {
        self.stat.total_cancel += req.len as u64;
        self.inflight.cancel(req)
    }

    pub fn max_inflight(&self) -> usize {
        const MAX_IN_FLIGHT: usize = 500;
        if self.conn.info().reqq_limit > 0 {
            self.conn.info().reqq_limit
        } else {
            MAX_IN_FLIGHT
        }
    }
}

//! Prometheus observability (only compiled with the `metrics` feature).
//!
//! Design: counters/histograms are labeled vectors (`torrent`, `peer`, `kind`,
//! `mode`) sliced in Prometheus. Hot-path code caches the resolved handles in
//! [`PeerMetrics`] at peer-connect time and only does atomic `increment`/`set`.

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;

use metrics::{
    Counter, Gauge, Histogram, counter, describe_counter, describe_gauge, describe_histogram,
    gauge, histogram,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// When false, the `peer` label collapses to a constant so long-running
/// sessions avoid per-peer series churn. Read once from `METRICS_PER_PEER`.
fn per_peer_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("METRICS_PER_PEER")
            .map(|v| !matches!(v.trim(), "0" | "false" | "off" | "no"))
            .unwrap_or(true)
    })
}

fn peer_label(peer: &SocketAddr) -> String {
    if per_peer_enabled() {
        peer.to_string()
    } else {
        "aggregate".to_string()
    }
}

/// Install the global Prometheus recorder and register metric descriptions.
/// Returns the handle used by the `/metrics` endpoint to render exposition.
pub fn install() -> PrometheusHandle {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("install prometheus recorder");

    describe_counter!("blocks_received_total", "blocks accepted from a peer");
    describe_counter!("bytes_downloaded_total", "payload bytes accepted from a peer");
    describe_counter!("blocks_duplicate_total", "blocks discarded as already-have (duplicate)");
    describe_counter!("bytes_wasted_total", "payload bytes discarded as duplicate");
    describe_counter!("requests_sent_total", "block requests sent, split by kind (new_pick/repick)");
    describe_counter!("cancels_sent_total", "cancel messages sent to peers");
    describe_histogram!("block_response_seconds", "per-block response time (RTT sample)");
    describe_gauge!("inflight_requests", "outstanding (requested, unanswered) blocks per peer");
    describe_gauge!("peer_min_rtt_seconds", "all-time minimum RTT observed for a peer");
    describe_gauge!("peer_rtt_seconds", "smoothed RTT estimate (mean) for a peer");
    describe_gauge!("peer_rtt_var_seconds", "RTT variation (sigma) of a peer's estimate");
    describe_gauge!("peers_connected", "connected peers per torrent");
    describe_gauge!("waiting_for_piecebuf", "sub-pieces waiting for their PieceBuf to load");
    describe_gauge!("pending_flushes", "in-flight spawn_blocking flush tasks");
    describe_counter!("peer_bw_mode_seconds_total", "time a peer spent in each bandwidth mode");
    describe_counter!("piece_verified_total", "piece SHA1 verifications, split by result (pass/fail)");

    spawn_process_collector();

    handle
}

/// Spawn a periodic collector for process CPU/RSS/FD (Linux-only values).
fn spawn_process_collector() {
    let collector = metrics_process::Collector::default();
    collector.describe();
    tokio::spawn(async move {
        loop {
            collector.collect();
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Cached metric handles for one peer connection. Built once at connect time
/// (label resolution happens here); hot paths only touch the stored handles.
pub struct PeerMetrics {
    blocks_received: Counter,
    bytes_downloaded: Counter,
    blocks_duplicate: Counter,
    bytes_wasted: Counter,
    cancels_sent: Counter,
    block_response: Histogram,
    inflight: Gauge,
    min_rtt: Gauge,
    rtt: Gauge,
    rtt_var: Gauge,
}

impl PeerMetrics {
    pub fn new(torrent_hex: &str, peer: &SocketAddr) -> Self {
        let t = torrent_hex.to_string();
        let p = peer_label(peer);
        Self {
            blocks_received: counter!("blocks_received_total", "torrent" => t.clone(), "peer" => p.clone()),
            bytes_downloaded: counter!("bytes_downloaded_total", "torrent" => t.clone(), "peer" => p.clone()),
            blocks_duplicate: counter!("blocks_duplicate_total", "torrent" => t.clone(), "peer" => p.clone()),
            bytes_wasted: counter!("bytes_wasted_total", "torrent" => t.clone(), "peer" => p.clone()),
            cancels_sent: counter!("cancels_sent_total", "torrent" => t.clone(), "peer" => p.clone()),
            block_response: histogram!("block_response_seconds", "torrent" => t.clone(), "peer" => p.clone()),
            inflight: gauge!("inflight_requests", "torrent" => t.clone(), "peer" => p.clone()),
            min_rtt: gauge!("peer_min_rtt_seconds", "torrent" => t.clone(), "peer" => p.clone()),
            rtt: gauge!("peer_rtt_seconds", "torrent" => t.clone(), "peer" => p.clone()),
            rtt_var: gauge!("peer_rtt_var_seconds", "torrent" => t, "peer" => p),
        }
    }

    #[inline]
    pub fn record_block_received(&self, bytes: u64) {
        self.blocks_received.increment(1);
        self.bytes_downloaded.increment(bytes);
    }

    #[inline]
    pub fn record_duplicate(&self, bytes: u64) {
        self.blocks_duplicate.increment(1);
        self.bytes_wasted.increment(bytes);
    }

    #[inline]
    pub fn record_response(&self, secs: f64) {
        self.block_response.record(secs);
    }

    #[inline]
    pub fn set_inflight(&self, n: f64) {
        self.inflight.set(n);
    }

    #[inline]
    pub fn set_min_rtt(&self, secs: f64) {
        self.min_rtt.set(secs);
    }

    #[inline]
    pub fn set_rtt(&self, secs: f64) {
        self.rtt.set(secs);
    }

    #[inline]
    pub fn set_rtt_var(&self, secs: f64) {
        self.rtt_var.set(secs);
    }

    #[inline]
    pub fn record_cancel(&self) {
        self.cancels_sent.increment(1);
    }
}

/// Emit `requests_sent_total{torrent,peer,kind,mode}` for one pick round.
/// Called once per `pick()` call (not per block), so the macro lookup is fine.
pub fn record_requests(
    torrent_hex: &str,
    peer: &SocketAddr,
    mode: &'static str,
    new_pick: u64,
    repick: u64,
) {
    let p = peer_label(peer);
    if new_pick > 0 {
        counter!("requests_sent_total", "torrent" => torrent_hex.to_string(), "peer" => p.clone(), "kind" => "new_pick", "mode" => mode)
            .increment(new_pick);
    }
    if repick > 0 {
        counter!("requests_sent_total", "torrent" => torrent_hex.to_string(), "peer" => p, "kind" => "repick", "mode" => mode)
            .increment(repick);
    }
}

/// Set per-torrent level gauges. Called once per sampler tick.
pub fn set_torrent_gauges(torrent_hex: &str, peers: f64, waiting_piecebuf: f64, pending_flushes: f64) {
    gauge!("peers_connected", "torrent" => torrent_hex.to_string()).set(peers);
    gauge!("waiting_for_piecebuf", "torrent" => torrent_hex.to_string()).set(waiting_piecebuf);
    gauge!("pending_flushes", "torrent" => torrent_hex.to_string()).set(pending_flushes);
}

/// Accumulate residency time (whole seconds) for a peer's current bandwidth
/// mode. Called once per sampler tick with the tick period in seconds.
pub fn add_bw_mode_time(torrent_hex: &str, peer: &SocketAddr, mode: &'static str, secs: u64) {
    counter!("peer_bw_mode_seconds_total", "torrent" => torrent_hex.to_string(), "peer" => peer_label(peer), "mode" => mode)
        .increment(secs);
}

/// Count one piece SHA1 verification, labeled by result. Called once per verify.
pub fn record_piece_verified(torrent_hex: &str, pass: bool) {
    let result = if pass { "pass" } else { "fail" };
    counter!("piece_verified_total", "torrent" => torrent_hex.to_string(), "result" => result).increment(1);
}

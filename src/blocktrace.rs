//! Per-block event tracing into SQLite (only compiled with `blocktrace`).
//!
//! Mirrors `metrics.rs`: hot paths call `record_*`; connect paths `.await`
//! `register_*` to resolve integer FK ids. A dedicated writer thread owns the
//! `Connection` and batches WAL transactions. The bounded channel blocks the
//! sender when full (backpressure = lossless).

use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use tokio::sync::oneshot;
use tracing::{error, info};

const CHANNEL_CAP: usize = 65536;
const BATCH_MAX: usize = 10240;
const BATCH_MS: u64 = 1000;

/// A block lifecycle event kind. The explicit discriminant IS the
/// `event_kind.id` FK stored in `block_event`.
#[derive(Clone, Copy, Debug)]
pub enum EventKind {
    Pick = 1,
    Repick = 2,
    Receive = 3,
    Duplicate = 4,
    Cancel = 5,
    Reject = 6,
    Timeout = 7,
}

impl EventKind {
    /// Variant list, used only to seed the `event_kind` lookup table.
    const ALL: [EventKind; 7] = [
        Self::Pick,
        Self::Repick,
        Self::Receive,
        Self::Duplicate,
        Self::Cancel,
        Self::Reject,
        Self::Timeout,
    ];
    pub fn id(self) -> i64 {
        self as i64
    }
    fn name(self) -> &'static str {
        match self {
            Self::Pick => "pick",
            Self::Repick => "repick",
            Self::Receive => "receive",
            Self::Duplicate => "duplicate",
            Self::Cancel => "cancel",
            Self::Reject => "reject",
            Self::Timeout => "timeout",
        }
    }
}

/// Why a block was repicked / revoked. The explicit discriminant IS the
/// `repick_reason.id` FK.
#[derive(Clone, Copy, Debug)]
pub enum RepickReason {
    Faster = 1,
    Endgame = 2,
    Timeout = 3,
    Reject = 4,
    ReceivedElsewhere = 5,
}

impl RepickReason {
    /// Variant list, used only to seed the `repick_reason` lookup table.
    const ALL: [RepickReason; 5] = [
        Self::Faster,
        Self::Endgame,
        Self::Timeout,
        Self::Reject,
        Self::ReceivedElsewhere,
    ];
    pub fn id(self) -> i64 {
        self as i64
    }
    fn name(self) -> &'static str {
        match self {
            Self::Faster => "faster",
            Self::Endgame => "endgame",
            Self::Timeout => "timeout",
            Self::Reject => "reject",
            Self::ReceivedElsewhere => "received_elsewhere",
        }
    }
}

/// A peer connection state transition. The discriminant is stored directly in
/// `peer_state_event.kind` (no lookup table; the set is tiny and fixed).
///
/// Deliberately coarse: only the connection lifecycle plus bandwidth-mode
/// transitions. Choke/unchoke and interested/uninterested are orthogonal
/// dimensions (not one mutually-exclusive state), and the stalled/choked case
/// is already expressed by a `BwMode` event carrying `BwMode::Choked`.
#[derive(Clone, Copy, Debug)]
pub enum PeerStateKind {
    Connect = 1,
    Disconnect = 2,
    BwMode = 3,
}

impl PeerStateKind {
    pub fn id(self) -> i64 {
        self as i64
    }
}

/// BBR-inspired bandwidth mode, stored in the `mode` column of
/// `peer_state_event` (kind=BwMode) and `bw_sample`.
#[derive(Clone, Copy, Debug)]
pub enum BwMode {
    Startup = 1,
    ProbeBw = 2,
    SlowDown = 3,
    ProbeRtt = 4,
    Choked = 5,
}

impl BwMode {
    pub fn id(self) -> i64 {
        self as i64
    }
}

/// One block lifecycle event. Copy; hot paths build it and hand it to the
/// writer via a single channel send. Optional columns are filled per `kind`
/// (`exp_us` at pick/repick, `rtt_us`/`bytes` at receive, etc.).
#[derive(Clone, Copy)]
pub struct BlockEvent {
    pub torrent_id: i64,
    pub peer_id: i64,
    pub piece: i64,
    pub block: i64,
    pub ts_us: i64,
    pub kind: EventKind,
    pub reason: Option<RepickReason>,
    pub n_inflight: Option<i64>,
    pub exp_us: Option<i64>,
    pub rtt_us: Option<i64>,
    pub bytes: Option<i64>,
}

impl BlockEvent {
    /// Build an event with `ts_us = now` and all optional columns unset.
    pub fn at(torrent_id: i64, peer_id: i64, piece: i64, block: i64, kind: EventKind) -> Self {
        Self {
            torrent_id,
            peer_id,
            piece,
            block,
            ts_us: now_us(),
            kind,
            reason: None,
            n_inflight: None,
            exp_us: None,
            rtt_us: None,
            bytes: None,
        }
    }
}

#[derive(Clone, Copy)]
pub struct PeerStateEvent {
    pub peer_id: i64,
    pub ts_us: i64,
    pub kind: PeerStateKind,
    pub mode: Option<BwMode>,
}

#[derive(Clone, Copy)]
pub struct BwSampleEvent {
    pub peer_id: i64,
    pub ts_us: i64,
    pub rtt_us: Option<i64>,
    pub rtt_var_us: Option<i64>,
    pub min_rtt_us: Option<i64>,
    pub inflight: Option<i64>,
    pub avg_bw: Option<f64>,
    pub max_bw: Option<f64>,
    pub mode: Option<BwMode>,
}

/// Wall-clock microseconds since the Unix epoch (used as the `ts_us` on every
/// row, so events across tables share one timeline for as-of joins).
pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

enum TraceMsg {
    RegisterTorrent {
        info_hash: [u8; 20],
        name: Option<String>,
        total_len: i64,
        piece_len: i64,
        reply: oneshot::Sender<i64>,
    },
    RegisterPeer {
        torrent_id: i64,
        bt_peer_id: [u8; 20],
        addr: String,
        reply: oneshot::Sender<i64>,
    },
    Block(BlockEvent),
    PeerState(PeerStateEvent),
    BwSample(BwSampleEvent),
    /// Flush the open transaction, close the DB, and stop the writer thread.
    Shutdown,
}

struct TraceSink {
    tx: SyncSender<TraceMsg>,
}

static SINK: OnceLock<TraceSink> = OnceLock::new();

/// Held by `app`; dropping it drains and commits the writer's final
/// transaction (RAII). Everything here is synchronous — the writer is a std
/// thread and `join()` blocks — so shutdown lives in `Drop`, not a method.
///
/// A `Shutdown` sentinel is still needed (rather than relying on sender-drop):
/// the global `SINK` holds a `SyncSender` for the whole process, so the
/// channel never closes on its own.
pub struct TraceHandle {
    join: Option<JoinHandle<()>>,
}

impl Drop for TraceHandle {
    fn drop(&mut self) {
        if let Some(s) = SINK.get() {
            let _ = s.tx.send(TraceMsg::Shutdown);
        }
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// Open the DB (WAL), create schema, seed lookup tables, and spawn the writer
/// thread. Installs the global sink so `record_*`/`register_*` become live.
pub fn install(db_path: &str) -> rusqlite::Result<TraceHandle> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(SCHEMA)?;
    for k in EventKind::ALL {
        conn.execute(
            "INSERT OR IGNORE INTO event_kind(id, name) VALUES (?1, ?2)",
            params![k.id(), k.name()],
        )?;
    }
    for r in RepickReason::ALL {
        conn.execute(
            "INSERT OR IGNORE INTO repick_reason(id, name) VALUES (?1, ?2)",
            params![r.id(), r.name()],
        )?;
    }

    let (tx, rx) = sync_channel::<TraceMsg>(CHANNEL_CAP);
    let join = std::thread::Builder::new()
        .name("blocktrace-writer".into())
        .spawn(move || writer_loop(conn, rx))
        .expect("spawn blocktrace writer");

    if SINK.set(TraceSink { tx }).is_err() {
        error!("blocktrace sink already installed");
    }
    info!("blocktrace installed at {db_path}");
    Ok(TraceHandle { join: Some(join) })
}

/// Owns the `Connection`. Data events accumulate in a batch, committed as one
/// transaction per `BATCH_MAX` events or every `BATCH_MS`. Register messages are
/// handled inline (they reply with an FK id). On `Shutdown` (or all senders
/// dropping) the partial batch is flushed before exit, so no committed-in-memory
/// event is lost on graceful shutdown.
fn writer_loop(conn: Connection, rx: Receiver<TraceMsg>) {
    let mut torrents: HashMap<[u8; 20], i64> = HashMap::new();
    let mut peers: HashMap<(i64, [u8; 20]), i64> = HashMap::new();
    let mut batch: Vec<TraceMsg> = Vec::with_capacity(BATCH_MAX);
    loop {
        match rx.recv_timeout(Duration::from_millis(BATCH_MS)) {
            Ok(msg) => match msg {
                TraceMsg::Shutdown => {
                    flush_batch(&conn, &mut batch);
                    break;
                }
                TraceMsg::RegisterTorrent { .. } | TraceMsg::RegisterPeer { .. } => {
                    handle_register(&conn, &mut torrents, &mut peers, msg);
                }
                data => {
                    batch.push(data);
                    if batch.len() >= BATCH_MAX {
                        flush_batch(&conn, &mut batch);
                    }
                }
            },
            Err(RecvTimeoutError::Timeout) => flush_batch(&conn, &mut batch),
            Err(RecvTimeoutError::Disconnected) => {
                flush_batch(&conn, &mut batch);
                break;
            }
        }
    }
}

/// Commit the accumulated data events as a single transaction, regardless of
/// count (a partial batch is committed too). No-op on empty.
fn flush_batch(conn: &Connection, batch: &mut Vec<TraceMsg>) {
    if batch.is_empty() {
        return;
    }
    let tx = match conn.unchecked_transaction() {
        Ok(tx) => tx,
        Err(e) => {
            error!("blocktrace begin txn: {e}");
            batch.clear();
            return;
        }
    };
    for msg in batch.drain(..) {
        let res = match msg {
            TraceMsg::Block(e) => insert_block(&tx, &e),
            TraceMsg::PeerState(e) => insert_peer_state(&tx, &e),
            TraceMsg::BwSample(e) => insert_bw(&tx, &e),
            _ => unreachable!("only data events are batched"),
        };
        if let Err(err) = res {
            error!("blocktrace batch insert: {err}");
        }
    }
    if let Err(e) = tx.commit() {
        error!("blocktrace commit: {e}");
    }
}

fn handle_register(
    conn: &Connection,
    torrents: &mut HashMap<[u8; 20], i64>,
    peers: &mut HashMap<(i64, [u8; 20]), i64>,
    msg: TraceMsg,
) {
    match msg {
        TraceMsg::RegisterTorrent {
            info_hash,
            name,
            total_len,
            piece_len,
            reply,
        } => {
            let id = match torrents.get(&info_hash) {
                Some(id) => Some(*id),
                None => {
                    match upsert_torrent(conn, &info_hash, name.as_deref(), total_len, piece_len) {
                        Ok(id) => {
                            torrents.insert(info_hash, id);
                            Some(id)
                        }
                        Err(e) => {
                            error!("blocktrace register torrent: {e}");
                            None
                        }
                    }
                }
            };
            // On failure, drop `reply` unsent: the caller's `rx.await` errors
            // and `register_torrent` returns None (no sentinel value).
            if let Some(id) = id {
                let _ = reply.send(id);
            }
        }
        TraceMsg::RegisterPeer {
            torrent_id,
            bt_peer_id,
            addr,
            reply,
        } => {
            let key = (torrent_id, bt_peer_id);
            let id = match peers.get(&key) {
                Some(id) => Some(*id),
                None => match upsert_peer(conn, torrent_id, &bt_peer_id, &addr) {
                    Ok(id) => {
                        peers.insert(key, id);
                        Some(id)
                    }
                    Err(e) => {
                        error!("blocktrace register peer: {e}");
                        None
                    }
                },
            };
            if let Some(id) = id {
                let _ = reply.send(id);
            }
        }
        _ => unreachable!("handle_register only receives register messages"),
    }
}

fn upsert_torrent(
    conn: &Connection,
    info_hash: &[u8; 20],
    name: Option<&str>,
    total_len: i64,
    piece_len: i64,
) -> rusqlite::Result<i64> {
    conn.query_row(
        "INSERT INTO torrent(info_hash, name, total_len, piece_len, started_at_us)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(info_hash) DO UPDATE SET name = coalesce(excluded.name, name)
         RETURNING id",
        params![hex::encode(info_hash), name, total_len, piece_len, now_us()],
        |r| r.get(0),
    )
}

fn upsert_peer(
    conn: &Connection,
    torrent_id: i64,
    bt_peer_id: &[u8; 20],
    addr: &str,
) -> rusqlite::Result<i64> {
    conn.query_row(
        "INSERT INTO peer(torrent_id, bt_peer_id, addr, first_seen_us)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(torrent_id, bt_peer_id) DO UPDATE SET addr = excluded.addr
         RETURNING id",
        params![torrent_id, hex::encode(bt_peer_id), addr, now_us()],
        |r| r.get(0),
    )
}

fn insert_block(conn: &Connection, e: &BlockEvent) -> rusqlite::Result<usize> {
    conn.prepare_cached(
        "INSERT INTO block_event
         (torrent_id, peer_id, piece, block, ts_us, kind, reason, n_inflight, exp_us, rtt_us, bytes)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )?
    .execute(params![
        e.torrent_id,
        e.peer_id,
        e.piece,
        e.block,
        e.ts_us,
        e.kind.id(),
        e.reason.map(|r| r.id()),
        e.n_inflight,
        e.exp_us,
        e.rtt_us,
        e.bytes,
    ])
}

fn insert_peer_state(conn: &Connection, e: &PeerStateEvent) -> rusqlite::Result<usize> {
    conn.prepare_cached(
        "INSERT INTO peer_state_event(peer_id, ts_us, kind, mode) VALUES (?1, ?2, ?3, ?4)",
    )?
    .execute(params![
        e.peer_id,
        e.ts_us,
        e.kind.id(),
        e.mode.map(|m| m.id())
    ])
}

fn insert_bw(conn: &Connection, e: &BwSampleEvent) -> rusqlite::Result<usize> {
    conn.prepare_cached(
        "INSERT INTO bw_sample
         (peer_id, ts_us, rtt_us, rtt_var_us, min_rtt_us, inflight, avg_bw, max_bw, mode)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?
    .execute(params![
        e.peer_id,
        e.ts_us,
        e.rtt_us,
        e.rtt_var_us,
        e.min_rtt_us,
        e.inflight,
        e.avg_bw,
        e.max_bw,
        e.mode.map(|m| m.id()),
    ])
}

/// Resolve (and cache in-DB) the FK id for a torrent. Off hot path: called
/// once per torrent at startup/registration. Returns `None` if tracing is off
/// or the write failed.
pub async fn register_torrent(
    info_hash: [u8; 20],
    name: Option<String>,
    total_len: i64,
    piece_len: i64,
) -> Option<i64> {
    let s = SINK.get()?;
    let (reply, rx) = oneshot::channel();
    s.tx.send(TraceMsg::RegisterTorrent {
        info_hash,
        name,
        total_len,
        piece_len,
        reply,
    })
    .ok()?;
    rx.await.ok()
}

/// Resolve (and cache in-DB) the FK id for a peer. Off hot path: called once
/// per peer connection. Returns `None` if tracing is off or the write failed.
pub async fn register_peer(torrent_id: i64, bt_peer_id: [u8; 20], addr: String) -> Option<i64> {
    let s = SINK.get()?;
    let (reply, rx) = oneshot::channel();
    s.tx.send(TraceMsg::RegisterPeer {
        torrent_id,
        bt_peer_id,
        addr,
        reply,
    })
    .ok()?;
    rx.await.ok()
}

/// Hot-path emit. Blocks the caller if the channel is full (backpressure =
/// lossless). No-op if tracing is off.
pub fn record_block_event(ev: BlockEvent) {
    if let Some(s) = SINK.get() {
        let _ = s.tx.send(TraceMsg::Block(ev));
    }
}

pub fn record_peer_state(ev: PeerStateEvent) {
    if let Some(s) = SINK.get() {
        let _ = s.tx.send(TraceMsg::PeerState(ev));
    }
}

pub fn record_bw_sample(ev: BwSampleEvent) {
    if let Some(s) = SINK.get() {
        let _ = s.tx.send(TraceMsg::BwSample(ev));
    }
}

const SCHEMA: &str = "
PRAGMA journal_size_limit = 8388608;
PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;

CREATE TABLE IF NOT EXISTS torrent(
  id INTEGER PRIMARY KEY,
  info_hash TEXT NOT NULL UNIQUE,
  name TEXT,
  total_len INTEGER,
  piece_len INTEGER,
  started_at_us INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS peer(
  id INTEGER PRIMARY KEY,
  torrent_id INTEGER NOT NULL REFERENCES torrent(id),
  bt_peer_id TEXT NOT NULL,
  addr TEXT NOT NULL,
  first_seen_us INTEGER NOT NULL,
  UNIQUE(torrent_id, bt_peer_id));
CREATE INDEX IF NOT EXISTS ix_peer_torrent ON peer(torrent_id);

CREATE TABLE IF NOT EXISTS event_kind(id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS repick_reason(id INTEGER PRIMARY KEY, name TEXT UNIQUE);

CREATE TABLE IF NOT EXISTS block_event(
  id INTEGER PRIMARY KEY,
  torrent_id INTEGER NOT NULL REFERENCES torrent(id),
  peer_id INTEGER NOT NULL REFERENCES peer(id),
  piece INTEGER NOT NULL,
  block INTEGER NOT NULL,
  ts_us INTEGER NOT NULL,
  kind INTEGER NOT NULL REFERENCES event_kind(id),
  reason INTEGER REFERENCES repick_reason(id),
  n_inflight INTEGER,
  exp_us INTEGER,
  rtt_us INTEGER,
  bytes INTEGER);
CREATE INDEX IF NOT EXISTS ix_be_block ON block_event(torrent_id, piece, block, ts_us);
CREATE INDEX IF NOT EXISTS ix_be_peer ON block_event(peer_id, ts_us);
CREATE INDEX IF NOT EXISTS ix_be_kind ON block_event(kind);

CREATE TABLE IF NOT EXISTS peer_state_event(
  id INTEGER PRIMARY KEY,
  peer_id INTEGER NOT NULL REFERENCES peer(id),
  ts_us INTEGER NOT NULL,
  kind INTEGER NOT NULL,
  mode INTEGER);
CREATE INDEX IF NOT EXISTS ix_pse_peer ON peer_state_event(peer_id, ts_us);

CREATE TABLE IF NOT EXISTS bw_sample(
  id INTEGER PRIMARY KEY,
  peer_id INTEGER NOT NULL REFERENCES peer(id),
  ts_us INTEGER NOT NULL,
  rtt_us INTEGER,
  rtt_var_us INTEGER,
  min_rtt_us INTEGER,
  inflight INTEGER,
  avg_bw REAL,
  max_bw REAL,
  mode INTEGER);
CREATE INDEX IF NOT EXISTS ix_bw_peer ON bw_sample(peer_id, ts_us);
";

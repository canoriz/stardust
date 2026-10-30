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
/// Longest an event may sit in the batch before it is committed.
const BATCH_MS: u64 = 2000;

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
    Kicked = 4,
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
    const ALL: [BwMode; 5] = [
        Self::Startup,
        Self::ProbeBw,
        Self::SlowDown,
        Self::ProbeRtt,
        Self::Choked,
    ];

    pub fn id(self) -> i64 {
        self as i64
    }

    fn name(self) -> &'static str {
        match self {
            Self::Startup => "Startup",
            Self::ProbeBw => "ProbeBw",
            Self::SlowDown => "SlowDown",
            Self::ProbeRtt => "ProbeRtt",
            Self::Choked => "Choked",
        }
    }
}

/// One cache-slot event kind. The explicit discriminant IS the
/// `cache_event_kind.id` FK stored in `cache_slot_event`.
#[derive(Clone, Copy, Debug)]
pub enum CacheEventKind {
    LoadBegin = 1,
    LoadEnd = 2,
    Evict = 3,
    FlushEnd = 4,
}

impl CacheEventKind {
    const ALL: [CacheEventKind; 4] = [Self::LoadBegin, Self::LoadEnd, Self::Evict, Self::FlushEnd];

    pub fn id(self) -> i64 {
        self as i64
    }

    fn name(self) -> &'static str {
        match self {
            Self::LoadBegin => "load_begin",
            Self::LoadEnd => "load_end",
            Self::Evict => "evict",
            Self::FlushEnd => "flush_end",
        }
    }
}

/// Why a cache slot was evicted / flushed. The explicit discriminant IS the
/// `cache_reason.id` FK.
#[derive(Clone, Copy, Debug)]
pub enum CacheReason {
    LeastAccessed = 1,
    CacheFlush = 2,
    LeaseFlush = 3,
    DropFlush = 4,
    Shutdown = 5,
}

impl CacheReason {
    const ALL: [CacheReason; 5] = [
        Self::LeastAccessed,
        Self::CacheFlush,
        Self::LeaseFlush,
        Self::DropFlush,
        Self::Shutdown,
    ];

    pub fn id(self) -> i64 {
        self as i64
    }

    fn name(self) -> &'static str {
        match self {
            Self::LeastAccessed => "least_accessed",
            Self::CacheFlush => "cache_flush",
            Self::LeaseFlush => "lease_flush",
            Self::DropFlush => "drop_flush",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Slot occupancy state. Clean/dirty is NOT encoded here — it is carried by the
/// raw `DIRTY`/`FLUSHING` bits, so the three columns are independent.
#[derive(Clone, Copy, Debug)]
pub enum CacheSlotState {
    Cached = 1,
    Lent = 2,
    Reading = 3,
}

impl CacheSlotState {
    const ALL: [CacheSlotState; 3] = [Self::Cached, Self::Lent, Self::Reading];

    pub fn id(self) -> i64 {
        self as i64
    }

    fn name(self) -> &'static str {
        match self {
            Self::Cached => "cached",
            Self::Lent => "lent",
            Self::Reading => "reading",
        }
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
    pub n_received: i32,
    pub inflight: Option<i64>,
    pub avg_bw: Option<f64>,
    pub max_bw: Option<f64>,
    pub app_limited: bool,
    pub mode: Option<BwMode>,
    /// Last-wait share over useful-byte share among currently connected peers;
    /// `inf` for a peer that held a sub-piece's last wait but delivered nothing.
    pub lift: Option<f64>,
    /// Slot time per block: cumulative attributed fill time / accepted block count.
    pub st_blk: Option<f64>,
}

/// One cache slot lifecycle row. `piece`/`sub` are the cache-native identity
/// (`JointIndex`); byte ranges are derived by joining `torrent`.
#[derive(Clone, Copy)]
pub struct CacheSlotEvent {
    pub torrent_id: i64,
    pub ts_us: i64,
    pub piece: i64,
    pub sub: i64,
    /// Buffer instance id; `None` on failed loads, whose buffer is consumed by the read.
    pub buf_id: Option<i64>,
    pub kind: CacheEventKind,
    pub reason: Option<CacheReason>,
    pub state: Option<CacheSlotState>,
    /// Raw `DIRTY` bit; only meaningful when `state = cached`.
    pub dirty: Option<i64>,
    /// Raw `FLUSHING` bit; only meaningful when `state = cached`.
    pub flushing: Option<i64>,
    /// `load_end` / `flush_end`: 1 ok, 0 failed.
    pub ok: Option<i64>,
    /// `flush_end`: write-back duration.
    pub dst_us: Option<i64>,
    /// `load_end`: waiters queued for the key when the read finished.
    pub n_waiters: Option<i64>,
    /// `evict`: `now - access_time` (last write).
    pub age_us: Option<i64>,
}

impl CacheSlotEvent {
    /// Build an event with `ts_us = now` and all optional columns unset.
    pub fn at(torrent_id: i64, piece: i64, sub: i64, kind: CacheEventKind) -> Self {
        Self {
            torrent_id,
            ts_us: now_us(),
            piece,
            sub,
            buf_id: None,
            kind,
            reason: None,
            state: None,
            dirty: None,
            flushing: None,
            ok: None,
            dst_us: None,
            n_waiters: None,
            age_us: None,
        }
    }
}

/// One line of the periodic full slot snapshot.
#[derive(Clone, Copy)]
pub struct CacheSlotSample {
    pub ts_us: i64,
    pub torrent_id: i64,
    pub piece: i64,
    pub sub: i64,
    pub state: CacheSlotState,
    pub dirty: Option<i64>,
    pub flushing: Option<i64>,
    pub age_us: Option<i64>,
}

/// Per-tick aggregate row, derived from the same slot scan plus the completed
/// stats window.
#[derive(Clone, Copy, Default)]
pub struct CacheSampleEvent {
    pub capacity: i64,
    pub lent: i64,
    pub reading: i64,
    pub cached_clean: i64,
    pub cached_dirty: i64,
    pub waiting_keys: i64,
    pub pending_waiters: i64,
    /// Rates are `stats.last / TIMER_INTERVAL` (pure single window), unlike
    /// `CacheStats` which blends last/current 50/50.
    pub request_rate: f64,
    pub lend_rate: f64,
    pub return_rate: f64,
    pub flush_rate: f64,
    pub cache_flush_rate: f64,
    pub evict_rate: f64,
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
        total_len: Option<i64>,
        piece_len: Option<i64>,
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
    CacheEvent(CacheSlotEvent),
    /// Aggregate row plus all slot rows of one tick, sent as a unit.
    CacheTick {
        ts_us: i64,
        sample: CacheSampleEvent,
        slots: Vec<CacheSlotSample>,
    },
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
    for r in BwMode::ALL {
        conn.execute(
            "INSERT OR IGNORE INTO bw_mode(id, name) VALUES (?1, ?2)",
            params![r.id(), r.name()],
        )?;
    }
    for k in CacheEventKind::ALL {
        conn.execute(
            "INSERT OR IGNORE INTO cache_event_kind(id, name) VALUES (?1, ?2)",
            params![k.id(), k.name()],
        )?;
    }
    for r in CacheReason::ALL {
        conn.execute(
            "INSERT OR IGNORE INTO cache_reason(id, name) VALUES (?1, ?2)",
            params![r.id(), r.name()],
        )?;
    }
    for s in CacheSlotState::ALL {
        conn.execute(
            "INSERT OR IGNORE INTO cache_slot_state(id, name) VALUES (?1, ?2)",
            params![s.id(), s.name()],
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
/// transaction per `BATCH_MAX` events or once `BATCH_MS` has passed since the
/// last flush. Register messages are
/// handled inline (they reply with an FK id). On `Shutdown` (or all senders
/// dropping) the partial batch is flushed before exit, so no committed-in-memory
/// event is lost on graceful shutdown.
fn writer_loop(conn: Connection, rx: Receiver<TraceMsg>) {
    let mut peers: HashMap<(i64, [u8; 20]), i64> = HashMap::new();
    let mut batch: Vec<TraceMsg> = Vec::with_capacity(BATCH_MAX);
    let max_wait = Duration::from_millis(BATCH_MS);
    let mut last_flush = Instant::now();
    loop {
        match rx.recv_timeout(max_wait) {
            Ok(msg) => match msg {
                TraceMsg::Shutdown => {
                    flush_batch(&conn, &mut batch);
                    break;
                }
                TraceMsg::RegisterTorrent { .. } | TraceMsg::RegisterPeer { .. } => {
                    handle_register(&conn, &mut peers, msg);
                }
                data => {
                    batch.push(data);
                    if batch.len() >= BATCH_MAX || last_flush.elapsed() >= max_wait {
                        flush_batch(&conn, &mut batch);
                        last_flush = Instant::now();
                    }
                }
            },
            Err(RecvTimeoutError::Timeout) => {
                flush_batch(&conn, &mut batch);
                last_flush = Instant::now();
            }
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
            TraceMsg::CacheEvent(e) => insert_cache_event(&tx, &e),
            TraceMsg::CacheTick {
                ts_us,
                sample,
                slots,
            } => insert_cache_tick(&tx, ts_us, &sample, &slots),
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

fn handle_register(conn: &Connection, peers: &mut HashMap<(i64, [u8; 20]), i64>, msg: TraceMsg) {
    match msg {
        TraceMsg::RegisterTorrent {
            info_hash,
            name,
            total_len,
            piece_len,
            reply,
        } => {
            // Always upsert: the row is created at add time from the info hash
            // alone and completed with len/piece_len once metadata arrives.
            match upsert_torrent(conn, &info_hash, name.as_deref(), total_len, piece_len) {
                // On failure, drop `reply` unsent: the caller's `rx.await`
                // errors and `register_torrent` returns `None`.
                Ok(id) => {
                    let _ = reply.send(id);
                }
                Err(e) => error!("blocktrace register torrent: {e}"),
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
    total_len: Option<i64>,
    piece_len: Option<i64>,
) -> rusqlite::Result<i64> {
    conn.query_row(
        "INSERT INTO torrent(info_hash, name, total_len, piece_len, started_at_us)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(info_hash) DO UPDATE SET
             name      = coalesce(excluded.name, name),
             total_len = coalesce(excluded.total_len, total_len),
             piece_len = coalesce(excluded.piece_len, piece_len)
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
         (peer_id, ts_us, rtt_us, rtt_var_us, min_rtt_us, n_received, inflight, avg_bw, max_bw, app_limited, mode, lift,
          st_blk)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
    )?
    .execute(params![
        e.peer_id,
        e.ts_us,
        e.rtt_us,
        e.rtt_var_us,
        e.min_rtt_us,
        e.n_received,
        e.inflight,
        e.avg_bw,
        e.max_bw,
        e.app_limited,
        e.mode.map(|m| m.id()),
        e.lift,
        e.st_blk,
    ])
}

fn insert_cache_event(conn: &Connection, e: &CacheSlotEvent) -> rusqlite::Result<usize> {
    conn.prepare_cached(
        "INSERT INTO cache_slot_event
         (ts_us, torrent_id, piece, sub, kind, reason, state, dirty, flushing, ok, dst_us, n_waiters, age_us, buf_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
    )?
    .execute(params![
        e.ts_us,
        e.torrent_id,
        e.piece,
        e.sub,
        e.kind.id(),
        e.reason.map(|r| r.id()),
        e.state.map(|s| s.id()),
        e.dirty,
        e.flushing,
        e.ok,
        e.dst_us,
        e.n_waiters,
        e.age_us,
        e.buf_id,
    ])
}

/// Insert one tick: the aggregate row plus every slot row, inside the caller's
/// transaction so both are committed together.
fn insert_cache_tick(
    conn: &Connection,
    ts_us: i64,
    s: &CacheSampleEvent,
    slots: &[CacheSlotSample],
) -> rusqlite::Result<usize> {
    conn.prepare_cached(
        "INSERT INTO cache_sample
         (ts_us, capacity, lent, reading, cached_clean, cached_dirty,
          waiting_keys, pending_waiters,
          request_rate, lend_rate, return_rate, flush_rate, cache_flush_rate, evict_rate)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
    )?
    .execute(params![
        ts_us,
        s.capacity,
        s.lent,
        s.reading,
        s.cached_clean,
        s.cached_dirty,
        s.waiting_keys,
        s.pending_waiters,
        s.request_rate,
        s.lend_rate,
        s.return_rate,
        s.flush_rate,
        s.cache_flush_rate,
        s.evict_rate,
    ])?;

    let mut stmt = conn.prepare_cached(
        "INSERT INTO cache_slot_sample
         (ts_us, torrent_id, piece, sub, state, dirty, flushing, age_us)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )?;
    for slot in slots {
        stmt.execute(params![
            slot.ts_us,
            slot.torrent_id,
            slot.piece,
            slot.sub,
            slot.state.id(),
            slot.dirty,
            slot.flushing,
            slot.age_us,
        ])?;
    }
    Ok(slots.len())
}

/// Resolve (and cache in-DB) the FK id for a torrent. Off hot path: called
/// when the torrent is added (info hash only, for a magnet) and again once
/// metadata arrives to fill in name/len/piece_len — same row, same id.
/// Returns `None` if tracing is off or the write failed.
pub async fn register_torrent(
    info_hash: [u8; 20],
    name: Option<String>,
    total_len: Option<i64>,
    piece_len: Option<i64>,
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

pub fn record_cache_event(ev: CacheSlotEvent) {
    if let Some(s) = SINK.get() {
        let _ = s.tx.send(TraceMsg::CacheEvent(ev));
    }
}

pub fn record_cache_tick(ts_us: i64, sample: CacheSampleEvent, slots: Vec<CacheSlotSample>) {
    if let Some(s) = SINK.get() {
        let _ = s.tx.send(TraceMsg::CacheTick {
            ts_us,
            sample,
            slots,
        });
    }
}

const SCHEMA: &str = "
PRAGMA journal_size_limit = 8388608;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

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
CREATE TABLE IF NOT EXISTS bw_mode(id INTEGER PRIMARY KEY, name TEXT UNIQUE);

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
  n_received INTEGER,
  inflight INTEGER,
  avg_bw REAL,
  max_bw REAL,
  app_limited INTEGER,
  mode INTEGER,
  lift REAL,
  st_blk REAL);
-- st_blk (slot time per block): this connection's cumulative attributed sub-piece
-- fill time / accepted block count, in slot-seconds/block. Short tails count as one
-- block; duplicates are excluded. NULL if metadata is unavailable or both values
-- are zero; positive slot time with zero accepted blocks yields infinity.
CREATE INDEX IF NOT EXISTS ix_bw_peer ON bw_sample(peer_id, ts_us);

CREATE TABLE IF NOT EXISTS cache_event_kind(id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS cache_reason(id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS cache_slot_state(id INTEGER PRIMARY KEY, name TEXT UNIQUE);

-- Cache slot events. Identity is (torrent_id, piece, sub); the info hash lives
-- once in torrent.info_hash, byte ranges are derived by joining torrent.
CREATE TABLE IF NOT EXISTS cache_slot_event(
  id INTEGER PRIMARY KEY,
  ts_us INTEGER NOT NULL,
  torrent_id INTEGER NOT NULL REFERENCES torrent(id),
  piece INTEGER NOT NULL,
  sub INTEGER NOT NULL,
  buf_id INTEGER,
  kind INTEGER NOT NULL REFERENCES cache_event_kind(id),
  reason INTEGER REFERENCES cache_reason(id),
  state INTEGER REFERENCES cache_slot_state(id),
  dirty INTEGER,
  flushing INTEGER,
  ok INTEGER,
  dst_us INTEGER,
  n_waiters INTEGER,
  age_us INTEGER);
CREATE INDEX IF NOT EXISTS ix_cse_kind ON cache_slot_event(kind, ts_us);
CREATE INDEX IF NOT EXISTS ix_cse_key ON cache_slot_event(torrent_id, piece, sub, ts_us);
CREATE INDEX IF NOT EXISTS ix_cse_buf ON cache_slot_event(buf_id, ts_us);

-- Per-tick aggregate. Derived values are deliberately not stored:
-- occupied = lent + reading + cached_clean + cached_dirty,
-- untracked = capacity - occupied, available = cached_clean + untracked.
CREATE TABLE IF NOT EXISTS cache_sample(
  ts_us INTEGER PRIMARY KEY,
  capacity INTEGER NOT NULL,
  lent INTEGER NOT NULL,
  reading INTEGER NOT NULL,
  cached_clean INTEGER NOT NULL,
  cached_dirty INTEGER NOT NULL,
  waiting_keys INTEGER NOT NULL,
  pending_waiters INTEGER NOT NULL,
  request_rate REAL,
  lend_rate REAL,
  return_rate REAL,
  flush_rate REAL,
  cache_flush_rate REAL,
  evict_rate REAL);

-- Per-tick full slot snapshot (~capacity rows per tick).
CREATE TABLE IF NOT EXISTS cache_slot_sample(
  ts_us INTEGER NOT NULL,
  torrent_id INTEGER NOT NULL REFERENCES torrent(id),
  piece INTEGER NOT NULL,
  sub INTEGER NOT NULL,
  state INTEGER NOT NULL REFERENCES cache_slot_state(id),
  dirty INTEGER,
  flushing INTEGER,
  age_us INTEGER,
  PRIMARY KEY (ts_us, torrent_id, piece, sub)) WITHOUT ROWID;
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bw_sample_contains_slot_time_per_block() {
        let c = Connection::open_in_memory().unwrap();
        init_schema(&c).unwrap();
        c.execute_batch(
            "INSERT INTO torrent VALUES (1, 'hash', 'test', 16385, 16385, 0);
             INSERT INTO peer VALUES (7, 1, 'peer', '127.0.0.1:1', 0);",
        )
        .unwrap();
        insert_bw(
            &c,
            &BwSampleEvent {
                peer_id: 7,
                ts_us: 100,
                rtt_us: Some(20),
                rtt_var_us: None,
                min_rtt_us: None,
                n_received: 2,
                inflight: None,
                avg_bw: None,
                max_bw: None,
                app_limited: false,
                mode: None,
                lift: None,
                st_blk: Some(1.5),
            },
        )
        .unwrap();
        let row: (i64, f64) = c
            .query_row("SELECT peer_id, st_blk FROM bw_sample", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(row, (7, 1.5));
    }
}

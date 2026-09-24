use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{io, time};

use bytes::BytesMut;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::{
    sync::{mpsc, oneshot},
    time::Interval,
};
use tracing::warn;

use super::MutexBackFile;
use crate::cache::simple_buffer::{
    read_from_file, FlushErr, JointIndex, PieceBuf, Pool, POOL_SIZE, SUB_PIECE_SIZE,
};
use crate::math_helper::piece_total_and_last_size;
use crate::transmit_manager::Msg as TmMsg;

pub use crate::protocol::InfoHash;

/// Identifies a sub-piece uniquely across all torrents.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Derivative)]
#[derivative(Debug)]
pub struct GlobalPieceKey {
    // TODO: make this info hash a type so we don't have to manually implement Debug
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    pub info_hash: InfoHash,
    pub index: JointIndex,
}

/// State of a piece in the cache.
enum CacheEntry {
    /// Piece is loaded into cache
    /// if it's Some, the piece is available;
    /// if None, the piece is currently lent out as a PieceLease and not available.
    Loaded(Option<PieceBuf>),
    /// File-read is in progress; further GetPiece requests are queued in pending.
    Reading,
}

/// FIFO queue of cache keys waiting for a slot, with a side map that prevents
/// the same key from being queued more than once.
#[derive(Default)]
struct WaitingSlots {
    queue: VecDeque<GlobalPieceKey>,
    keys: HashSet<GlobalPieceKey>,
}

impl WaitingSlots {
    fn push(&mut self, key: GlobalPieceKey) {
        if self.keys.insert(key) {
            self.queue.push_back(key);
        }
    }

    fn pop(&mut self) -> Option<GlobalPieceKey> {
        let key = self.queue.pop_front()?;
        self.keys.remove(&key);
        Some(key)
    }

    fn remove_torrent(&mut self, info_hash: InfoHash) {
        for k in self.queue.iter().filter(|key| key.info_hash == info_hash) {
            self.keys.remove(&k);
        }
        self.queue.retain(|key| key.info_hash != info_hash);
    }

    /// Remove and return this torrent's queued keys (in queue order). Their
    /// waiters stay parked in `CacheManager::pending`; the caller re-`push`es
    /// these keys once its file-op barrier clears.
    fn drain_torrent(&mut self, info_hash: InfoHash) -> Vec<GlobalPieceKey> {
        let mut drained = Vec::new();
        self.queue.retain(|key| {
            if key.info_hash == info_hash {
                self.keys.remove(key);
                drained.push(*key);
                false
            } else {
                true
            }
        });
        drained
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_empty(&self) -> bool {
        self.queue.len() == 0
    }
}

/// Metadata about a registered torrent, needed to allocate and load pieces.
#[derive(Clone)]
struct TorrentInfo {
    back_file: MutexBackFile,
    piece_size: usize,
    last_piece_size: usize,
    piece_total: usize,

    /// Shared in-flight flush counter for this torrent's pieces.
    /// for transmit worker
    flush_count: Option<Arc<AtomicU32>>,

    /// Shared in-flight flush counter for this torrent's pieces.
    /// for cache manager
    flush_count2: Option<Arc<AtomicU32>>,

    /// Sends FlushComplete to the worker's main loop after each flush.
    msg_sender: Option<mpsc::UnboundedSender<TmMsg>>,
}

/// Messages handled by the CacheManager actor.
pub enum CacheMsg {
    RegisterTorrent {
        info_hash: InfoHash,
        piece_size: usize,
        total_length: u64,
        back_file: MutexBackFile,
        flush_count: Option<Arc<AtomicU32>>,
        msg_sender: Option<mpsc::UnboundedSender<TmMsg>>,
    },
    UnregisterTorrent(InfoHash, oneshot::Sender<()>),
    /// Request a piece; the sender receives `TmMsg::PieceBufReady` when it is ready.
    GetPiece {
        key: GlobalPieceKey,
        sender: mpsc::UnboundedSender<TmMsg>,
    },
    /// Internal: file-read task completed.
    PieceLoaded {
        key: GlobalPieceKey,
        buf: io::Result<PieceBuf>,
    },
    /// The PieceLease was dropped and the piece is being returned to the cache.
    ReturnPiece {
        key: GlobalPieceKey,
        piece: PieceBuf,
    },
    /// Rename a file.
    /// Renaming is one of file operations.
    /// File operations are mutually exclusive with new cache requests.
    /// Already submitted reads, leases, and flushes are allowed to complete;
    /// the operation runs after the required I/O barrier is clear.
    Fop {
        info_hash: InfoHash,
        sender: oneshot::Sender<Result<FileOpID, &'static str>>,
        fop: Fop,
    },
    /// A flush attempt completed.
    /// Note: this does not mean it's clear, new data may come after flush started,
    /// so piece may still be dirty.
    /// A check for dirty is always required
    PieceFlushed {
        key: GlobalPieceKey,
        result: Result<(), String>,
    },
    /// Internal: the blocking task for the front `pending_ops` entry finished.
    /// The op's `FileOpID` is recovered by popping the deque front at handling
    /// time (the front is the op that just ran).
    FileOpBlockingDone {
        info_hash: InfoHash,
        result: io::Result<()>,
    },
    /// Query current cache statistics
    GetStats(oneshot::Sender<CacheStats>),
    Shutdown(oneshot::Sender<()>),
}

/// A cheap-to-clone handle to the CacheManager.
#[derive(Clone)]
pub struct CacheManagerHandle {
    sender: UnboundedSender<CacheMsg>,
    /// Approximate number of cache slots currently available (updated after each message).
    pub vacant_count: Arc<AtomicUsize>,
}

impl CacheManagerHandle {
    pub fn send_get_piece(&self, key: GlobalPieceKey, sender: UnboundedSender<TmMsg>) {
        let _ = self.sender.send(CacheMsg::GetPiece { key, sender });
    }

    pub fn register_torrent(
        &self,
        info_hash: InfoHash,
        piece_size: usize,
        total_length: u64,
        back_file: MutexBackFile,
        flush_count: Option<Arc<AtomicU32>>,
        msg_sender: Option<mpsc::UnboundedSender<TmMsg>>,
    ) {
        let _ = self.sender.send(CacheMsg::RegisterTorrent {
            info_hash,
            piece_size,
            total_length,
            back_file,
            flush_count,
            msg_sender,
        });
    }

    pub async fn send_file_op(
        &self,
        info_hash: InfoHash,
        fop: Fop,
    ) -> Result<FileOpID, &'static str> {
        self.queue_file_op(info_hash, fop)
            .await
            .expect("should be some")
    }

    /// Queue without awaiting acceptance, so startup can place file operations
    /// before its first GetPiece even though worker construction is synchronous.
    pub(crate) fn queue_file_op(
        &self,
        info_hash: InfoHash,
        fop: Fop,
    ) -> oneshot::Receiver<Result<FileOpID, &'static str>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(CacheMsg::Fop {
            info_hash,
            fop,
            sender: tx,
        });
        rx
    }

    /// Unregister a torrent.
    /// for all returned(auto dropped) `PieceLease` before calling `unregister_torrent`
    /// It's ensured these changes are flushed to disk before `unregister_torrent` returns.
    pub async fn unregister_torrent(&self, info_hash: InfoHash) {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send(CacheMsg::UnregisterTorrent(info_hash, tx));
        let _ = rx.await;
    }

    pub fn vacant_count(&self) -> usize {
        self.vacant_count.load(Ordering::Relaxed)
    }

    /// Query a snapshot of cache statistics. Returns `None` if the manager task
    /// has already stopped.
    pub async fn cache_stats(&self) -> Option<CacheStats> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(CacheMsg::GetStats(tx)).ok()?;
        rx.await.ok()
    }

    /// Called by `PieceLease::drop` to return the piece to the cache.
    pub(crate) fn return_piece(&self, key: GlobalPieceKey, piece: PieceBuf) {
        let _ = self.sender.send(CacheMsg::ReturnPiece { key, piece });
    }

    /// Called by the file-read spawn_blocking task.
    fn piece_loaded(&self, key: GlobalPieceKey, buf: io::Result<PieceBuf>) {
        let _ = self.sender.send(CacheMsg::PieceLoaded { key, buf });
    }

    /// Called by the file-op spawn_blocking task when the blocking part finished.
    fn file_op_done(&self, info_hash: InfoHash, result: io::Result<()>) {
        let _ = self
            .sender
            .send(CacheMsg::FileOpBlockingDone { info_hash, result });
    }

    /// Flush `PieceBuf` and report to the cache at completion
    fn flush_piece<F>(&self, key: GlobalPieceKey, piece: &mut PieceBuf, callback: F)
    where
        F: FnOnce(&Result<(), FlushErr>) + Send + 'static,
    {
        let sender = self.sender.clone();
        piece.flush(move |result| {
            callback(result);
            let _ = sender.send(CacheMsg::PieceFlushed {
                key,
                result: result.as_ref().map(|_| ()).map_err(|e| format!("{e:?}")),
            });
        });
    }
}

/// RAII wrapper that holds a `PieceBuf` on behalf of a torrent task.
/// When dropped, the piece is automatically returned to `CacheManager`.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct PieceLease {
    inner: Option<PieceBuf>,
    key: GlobalPieceKey,
    #[derivative(Debug = "ignore")]
    return_tx: CacheManagerHandle,
}

impl PieceLease {
    fn new(piece: PieceBuf, key: GlobalPieceKey, handle: CacheManagerHandle) -> Self {
        Self {
            inner: Some(piece),
            key,
            return_tx: handle,
        }
    }

    pub fn flush<F>(&mut self, result_callback: F)
    where
        F: FnOnce(&Result<(), FlushErr>) + Send + 'static,
    {
        self.return_tx
            .flush_piece(self.key, self.inner.as_mut().unwrap(), result_callback);
    }
}

impl Deref for PieceLease {
    type Target = PieceBuf;

    fn deref(&self) -> &PieceBuf {
        self.inner.as_ref().unwrap()
    }
}

impl DerefMut for PieceLease {
    fn deref_mut(&mut self) -> &mut PieceBuf {
        self.inner.as_mut().unwrap()
    }
}

impl Drop for PieceLease {
    fn drop(&mut self) {
        if let Some(piece) = self.inner.take() {
            self.return_tx.return_piece(self.key, piece);
        }
    }
}

/// A snapshot of cache statistics returned by `CacheMsg::GetStats`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct CacheStats {
    /// Total number of piece slots.
    pub capacity: usize,
    /// Slots currently tracked in the cache (loaded, lent out, or reading).
    pub occupied: usize,
    /// Slots not tracked at all.
    pub vacant: usize,
    /// Cached pieces that are clean (safe to evict without a write-back).
    pub clean_pieces: usize,
    /// Cached pieces with unflushed writes.
    pub dirty_pieces: usize,
    /// Pieces currently lent out as a `PieceLease`.
    pub lent_pieces: usize,
    /// Pieces with a file read in flight.
    pub reading_pieces: usize,
    /// requests waiting for slots
    pub waiting_requests: usize,
    /// `clean_pieces / capacity`.
    pub clear_ratio: f64,
    /// `dirty_pieces / capacity`.
    pub dirty_ratio: f64,
    /// `GetPiece` requests per second.
    pub request_rate: f64,
    /// Pieces lent out (as `PieceLease`) per second.
    pub lend_rate: f64,
    /// Pieces returned to the cache per second.
    pub return_rate: f64,
    /// Flush completions per second (`PieceFlushed`), regardless of who
    /// initiated the flush.
    pub flush_rate: f64,
    /// Flushes the cache itself initiated per second to free slots
    /// (`flush_least_accessed_dirty_pieces`). Excludes lease-drop writebacks.
    pub cache_flush_rate: f64,
    /// Clean pieces proactively evicted (dropped) to free slots per second
    /// (`purge_least_accessed_clear_pieces`). A high value signals cache
    /// pressure: these pieces will have to be re-read later.
    pub evict_rate: f64,
}

/// Event counts accumulated within one sampling window (the evict-timer period).
#[derive(Debug, Clone, Copy, Default)]
struct StatCounts {
    requests: u64,
    lends: u64,
    returns: u64,
    /// Flush completions (`PieceFlushed`), regardless of initiator.
    flushes: u64,
    /// Flushes the cache initiated to free slots (`flush_least_accessed_dirty_pieces`).
    cache_flushes: u64,
    /// Clean pieces evicted (`purge_least_accessed_clear_pieces`).
    evicts: u64,
}

/// All count-related state: the in-progress window (`current`) and the previous
/// completed window (`last`) of event counts, plus the live gauges (`lent`,
/// `reading`) maintained on each cache transition. `current` is reset into
/// `last` on each evict-timer tick; `GetStats` blends the two windows 50/50.
#[derive(Debug, Clone, Copy, Default)]
struct WindowStats {
    current: StatCounts,
    last: StatCounts,
    /// Live gauge: pieces currently lent out (`Loaded(None)`).
    lent: usize,
    /// Live gauge: pieces with a file read in flight (`Reading`).
    reading: usize,
}

/// Lifecycle of a registered torrent inside the cache.
enum TorrentState {
    Registed(TorrentInfo),
    /// Unregister requested; holds the waiters to notify once pending flushes drain.
    Unregisting((Vec<oneshot::Sender<()>>, TorrentInfo)),
}

/// A file-level operation (e.g. rename) deferred behind the cache's I/O barrier.
pub enum Fop {
    // TODO: change begin, end and flush_all to option((begin, end))
    Rename {
        file_index: usize,
        to: String,
        /// Logical byte range covered by the rename, expressed as [begin, end).
        begin: u64,
        end: u64,
        /// Drop and flush pieces in the range once when handling the Fop message.
        flush_all: bool,
        sender: UnboundedSender<TmMsg>,
    },
    /// Flush all dirty pieces in [begin, end), purge every cached piece for the
    /// torrent (dirty and clean), then close all back-file fds. Used by
    /// force-recheck to guarantee subsequent reads hit disk.
    FlushAndClose {
        begin: u64,
        end: u64,
        sender: UnboundedSender<TmMsg>,
    },
    /// Point one file's tracked path at whichever name exists on disk
    /// (`<final>` if present, else `<final>.part`) without renaming on disk.
    /// Runs the blocking `exists` stat off-thread. Used by force-recheck so
    /// checking reads real bytes.
    FixPath {
        file_index: usize,
        final_name: String,
        sender: UnboundedSender<TmMsg>,
    },
}

pub type FileOpID = u64;

/// Per-torrent I/O bookkeeping. File ops must not run concurrently with piece
/// I/O, so they wait here until outstanding I/O drains.
#[derive(Default)]
struct IoState {
    /// number of processing io jobs (reading + lease)
    in_io: u32,
    /// File ops accepted but not yet fully executed. The front op may be
    /// in-flight on a spawn_blocking task (see `op_executing`); it is popped
    /// only after that task reports back.
    pending_ops: VecDeque<(FileOpID, Fop)>,
    /// True while the front `pending_ops` entry is running on a blocking task.
    /// Keeps `maybe_do_all_file_op` from launching it twice and gates
    /// unregister finalization.
    op_executing: bool,
    /// Keys drained out of `waiting_slot` when the first file op was accepted.
    /// Their waiters remain in `CacheManager::pending`; re-queued once the whole
    /// `pending_ops` deque drains.
    deferred_load_keys: Vec<GlobalPieceKey>,
    /// Piece requests deferred until queued file ops finish.
    pending_req: Vec<CacheMsg>,
}

/// The CacheManager actor. Spawn via `tokio::spawn(manager.run())`.
pub struct CacheManager {
    receiver: UnboundedReceiver<CacheMsg>,

    /// Retry timer for a pending read-piece request
    /// Once timer set, piece not accessed for a while
    /// will be freed to give slot to newly read pieces
    piece_evict_timer: Interval,

    self_handle: CacheManagerHandle,

    /// All tracked pieces by state.
    cache: HashMap<GlobalPieceKey, CacheEntry>,

    /// Pending senders waiting for a specific piece.
    pending: HashMap<GlobalPieceKey, VecDeque<UnboundedSender<TmMsg>>>,

    /// Pending requests waiting for a cache slot.
    waiting_slot: WaitingSlots,

    /// Registered torrent metadata.
    torrents: HashMap<InfoHash, (TorrentState, IoState)>,

    /// Shared `BytesMut` allocator pool.
    pool: Arc<Mutex<Pool<BytesMut>>>,

    /// Clean buffers currently owned by the cache, indexed for eviction without
    /// scanning every entry.
    /// NOTE: this is an "at least" map, entries in the map are guaranteed clean
    /// but some clean slot may not be in the map at every moment (eventually will)
    assume_clear: HashSet<GlobalPieceKey>,

    capacity: usize,

    /// All count-related state: current + last window and the live `lent` /
    /// `reading` gauges (maintained on each cache transition, no scan). See
    /// [`WindowStats`].
    stats: WindowStats,

    file_op_id: u64,
}

/// Interval of `piece_evict_timer`.
const TIMER_INTERVAL: time::Duration = time::Duration::from_secs(2);

impl CacheManager {
    pub fn new() -> (Self, CacheManagerHandle) {
        Self::with_capacity(POOL_SIZE)
    }

    /// Build a manager that holds at most `capacity` pieces.
    /// Tests use a small capacity to reach the cache-full paths deterministically.
    fn with_capacity(capacity: usize) -> (Self, CacheManagerHandle) {
        let (tx, rx) = mpsc::unbounded_channel();
        let vacant = Arc::new(AtomicUsize::new(capacity));
        let handle = CacheManagerHandle {
            sender: tx,
            vacant_count: vacant,
        };
        let manager = Self {
            receiver: rx,
            piece_evict_timer: tokio::time::interval(TIMER_INTERVAL),
            self_handle: handle.clone(),
            cache: HashMap::new(),
            pending: HashMap::new(),
            waiting_slot: WaitingSlots::default(),
            torrents: HashMap::new(),
            capacity,
            assume_clear: HashSet::new(),
            pool: Arc::new(Mutex::new(Pool::new(capacity))),
            stats: WindowStats::default(),
            file_op_id: 0,
        };
        (manager, handle)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl CacheManager {
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                msg = self.receiver.recv() => {
                    match msg {
                        Some(msg) => self.handle_msg(msg),
                        None => break,
                    }
                }
                _ = self.piece_evict_timer.tick() => {
                    self.handle_evict_timeout();
                }
            }
        }
    }

    pub fn handle_msg(&mut self, msg: CacheMsg) {
        match msg {
            CacheMsg::RegisterTorrent {
                info_hash,
                piece_size,
                total_length,
                back_file,
                flush_count,
                msg_sender,
            } => {
                let (piece_total, last_piece_size) =
                    piece_total_and_last_size(total_length, piece_size);
                let flush_count2 = Some(Arc::new(AtomicU32::new(0)));
                // TODO: Tag registrations and asynchronous piece messages with a
                // generation. Unregister can finish before old reads or leases
                // return; re-registering the same info_hash must not let their
                // PieceLoaded/ReturnPiece/PieceFlushed messages update the new
                // registration's buffers, waiters, or I/O/flush counters.
                self.torrents.insert(
                    info_hash,
                    (
                        TorrentState::Registed(TorrentInfo {
                            back_file,
                            piece_size,
                            last_piece_size,
                            piece_total,
                            flush_count,
                            flush_count2,
                            msg_sender,
                        }),
                        IoState::default(),
                    ),
                );
            }

            CacheMsg::UnregisterTorrent(info_hash, done) => {
                self.handle_unregister_torrent(info_hash, done);
            }

            CacheMsg::GetPiece { key, sender } => {
                self.handle_get_piece(key, sender);
            }

            CacheMsg::PieceLoaded { key, buf } => {
                self.handle_piece_loaded(key, buf);
            }

            CacheMsg::Fop {
                info_hash,
                fop,
                sender,
            } => {
                self.handle_file_op(info_hash, fop, sender);
            }

            CacheMsg::ReturnPiece { key, piece } => {
                self.handle_return_piece(key, piece);
            }

            CacheMsg::PieceFlushed { key, result } => {
                self.handle_piece_flushed(key, result);
            }

            CacheMsg::FileOpBlockingDone { info_hash, result } => {
                self.handle_file_op_blocking_done(info_hash, result);
            }

            CacheMsg::GetStats(reply) => {
                let _ = reply.send(self.build_stats());
            }

            CacheMsg::Shutdown(tx) => {
                for (key, entry) in self.cache.iter_mut() {
                    if let CacheEntry::Loaded(Some(p)) = entry {
                        self.self_handle.flush_piece(*key, p, |_| {});
                    }
                }
                let _ = tx.send(());
                return;
            }
        }
        self.update_vacant_count();
    }

    // TODO: if too many get_piece requests, make a queue and only read when there are available cache slots
    fn handle_get_piece(&mut self, key: GlobalPieceKey, sender: UnboundedSender<TmMsg>) {
        self.stats.current.requests += 1;
        let ios = match self.torrents.get_mut(&key.info_hash) {
            Some((TorrentState::Unregisting(_), _)) => {
                let _ = sender.send(TmMsg::PieceBufReady {
                    index: key.index,
                    buf: Err(io::Error::new(
                        io::ErrorKind::Other,
                        "torrent is unregistering",
                    )),
                });
                return;
            }
            Some((
                _,
                IoState {
                    pending_ops,
                    pending_req,
                    ..
                },
            )) if pending_ops.len() > 0 => {
                pending_req.push(CacheMsg::GetPiece { key, sender });
                return;
            }
            Some((TorrentState::Registed(_), ios)) => ios,
            None => {
                let _ = sender.send(TmMsg::PieceBufReady {
                    index: key.index,
                    buf: Err(io::Error::new(
                        io::ErrorKind::Other,
                        "torrent is not registered",
                    )),
                });
                return;
            }
        };

        match self.cache.get_mut(&key) {
            Some(CacheEntry::Loaded(piece @ Some(_))) => {
                // Piece is available: remove, mark Sent, deliver as PieceLease.
                let pb = piece.take().unwrap();
                self.assume_clear.remove(&key);
                let lease = PieceLease::new(pb, key, self.self_handle.clone());
                self.stats.lent += 1;
                self.stats.current.lends += 1;
                ios.in_io += 1;
                let _ = sender.send(TmMsg::PieceBufReady {
                    index: key.index,
                    buf: Ok(lease),
                });
            }
            Some(CacheEntry::Loaded(None)) | Some(CacheEntry::Reading) => {
                // In-flight or lent out: queue the sender.
                self.pending.entry(key).or_default().push_back(sender);
            }
            None => {
                // Just put it into pending pieces queue, then try load pending pieces
                // if the cache is not full, the request will be executed immediately
                let waiters = self.pending.entry(key).or_insert_with(|| VecDeque::new());
                waiters.push_back(sender);
                // Several callers may request the same key before its read starts.
                // Keep one cache-load entry for that key and fan out the result to
                // all waiters through `pending`.
                self.waiting_slot.push(key);
                self.load_pending_pieces();
            }
        }
    }

    fn handle_piece_loaded(&mut self, key: GlobalPieceKey, result: io::Result<PieceBuf>) {
        self.stats.reading -= 1;
        match result {
            Ok(piece) => {
                if let Some(q) = self.pending.get_mut(&key) {
                    if let Some(first_sender) = q.pop_front() {
                        // Reading -> lent out.
                        self.stats.lent += 1;
                        self.cache.insert(key, CacheEntry::Loaded(None));
                        let lease = PieceLease::new(piece, key, self.self_handle.clone());
                        self.stats.current.lends += 1;
                        let _ = first_sender.send(TmMsg::PieceBufReady {
                            index: key.index,
                            buf: Ok(lease),
                        });
                        if q.is_empty() {
                            self.pending.remove(&key);
                        }
                        return;
                    }
                }

                // No waiters: cache the loaded piece.
                if let Some(i) = self.torrents.get_mut(&key.info_hash).map(|(_, i)| i) {
                    i.in_io -= 1;
                }
                match self.torrents.get(&key.info_hash) {
                    Some((TorrentState::Registed(_), _)) => {
                        // Reading -> clean cached.
                        self.cache.insert(key, CacheEntry::Loaded(Some(piece)));
                        self.assume_clear.insert(key);
                    }
                    _ => {}
                }
            }
            Err(e) => {
                // Read failed: remove Reading entry and notify all waiters.
                self.cache.remove(&key);
                if let Some(i) = self.torrents.get_mut(&key.info_hash).map(|(_, i)| i) {
                    i.in_io -= 1;
                }
                let msg = format!("{e}");
                let kind = e.kind();
                if let Some(waiters) = self.pending.remove(&key) {
                    for s in waiters {
                        let _ = s.send(TmMsg::PieceBufReady {
                            index: key.index,
                            buf: Err(io::Error::new(kind, msg.clone())),
                        });
                    }
                }
                self.maybe_do_all_file_op(key.info_hash);
                self.load_pending_pieces();
            }
        }
    }

    fn handle_return_piece(&mut self, key: GlobalPieceKey, piece: PieceBuf) {
        self.stats.current.returns += 1;
        if let Some(q) = self.pending.get_mut(&key) {
            if let Some(first_sender) = q.pop_front() {
                if q.is_empty() {
                    self.pending.remove(&key);
                }
                assert!(matches!(
                    self.cache.get(&key),
                    Some(CacheEntry::Loaded(None))
                ));

                let lease = PieceLease::new(piece, key, self.self_handle.clone());
                self.stats.current.lends += 1;
                let _ = first_sender.send(TmMsg::PieceBufReady {
                    index: key.index,
                    buf: Ok(lease),
                });
                return;
            }
        }

        if let Some(i) = self.torrents.get_mut(&key.info_hash).map(|(_, i)| i) {
            i.in_io -= 1;
        }

        // No waiters: re-cache the piece.
        // Only re-cache if the torrent is still registered; if not, just drop the piece.
        // drop piece will auto write back if it's dirty, so we don't need to explicitly flush here.
        if matches!(
            self.torrents.get(&key.info_hash),
            Some((TorrentState::Registed(_), _))
        ) {
            let is_clear = !piece.is_dirty();
            // lent out -> cached.
            if matches!(
                self.cache.insert(key, CacheEntry::Loaded(Some(piece))),
                Some(CacheEntry::Loaded(None))
            ) {
                self.stats.lent -= 1;
            }

            if is_clear {
                self.assume_clear.insert(key);
            }
            self.maybe_do_all_file_op(key.info_hash);
            if is_clear {
                // TODO: maybe do nothing, let only timeout to call `load_pending_pieces`
                self.load_pending_pieces();
            }
        }
    }

    /// called when cache slots become available,
    /// load pending pieces
    fn load_pending_pieces(&mut self) {
        if self.waiting_slot.is_empty() {
            return;
        }

        let n_to_evict = (self.waiting_slot.len() + self.cache.len()).saturating_sub(self.capacity);
        self.purge_least_accessed_clear_pieces(n_to_evict);

        // TODO: FIXME: load pending pieces by priority order
        // Only pop a key once there is room for it: a popped key that is not
        // scheduled is lost, and nothing will ever put it back.
        while self.cache.len() < self.capacity && !self.waiting_slot.is_empty() {
            let key = self.waiting_slot.pop().expect("checked non-empty");
            // A duplicate stale queue entry may remain from an older request
            // sequence. The cache entry is authoritative; do not start a second
            // read for a key that is already being read or lent out.
            if self.cache.contains_key(&key) {
                continue;
            }
            self.spawn_piece_read(key);
            self.cache.insert(key, CacheEntry::Reading);
            self.stats.reading += 1;
        }
    }

    /// Start unregistering a torrent: mark it `Unregisting`, drop its cached
    /// pieces, and record `done` to be notified once pending flushes finish.
    fn handle_unregister_torrent(&mut self, info_hash: InfoHash, done: oneshot::Sender<()>) {
        match self.torrents.remove(&info_hash) {
            Some((TorrentState::Registed(t), i)) => {
                self.torrents
                    .insert(info_hash, (TorrentState::Unregisting((vec![], t)), i));
            }
            Some((TorrentState::Unregisting(t), i)) => {
                self.torrents
                    .insert(info_hash, (TorrentState::Unregisting(t), i));
            }
            _ => {
                let _ = done.send(());
                return;
            }
        };

        for (key, entry) in &self.cache {
            if key.info_hash == info_hash {
                match entry {
                    CacheEntry::Loaded(None) => self.stats.lent -= 1,
                    CacheEntry::Reading => { /* handled by piece_loaded */ }
                    CacheEntry::Loaded(Some(_)) => {}
                }
            }
        }

        // TODO: notify waiters with "unregisting torrent"
        self.cache.retain(|key, _| key.info_hash != info_hash);
        self.assume_clear.retain(|key| key.info_hash != info_hash);
        for (k, ws) in self.pending.extract_if(|key, _| key.info_hash != info_hash) {
            for w in ws {
                let _ = w.send(TmMsg::PieceBufReady {
                    index: k.index,
                    buf: Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("unregisted torrent"),
                    )),
                });
            }
        }
        self.pending.retain(|key, _| key.info_hash != info_hash);
        self.waiting_slot.remove_torrent(info_hash);

        match self
            .torrents
            .get_mut(&info_hash)
            .expect("unregisting torrent should exist")
        {
            (TorrentState::Registed(_), _) => unreachable!("should be unregisting"),
            (TorrentState::Unregisting((waiters, _)), _) => waiters.push(done),
        };
        self.handle_unregister_after(info_hash);
    }

    /// Try to finish an in-progress unregister. Safe to call at any time.
    /// Finalizes (notify waiters, remove torrent, replay deferred requests) only
    /// once flushes are done AND every queued file op has run — a pending op is
    /// launched here and re-drives this on completion, so no op is dropped.
    fn handle_unregister_after(&mut self, info_hash: InfoHash) {
        let (flushed, op_executing, has_pending_ops) = match self.torrents.get(&info_hash) {
            Some((TorrentState::Registed(_), _)) => return,
            Some((TorrentState::Unregisting((_, ti)), ios)) => {
                let flushed = ti
                    .flush_count2
                    .as_ref()
                    .is_none_or(|c| c.load(Ordering::Relaxed) == 0);
                (flushed, ios.op_executing, !ios.pending_ops.is_empty())
            }
            None => return,
        };
        if !flushed || op_executing {
            // Barrier not clear or an op is in flight; the flush / op completion
            // handler will re-drive us.
            return;
        }
        if has_pending_ops {
            // Run remaining ops to completion before finalizing.
            self.maybe_do_all_file_op(info_hash);
            return;
        }

        let (waiters, pending_req) = match self.torrents.get_mut(&info_hash) {
            Some((TorrentState::Unregisting((waiters, _)), ios)) => {
                // Drop the keys drained out of waiting_slot at file-op acceptance.
                // waiting_slot was already purged when unregister arrived and no new
                // request for this torrent will ever come, so there is nothing to
                // restore.
                (
                    std::mem::take(waiters),
                    std::mem::take(&mut ios.pending_req),
                )
            }
            _ => return,
        };
        for w in waiters {
            let _ = w.send(());
        }
        self.torrents.remove(&info_hash);
        // Replay requests deferred behind the file ops: with the torrent now
        // removed, each routes to handle_get_piece's untracked-torrent branch and
        // its waiter gets an explicit "not registered" error instead of a silently
        // closed channel.
        for msg in pending_req {
            self.handle_msg(msg);
        }
    }

    fn handle_piece_flushed(&mut self, key: GlobalPieceKey, result: Result<(), String>) {
        if let Err(e) = result {
            warn!("cache flush error for {key:?}: {e}");
        }
        self.stats.current.flushes += 1;

        let ti = match self.torrents.get(&key.info_hash) {
            Some((TorrentState::Registed(ti), _)) => ti,
            Some((TorrentState::Unregisting((_, ti)), _)) => ti,
            None => return,
        };
        if let Some(fc) = &ti.flush_count2 {
            fc.fetch_sub(1, Ordering::Relaxed);
        }
        self.maybe_do_all_file_op(key.info_hash);
        self.handle_unregister_after(key.info_hash);

        // The buffer may have been borrowed or written again since this
        // flush started. Only its current state determines whether it is clean.
        if matches!(self.cache.get(&key), Some(CacheEntry::Loaded(Some(p))) if !p.is_dirty()) {
            self.assume_clear.insert(key);
        }
        self.load_pending_pieces();
    }

    /// Handles a file operation.
    /// `flush_all` is evaluated once when the Fop message is handled. It
    /// drops all cache-owned pieces in the requested range at that point; the
    /// actual operation waits for those drop-triggered flushes and existing
    /// I/O to finish.
    fn handle_file_op(
        &mut self,
        info_hash: InfoHash,
        op: Fop,
        sender: oneshot::Sender<Result<FileOpID, &'static str>>,
    ) {
        let flush_range = match &op {
            Fop::Rename {
                begin,
                end,
                flush_all: true, // TODO:
                ..
            } => Some((*begin, *end)),
            Fop::FlushAndClose { begin, end, .. } => Some((*begin, *end)),
            _ => None,
        };
        match self.torrents.get_mut(&info_hash) {
            Some((TorrentState::Unregisting(_), _)) => {
                let _ = sender.send(Err("unregisting torrent"));
                return;
            }
            Some((TorrentState::Registed(_), ios)) => {
                self.file_op_id += 1;
                ios.pending_ops.push_back((self.file_op_id, op));
                // Stop `load_pending_pieces` from starting reads for this torrent
                // while the op(s) run; their waiters stay parked in `self.pending`
                // and are re-queued once the deque drains. Idempotent for later
                // ops (drain returns empty), and new GetPiece during the window
                // are deferred into `pending_req`, not `waiting_slot`.
                ios.deferred_load_keys
                    .extend(self.waiting_slot.drain_torrent(info_hash));
                let _ = sender.send(Ok(self.file_op_id));
            }
            None => {
                let _ = sender.send(Err("untracked torrent"));
                return;
            }
        };

        if let Some((begin, end)) = flush_range {
            self.drop_fop_related_dirty_pieces(info_hash, begin, end);
        }
        self.maybe_do_all_file_op(info_hash);
    }

    /// Drop all cache-owned pieces intersecting [begin, end). Their Drop
    /// implementation submits the final buffer contents for write-back.
    fn drop_fop_related_dirty_pieces(&mut self, info_hash: InfoHash, begin: u64, end: u64) {
        if begin >= end {
            return;
        }
        let (piece_size, last_piece_size, piece_total) = match self.torrents.get(&info_hash) {
            Some((TorrentState::Registed(ti), _)) => {
                (ti.piece_size, ti.last_piece_size, ti.piece_total)
            }
            Some((TorrentState::Unregisting((_, ti)), _)) => {
                (ti.piece_size, ti.last_piece_size, ti.piece_total)
            }
            None => return,
        };
        let fop_range = (begin, end - begin);
        let keys: Vec<_> = self
            .cache
            .iter()
            .filter(|(key, _)| key.info_hash == info_hash)
            .filter_map(|(key, entry)| {
                if matches!(entry, CacheEntry::Loaded(Some(p)) if p.is_dirty()) {
                    let piece_range = piece_range(piece_size, last_piece_size, piece_total, *key)?;
                    ranges_overlap(piece_range, fop_range).then_some(*key)
                } else {
                    None
                }
            })
            .collect();

        for key in keys {
            self.assume_clear.remove(&key);
            self.cache.remove(&key);
        }
    }

    /// Launch the front queued file op if the torrent's I/O barrier is clear.
    /// Runs the blocking syscall off-thread via `spawn_blocking`; the front op
    /// stays in the deque (keeping same-torrent reads deferred) until its
    /// completion message pops it in `handle_file_op_blocking_done`.
    fn maybe_do_all_file_op(&mut self, info_hash: InfoHash) {
        let (t, ios) = match self.torrents.get_mut(&info_hash) {
            Some(entry) => entry,
            None => return,
        };
        if ios.op_executing {
            return;
        }
        let ti = match t {
            TorrentState::Registed(ti) => ti,
            TorrentState::Unregisting((_, ti)) => ti,
        };
        let all_flushed = ti
            .flush_count2
            .as_ref()
            .is_none_or(|fc| fc.load(Ordering::Relaxed) == 0);
        if !(all_flushed && ios.in_io == 0) {
            return;
        }
        let back_file = ti.back_file.clone();
        let handle = self.self_handle.clone();
        if ios.pending_ops.is_empty() {
            return;
        }
        ios.op_executing = true;
        match &ios.pending_ops.front().unwrap().1 {
            Fop::Rename { file_index, to, .. } => {
                let (file_index, to) = (*file_index, to.clone());
                tokio::task::spawn_blocking(move || {
                    let result = back_file.lock().unwrap().rename(file_index, to);
                    handle.file_op_done(info_hash, result);
                });
            }
            Fop::FlushAndClose { .. } => {
                tokio::task::spawn_blocking(move || {
                    back_file.lock().unwrap().close_all();
                    handle.file_op_done(info_hash, Ok(()));
                });
            }
            Fop::FixPath {
                file_index,
                final_name,
                ..
            } => {
                let (file_index, final_name) = (*file_index, final_name.clone());
                tokio::task::spawn_blocking(move || {
                    let mut bf = back_file.lock().unwrap();
                    let path = if bf.exists(final_name.as_ref()) {
                        final_name
                    } else {
                        format!("{final_name}.part")
                    };
                    bf.set_path(file_index, path);
                    handle.file_op_done(info_hash, Ok(()));
                });
            }
        }
    }

    /// Completion of the front file op's blocking task: pop it, notify its
    /// waiter, run any actor-thread cleanup, then launch the next queued op. Once
    /// the deque drains, either finalize a pending unregister or restore the
    /// reads that were deferred during the op window.
    fn handle_file_op_blocking_done(&mut self, info_hash: InfoHash, result: io::Result<()>) {
        let (done_op, is_unregistering) = match self.torrents.get_mut(&info_hash) {
            Some((t, ios)) => {
                ios.op_executing = false;
                // op_executing was set when we launched, so the front op is still
                // queued (only this handler pops it, and unregister waits for us).
                let done_op = match ios.pending_ops.pop_front() {
                    Some(op) => op,
                    None => unreachable!("op_executing set but deque front missing"),
                };
                (done_op, matches!(t, TorrentState::Unregisting(_)))
            }
            None => return,
        };

        let (id, fop) = done_op;
        match fop {
            Fop::Rename { sender, .. } => {
                _ = sender.send(TmMsg::FileOpDone { id, result });
            }
            Fop::FlushAndClose { sender, .. } => {
                // drop_fop_related_dirty_pieces already purged dirty pieces;
                // clear clean cached pieces too so the cache is fully abandoned
                // (in_io==0 ⇒ no leases out) and recheck reads real disk bytes.
                self.cache.retain(|k, _| k.info_hash != info_hash);
                self.assume_clear.retain(|k| k.info_hash != info_hash);
                _ = sender.send(TmMsg::FileOpDone { id, result });
            }
            Fop::FixPath { sender, .. } => {
                _ = sender.send(TmMsg::FileOpDone { id, result });
            }
        }

        let deque_empty = match self.torrents.get(&info_hash) {
            Some((_, ios)) => ios.pending_ops.is_empty(),
            None => return,
        };
        if !deque_empty {
            // More ops queued: run the next before finalizing anything.
            self.maybe_do_all_file_op(info_hash);
            return;
        }

        if is_unregistering {
            // All queued ops done; now the unregister can finalize.
            self.handle_unregister_after(info_hash);
            return;
        }

        let (keys, reqs) = match self.torrents.get_mut(&info_hash) {
            Some((_, ios)) => (
                std::mem::take(&mut ios.deferred_load_keys),
                std::mem::take(&mut ios.pending_req),
            ),
            None => return,
        };
        for k in keys {
            self.waiting_slot.push(k);
        }
        for req in reqs {
            self.handle_msg(req);
        }
        self.load_pending_pieces();
    }

    /// Swap old pieces out and new pieces in.
    /// Old pieces may not be frequently used, even if they are
    /// frequently used, sometimes they should give change to fewer used
    /// pieces.
    fn handle_evict_timeout(&mut self) {
        self.load_pending_pieces();
        self.flush_least_accessed_dirty_pieces(self.waiting_slot.len());
        // Roll the window: the just-finished window becomes `last`, start fresh.
        self.stats.last = self.stats.current;
        self.stats.current = StatCounts::default();
        self.update_vacant_count();
    }

    fn spawn_piece_read(&mut self, key: GlobalPieceKey) {
        let info = match self.torrents.get_mut(&key.info_hash) {
            Some((TorrentState::Registed(ti), i)) => {
                i.in_io += 1;
                ti
            }
            Some((TorrentState::Unregisting(_), i)) => {
                unreachable!("does this even happen?");
                i.in_io += 1;
                warn!(
                    "spawn_piece_read: torrent is unregisting for key {:?}",
                    key.index
                );
                // Notify pending waiters with an error.
                let handle = self.self_handle.clone();
                let err = io::Error::new(io::ErrorKind::NotFound, "torrent unregisting");
                tokio::spawn(async move {
                    handle.piece_loaded(key, Err(err));
                });
                return;
            }
            None => {
                warn!(
                    "spawn_piece_read: torrent not registered for key {:?}",
                    key.index
                );
                // Notify pending waiters with an error.
                let handle = self.self_handle.clone();
                let err = io::Error::new(io::ErrorKind::NotFound, "torrent not registered");
                tokio::spawn(async move {
                    handle.piece_loaded(key, Err(err));
                });
                return;
            }
        };

        let piece_idx = key.index.index() as usize;
        let in_piece_offset = key.index.in_piece_offset();
        let this_piece_size = if piece_idx + 1 == info.piece_total {
            info.last_piece_size
        } else {
            info.piece_size
        };

        if piece_idx >= info.piece_total || in_piece_offset >= this_piece_size {
            let handle = self.self_handle.clone();
            let err = io::Error::new(io::ErrorKind::InvalidInput, "invalid piece index");
            tokio::spawn(async move {
                handle.piece_loaded(key, Err(err));
            });
            return;
        }

        let offset = piece_idx as u64 * info.piece_size as u64 + in_piece_offset as u64;
        let len = (SUB_PIECE_SIZE as usize).min(this_piece_size - in_piece_offset);
        let file = info.back_file.clone();
        let flush_count = info.flush_count.clone();
        let flush_count2 = info.flush_count2.clone();
        let msg_sender = info.msg_sender.clone();
        let pool = self.pool.clone();
        let handle = self.self_handle.clone();

        let piece = PieceBuf::alloc(
            pool,
            key.info_hash,
            key.index,
            offset,
            len,
            file.clone(),
            flush_count,
            flush_count2,
            msg_sender,
            Some(self.self_handle.sender.clone()),
        );

        tokio::task::spawn_blocking(move || match read_from_file(piece, file) {
            Ok(p) => handle.piece_loaded(key, Ok(p)),
            Err(e) => handle.piece_loaded(key, Err(e)),
        });
    }

    ///! Evict least accessed clear pieces
    ///! returns number of evicted pieces
    fn purge_least_accessed_clear_pieces(&mut self, n_evict: usize) -> usize {
        if n_evict == 0 {
            return 0;
        }
        let clear_pieces: BTreeSet<_> = self
            .assume_clear
            .iter()
            .map(|key| match self.cache.get(key).unwrap() {
                CacheEntry::Loaded(Some(p)) => (p.access_time(), *key),
                _ => unreachable!("only cache-owned buffers may be indexed as clean"),
            })
            .collect();
        let mut n = 0;
        for (_, k) in clear_pieces.into_iter().take(n_evict) {
            let removed = self.cache.remove(&k);
            self.assume_clear.remove(&k);
            match removed {
                Some(CacheEntry::Loaded(Some(p))) => assert!(!p.is_dirty()),
                _ => unreachable!(),
            }
            n += 1;
        }
        self.stats.current.evicts += n as u64;
        n
    }

    ///! Flush least recently written dirty pieces
    ///! returns number of pieces scheduled for flushing
    /// TODO: shall me mark flushed pieces as `Retired` so they
    /// cannot be accessed again before evicted?
    fn flush_least_accessed_dirty_pieces(&mut self, n_flush: usize) -> usize {
        const MIN_STAY_PERIOD: std::time::Duration = std::time::Duration::from_millis(3000);
        let to_flush: BTreeMap<_, _> = self
            .cache
            .iter_mut()
            .filter_map(|(k, v)| match v {
                CacheEntry::Loaded(Some(p))
                    if p.is_dirty() && p.write_time().elapsed() > MIN_STAY_PERIOD =>
                {
                    Some(((p.write_time(), *k), p))
                }
                _ => None,
            })
            .collect();
        let mut flushed = 0;
        for ((_, key), p) in to_flush.into_iter().take(n_flush) {
            self.self_handle.flush_piece(key, p, |_| {});
            flushed += 1;
        }
        self.stats.current.cache_flushes += flushed as u64;
        flushed
    }

    /// Returns immediately available slots
    /// sum of number of vacant slots and occupied but clean slots
    fn available_slots(&self) -> usize {
        self.assume_clear.len() + self.capacity.saturating_sub(self.cache.len())
    }

    fn update_vacant_count(&self) {
        self.self_handle
            .vacant_count
            .store(self.available_slots(), Ordering::Relaxed);
    }

    /// Build a statistics snapshot in O(1): gauges are derived from the
    /// maintained `stats.lent` / `stats.reading` gauges and `assume_clear` (no per-entry
    /// scan); rates blend the last completed window and the in-progress window 50/50.
    fn build_stats(&self) -> CacheStats {
        let occupied = self.cache.len();
        let lent = self.stats.lent;
        let reading = self.stats.reading;
        // Loaded(Some) pieces = occupied minus lent-out and in-flight reads.
        let cached = occupied.saturating_sub(lent + reading);
        // `assume_clear` is a guaranteed-clean lower bound; the rest of `cached`
        // is treated as dirty.
        let clean = self.assume_clear.len().min(cached);
        let dirty = cached - clean;
        let cap = self.capacity as f64;
        let w = TIMER_INTERVAL.as_secs_f64();
        let (last, cur) = (&self.stats.last, &self.stats.current);
        let rate = |l: u64, c: u64| (l as f64 * 0.5 + c as f64 * 0.5) / w;
        CacheStats {
            capacity: self.capacity,
            occupied,
            vacant: self.capacity.saturating_sub(occupied),
            clean_pieces: clean,
            dirty_pieces: dirty,
            lent_pieces: lent,
            reading_pieces: reading,
            waiting_requests: self.waiting_slot.len(),
            clear_ratio: if cap > 0.0 { clean as f64 / cap } else { 0.0 },
            dirty_ratio: if cap > 0.0 { dirty as f64 / cap } else { 0.0 },
            request_rate: rate(last.requests, cur.requests),
            lend_rate: rate(last.lends, cur.lends),
            return_rate: rate(last.returns, cur.returns),
            flush_rate: rate(last.flushes, cur.flushes),
            cache_flush_rate: rate(last.cache_flushes, cur.cache_flushes),
            evict_rate: rate(last.evicts, cur.evicts),
        }
    }
}

/// Absolute byte range `(offset, len)` covered by a sub-piece, or `None` if the
/// key falls outside the torrent.
// TODO: maybe remove option, those None cases should be unreachable
fn piece_range(
    piece_size: usize,
    last_piece_size: usize,
    piece_total: usize,
    key: GlobalPieceKey,
) -> Option<(u64, u64)> {
    let piece_index = key.index.index() as usize;
    if piece_index >= piece_total {
        return None;
    }
    let piece_len = if piece_index + 1 == piece_total {
        last_piece_size
    } else {
        piece_size
    };
    let in_piece_offset = key.index.in_piece_offset();
    if in_piece_offset >= piece_len {
        return None;
    }
    let offset = piece_index as u64 * piece_size as u64 + in_piece_offset as u64;
    let len = (SUB_PIECE_SIZE as usize).min(piece_len - in_piece_offset) as u64;
    Some((offset, len))
}

/// Whether two `(offset, len)` byte ranges intersect.
fn ranges_overlap(a: (u64, u64), b: (u64, u64)) -> bool {
    let a_end = a.0.saturating_add(a.1);
    let b_end = b.0.saturating_add(b.1);
    a.0 < b_end && b.0 < a_end
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use crate::backfile::{BackFile, VoidFile};

    mod regression;

    // Create a back file that accepts reads and writes without accessing the filesystem.
    fn void_file() -> MutexBackFile {
        Arc::new(Mutex::new(BackFile::new::<VoidFile>().build()))
    }

    /// Drive the actor by hand until it has been idle for `IDLE`, long enough
    /// for the `spawn_blocking` file read to report back through the channel.
    /// Hand-pumping instead of `run()` keeps the cache state inspectable
    /// between steps, and never ticks `piece_evict_timer`, so nothing depends
    /// on the 2s retry.
    async fn pump(mgr: &mut CacheManager) {
        const IDLE: Duration = Duration::from_millis(100);
        while let Ok(Some(msg)) = tokio::time::timeout(IDLE, mgr.receiver.recv()).await {
            mgr.handle_msg(msg);
        }
    }

    // Collect all successful PieceBufReady messages currently available to the test.
    fn drain_ready(rx: &mut UnboundedReceiver<TmMsg>) -> Vec<(JointIndex, PieceLease)> {
        let mut ready = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            match msg {
                TmMsg::PieceBufReady { index, buf: Ok(l) } => ready.push((index, l)),
                other => panic!("unexpected message {other:?}"),
            }
        }
        ready
    }

    // Build a cache manager with one registered torrent and a configurable capacity.
    fn registered(capacity: usize) -> (CacheManager, CacheManagerHandle, InfoHash) {
        let (mgr, handle) = CacheManager::with_capacity(capacity);
        let info_hash = [7u8; 20];
        let piece_size = SUB_PIECE_SIZE as usize;
        handle.register_torrent(
            info_hash,
            piece_size,
            piece_size as u64 * 8,
            void_file(),
            None,
            None,
        );
        (mgr, handle, info_hash)
    }

    /// A request that arrives while the cache is full must be parked, not
    /// dropped: the requester waits for `PieceBufReady` forever and has no
    /// retry, so losing its sender stalls the download permanently.
    #[tokio::test]
    async fn request_parked_by_full_cache_is_served_when_a_slot_frees() {
        let (mut mgr, handle, info_hash) = registered(2);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let key = |i| GlobalPieceKey {
            info_hash,
            index: JointIndex::new(i, 0),
        };

        // Fill the cache and keep both leases, so nothing is evictable:
        // cache is at capacity and no piece is known clear.
        handle.send_get_piece(key(0), tx.clone());
        handle.send_get_piece(key(1), tx.clone());
        pump(&mut mgr).await;
        let mut held = drain_ready(&mut rx);
        assert_eq!(held.len(), 2);
        assert_eq!(mgr.cache.len(), mgr.capacity());
        assert_eq!(handle.vacant_count(), 0);

        handle.send_get_piece(key(2), tx.clone());
        pump(&mut mgr).await;
        assert!(
            drain_ready(&mut rx).is_empty(),
            "cache is full, nothing can be delivered yet"
        );
        assert_eq!(mgr.waiting_slot.len(), 1, "the request must be parked");

        // Free a slot: the parked request must now be answered.
        drop(held.pop());
        pump(&mut mgr).await;
        let served = drain_ready(&mut rx);
        assert_eq!(
            served.len(),
            1,
            "parked request must be answered once a slot frees"
        );
        assert_eq!(served[0].0, key(2).index);
    }

    /// When more requests are parked than there are free slots, the ones that
    /// do not fit must stay parked. Popping a key off `waiting_slot` without
    /// scheduling its read loses it: no later event will ever put it back.
    #[tokio::test]
    async fn parked_requests_are_served_one_slot_at_a_time() {
        let (mut mgr, handle, info_hash) = registered(1);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let key = |i| GlobalPieceKey {
            info_hash,
            index: JointIndex::new(i, 0),
        };

        // Occupy the single slot with a lease we hold.
        handle.send_get_piece(key(0), tx.clone());
        pump(&mut mgr).await;
        let mut held = drain_ready(&mut rx);
        assert_eq!(held.len(), 1);

        // Two requests park behind it; only one can fit at a time.
        handle.send_get_piece(key(1), tx.clone());
        handle.send_get_piece(key(2), tx.clone());
        pump(&mut mgr).await;
        assert!(drain_ready(&mut rx).is_empty());
        assert_eq!(mgr.waiting_slot.len(), 2);

        drop(held.pop());
        pump(&mut mgr).await;
        let first = drain_ready(&mut rx);
        assert_eq!(first.len(), 1, "one parked request fits now");
        assert_eq!(
            mgr.waiting_slot.len(),
            1,
            "the request that does not fit yet must stay parked"
        );

        // Free the slot again: the remaining parked request must be served.
        drop(first);
        pump(&mut mgr).await;
        assert_eq!(
            drain_ready(&mut rx).len(),
            1,
            "the last parked request must be served too"
        );
    }
}

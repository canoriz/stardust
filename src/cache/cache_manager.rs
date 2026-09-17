use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{io, time};

use bytes::BytesMut;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
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

/// Identifies a sub-piece uniquely across all torrents.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Derivative)]
#[derivative(Debug)]
pub struct GlobalPieceKey {
    // TODO: make this info hash a type so we don't have to manually implement Debug
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    pub info_hash: [u8; 20],
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

    fn remove_torrent(&mut self, info_hash: [u8; 20]) {
        for k in self.queue.iter().filter(|key| key.info_hash == info_hash) {
            self.keys.remove(&k);
        }
        self.queue.retain(|key| key.info_hash != info_hash);
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_empty(&self) -> bool {
        self.queue.len() == 0
    }
}

/// Metadata about a registered torrent, needed to allocate and load pieces.
struct TorrentInfo {
    back_file: MutexBackFile,
    piece_size: usize,
    last_piece_size: usize,
    piece_total: usize,
    /// Shared in-flight flush counter for this torrent's pieces.
    flush_count: Option<Arc<AtomicUsize>>,
    /// Sends FlushComplete to the worker's main loop after each flush.
    msg_sender: Option<mpsc::UnboundedSender<TmMsg>>,
}

/// Messages handled by the CacheManager actor.
pub enum CacheMsg {
    RegisterTorrent {
        info_hash: [u8; 20],
        piece_size: usize,
        total_length: usize,
        back_file: MutexBackFile,
        flush_count: Option<Arc<AtomicUsize>>,
        msg_sender: Option<mpsc::UnboundedSender<TmMsg>>,
    },
    UnregisterTorrent([u8; 20], oneshot::Sender<()>),
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
    /// A flush attempt completed.
    /// Note: this does not mean it's clear, new data may come after flush started,
    /// so piece may still be dirty.
    /// A check for dirty is always required
    PieceFlushed {
        key: GlobalPieceKey,
        result: Result<(), String>,
    },
    /// Query current cache statistics
    GetStats(oneshot::Sender<CacheStats>),
    Shutdown(oneshot::Sender<()>),
}

/// A cheap-to-clone handle to the CacheManager.
#[derive(Clone)]
pub struct CacheManagerHandle {
    sender: mpsc::UnboundedSender<CacheMsg>,
    /// Approximate number of cache slots currently available (updated after each message).
    pub vacant_count: Arc<AtomicUsize>,
}

impl CacheManagerHandle {
    pub fn send_get_piece(&self, key: GlobalPieceKey, sender: mpsc::UnboundedSender<TmMsg>) {
        let _ = self.sender.send(CacheMsg::GetPiece { key, sender });
    }

    pub fn register_torrent(
        &self,
        info_hash: [u8; 20],
        piece_size: usize,
        total_length: usize,
        back_file: MutexBackFile,
        flush_count: Option<Arc<AtomicUsize>>,
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

    pub async fn unregister_torrent(&self, info_hash: [u8; 20]) {
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

/// The CacheManager actor. Spawn via `tokio::spawn(manager.run())`.
pub struct CacheManager {
    receiver: mpsc::UnboundedReceiver<CacheMsg>,

    /// Retry timer for a pending read-piece request
    /// Once timer set, piece not accessed for a while
    /// will be freed to give slot to newly read pieces
    piece_evict_timer: Interval,

    self_handle: CacheManagerHandle,
    /// All tracked pieces by state.
    cache: HashMap<GlobalPieceKey, CacheEntry>,
    /// Pending senders waiting for a specific piece.
    pending: HashMap<GlobalPieceKey, VecDeque<mpsc::UnboundedSender<TmMsg>>>,

    /// Pending requests waiting for a cache slot.
    waiting_slot: WaitingSlots,

    /// Registered torrent metadata.
    torrents: HashMap<[u8; 20], TorrentInfo>,
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
                self.torrents.insert(
                    info_hash,
                    TorrentInfo {
                        back_file,
                        piece_size,
                        last_piece_size,
                        piece_total,
                        flush_count,
                        msg_sender,
                    },
                );
            }

            CacheMsg::UnregisterTorrent(info_hash, done) => {
                self.torrents.remove(&info_hash);
                for (key, entry) in &self.cache {
                    if key.info_hash == info_hash {
                        match entry {
                            CacheEntry::Loaded(None) => self.stats.lent -= 1,
                            CacheEntry::Reading => self.stats.reading -= 1,
                            CacheEntry::Loaded(Some(_)) => {}
                        }
                    }
                }
                self.cache.retain(|key, _| key.info_hash != info_hash);
                self.assume_clear.retain(|key| key.info_hash != info_hash);
                self.pending.retain(|key, _| key.info_hash != info_hash);
                self.waiting_slot.remove_torrent(info_hash);
                let _ = done.send(());
                self.load_pending_pieces();
            }

            CacheMsg::GetPiece { key, sender } => {
                self.handle_get_piece(key, sender);
            }

            CacheMsg::PieceLoaded { key, buf } => {
                self.handle_piece_loaded(key, buf);
            }

            CacheMsg::ReturnPiece { key, piece } => {
                self.handle_return_piece(key, piece);
            }

            CacheMsg::PieceFlushed { key, result } => {
                if let Err(e) = result {
                    warn!("cache flush error for {key:?}: {e}");
                }
                self.stats.current.flushes += 1;
                // The buffer may have been borrowed or written again since this
                // flush started. Only its current state determines whether it is clean.
                if matches!(self.cache.get(&key), Some(CacheEntry::Loaded(Some(p))) if !p.is_dirty())
                {
                    self.assume_clear.insert(key);
                }
                self.load_pending_pieces();
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
    fn handle_get_piece(&mut self, key: GlobalPieceKey, sender: mpsc::UnboundedSender<TmMsg>) {
        self.stats.current.requests += 1;
        match self.cache.get_mut(&key) {
            Some(CacheEntry::Loaded(piece @ Some(_))) => {
                // Piece is available: remove, mark Sent, deliver as PieceLease.
                let pb = piece.take().unwrap();
                self.assume_clear.remove(&key);
                let lease = PieceLease::new(pb, key, self.self_handle.clone());
                self.stats.lent += 1;
                self.stats.current.lends += 1;
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
        match result {
            Ok(piece) => {
                if let Some(q) = self.pending.get_mut(&key) {
                    if let Some(first_sender) = q.pop_front() {
                        // Reading -> lent out.
                        if self.cache.insert(key, CacheEntry::Loaded(None)).is_some() {
                            self.stats.reading -= 1;
                        }
                        self.stats.lent += 1;
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
                if self.torrents.contains_key(&key.info_hash) {
                    // Reading -> clean cached.
                    if self
                        .cache
                        .insert(key, CacheEntry::Loaded(Some(piece)))
                        .is_some()
                    {
                        self.stats.reading -= 1;
                    }
                    self.assume_clear.insert(key);
                }
            }
            Err(e) => {
                // Read failed: remove Reading entry and notify all waiters.
                if matches!(self.cache.remove(&key), Some(CacheEntry::Reading)) {
                    self.stats.reading -= 1;
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
                self.load_pending_pieces();
            }
        }
    }

    fn handle_return_piece(&mut self, key: GlobalPieceKey, piece: PieceBuf) {
        self.stats.current.returns += 1;
        if let Some(mut q) = self.pending.remove(&key) {
            if let Some(first_sender) = q.pop_front() {
                if !q.is_empty() {
                    self.pending.insert(key, q);
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

        // No waiters: re-cache the piece.
        // Only re-cache if the torrent is still registered; if not, just drop the piece.
        // drop piece will auto write back if it's dirty, so we don't need to explicitly flush here.
        if self.torrents.contains_key(&key.info_hash) {
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

    fn spawn_piece_read(&self, key: GlobalPieceKey) {
        let info = match self.torrents.get(&key.info_hash) {
            Some(i) => i,
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

        let offset = piece_idx * info.piece_size + in_piece_offset;
        let len = (SUB_PIECE_SIZE as usize).min(this_piece_size - in_piece_offset);
        let file = info.back_file.clone();
        let flush_count = info.flush_count.clone();
        let msg_sender = info.msg_sender.clone();
        let pool = self.pool.clone();
        let handle = self.self_handle.clone();

        let piece = PieceBuf::alloc(
            pool,
            key.index,
            offset,
            len,
            file.clone(),
            flush_count,
            msg_sender,
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
    fn drain_ready(rx: &mut mpsc::UnboundedReceiver<TmMsg>) -> Vec<(JointIndex, PieceLease)> {
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
    fn registered(capacity: usize) -> (CacheManager, CacheManagerHandle, [u8; 20]) {
        let (mgr, handle) = CacheManager::with_capacity(capacity);
        let info_hash = [7u8; 20];
        let piece_size = SUB_PIECE_SIZE as usize;
        handle.register_torrent(
            info_hash,
            piece_size,
            piece_size * 8,
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

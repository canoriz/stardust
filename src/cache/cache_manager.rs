use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{io, time};

use bytes::BytesMut;
use derivative::Derivative;
use tokio::{
    sync::{mpsc, oneshot},
    time::Interval,
};
use tracing::{info, warn};

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
    /// A piece is flushed successfully.
    /// Note: this does not mean it's clear, new data may come after flush started,
    /// so piece may still be dirty.
    /// A check for dirty is always required
    PieceFlushed {
        key: GlobalPieceKey,
        result: Result<(), String>,
    },
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

    /// Called by `PieceLease::drop` to return the piece to the cache.
    pub(crate) fn return_piece(&self, key: GlobalPieceKey, piece: PieceBuf) {
        let _ = self.sender.send(CacheMsg::ReturnPiece { key, piece });
    }

    /// Called by the file-read spawn_blocking task.
    fn piece_loaded(&self, key: GlobalPieceKey, buf: io::Result<PieceBuf>) {
        let _ = self.sender.send(CacheMsg::PieceLoaded { key, buf });
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
        let sender = self.return_tx.clone();
        let key = self.key;
        self.inner.as_mut().unwrap().flush(move |e| {
            result_callback(e);
            let _ = sender.sender.send(CacheMsg::PieceFlushed {
                key,
                result: match e {
                    Ok(_) => Ok(()),
                    Err(e) => Err(format!("{e:?}")),
                },
            });
        });
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
    waiting_slot: VecDeque<GlobalPieceKey>,

    /// Registered torrent metadata.
    torrents: HashMap<[u8; 20], TorrentInfo>,
    /// Shared `BytesMut` allocator pool.
    pool: Arc<Mutex<Pool<BytesMut>>>,

    /// pieces we assume it's clear
    assume_clear: HashSet<GlobalPieceKey>,

    capacity: usize,
}

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
            piece_evict_timer: tokio::time::interval(time::Duration::from_secs(2)),
            self_handle: handle.clone(),
            cache: HashMap::new(),
            pending: HashMap::new(),
            waiting_slot: VecDeque::new(),
            torrents: HashMap::new(),
            capacity,
            assume_clear: HashSet::new(),
            pool: Arc::new(Mutex::new(Pool::new(capacity))),
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
                self.cache.retain(|key, _| key.info_hash != info_hash);
                self.assume_clear.retain(|key| key.info_hash != info_hash);
                self.pending.retain(|key, _| key.info_hash != info_hash);
                self.waiting_slot.retain(|key| key.info_hash != info_hash);
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
                let piece = self.cache.get(&key);
                let evict = match piece {
                    Some(CacheEntry::Loaded(Some(p))) if !p.is_dirty() => {
                        // There are lots of possibilities
                        // 1. PieceLease.flush()
                        // 2. PieceLease returned (dirty at return, n_clear should not change)
                        // 3. PieceFlushed (dirty becomes clear, n_clear++)
                        //
                        // 1. PieceLease.flush()
                        // 2. PieceFlushed (dirty becomes clear, not in cache
                        //    n_clear should not increase)
                        // 3. PieceLease returned (clear at return, n_clear++)
                        //
                        // 1. PieceLease.flush()
                        // 2. PieceLease returned (dirty at return, n_clear should not change)
                        // 3. PieceLease lent out again
                        // 4. flush finish, atomic cleared, but PieceFlushed not come yet
                        // 5. PieceLease returned (clear at return, n_clear++)
                        // 6. PieceFlushed of 1 returns (clear, BUT SHOULD NOT ++,
                        //    SHOULD NOT DOUBLE COUNT)
                        //
                        // At this time, there is no unified way to maintain correct n_clear

                        // Anyway, it's definitely clear now
                        true
                    }
                    Some(CacheEntry::Loaded(Some(_))) => {
                        // It's flushed, but it's dirty and held by manager
                        // likely: steps
                        // 1. PieceLease.flush()
                        // 2. PieceLease returned
                        // 3. PieceLease lent out again
                        // 4. PieceLease returned
                        // 5. flush of 1 completes, but 3,4 modified it again
                        info!("key {key:?} dirty after flushed, what should we do?");
                        false
                    }
                    Some(CacheEntry::Loaded(None)) => {
                        info!("key {key:?} dispatched after flushed, what should we do?");
                        // It's lent out again
                        false
                    }
                    Some(CacheEntry::Reading) => {
                        info!("key {key:?} is reading again after flushed, what should we do?");
                        // Will this even happen?
                        false
                    }
                    None => true, // will this even happen?
                };

                if evict {
                    self.assume_clear.remove(&key);
                    self.cache.remove(&key);
                }
                self.load_pending_pieces();
            }

            CacheMsg::Shutdown(tx) => {
                for (_, entry) in self.cache.iter_mut() {
                    if let CacheEntry::Loaded(Some(p)) = entry {
                        p.flush(|_| {});
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
        match self.cache.get_mut(&key) {
            Some(CacheEntry::Loaded(piece @ Some(_))) => {
                // Piece is available: remove, mark Sent, deliver as PieceLease.
                let pb = piece.take().unwrap();
                self.assume_clear.remove(&key);
                let lease = PieceLease::new(pb, key, self.self_handle.clone());
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
                if self.cache.len() >= self.capacity && !self.assume_clear.is_empty() {
                    // cache no space, but we can evict
                    self.purge_least_accessed_clear_pieces(1);
                }

                if self.cache.len() < self.capacity {
                    // cache have space: queue sender and spawn file-read.
                    self.pending.entry(key).or_default().push_back(sender);
                    self.spawn_piece_read(key);
                    self.cache.insert(key, CacheEntry::Reading);
                } else {
                    // we are full of cache, this request shall wait until some piece
                    // being actively or passively evicted
                    match self.pending.get_mut(&key) {
                        Some(c) => c.push_back(sender),
                        None => {
                            self.waiting_slot.push_back(key);
                            self.pending.insert(key, VecDeque::from([sender]));
                        }
                    }
                }
            }
        }
    }

    fn handle_piece_loaded(&mut self, key: GlobalPieceKey, result: io::Result<PieceBuf>) {
        match result {
            Ok(piece) => {
                if let Some(q) = self.pending.get_mut(&key) {
                    if let Some(first_sender) = q.pop_front() {
                        self.cache.insert(key, CacheEntry::Loaded(None));
                        let lease = PieceLease::new(piece, key, self.self_handle.clone());
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
                    self.cache.insert(key, CacheEntry::Loaded(Some(piece)));
                    self.assume_clear.insert(key);
                }
            }
            Err(e) => {
                // Read failed: remove Reading entry and notify all waiters.
                self.cache.remove(&key);
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
            }
        }
    }

    fn handle_return_piece(&mut self, key: GlobalPieceKey, piece: PieceBuf) {
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
            self.cache.insert(key, CacheEntry::Loaded(Some(piece)));
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
        let n_evicted = self.purge_least_accessed_clear_pieces(n_to_evict);

        // TODO: FIXME: load pending pieces by priority order
        // Only pop a key once there is room for it: a popped key that is not
        // scheduled is lost, and nothing will ever put it back.
        while self.cache.len() < self.capacity && !self.waiting_slot.is_empty() {
            let key = self.waiting_slot.pop_front().expect("checked non-empty");
            self.spawn_piece_read(key);
            self.cache.insert(key, CacheEntry::Reading);
        }
        if n_evicted < n_to_evict {
            self.flush_least_accessed_dirty_pieces(n_to_evict - n_evicted);
        }
    }

    /// Swap old pieces out and new pieces in.
    /// Old pieces may not be frequently used, even if they are
    /// frequently used, sometimes they should give change to fewer used
    /// pieces.
    fn handle_evict_timeout(&mut self) {
        self.load_pending_pieces();
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
            .map(|k| match self.cache.get(k).unwrap() {
                CacheEntry::Loaded(Some(p)) => (p.access_time(), *k),
                _ => unreachable!(),
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
        n
    }

    ///! Flush least accessed clear pieces
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
        for (_, p) in to_flush.into_iter().take(n_flush) {
            p.flush(|_| {});
            flushed += 1;
        }
        flushed
    }

    fn update_vacant_count(&self) {
        // TODO: optimize
        let known_clear_pieces = self
            .cache
            .iter()
            .filter(|(_, entry)| match entry {
                CacheEntry::Loaded(Some(p)) if !p.is_dirty() => true,
                _ => false,
            })
            .count();
        let remain = POOL_SIZE.saturating_sub(self.cache.len());
        let vacant = known_clear_pieces + remain;
        self.self_handle
            .vacant_count
            .store(vacant, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use crate::backfile::{BackFile, VoidFile};

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

    fn registered(capacity: usize) -> (CacheManager, CacheManagerHandle, [u8; 20]) {
        let (mgr, handle) = CacheManager::with_capacity(capacity);
        let info_hash = [7u8; 20];
        let piece_size = SUB_PIECE_SIZE as usize;
        handle.register_torrent(info_hash, piece_size, piece_size * 8, void_file(), None, None);
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
        assert!(mgr.assume_clear.is_empty());

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

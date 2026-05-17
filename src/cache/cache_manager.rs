use std::collections::{BTreeSet, HashMap, VecDeque};
use std::io;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use derivative::Derivative;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use super::MutexBackFile;
use crate::cache::simple_buffer::{
    read_from_file, ErrorCallback, FlushErr, JointIndex, PieceBuf, Pool, POOL_SIZE, SUB_PIECE_SIZE,
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
    /// Channel for routing flush errors back to the TM event loop.
    error_sender: mpsc::UnboundedSender<TmMsg>,
}

/// Messages handled by the CacheManager actor.
pub enum CacheMsg {
    RegisterTorrent {
        info_hash: [u8; 20],
        piece_size: usize,
        total_length: usize,
        back_file: MutexBackFile,
        error_sender: mpsc::UnboundedSender<TmMsg>,
    },
    UnregisterTorrent([u8; 20]),
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
        error_sender: mpsc::UnboundedSender<TmMsg>,
    ) {
        let _ = self.sender.send(CacheMsg::RegisterTorrent {
            info_hash,
            piece_size,
            total_length,
            back_file,
            error_sender,
        });
    }

    pub fn unregister_torrent(&self, info_hash: [u8; 20]) {
        let _ = self.sender.send(CacheMsg::UnregisterTorrent(info_hash));
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

    // TODO: do we need this result_callback, shall we just inform cache manager?
    pub fn flush<F>(&mut self, result_callback: F)
    where
        F: FnOnce(Result<(), FlushErr>) + Send + 'static,
    {
        self.inner.as_mut().unwrap().flush(result_callback);
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
    self_handle: CacheManagerHandle,
    /// All tracked pieces by state.
    cache: HashMap<GlobalPieceKey, CacheEntry>,
    /// Pending senders waiting for a specific piece.
    pending: HashMap<GlobalPieceKey, VecDeque<mpsc::UnboundedSender<TmMsg>>>,
    /// Registered torrent metadata.
    torrents: HashMap<[u8; 20], TorrentInfo>,
    /// Shared `BytesMut` allocator pool.
    pool: Arc<Mutex<Pool<BytesMut>>>,
}

impl CacheManager {
    pub fn new() -> (Self, CacheManagerHandle) {
        let (tx, rx) = mpsc::unbounded_channel();
        let vacant = Arc::new(AtomicUsize::new(POOL_SIZE));
        let handle = CacheManagerHandle {
            sender: tx,
            vacant_count: vacant,
        };
        let manager = Self {
            receiver: rx,
            self_handle: handle.clone(),
            cache: HashMap::new(),
            pending: HashMap::new(),
            torrents: HashMap::new(),
            pool: Arc::new(Mutex::new(Pool::new(POOL_SIZE))),
        };
        (manager, handle)
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                CacheMsg::RegisterTorrent {
                    info_hash,
                    piece_size,
                    total_length,
                    back_file,
                    error_sender,
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
                            error_sender,
                        },
                    );
                }

                CacheMsg::UnregisterTorrent(info_hash) => {
                    self.torrents.remove(&info_hash);
                    // Flush all Loaded pieces for this torrent so data is not lost.
                    for (key, entry) in self.cache.iter_mut() {
                        if key.info_hash == info_hash {
                            if let CacheEntry::Loaded(Some(p)) = entry {
                                p.flush(|_| {});
                            }
                        }
                    }
                    // TODO: remove pending entries
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
    }

    // TODO: if too many get_piece requests, make a queue and only read when there are available cache slots
    fn handle_get_piece(&mut self, key: GlobalPieceKey, sender: mpsc::UnboundedSender<TmMsg>) {
        match self.cache.get_mut(&key) {
            Some(CacheEntry::Loaded(piece @ Some(_))) => {
                // Piece is available: remove, mark Sent, deliver as PieceLease.
                // TODO: maybe use a Loaded(Option<PieceBuf>), so we don't need CacheEntry::Sent?
                let lease = PieceLease::new(piece.take().unwrap(), key, self.self_handle.clone());
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
                // Not in cache at all: queue sender and spawn file-read.
                self.pending.entry(key).or_default().push_back(sender);
                self.spawn_piece_read(key);
                self.cache.insert(key, CacheEntry::Reading);
            }
        }
    }

    fn handle_piece_loaded(&mut self, key: GlobalPieceKey, result: io::Result<PieceBuf>) {
        match result {
            Ok(piece) => {
                self.purge_by_size(POOL_SIZE - 1);
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
                self.cache.insert(key, CacheEntry::Loaded(Some(piece)));
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

    fn handle_return_piece(&mut self, key: GlobalPieceKey, mut piece: PieceBuf) {
        self.purge_by_size(POOL_SIZE - 1);
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
        // If the torrent is no longer registered, flush so data is not lost.
        if !self.torrents.contains_key(&key.info_hash) {
            piece.flush(|_| {});
        }
        self.cache.insert(key, CacheEntry::Loaded(Some(piece)));
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
        let error_sender = info.error_sender.clone();
        let pool = self.pool.clone();
        let handle = self.self_handle.clone();

        let piece = PieceBuf::alloc(
            &pool,
            key.index,
            offset,
            len,
            file.clone(),
            Box::new(move |e| {
                let _ = error_sender.send(TmMsg::FlushError(e));
            }),
        );

        tokio::task::spawn_blocking(move || match read_from_file(piece, file) {
            Ok(p) => handle.piece_loaded(key, Ok(p)),
            Err(e) => handle.piece_loaded(key, Err(e)),
        });
    }

    fn purge_by_size(&mut self, keep: usize) {
        // TODO: OPTIMIZE
        if self.cache.len() <= keep {
            return;
        }
        let mut n_purge = self.cache.len() - keep;

        let mut to_evict = BTreeSet::new();

        for time_key in self.cache.iter().filter_map(|(k, v)| match v {
            CacheEntry::Loaded(Some(p)) if !p.is_dirty() => Some((p.access_time(), *k)),
            _ => None,
        }) {
            to_evict.insert(time_key);
            if to_evict.len() > n_purge {
                to_evict.pop_last();
            }
        }
        while n_purge > 0 {
            if let Some((_, k)) = to_evict.pop_first() {
                self.cache.remove(&k);
                info!("purge clear piece {k:?}");
                n_purge -= 1;
            } else {
                break;
            }
        }

        if n_purge > 0 {
            warn!(
                "purge_by_size: cannot purge {n_purge} sub pieces; all remaining are DIRTY/FLUSHING"
            );
            for time_key in self.cache.iter().filter_map(|(k, v)| match v {
                CacheEntry::Loaded(Some(p))
                    if p.access_time().elapsed() > std::time::Duration::from_millis(500) =>
                {
                    Some((p.access_time(), *k))
                }
                _ => None,
            }) {
                to_evict.insert(time_key);
                if to_evict.len() > n_purge {
                    to_evict.pop_last();
                }
            }
            while n_purge > 0 {
                if let Some((_, k)) = to_evict.pop_first() {
                    info!("purge flush dirty piece {k:?}");
                    // IMPORTANT: we really can't remove them because they are dirty
                    // if we remove them, though they will be flushed, but we may read
                    // stale data subsequently.
                    match self.cache.get_mut(&k).unwrap() {
                        CacheEntry::Loaded(Some(piece_buf)) => {
                            piece_buf.flush(|_| {});
                            n_purge -= 1;
                        }
                        _ => {}
                    }
                    // can't remove it because it's dirty, we flush them
                    // TODO: FIXME: will we flush multiple times?
                } else {
                    break;
                }
            }
        }
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

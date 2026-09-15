use super::*;
use crate::backfile::{Access, FileMetadata};
use std::path::Path;
use std::sync::atomic::AtomicUsize;

// Populate one cache from manager and modified data then return to the manager.
// the manager should see a dirty slot
async fn dirty_cached_piece(
    mgr: &mut CacheManager,
    handle: &CacheManagerHandle,
    key: GlobalPieceKey,
    tx: &mpsc::UnboundedSender<TmMsg>,
    rx: &mut mpsc::UnboundedReceiver<TmMsg>,
) {
    handle.send_get_piece(key, tx.clone());
    pump(mgr).await;
    let (_, mut lease) = drain_ready(rx).pop().expect("initial lease");
    lease.as_mut()[0] = 0x5a;
    drop(lease);
    pump(mgr).await;
}

// Advance the simulated clock beyond the minimum dirty-page residency period.
async fn age_dirty_pages() {
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::time::resume();
}

// Verify that a cache-owned flush wakes a request parked by a full cache.
#[tokio::test]
async fn background_flush_serves_waiting_request() {
    // Fill the only slot with dirty data, then park a request for another key.
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    dirty_cached_piece(&mut mgr, &handle, key(0), &tx, &mut rx).await;
    assert_eq!(handle.vacant_count(), 0);
    handle.send_get_piece(key(1), tx.clone());
    pump(&mut mgr).await;
    assert!(drain_ready(&mut rx).is_empty());
    // Age the dirty page and trigger the background flush path.
    age_dirty_pages().await;
    mgr.handle_evict_timeout();
    // Only process completion messages: a second eviction tick must not be needed.
    pump(&mut mgr).await;
    let served = drain_ready(&mut rx);
    assert_eq!(served.len(), 1, "cache-owned flush must wake its waiter");
    assert_eq!(served[0].0, key(1).index);
    assert!(mgr.waiting_slot.is_empty());
}

// Verify that duplicate requests for one key share a waiting slot and each receive a lease.
#[tokio::test]
async fn duplicate_waiters_share_one_slot_entry() {
    // Hold the only slot so two requests for the same new key must wait.
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    handle.send_get_piece(key(0), tx.clone());
    pump(&mut mgr).await;
    let held = drain_ready(&mut rx);
    assert_eq!(held.len(), 1);

    handle.send_get_piece(key(1), tx.clone());
    handle.send_get_piece(key(1), tx.clone());
    pump(&mut mgr).await;
    assert_eq!(mgr.waiting_slot.len(), 1);
    assert_eq!(mgr.pending[&key(1)].len(), 2);

    // Free the slot; both waiters should be served from one cache read.
    drop(held);
    pump(&mut mgr).await;
    let first = drain_ready(&mut rx);
    assert_eq!(first.len(), 1);
    assert!(mgr.waiting_slot.is_empty());
    drop(first);
    pump(&mut mgr).await;
    assert_eq!(drain_ready(&mut rx).len(), 1);
    assert!(mgr.waiting_slot.is_empty());
}

// Verify that returning a clean buffer updates the eviction index and available-slot count.
#[tokio::test]
async fn clean_return_updates_eviction_index() {
    // Load and return a clean buffer, then verify it becomes evictable.
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    handle.send_get_piece(key(0), tx.clone());
    pump(&mut mgr).await;
    let held = drain_ready(&mut rx);
    assert_eq!(held.len(), 1);
    assert!(mgr.assume_clear.is_empty());
    assert_eq!(handle.vacant_count(), 0);
    drop(held);
    pump(&mut mgr).await;
    assert!(mgr.assume_clear.contains(&key(0)));
    assert_eq!(handle.vacant_count(), 1);
    // A new key should evict the indexed clean buffer and reuse its slot.
    handle.send_get_piece(key(1), tx.clone());
    pump(&mut mgr).await;
    assert_eq!(drain_ready(&mut rx).len(), 1);
    assert!(!mgr.cache.contains_key(&key(0)));
    assert!(mgr.assume_clear.is_empty());
    assert_eq!(handle.vacant_count(), 0);
}

// Verify that writes made during a flush are not evicted by the older completion message.
#[tokio::test]
async fn write_during_flush_preserves_newer_data() {
    // Start flushing one version, then write newer data before the flush completes.
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    handle.send_get_piece(key(0), tx.clone());
    pump(&mut mgr).await;
    let (_, mut lease) = drain_ready(&mut rx).pop().unwrap();
    let file = mgr.torrents[&info_hash].back_file.clone();
    // Hold the disk mutex so a second write deterministically precedes flush completion.
    let disk = file.lock().unwrap();
    lease.as_mut()[0] = 0x11;
    let (done, completed) = oneshot::channel();
    lease.flush(move |r| {
        let _ = done.send(r.is_ok());
    });
    lease.as_mut()[0] = 0x22;
    drop(lease);
    handle.send_get_piece(key(1), tx.clone());
    // Both messages are already queued; avoid holding a mutex across await.
    for _ in 0..2 {
        let msg = mgr.receiver.try_recv().unwrap();
        mgr.handle_msg(msg);
    }
    drop(disk);
    assert!(completed.await.unwrap());
    pump(&mut mgr).await;
    assert!(
        drain_ready(&mut rx).is_empty(),
        "the newer dirty bytes must stay resident"
    );
    assert!(
        matches!(mgr.cache.get(&key(0)), Some(CacheEntry::Loaded(Some(p))) if p.is_dirty() && p[0] == 0x22)
    );
    // The newer dirty version must require a later flush before eviction.
    age_dirty_pages().await;
    mgr.handle_evict_timeout();
    pump(&mut mgr).await;
    assert_eq!(
        drain_ready(&mut rx).len(),
        1,
        "the newer version must eventually flush too"
    );
}

// Verify that a flush completing while leased waits for the lease to be returned before waking a waiter.
#[tokio::test]
async fn flush_completion_waits_for_lease_return() {
    // Keep the lease alive while its flush completes and a different key waits.
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    handle.send_get_piece(key(0), tx.clone());
    pump(&mut mgr).await;
    let (_, mut lease) = drain_ready(&mut rx).pop().unwrap();
    handle.send_get_piece(key(1), tx.clone());
    pump(&mut mgr).await;
    lease.as_mut()[0] = 1;
    let (done, completed) = oneshot::channel();
    lease.flush(move |r| {
        let _ = done.send(r.is_ok());
    });
    assert!(completed.await.unwrap());
    pump(&mut mgr).await;
    assert!(drain_ready(&mut rx).is_empty());
    assert!(matches!(
        mgr.cache.get(&key(0)),
        Some(CacheEntry::Loaded(None))
    ));
    // Returning the lease makes the buffer available to the waiting request.
    drop(lease);
    pump(&mut mgr).await;
    assert_eq!(drain_ready(&mut rx).len(), 1);
}

static WRITE_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
// Simulate a storage backend that fails its first write and accepts later retries.
struct FailFirstWrite;
impl Access for FailFirstWrite {
    // Construct the fake backend without opening a real file.
    fn open<P: AsRef<Path>>(_: P, _: usize) -> io::Result<Self> {
        Ok(Self)
    }
    // Inject one write failure using the attempt counter shared with the test.
    fn write_all_at(&mut self, _: &[u8], _: usize) -> io::Result<()> {
        if WRITE_ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(io::Error::other("injected disk error"))
        } else {
            Ok(())
        }
    }
    // Supply deterministic zero-filled data when the cache loads a buffer.
    fn read_exact_at(&mut self, buf: &mut [u8], _: usize) -> io::Result<()> {
        buf.fill(0);
        Ok(())
    }
    // Report a length large enough for every offset used by the test.
    fn metadata(&self) -> io::Result<FileMetadata> {
        Ok(FileMetadata { len: usize::MAX })
    }
}

// Verify that a failed background flush preserves its waiter and retries on a later timer tick.
#[tokio::test]
async fn failed_flush_retries_without_losing_waiter() {
    // Inject one write failure while another key waits for the cache slot.
    WRITE_ATTEMPTS.store(0, Ordering::SeqCst);
    let (mut mgr, handle) = CacheManager::with_capacity(1);
    let info_hash = [19; 20];
    handle.register_torrent(
        info_hash,
        SUB_PIECE_SIZE as usize,
        2 * SUB_PIECE_SIZE as usize,
        Arc::new(Mutex::new(BackFile::new::<FailFirstWrite>().build())),
        None,
        None,
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    dirty_cached_piece(&mut mgr, &handle, key(0), &tx, &mut rx).await;
    handle.send_get_piece(key(1), tx.clone());
    pump(&mut mgr).await;
    age_dirty_pages().await;
    // The first timer tick must report the failure without hot-looping.
    mgr.handle_evict_timeout();
    pump(&mut mgr).await;
    assert_eq!(
        WRITE_ATTEMPTS.load(Ordering::SeqCst),
        1,
        "completion must not trigger a hot retry loop"
    );
    assert_eq!(handle.vacant_count(), 0);
    assert_eq!(mgr.waiting_slot.len(), 1);
    assert!(drain_ready(&mut rx).is_empty());
    // A later timer tick retries the flush and serves the waiting request.
    mgr.handle_evict_timeout();
    pump(&mut mgr).await;
    assert_eq!(WRITE_ATTEMPTS.load(Ordering::SeqCst), 2);
    assert_eq!(drain_ready(&mut rx).len(), 1);
}

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
    let file = match &mgr.torrents[&info_hash].0 {
        TorrentState::Registed(t) => t.back_file.clone(),
        TorrentState::Unregisting((_, t)) => t.back_file.clone(),
    };
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
    fn open<P: AsRef<Path>>(_: P, _: u64) -> io::Result<Self> {
        Ok(Self)
    }
    // Inject one write failure using the attempt counter shared with the test.
    fn write_all_at(&mut self, _: &[u8], _: u64) -> io::Result<()> {
        if WRITE_ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(io::Error::other("injected disk error"))
        } else {
            Ok(())
        }
    }
    // Supply deterministic zero-filled data when the cache loads a buffer.
    fn read_exact_at(&mut self, buf: &mut [u8], _: u64) -> io::Result<()> {
        buf.fill(0);
        Ok(())
    }
    // Report a length large enough for every offset used by the test.
    fn metadata(&self) -> io::Result<FileMetadata> {
        Ok(FileMetadata { len: u64::MAX })
    }
    // No real files are opened, so renaming is a no-op.
    fn rename<P: AsRef<Path>>(_: P, _: P) -> io::Result<()> {
        Ok(())
    }
    // No real files are opened, so nothing exists on disk.
    fn exists<P: AsRef<Path>>(_: P) -> bool {
        false
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
        2 * SUB_PIECE_SIZE as u64,
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

// Queue a file op the way `send_file_op` does, returning the accept channel so
// the test can drive the manager by hand.
fn queue_file_op(
    handle: &CacheManagerHandle,
    info_hash: InfoHash,
    fop: Fop,
) -> oneshot::Receiver<Result<FileOpID, &'static str>> {
    let (tx, rx) = oneshot::channel();
    handle
        .sender
        .send(CacheMsg::Fop {
            info_hash,
            fop,
            sender: tx,
        })
        .unwrap();
    rx
}

// Build a Rename file op over `[begin, end)`, reporting completion on `sender`.
fn rename_op(
    file_index: usize,
    to: &str,
    begin: u64,
    end: u64,
    flush_all: bool,
    sender: &mpsc::UnboundedSender<TmMsg>,
) -> Fop {
    Fop::Rename {
        file_index,
        to: to.to_string(),
        flush: flush_all.then_some((begin, end)),
        sender: sender.clone(),
    }
}

// Take a single FileOpDone completion if one is queued; panics on any other message.
fn take_file_op_done(
    rx: &mut mpsc::UnboundedReceiver<TmMsg>,
) -> Option<(FileOpID, io::Result<()>)> {
    match rx.try_recv() {
        Ok(TmMsg::FileOpDone { id, result }) => Some((id, result)),
        Ok(other) => panic!("unexpected message {other:?}"),
        Err(_) => None,
    }
}

// Drain every FileOpDone completion currently queued, in arrival order.
fn drain_file_op_done(rx: &mut mpsc::UnboundedReceiver<TmMsg>) -> Vec<(FileOpID, io::Result<()>)> {
    let mut done = Vec::new();
    while let Some(d) = take_file_op_done(rx) {
        done.push(d);
    }
    done
}

// A storage backend whose rename always fails, so the file op must surface the error.
struct FailRename;
impl Access for FailRename {
    fn open<P: AsRef<Path>>(_: P, _: u64) -> io::Result<Self> {
        Ok(Self)
    }
    fn write_all_at(&mut self, _: &[u8], _: u64) -> io::Result<()> {
        Ok(())
    }
    fn read_exact_at(&mut self, buf: &mut [u8], _: u64) -> io::Result<()> {
        buf.fill(0);
        Ok(())
    }
    fn metadata(&self) -> io::Result<FileMetadata> {
        Ok(FileMetadata { len: u64::MAX })
    }
    fn rename<P: AsRef<Path>>(_: P, _: P) -> io::Result<()> {
        Err(io::Error::other("injected rename failure"))
    }
    // Report the source as present so the rename path actually invokes `rename`.
    fn exists<P: AsRef<Path>>(_: P) -> bool {
        true
    }
}

// Verify a file op runs immediately and reports success when no I/O is in flight.
#[tokio::test]
async fn file_op_runs_when_no_io_in_flight() {
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accept = queue_file_op(&handle, info_hash, rename_op(0, "done", 0, 0, false, &tx));
    pump(&mut mgr).await;
    let id = accept.await.unwrap().expect("file op accepted");
    let (done_id, result) = take_file_op_done(&mut rx).expect("file op runs immediately when idle");
    assert_eq!(done_id, id);
    assert!(result.is_ok());
    assert!(mgr.torrents[&info_hash].1.pending_ops.is_empty());
}

// Verify a file op waits while a lease is out and runs once the lease is returned.
#[tokio::test]
async fn file_op_defers_until_lease_returned() {
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    // Hold the lease so the torrent's I/O barrier stays raised (in_io > 0).
    handle.send_get_piece(key(0), tx.clone());
    pump(&mut mgr).await;
    let held = drain_ready(&mut rx);
    assert_eq!(held.len(), 1);

    let accept = queue_file_op(&handle, info_hash, rename_op(0, "later", 0, 0, false, &tx));
    pump(&mut mgr).await;
    let id = accept.await.unwrap().expect("file op accepted");
    assert!(
        take_file_op_done(&mut rx).is_none(),
        "file op must wait while a lease is out"
    );
    assert_eq!(mgr.torrents[&info_hash].1.pending_ops.len(), 1);
    assert_eq!(mgr.torrents[&info_hash].1.in_io, 1);

    // Returning the lease drains in_io and releases the queued op.
    drop(held);
    pump(&mut mgr).await;
    let (done_id, result) = take_file_op_done(&mut rx).expect("file op runs after lease return");
    assert_eq!(done_id, id);
    assert!(result.is_ok());
    assert!(mgr.torrents[&info_hash].1.pending_ops.is_empty());
}

// Verify a flush_all rename drops dirty pieces in range and waits for that flush before running.
#[tokio::test]
async fn file_op_waits_for_dirty_flush_in_range() {
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    dirty_cached_piece(&mut mgr, &handle, key(0), &tx, &mut rx).await;
    assert!(
        matches!(mgr.cache.get(&key(0)), Some(CacheEntry::Loaded(Some(p))) if p.is_dirty()),
        "the piece must be resident and dirty before the rename"
    );

    let end = SUB_PIECE_SIZE as u64 * 8;
    let accept = queue_file_op(
        &handle,
        info_hash,
        rename_op(0, "renamed", 0, end, true, &tx),
    );
    // Handle only the Fop: dropping the dirty piece raises flush_count2, so the
    // rename must stay queued and unexecuted until the flush completes.
    let msg = mgr.receiver.try_recv().unwrap();
    mgr.handle_msg(msg);
    assert!(
        !mgr.cache.contains_key(&key(0)),
        "flush_all must drop the dirty piece in range"
    );
    assert_eq!(
        mgr.torrents[&info_hash].1.pending_ops.len(),
        1,
        "the rename must wait behind the dirty flush"
    );
    assert!(
        take_file_op_done(&mut rx).is_none(),
        "the rename must not run before the flush completes"
    );

    // Let the flush complete: the rename now runs and reports success.
    pump(&mut mgr).await;
    let (id, result) = take_file_op_done(&mut rx).expect("rename completes after the flush");
    assert!(result.is_ok());
    assert_eq!(accept.await.unwrap(), Ok(id));
    assert!(mgr.torrents[&info_hash].1.pending_ops.is_empty());
}

// Verify a failing rename reports its error through FileOpDone.
#[tokio::test]
async fn file_op_failure_is_reported() {
    let (mut mgr, handle) = CacheManager::with_capacity(1);
    let info_hash = [23; 20];
    handle.register_torrent(
        info_hash,
        SUB_PIECE_SIZE as usize,
        2 * SUB_PIECE_SIZE as u64,
        Arc::new(Mutex::new(BackFile::new::<FailRename>().build())),
        None,
        None,
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accept = queue_file_op(&handle, info_hash, rename_op(0, "fail", 0, 0, false, &tx));
    pump(&mut mgr).await;
    let id = accept
        .await
        .unwrap()
        .expect("file op accepted even if the rename will fail");
    let (done_id, result) = take_file_op_done(&mut rx).expect("failure must be reported");
    assert_eq!(done_id, id);
    assert!(result.is_err());
}

// A storage backend that reports the final name as present on disk.
struct FinalPresent;
impl Access for FinalPresent {
    fn open<P: AsRef<Path>>(_: P, _: u64) -> io::Result<Self> {
        Ok(Self)
    }
    fn write_all_at(&mut self, _: &[u8], _: u64) -> io::Result<()> {
        Ok(())
    }
    fn read_exact_at(&mut self, buf: &mut [u8], _: u64) -> io::Result<()> {
        buf.fill(0);
        Ok(())
    }
    fn metadata(&self) -> io::Result<FileMetadata> {
        Ok(FileMetadata { len: u64::MAX })
    }
    fn rename<P: AsRef<Path>>(_: P, _: P) -> io::Result<()> {
        Ok(())
    }
    fn exists<P: AsRef<Path>>(_: P) -> bool {
        true
    }
}

// A storage backend where the final name is absent (only a `.part` exists).
struct FinalAbsent;
impl Access for FinalAbsent {
    fn open<P: AsRef<Path>>(_: P, _: u64) -> io::Result<Self> {
        Ok(Self)
    }
    fn write_all_at(&mut self, _: &[u8], _: u64) -> io::Result<()> {
        Ok(())
    }
    fn read_exact_at(&mut self, buf: &mut [u8], _: u64) -> io::Result<()> {
        buf.fill(0);
        Ok(())
    }
    fn metadata(&self) -> io::Result<FileMetadata> {
        Ok(FileMetadata { len: u64::MAX })
    }
    fn rename<P: AsRef<Path>>(_: P, _: P) -> io::Result<()> {
        Ok(())
    }
    fn exists<P: AsRef<Path>>(_: P) -> bool {
        false
    }
}

// FixPath points the tracked path at the final name when it exists on disk,
// and reports completion through FileOpDone.
#[tokio::test]
async fn fixpath_uses_final_name_when_present() {
    let (mut mgr, handle) = CacheManager::with_capacity(1);
    let info_hash = [31; 20];
    let bf = Arc::new(Mutex::new(BackFile::new::<FinalPresent>().build()));
    handle.register_torrent(
        info_hash,
        SUB_PIECE_SIZE as usize,
        2 * SUB_PIECE_SIZE as u64,
        bf.clone(),
        None,
        None,
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let fop = Fop::FixPath {
        file_index: 0,
        final_name: "movie".to_string(),
        sender: tx.clone(),
    };
    let accept = queue_file_op(&handle, info_hash, fop);
    pump(&mut mgr).await;
    let id = accept.await.unwrap().expect("fixpath accepted");
    let (done_id, result) = take_file_op_done(&mut rx).expect("fixpath reports completion");
    assert_eq!(done_id, id);
    assert!(result.is_ok());
    assert_eq!(bf.lock().unwrap().tracked_path(0), Some("movie"));
    assert!(mgr.torrents[&info_hash].1.pending_ops.is_empty());
}

// FixPath falls back to the `.part` name when the final name is absent on disk.
#[tokio::test]
async fn fixpath_falls_back_to_part_when_absent() {
    let (mut mgr, handle) = CacheManager::with_capacity(1);
    let info_hash = [32; 20];
    let bf = Arc::new(Mutex::new(BackFile::new::<FinalAbsent>().build()));
    handle.register_torrent(
        info_hash,
        SUB_PIECE_SIZE as usize,
        2 * SUB_PIECE_SIZE as u64,
        bf.clone(),
        None,
        None,
    );
    let (tx, mut rx) = mpsc::unbounded_channel();
    let fop = Fop::FixPath {
        file_index: 0,
        final_name: "movie".to_string(),
        sender: tx.clone(),
    };
    let accept = queue_file_op(&handle, info_hash, fop);
    pump(&mut mgr).await;
    let id = accept.await.unwrap().expect("fixpath accepted");
    let (done_id, result) = take_file_op_done(&mut rx).expect("fixpath reports completion");
    assert_eq!(done_id, id);
    assert!(result.is_ok());
    assert_eq!(bf.lock().unwrap().tracked_path(0), Some("movie.part"));
}

// Verify an out-of-range file index reports an error rather than renaming.
#[tokio::test]
async fn file_op_out_of_range_index_reports_error() {
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accept = queue_file_op(&handle, info_hash, rename_op(5, "x", 0, 0, false, &tx));
    pump(&mut mgr).await;
    let id = accept.await.unwrap().expect("file op accepted");
    let (done_id, result) = take_file_op_done(&mut rx).expect("out-of-range index reported");
    assert_eq!(done_id, id);
    assert!(result.is_err());
}

// Verify a file op on an untracked torrent is rejected at accept time.
#[tokio::test]
async fn file_op_on_untracked_torrent_is_rejected() {
    let (mut mgr, handle, _info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let accept = queue_file_op(&handle, [99; 20], rename_op(0, "x", 0, 0, false, &tx));
    pump(&mut mgr).await;
    assert_eq!(accept.await.unwrap(), Err("untracked torrent"));
    assert!(take_file_op_done(&mut rx).is_none());
}

// Verify a piece request arriving behind a queued file op is deferred, then replayed after it runs.
#[tokio::test]
async fn piece_request_deferred_by_pending_file_op_replays() {
    let (mut mgr, handle, info_hash) = registered(2);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let key = |i| GlobalPieceKey {
        info_hash,
        index: JointIndex::new(i, 0),
    };
    // Hold a lease so the rename cannot run yet.
    handle.send_get_piece(key(0), tx.clone());
    pump(&mut mgr).await;
    let held = drain_ready(&mut rx);
    assert_eq!(held.len(), 1);

    let accept = queue_file_op(&handle, info_hash, rename_op(0, "x", 0, 0, false, &tx));
    // A new request arrives while the op is queued: it must be deferred, not served.
    handle.send_get_piece(key(1), tx.clone());
    pump(&mut mgr).await;
    let id = accept.await.unwrap().expect("file op accepted");
    assert!(
        drain_ready(&mut rx).is_empty(),
        "the request must wait behind the file op"
    );
    assert_eq!(mgr.torrents[&info_hash].1.pending_req.len(), 1);

    // Release the lease: the rename runs, then the deferred request is replayed.
    drop(held);
    pump(&mut mgr).await;
    let (done_id, result) = take_file_op_done(&mut rx).expect("rename runs");
    assert_eq!(done_id, id);
    assert!(result.is_ok());
    let served = drain_ready(&mut rx);
    assert_eq!(
        served.len(),
        1,
        "the deferred request is served after the file op"
    );
    assert_eq!(served[0].0, key(1).index);
}

// Verify several file ops queued together each run to completion, in order,
// with distinct ids: the deque keeps its front until the blocking task reports
// back, so ops are executed one at a time rather than dropped or reordered.
#[tokio::test]
async fn multiple_queued_file_ops_all_complete_in_order() {
    let (mut mgr, handle, info_hash) = registered(1);
    let (tx, mut rx) = mpsc::unbounded_channel();

    let a = queue_file_op(&handle, info_hash, rename_op(0, "a", 0, 0, false, &tx));
    let b = queue_file_op(&handle, info_hash, rename_op(0, "b", 0, 0, false, &tx));
    let c = queue_file_op(&handle, info_hash, rename_op(0, "c", 0, 0, false, &tx));
    pump(&mut mgr).await;

    let id_a = a.await.unwrap().expect("first op accepted");
    let id_b = b.await.unwrap().expect("second op accepted");
    let id_c = c.await.unwrap().expect("third op accepted");
    assert!(id_a != id_b && id_b != id_c, "each op gets a distinct id");

    let done = drain_file_op_done(&mut rx);
    assert_eq!(done.len(), 3, "every queued op must complete");
    assert!(done.iter().all(|(_, r)| r.is_ok()));
    assert_eq!(
        done.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![id_a, id_b, id_c],
        "ops complete in the order they were queued"
    );
    assert!(mgr.torrents[&info_hash].1.pending_ops.is_empty());
    assert!(!mgr.torrents[&info_hash].1.op_executing);
}

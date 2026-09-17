use super::*;
use crate::buffer_pool::BufferPool;
use crate::cache::cache_manager::CacheManager;
use crate::cache::simple_buffer::FlushErr;
use crate::metadata::{File, Info};
use bytes::BytesMut;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

static NEXT_FILE: AtomicUsize = AtomicUsize::new(0);

// Create two-sub-piece metadata backed by a temporary file containing valid data.
fn metadata() -> Metadata {
    let len = 2 * SUB_PIECE_SIZE as usize;
    let bytes = vec![0x5a; len];
    let path = std::env::temp_dir().join(format!(
        "stardust-hash-test-{}-{}.bin",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, &bytes).unwrap();
    let info: Info = serde_json::from_value(serde_json::json!({
        "name": "hash-test", "piece length": len, "length": len,
        "pieces": Sha1::digest(&bytes).to_vec()
    }))
    .unwrap();
    Metadata {
        info,
        raw_info: vec![],
        info_hash: [52; 20],
        len,
        files: vec![File {
            length: len,
            path: vec![path.to_str().unwrap().to_owned()],
        }],
        comment: None,
        created_by: None,
        creation_date: None,
    }
}

// Create a block picker with the test piece selected for download.
fn picker(meta: &Metadata) -> BlockPicker {
    let mut picker = BlockPicker::new(
        meta.len(),
        meta.regular_piece_size(),
        Box::new(RarestPicker::new(meta.len(), meta.regular_piece_size())),
        Duration::from_secs(120),
    );
    picker.select(0, true);
    picker
}

// Mark every block in one sub-piece as received by the picker.
fn mark_subpiece_received(picker: &mut BlockPicker, sub: u32) {
    for i in 0..(SUB_PIECE_SIZE / 16384) {
        picker.receive_block(Request {
            index: 0,
            begin: sub * SUB_PIECE_SIZE + i * 16384,
            len: 16384,
        });
    }
}

// Restore a worker from a serialized dump and start its cache manager task.
fn restore_worker(
    meta: Metadata,
    picker: &mut BlockPicker,
    running: StableState,
) -> (TransmitWorker, tokio::task::JoinHandle<()>) {
    let dump = TransmitDump {
        state: TorrentStateDump::Metadata {
            metadata: meta,
            picker: picker.dump(),
        },
        peers: vec![],
        announce_urls: vec![],
        running_state: RunningStateDump::StableState(running),
    };
    let dump = serde_json::from_slice(&serde_json::to_vec(&dump).unwrap()).unwrap();
    let (cache, handle) = CacheManager::new();
    let task = tokio::spawn(cache.run());
    let (tx, rx) = mpsc::unbounded_channel();
    let announce = AnnounceManagerHandle::new([53; 20], 0, [52; 20], tx.clone());
    let pool = BufferPool::new(4, || BytesMut::with_capacity(16384));
    (
        TransmitWorker::from_dump(dump, [53; 20], 0, None, announce, tx, rx, handle, pool),
        task,
    )
}

// Create a new downloading worker and start its cache manager task.
fn new_downloading_worker() -> (TransmitWorker, tokio::task::JoinHandle<()>) {
    let meta = metadata();
    let (cache, handle) = CacheManager::new();
    let task = tokio::spawn(cache.run());
    let (tx, rx) = mpsc::unbounded_channel();
    let announce = AnnounceManagerHandle::new([53; 20], 0, [52; 20], tx.clone());
    let pool = BufferPool::new(4, || BytesMut::with_capacity(16384));
    let mut worker = TransmitWorker::new(
        TorrentTask::Torrent(meta),
        [53; 20],
        0,
        None,
        announce,
        tx,
        rx,
        handle,
        pool,
    );
    worker.running_state = RunningState::StableState(StableState::Downloading);
    (worker, task)
}

// Return the worker's metadata-backed downloading state for assertions.
fn downloading_state(worker: &mut TransmitWorker) -> &mut Downloading {
    match &mut worker.torrent_state {
        TorrentState::Metadata(d) => d,
        _ => unreachable!(),
    }
}

// Queue all blocks of one sub-piece with deterministic test bytes.
fn queue_received_subpiece(worker: &mut TransmitWorker, sub: u32, byte: u8) {
    mark_subpiece_received(&mut downloading_state(worker).block_picker, sub);
    let ji = JointIndex::new(0, sub * SUB_PIECE_SIZE);
    worker.request_piecebuf(ji);
    let pending = worker.waiting_for_piecebuf.get_mut(&ji).unwrap();
    for i in 0..(SUB_PIECE_SIZE / 16384) {
        pending.blocks.push(BlockWaitingBuf {
            piece: Piece {
                index: 0,
                begin: sub * SUB_PIECE_SIZE + i * 16384,
                len: 16384,
                buf: None,
            },
            buf: BlockBuf::Owned(BytesMut::from(&vec![byte; 16384][..])),
        });
    }
}

// Drain worker messages until the test queue becomes idle.
async fn pump(worker: &mut TransmitWorker) {
    while let Ok(Some(msg)) =
        tokio::time::timeout(Duration::from_millis(100), worker.receiver.recv()).await
    {
        worker.handle_msg(msg).unwrap();
    }
}

// Send a running-state command and wait for the worker acknowledgement.
async fn change_state(worker: &mut TransmitWorker, cmd: RunningCmd) {
    let (tx, rx) = oneshot::channel();
    worker.handle_msg(Msg::ChangeState(cmd, tx)).unwrap();
    rx.await.unwrap();
}

// Drain pending buffers, unregister the torrent, stop the cache task, and remove the temp file.
async fn cleanup(mut worker: TransmitWorker, cache: tokio::task::JoinHandle<()>) {
    pump(&mut worker).await;
    assert!(worker.waiting_for_piecebuf.is_empty());
    let path = downloading_state(&mut worker).metadata.files[0].path[0].clone();
    worker
        .cache_handle
        .unregister_torrent(worker.info_hash)
        .await;
    while worker.pending_flushes.load(Ordering::Relaxed) > 0 {
        let msg = tokio::time::timeout(Duration::from_secs(5), worker.receiver.recv())
            .await
            .unwrap()
            .unwrap();
        worker.handle_msg(msg).unwrap();
    }
    cache.abort();
    let _ = cache.await;
    drop(worker);
    std::fs::remove_file(path).unwrap();
}

// Verify that restore rebuilds the hasher to the offset after received sub-piece 0.
#[tokio::test]
async fn restore_received_subpiece0_rebuilds_hasher_to_subpiece1_offset() {
    // Persist a piece whose first sub-piece is already received, but whose hasher was not saved.
    let meta = metadata();
    let mut picker = picker(&meta);
    mark_subpiece_received(&mut picker, 0);
    let (mut worker, cache) = restore_worker(meta, &mut picker, StableState::Downloading);
    // Restore must load sub-piece 0 and rebuild the hasher before any new network data arrives.
    pump(&mut worker).await;
    assert_eq!(
        downloading_state(&mut worker).hasher[&0].next_offset(),
        SUB_PIECE_SIZE as usize
    );
    // Deliver the remaining sub-piece and verify the piece completes normally.
    queue_received_subpiece(&mut worker, 1, 0x5a);
    pump(&mut worker).await;
    assert!(downloading_state(&mut worker).block_picker.have(0));
    assert_eq!(worker.runtime_status().process, 1.0);
    assert!(matches!(
        worker.running_state,
        RunningState::StableState(StableState::Seeding)
    ));
    cleanup(worker, cache).await;
}

// Verify that a fully received piece is checked automatically after restore without network events.
#[tokio::test]
async fn restore_downloading_worker_verifies_piece_when_have_all_subpieces() {
    // Persist a piece with every sub-piece received and no saved hasher state.
    let meta = metadata();
    let mut picker = picker(&meta);
    mark_subpiece_received(&mut picker, 0);
    mark_subpiece_received(&mut picker, 1);
    let (mut worker, cache) = restore_worker(meta, &mut picker, StableState::Downloading);
    // Restore must verify the piece without waiting for a peer message.
    pump(&mut worker).await;
    assert!(downloading_state(&mut worker).block_picker.have(0));
    assert!(downloading_state(&mut worker).hasher.is_empty());
    assert!(worker.waiting_for_piecebuf.is_empty());
    cleanup(worker, cache).await;
}

// Resume should verify fully received but unverified piece restored from a paused state.
#[tokio::test]
async fn restore_paused_worker_and_resume_verifies_piece_when_have_all_subpieces() {
    // Restore a piece whose all blocks are received, while the piece is still unverified and paused.
    let meta = metadata();
    let mut picker = picker(&meta);
    mark_subpiece_received(&mut picker, 0);
    mark_subpiece_received(&mut picker, 1);
    let (mut worker, cache) = restore_worker(meta, &mut picker, StableState::Paused);
    pump(&mut worker).await;
    assert!(worker.waiting_for_piecebuf.is_empty());
    assert!(!downloading_state(&mut worker).block_picker.have(0));
    // Resume must start hasher reconstruction and complete verification.
    change_state(&mut worker, RunningCmd::Resume).await;
    pump(&mut worker).await;
    assert!(downloading_state(&mut worker).block_picker.have(0));
    cleanup(worker, cache).await;
}

// Verify that a later sub-piece requests sub-piece 0 again when its hasher is missing.
#[tokio::test]
async fn receive_subpiece1_rebuilds_hasher_from_subpiece0() {
    // Simulate received sub-pieces whose original hasher delivery was lost.
    let (mut worker, cache) = new_downloading_worker();
    mark_subpiece_received(&mut downloading_state(&mut worker).block_picker, 0);
    mark_subpiece_received(&mut downloading_state(&mut worker).block_picker, 1);
    // Deliver a later sub-piece; this must recreate the hasher request at sub-piece 0.
    worker.request_piecebuf(JointIndex::new(0, SUB_PIECE_SIZE));
    pump(&mut worker).await;
    assert!(downloading_state(&mut worker).block_picker.have(0));
    cleanup(worker, cache).await;
}

// Verify that out-of-order sub-pieces eventually verify and duplicate deliveries do not rehash.
#[tokio::test]
async fn out_of_order_subpieces_verify_without_rehashing_duplicates() {
    // Deliver sub-piece 1 before sub-piece 0 and confirm the piece stays unverified.
    let (mut worker, cache) = new_downloading_worker();
    queue_received_subpiece(&mut worker, 1, 0x5a);
    pump(&mut worker).await;
    assert!(!downloading_state(&mut worker).block_picker.have(0));
    // Deliver sub-piece 0, then verify the whole piece in order.
    queue_received_subpiece(&mut worker, 0, 0x5a);
    pump(&mut worker).await;
    assert!(downloading_state(&mut worker).block_picker.have(0));
    change_state(&mut worker, RunningCmd::Pause).await;
    // Re-deliver both buffers after verification; they must not start a second hasher.
    worker.request_piecebuf(JointIndex::new(0, 0));
    worker.request_piecebuf(JointIndex::new(0, SUB_PIECE_SIZE));
    pump(&mut worker).await;
    assert!(downloading_state(&mut worker).hasher.is_empty());
    assert!(worker.waiting_for_piecebuf.is_empty());
    assert!(matches!(
        worker.running_state,
        RunningState::StableState(StableState::Paused)
    ));
    cleanup(worker, cache).await;
}

// Verify that piece verification does not incorrectly clear a paused or Fatal state.
#[tokio::test]
async fn piece_verification_preserves_paused_or_fatal_state() {
    for running in [StableState::Paused, StableState::Fatal("disk error".into())] {
        // Verify a fully received piece while the surrounding worker state is non-downloading.
        let meta = metadata();
        let mut picker = picker(&meta);
        mark_subpiece_received(&mut picker, 0);
        mark_subpiece_received(&mut picker, 1);
        let (mut worker, cache) = restore_worker(meta, &mut picker, StableState::Downloading);
        worker.running_state = RunningState::StableState(running);
        pump(&mut worker).await;
        assert!(downloading_state(&mut worker).block_picker.have(0));
        assert!(matches!(
            worker.running_state,
            RunningState::StableState(StableState::Paused | StableState::Fatal(_))
        ));
        cleanup(worker, cache).await;
    }
}

// Verify that a write error clears the old hasher and releases buffers waiting for other sub-pieces.
#[tokio::test]
async fn write_error_clears_hasher_and_waiting_buffers() {
    // Download sub-piece 0 first. Its bytes advance the SHA1 state to the
    // beginning of sub-piece 1.
    let (mut worker, cache) = new_downloading_worker();
    mark_subpiece_received(&mut downloading_state(&mut worker).block_picker, 0);
    worker.request_piecebuf(JointIndex::new(0, 0));
    pump(&mut worker).await;
    assert_eq!(
        downloading_state(&mut worker).hasher[&0].next_offset(),
        SUB_PIECE_SIZE as usize
    );
    let ji = JointIndex::new(0, SUB_PIECE_SIZE);

    // Simulate a block from sub-piece 1 arriving while its PieceBuf is still
    // being requested. This block must be released if the earlier write fails.
    worker.request_piecebuf(ji);
    let pool = BufferPool::new(1, || BytesMut::zeroed(16384));
    worker
        .waiting_for_piecebuf
        .get_mut(&ji)
        .unwrap()
        .blocks
        .push(BlockWaitingBuf {
            piece: Piece {
                index: 0,
                begin: SUB_PIECE_SIZE,
                len: 16384,
                buf: None,
            },
            buf: BlockBuf::Pooled(pool.acquire().await),
        });
    assert_eq!(pool.available(), 0);

    // Simulate the asynchronous flush reporting a disk write error. The worker
    // must invalidate this piece attempt, clear the hasher, discard the waiting
    // block data, and enter Fatal.
    worker.pending_flushes.fetch_add(1, Ordering::Relaxed);
    worker
        .handle_msg(Msg::FlushComplete(Err(FlushErr {
            ji: JointIndex::new(0, 0),
            offset: 0,
            len: SUB_PIECE_SIZE as usize,
            err: io::Error::other("injected disk error"),
        })))
        .unwrap();
    assert!(downloading_state(&mut worker).hasher.is_empty());
    assert!(!downloading_state(&mut worker)
        .block_picker
        .have_sub(JointIndex::new(0, 0)));
    assert_eq!(pool.available(), 1);
    let pending = &worker.waiting_for_piecebuf[&ji];
    assert!(pending.requested && pending.blocks.is_empty());
    assert!(matches!(
        worker.running_state,
        RunningState::StableState(StableState::Fatal(_))
    ));

    // Resume and redownload the piece with deliberately corrupt data in
    // sub-piece 0. Reusing the old hasher would incorrectly reuse the old valid
    // prefix and could make this corrupt replacement pass verification.
    change_state(&mut worker, RunningCmd::Resume).await;
    queue_received_subpiece(&mut worker, 0, 0x33);
    queue_received_subpiece(&mut worker, 1, 0x5a);
    pump(&mut worker).await;

    // The corrupt replacement must fail, proving hashing restarted at offset 0.
    assert!(!downloading_state(&mut worker).block_picker.have(0));
    assert!(downloading_state(&mut worker).hasher.is_empty());
    cleanup(worker, cache).await;
}

// Verify that a read error clears the hasher and discards pending data for the other sub-pieces.
#[tokio::test]
async fn read_error_clears_hasher_and_waiting_data() {
    // Leave data waiting for one sub-piece, then inject a read failure for another.
    let (mut worker, cache) = new_downloading_worker();
    mark_subpiece_received(&mut downloading_state(&mut worker).block_picker, 0);
    worker.request_piecebuf(JointIndex::new(0, 0));
    pump(&mut worker).await;

    // worker holds hasher of piece 0
    assert!(!downloading_state(&mut worker).hasher.is_empty());

    worker.request_piecebuf(JointIndex::new(0, 0));
    worker
        .waiting_for_piecebuf
        .get_mut(&JointIndex::new(0, 0))
        .unwrap()
        .blocks
        .push(BlockWaitingBuf {
            piece: Piece {
                index: 0,
                begin: 0,
                len: 16384,
                buf: None,
            },
            buf: BlockBuf::Owned(BytesMut::zeroed(16384)),
        });
    let failed_ji = JointIndex::new(0, SUB_PIECE_SIZE);
    // Also keep a block waiting for the sub-piece whose cache read will fail.
    // The error path must remove this waiting entry entirely.
    worker.request_piecebuf(failed_ji);
    worker
        .waiting_for_piecebuf
        .get_mut(&failed_ji)
        .unwrap()
        .blocks
        .push(BlockWaitingBuf {
            piece: Piece {
                index: 0,
                begin: SUB_PIECE_SIZE,
                len: 16384,
                buf: None,
            },
            buf: BlockBuf::Owned(BytesMut::zeroed(16384)),
        });
    worker
        .handle_piecebuf_ready(failed_ji, Err(io::Error::other("injected read error")))
        .unwrap();

    // worker clears hasher of fatal error piece 0
    assert!(downloading_state(&mut worker).hasher.is_empty());
    // The failed sub-piece entry is removed, while other sub-piece entries for
    // the same piece remain only as deduplicated requests with no stale blocks.
    assert!(!worker.waiting_for_piecebuf.contains_key(&failed_ji));
    assert!(worker.waiting_for_piecebuf[&JointIndex::new(0, 0)]
        .blocks
        .is_empty());
    assert!(worker.waiting_for_piecebuf[&JointIndex::new(0, 0)].requested);
    assert!(matches!(
        worker.running_state,
        RunningState::StableState(StableState::Fatal(_))
    ));
    cleanup(worker, cache).await;
}

// Verify that force check accepts correct disk data even when the in-memory hasher is wrong.
#[tokio::test]
async fn force_check_accepts_correct_disk_data_with_stale_hasher() {
    // The file contains the correct 0x5a data, but the stale hasher contains a wrong 0x33 prefix.
    let (mut worker, cache) = new_downloading_worker();
    let mut stale_hasher = HashState::new(Sha1::new());
    stale_hasher
        .write(&vec![0x33; SUB_PIECE_SIZE as usize])
        .unwrap();
    downloading_state(&mut worker)
        .hasher
        .insert(0, stale_hasher);

    let (tx, rx) = oneshot::channel();
    // Force check must clear the stale state and reread the correct file contents.
    worker.handle_check_file(tx).unwrap();
    pump(&mut worker).await;
    assert!(rx.await.unwrap());
    assert!(downloading_state(&mut worker).block_picker.have(0));
    cleanup(worker, cache).await;
}

// Verify that force check rejects corrupt disk data even when the in-memory hasher looks correct.
#[tokio::test]
async fn force_check_rejects_corrupt_disk_data_with_stale_hasher() {
    // Corrupt the first sub-piece on disk, but leave a stale hasher built from the original data.
    let (mut worker, cache) = new_downloading_worker();
    let path = downloading_state(&mut worker).metadata.files[0].path[0].clone();
    let mut corrupt = vec![0x5a; 2 * SUB_PIECE_SIZE as usize];
    corrupt[..SUB_PIECE_SIZE as usize].fill(0x33);
    std::fs::write(path, corrupt).unwrap();
    let mut stale_hasher = HashState::new(Sha1::new());
    stale_hasher
        .write(&vec![0x5a; SUB_PIECE_SIZE as usize])
        .unwrap();
    downloading_state(&mut worker)
        .hasher
        .insert(0, stale_hasher);

    let (tx, rx) = oneshot::channel();
    // Force check must ignore the stale correct-looking prefix and reread the corrupt file.
    worker.handle_check_file(tx).unwrap();
    pump(&mut worker).await;
    assert!(
        !rx.await.unwrap(),
        "force check must detect the corrupt disk range"
    );
    assert!(!downloading_state(&mut worker).block_picker.have(0));
    cleanup(worker, cache).await;
}

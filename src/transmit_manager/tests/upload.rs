use super::hashing::{
    cleanup, downloading_state, new_downloading_worker, pump, queue_received_subpiece,
};
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

const BLOCK: u32 = 16 * 1024;

// Use a wire-level peer so both Fast and legacy handshakes are exercised.
async fn peer(worker: &mut TransmitWorker, fast: bool) -> (PeerAddr, DuplexStream) {
    let (local, mut remote) = tokio::io::duplex(1024 * 1024);
    let opt = HandshakeOption::builder()
        .client_id([53; 20])
        .pex(false)
        .metadata(false)
        .dht_port(None)
        .build();
    let accept = tokio::spawn(async move {
        protocol::BTStream::accept(local, async |_| protocol::AcceptOpt::NoMetadata, opt)
            .await
            .unwrap()
    });
    remote.write_all(b"\x13BitTorrent protocol").await.unwrap();
    let mut reserved = [0; 8];
    if fast {
        reserved[7] = 4;
    }
    remote.write_all(&reserved).await.unwrap();
    remote.write_all(&worker.info_hash).await.unwrap();
    remote.write_all(&[54; 20]).await.unwrap();
    let mut handshake = [0; 68];
    remote.read_exact(&mut handshake).await.unwrap();
    let conn = accept.await.unwrap();
    let id = worker.next_conn_id;
    worker.next_conn_id += 1;
    let addr = PeerAddr(conn.peer_addr(), id);
    let handle = ConnectionManagerHandle::new(
        conn,
        worker.self_handle.clone(),
        id,
        worker.block_pool.clone(),
    );
    assert_eq!(handle.capability().have(Capability::FAST), fast);
    worker.connected_peers.insert(
        addr,
        PeerConn::new(
            handle,
            id,
            [54; 20],
            #[cfg(feature = "metrics")]
            worker.info_hash,
            #[cfg(feature = "metrics")]
            addr.0,
            #[cfg(feature = "blocktrace")]
            None,
        ),
    );
    // Upload slot peer; tests release its queued requests with `upload_tick`.
    worker
        .connected_peers
        .get_mut(&addr)
        .unwrap()
        .state
        .our_choke_status = Some(false);
    (addr, remote)
}

// A pulled io batch carrying just `requests`, as a recv stream would hand it over.
fn io_msg(peer: PeerAddr, requests: &[Request]) -> Msg {
    Msg::PeerMsg(PeerMsg::IoMsg(
        peer,
        IoMsg {
            requests: requests.iter().copied().map(UploadReq::Request).collect(),
            ..Default::default()
        },
    ))
}

async fn response(remote: &mut DuplexStream) -> Vec<u8> {
    time::timeout(time::Duration::from_secs(2), async {
        loop {
            let len = remote.read_u32().await.unwrap();
            if len == 0 {
                continue;
            }
            let mut msg = vec![0; len as usize];
            remote.read_exact(&mut msg).await.unwrap();
            return msg;
        }
    })
    .await
    .expect("peer response")
}

fn reject(request: Request) -> Vec<u8> {
    let mut msg = vec![16];
    msg.extend_from_slice(&request.index.to_be_bytes());
    msg.extend_from_slice(&request.begin.to_be_bytes());
    msg.extend_from_slice(&request.len.to_be_bytes());
    msg
}

fn assert_piece(msg: &[u8], request: Request, bytes: &[u8]) {
    assert_eq!(msg[0], 7);
    assert_eq!(&msg[1..5], &request.index.to_be_bytes());
    assert_eq!(&msg[5..9], &request.begin.to_be_bytes());
    assert_eq!(&msg[9..], bytes);
    assert_eq!(bytes.len(), request.len as usize);
}

async fn seeded_worker() -> (TransmitWorker, tokio::task::JoinHandle<()>) {
    let (mut worker, cache) = new_downloading_worker();
    queue_received_subpiece(&mut worker, 0, 0x5a);
    queue_received_subpiece(&mut worker, 1, 0x5a);
    pump(&mut worker).await;
    assert!(downloading_state(&mut worker).block_picker.have(0));
    assert!(matches!(worker.running_state, RunningState::Seeding));
    (worker, cache)
}

#[tokio::test]
async fn upload_wire_requests_share_cache_wait_and_return_exact_bytes() {
    let (mut worker, cache) = seeded_worker().await;
    let (addr, mut remote) = peer(&mut worker, true).await;
    let requests = [
        Request {
            index: 0,
            begin: 0,
            len: BLOCK,
        },
        Request {
            index: 0,
            begin: BLOCK * 2,
            len: BLOCK,
        },
    ];
    for request in requests {
        remote.write_u32(13).await.unwrap();
        remote.write_u8(6).await.unwrap();
        remote.write_u32(request.index).await.unwrap();
        remote.write_u32(request.begin).await.unwrap();
        remote.write_u32(request.len).await.unwrap();
    }
    // Hold cache completions until both requests have been admitted.
    let mut saved = Vec::new();
    let mut count = 0;
    while count < requests.len() {
        let msg = time::timeout(time::Duration::from_secs(2), worker.receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match msg {
            // EventReady is what makes the worker pull the buffered io.
            Msg::PeerMsg(PeerMsg::EventReady { .. }) => worker.handle_msg(msg).await,
            Msg::PeerMsg(PeerMsg::IoMsg(p, io)) => {
                assert_eq!(p, addr);
                count += io.requests.len();
                worker.handle_msg(Msg::PeerMsg(PeerMsg::IoMsg(p, io))).await;
            }
            msg => saved.push(msg),
        }
    }
    worker.upload_tick();
    assert_eq!(worker.waiting_for_piecebuf.len(), 1);
    assert_eq!(
        worker.waiting_for_piecebuf[&JointIndex::new(0, 0)]
            .reads
            .len(),
        2
    );
    for msg in saved {
        worker.handle_msg(msg).await;
    }
    pump(&mut worker).await;
    for request in requests {
        assert_piece(
            &response(&mut remote).await,
            request,
            &vec![0x5a; BLOCK as usize],
        );
    }
    assert!(worker.waiting_for_piecebuf.is_empty());
    cleanup(worker, cache).await;
}

#[tokio::test]
async fn upload_unverified_or_invalid_requests_reject_fast_and_ignore_legacy() {
    let (mut worker, cache) = new_downloading_worker();
    queue_received_subpiece(&mut worker, 0, 0x5a);
    pump(&mut worker).await;
    assert!(
        downloading_state(&mut worker)
            .block_picker
            .have_sub(JointIndex::new(0, 0))
    );
    assert!(!downloading_state(&mut worker).block_picker.have(0));
    let (fast, mut fast_wire) = peer(&mut worker, true).await;
    let (legacy, mut legacy_wire) = peer(&mut worker, false).await;
    let request = Request {
        index: 0,
        begin: 0,
        len: BLOCK,
    };
    worker.handle_upload_requests(fast, &[UploadReq::Request(request)]);
    worker.handle_upload_requests(legacy, &[UploadReq::Request(request)]);
    assert_eq!(response(&mut fast_wire).await, reject(request));
    assert!(
        time::timeout(time::Duration::from_millis(100), response(&mut legacy_wire))
            .await
            .is_err()
    );
    assert!(worker.waiting_for_piecebuf.is_empty());

    queue_received_subpiece(&mut worker, 1, 0x5a);
    pump(&mut worker).await;
    assert_eq!(response(&mut fast_wire).await, vec![4, 0, 0, 0, 0]);
    assert_eq!(response(&mut legacy_wire).await, vec![4, 0, 0, 0, 0]);
    for bad in [
        Request {
            index: 1,
            ..request
        },
        Request { len: 0, ..request },
        Request {
            len: BLOCK + 1,
            ..request
        },
        Request {
            begin: 2 * SUB_PIECE_SIZE - 1,
            ..request
        },
        Request {
            begin: u32::MAX,
            ..request
        },
    ] {
        worker.handle_upload_requests(fast, &[UploadReq::Request(bad)]);
        assert_eq!(response(&mut fast_wire).await, reject(bad));
        assert!(worker.waiting_for_piecebuf.is_empty());
    }
    cleanup(worker, cache).await;
}

#[tokio::test]
async fn upload_restored_seed_reads_final_bytes_from_disk() {
    let (mut worker, cache) = seeded_worker().await;
    let handle = worker.cache_handle.clone();
    let pool = worker.block_pool.clone();
    worker.cleanup_before_stop().await;
    let dump = worker.handle_dump_status();
    let (tx, rx) = mpsc::unbounded_channel();
    let announce = AnnounceManagerHandle::new([53; 20], 0, dump.info_hash(), tx.clone());
    let mut worker =
        TransmitWorker::from_dump(dump, [53; 20], 0, None, announce, tx, rx, handle, pool);
    let (addr, mut remote) = peer(&mut worker, false).await;
    let request = Request {
        index: 0,
        begin: 2 * SUB_PIECE_SIZE - BLOCK,
        len: BLOCK,
    };
    worker.handle_upload_requests(addr, &[UploadReq::Request(request)]);
    worker.upload_tick();
    pump(&mut worker).await;
    assert_piece(
        &response(&mut remote).await,
        request,
        &vec![0x5a; BLOCK as usize],
    );
    cleanup(worker, cache).await;
}

#[tokio::test]
async fn upload_inactive_states_reject_or_defer_requests_by_phase() {
    let (mut worker, cache) = seeded_worker().await;
    let (addr, mut remote) = peer(&mut worker, true).await;
    let request = Request {
        index: 0,
        begin: 0,
        len: BLOCK,
    };
    for state in [
        RunningState::Paused,
        RunningState::Stopped,
        RunningState::Fatal("test".into()),
    ] {
        worker.running_state = state;
        worker.handle_msg(io_msg(addr, &[request])).await;
        assert_eq!(response(&mut remote).await, reject(request));
        assert!(worker.waiting_for_piecebuf.is_empty());
    }
    worker.running_state = RunningState::Seeding;
    let (tx, _rx) = oneshot::channel();
    worker.handle_check_file(tx).await.unwrap();
    assert!(matches!(
        worker.running_state,
        RunningState::Draining { .. }
    ));
    worker.handle_msg(io_msg(addr, &[request])).await;
    assert!(
        time::timeout(time::Duration::from_millis(100), response(&mut remote))
            .await
            .is_err()
    );
    assert!(worker.waiting_for_piecebuf.is_empty());
    assert!(matches!(
        &worker.running_state,
        RunningState::Draining { deferred, .. }
            if deferred.iter().any(|m| matches!(m, Msg::PeerMsg(PeerMsg::IoMsg(_, io)) if io.requests.contains(&UploadReq::Request(request))))
    ));
    loop {
        let msg = time::timeout(time::Duration::from_secs(2), worker.receiver.recv())
            .await
            .unwrap()
            .unwrap();
        worker.handle_msg(msg).await;
        if matches!(worker.running_state, RunningState::Checking { .. }) {
            break;
        }
    }
    let waiting = worker.waiting_for_piecebuf.len();
    worker.handle_msg(io_msg(addr, &[request])).await;
    assert!(
        time::timeout(time::Duration::from_millis(100), response(&mut remote))
            .await
            .is_err()
    );
    assert_eq!(worker.waiting_for_piecebuf.len(), waiting);
    if let RunningState::Checking { deferred, .. } = &worker.running_state {
        assert!(deferred.iter().any(
            |m| matches!(m, Msg::PeerMsg(PeerMsg::IoMsg(_, io)) if io.requests.contains(&UploadReq::Request(request)))
        ));
    }
    pump(&mut worker).await;
    cleanup(worker, cache).await;
}

#[tokio::test]
async fn upload_shutdown_discards_pending_reads_and_new_requests() {
    let (mut worker, cache) = seeded_worker().await;
    let (addr, mut remote) = peer(&mut worker, true).await;
    let (legacy, mut legacy_remote) = peer(&mut worker, false).await;
    let existing = Request {
        index: 0,
        begin: 0,
        len: BLOCK,
    };
    let new_request = Request {
        begin: SUB_PIECE_SIZE,
        ..existing
    };
    // Enqueue the new request first so cleanup sees it before the cache reply.
    worker
        .self_handle
        .sender
        .send(io_msg(addr, &[new_request]))
        .unwrap();
    worker
        .self_handle
        .sender
        .send(io_msg(legacy, &[new_request]))
        .unwrap();
    worker.handle_upload_requests(addr, &[UploadReq::Request(existing)]);
    worker.handle_upload_requests(legacy, &[UploadReq::Request(new_request)]);
    worker.upload_tick();
    assert_eq!(worker.waiting_for_piecebuf.len(), 2);
    time::timeout(time::Duration::from_secs(2), worker.cleanup_before_stop())
        .await
        .unwrap();
    assert!(worker.waiting_for_piecebuf.is_empty());
    // Both pending and newly arrived requests are dropped without a reply,
    // even for Fast peers.
    assert!(
        time::timeout(time::Duration::from_millis(100), response(&mut remote))
            .await
            .is_err()
    );
    assert!(
        time::timeout(
            time::Duration::from_millis(100),
            response(&mut legacy_remote)
        )
        .await
        .is_err()
    );
    cleanup(worker, cache).await;
}

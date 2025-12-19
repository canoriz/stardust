use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::io::{BufReader, BufWriter};
use tokio::sync::{mpsc, oneshot};
use tokio::time;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

use crate::bandwidth::Bandwidth;
use crate::cache::{AbortErr, ArcCache, BufStorage, GetRefErr, PieceBuf, PieceKey, Ref};
use crate::metadata::{self, Metadata};
use crate::picker::{start_receive_piece_block, BlockRequests, HeapPiecePicker, PieceState};
use crate::protocol::{
    self, BTStream, Capability, CapabilityMap, Conn, ExtendedMetadata, ExtendedMsg, Message, Piece,
    ReadStream, Reader, Request, Split, WriteStream, Writer,
};
use crate::transmit_manager::{Downloading, PeerMsg, TransmitManagerHandle};
use crate::transmit_manager::{Msg as TransmitMsg, TorrentState};

const BANDWIDTH_TIME_SLICE: time::Duration = time::Duration::from_millis(250);

#[derive(Debug)]
pub(crate) enum WakeUpOption {
    TimeUp(time::Duration),
    NBlock(usize),
}

#[derive(Debug)]
pub(crate) enum Msg {
    RequestBlocks(BlockRequests),
    Have(u32),
    Extend(ExtendedMsg),
    Reject(Request),

    // SendBlocks(BlockRange),
    SetWakeUp(WakeUpOption),
    ResetWakeUp(WakeUpOption),
}

pub(crate) struct ConnectionManagerHandle {
    recv_stream: RecvStreamHandle,
    send_stream: SendStreamHandle,
    capability: CapabilityMap,
    metadata_size: usize,
}

impl ConnectionManagerHandle {
    pub fn new<T>(conn: BTStream<T>, trh: TransmitManagerHandle) -> Self
    where
        T: AsyncRead + AsyncWrite + Split + Unpin + Send + 'static,
    {
        let capability = conn.capability();
        let metadata_size = conn.metadata_size();
        let (read_stream, write_stream) = conn.split_buffered();
        Self::from_splitted_buffered(read_stream, write_stream, trh, capability, metadata_size)
    }

    pub fn new_dyn(conn: BTStream<Box<dyn Conn>>, trh: TransmitManagerHandle) -> Self {
        let capability = conn.capability();
        let metadata_size = conn.metadata_size();
        let (read_stream, write_stream) = conn.split_buffered();
        Self::from_splitted_buffered(read_stream, write_stream, trh, capability, metadata_size)
    }

    fn from_splitted_buffered<R, W>(
        read_stream: ReadStream<BufReader<R>>,
        write_stream: WriteStream<BufWriter<W>>,
        trh: TransmitManagerHandle,
        capability: CapabilityMap,
        metadata_size: usize,
    ) -> Self
    where
        R: protocol::Reader,
        W: protocol::Writer,
    {
        let (recv_tx, recv_rx) = mpsc::unbounded_channel();
        let (recv_done_tx, recv_done_rx) = oneshot::channel();
        let recv_cancel = CancellationToken::new();
        let addr = read_stream.peer_addr();
        let conn_break_guard = Arc::new(NotifyTransmitGuard {
            addr,
            transmit_handle: trh.clone(),
        });

        let n_sent_req = Arc::new(AtomicU32::new(0));
        let n_recv_req = Arc::new(AtomicU32::new(0));
        let recv_stream = RecvStream::<BufReader<R>> {
            receiver: recv_rx,
            read_stream,
            transmit_handle: trh.clone(),
            bw: Bandwidth::new(BANDWIDTH_TIME_SLICE),
            n_recv_req,
            n_sent_req: n_sent_req.clone(),
            history_n_recv_req: VecDeque::new(),
            history_n_sent_req: VecDeque::new(),
            _drop_guard: conn_break_guard.clone(),
        };

        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let send_cancel = CancellationToken::new();
        let (send_done_tx, send_done_rx) = oneshot::channel();
        let send_stream = SendStream::<BufWriter<W>> {
            receiver: send_rx,
            write_stream,
            n_sent_req,
            _drop_guard: conn_break_guard,
        };

        tokio::spawn(run_recv_stream(
            recv_stream,
            recv_cancel.clone(),
            recv_done_tx,
        ));
        tokio::spawn(run_send_stream(
            send_stream,
            send_cancel.clone(),
            send_done_tx,
        ));
        let recv_stream_handle = RecvStreamHandle {
            sender: recv_tx,
            cancel: recv_cancel.drop_guard(),
            done: recv_done_rx,
        };
        let send_stream_handle = SendStreamHandle {
            sender: send_tx,
            cancel: send_cancel.drop_guard(),
            done: send_done_rx,
        };

        Self {
            recv_stream: recv_stream_handle,
            send_stream: send_stream_handle,
            capability,
            metadata_size,
        }
    }

    // fn handle_msg(&mut self, m: Msg) {
    //     match m {
    //         Msg::RequestBlocks(r) => {
    //             const BLOCK_SIZE: usize = 16 * 1024;
    //             for index in r.from.index..=r.to.index {
    //                 for begin in (0..r.piece_size).step_by(BLOCK_SIZE) {
    //                     todo!();
    //                 }
    //             }
    //             // TODO: let send task to send data
    //         }
    //         Msg::SendBlocks(r) => {
    //             todo!();
    //         }
    //         _ => {
    //             todo!()
    //         }
    //     }
    // }

    pub fn send_stream_cmd(&self, m: Msg) {
        self.send_stream.sender.send(m);
    }

    pub fn support_metadata_extension(&self) -> bool {
        self.capability.contains(&Capability::Metadata)
    }

    pub fn capability(&self) -> &CapabilityMap {
        &self.capability
    }

    pub fn metadata_size(&self) -> usize {
        self.metadata_size
    }

    // pub fn request(&self, br: BlockRange) {
    //     let piece_length = self.metadata.info.piece_length;
    //     // send task notify
    // }
    pub async fn stop_wait(self) {
        self.recv_stream.cancel.disarm().cancel();
        self.send_stream.cancel.disarm().cancel();
        _ = self.send_stream.done.await;
        _ = self.recv_stream.done.await;
        info!("connection manager cancelled");
    }
}

type PeerAddr = SocketAddr;
struct NotifyTransmitGuard {
    addr: PeerAddr,
    transmit_handle: TransmitManagerHandle,
}

impl Drop for NotifyTransmitGuard {
    fn drop(&mut self) {
        println!("both send and recv end of {} stopped", self.addr);
        self.transmit_handle
            .sender
            .send(TransmitMsg::PeerLeave(self.addr));
    }
}

struct RecvStreamHandle {
    sender: mpsc::UnboundedSender<Msg>,
    cancel: DropGuard,
    done: oneshot::Receiver<()>,
}

struct RecvStream<T> {
    receiver: mpsc::UnboundedReceiver<Msg>,
    read_stream: ReadStream<T>,
    transmit_handle: TransmitManagerHandle,
    _drop_guard: Arc<NotifyTransmitGuard>,

    /// number or received pieces in a period
    n_recv_req: Arc<AtomicU32>,

    /// number or sent pieces in a period
    n_sent_req: Arc<AtomicU32>,

    /// history of number of received requests in every tick
    history_n_recv_req: VecDeque<u32>,

    /// history of number of sent requests in every tick
    history_n_sent_req: VecDeque<u32>,

    bw: Bandwidth<10>,
}

struct SendStreamHandle {
    sender: mpsc::UnboundedSender<Msg>,
    cancel: DropGuard,
    done: oneshot::Receiver<()>,
}

struct SendStream<T> {
    receiver: mpsc::UnboundedReceiver<Msg>,
    write_stream: WriteStream<T>,

    /// number or send requests in a period
    n_sent_req: Arc<AtomicU32>,

    _drop_guard: Arc<NotifyTransmitGuard>,
}

async fn run_recv_stream<T>(
    mut conn: RecvStream<T>,
    cancel: CancellationToken,
    done: oneshot::Sender<()>,
) where
    T: AsyncRead + Unpin,
{
    info!("in recv stream");
    let report_interval = time::Duration::from_millis(1000);
    let mut ticker = tokio::time::interval(report_interval);
    let addr = conn.read_stream.peer_addr();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("recv stream cancelled");
                break;
            }
            Some(msg) = conn.receiver.recv() => {
                // TODO: need handle None case
                // TODO: use buffer and tokio::Notify
                // info!("connection manager recv stream of {} received msg {msg:?}", &manager.conn);
            }
            _ = ticker.tick() => {
                // TODO: many ticks may come together, unfair
                // debug!("recv conn ticker tick {} block received in this epoch", conn.blk_recv_count);
                conn.handle_report_tick(report_interval);
            }
            r = conn.read_stream.recv_msg() => {
                // r = receive_peer_msg(&mut conn.read_stream, &mut conn.transmit_handle) => {
                match r {
                    Ok(msg) => {
                        // (handle_peer_hdr(&mut conn, addr, hdr));
                        conn.handle_peer_msg(addr, msg).await;
                    }
                    Err(e) => {
                        warn!("recv stream read header error {e}");
                        // TODO: notify controller and maybe try re-connect
                        break;
                    }
                }
            }
        };
    }
    let _ = done.send(());
    info!("done recv stream");
}

// TODO: socketaddr use ref?
// TODO: returns some more meaningful val
// returns if one block is received
impl<T> RecvStream<T>
where
    T: AsyncRead + Unpin,
{
    fn handle_report_tick(&mut self, interval: time::Duration) {
        const TRACE_WINDOW: usize = 30;
        if self.history_n_recv_req.len() < TRACE_WINDOW {
            self.history_n_recv_req
                .push_back(self.n_recv_req.load(Ordering::Relaxed));
            self.history_n_sent_req
                .push_back(self.n_sent_req.load(Ordering::Relaxed));
        } else {
            let n_recv_ago = self.history_n_recv_req.pop_front().unwrap();
            let n_sent_ago = self.history_n_sent_req.pop_front().unwrap();

            self.n_recv_req.fetch_sub(n_recv_ago, Ordering::Relaxed);
            self.n_sent_req.fetch_sub(n_sent_ago, Ordering::Relaxed);

            for v in self.history_n_recv_req.iter_mut() {
                *v = *v - n_recv_ago;
            }
            for v in self.history_n_sent_req.iter_mut() {
                *v = *v - n_sent_ago;
            }

            self.history_n_recv_req
                .push_back(self.n_recv_req.load(Ordering::Relaxed));
            self.history_n_sent_req
                .push_back(self.n_sent_req.load(Ordering::Relaxed));
        }

        let n_req_in_flight = {
            let sent = self.n_sent_req.load(Ordering::Relaxed) as i32;
            let recv = self.n_recv_req.load(Ordering::Relaxed) as i32;
            warn!("sent: {sent}, recv: {recv}");
            sent - recv
        };

        self.transmit_handle
            .sender
            .send(TransmitMsg::PeerMsg(PeerMsg::BlockReceived {
                peer: self.read_stream.peer_addr(),
                estimated_bw: self.bw.count(interval),
                n_req_in_flight: n_req_in_flight.max(0) as usize,
            }));
    }

    async fn handle_peer_msg(&mut self, addr: SocketAddr, m: Message) {
        // TODO: send statistics to transmit handle

        // TODO: shall we use mpsc or just lock the manager and set it
        // since this is generally a sync operation

        // TODO: maybe use bounded channel?
        let tmh = &mut self.transmit_handle;
        match m {
            Message::KeepAlive => {
                // do nothing
                info!("ka");
            }
            Message::Choke => {
                info!("ck");
                // TODO: drop all pending requests
                // stop sending all requests
                let r = tmh.sender.send(TransmitMsg::PeerMsg(PeerMsg::Choke(addr)));
                if let Err(e) = r {
                    warn!("error send unchoke to transmit manager {e}")
                }
            }
            Message::Unchoke => {
                info!("uck");
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::Unchoke(addr)));
            }
            Message::Interested => {
                // TODO: update peer state
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::Interested(addr)));
            }
            Message::NotInterested => {
                // TODO: update peer state
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::Uninterested(addr)));
            }
            Message::Have(i) => {
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::Have(addr, i)));
            }
            Message::BitField(bf) => {
                info!("bf");
                // TODO: handle error
                tmh.sender.send(TransmitMsg::PeerMsg(PeerMsg::PieceState(
                    addr,
                    PieceState::Bitfield(bf),
                )));
            }
            Message::Request(req) => {
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::Request(addr, req)));
            }
            Message::Piece(piece) => {
                self.bw.add(piece.len as usize);
                self.n_recv_req.fetch_add(1, Ordering::Relaxed);
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::Piece(addr, piece)));
            }
            Message::Cancel(req) => {
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::Cancel(addr, req)));
            }
            Message::Port(port) => {
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::DhtPort(addr, port)));
            }
            Message::Extended(extend) => {
                handle_extended_msg(&addr, tmh, extend).await;
            }
            Message::SuggestPiece(index) => {
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::SuggestPiece(addr, index)));
            }
            Message::AllowedFast(index) => {
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::AllowedFast(addr, index)));
            }
            Message::HaveAll => {
                tmh.sender.send(TransmitMsg::PeerMsg(PeerMsg::PieceState(
                    addr,
                    PieceState::HaveAll,
                )));
            }
            Message::HaveNone => {
                tmh.sender.send(TransmitMsg::PeerMsg(PeerMsg::PieceState(
                    addr,
                    PieceState::HaveNone,
                )));
            }
            Message::Reject(req) => {
                self.n_recv_req.fetch_add(1, Ordering::Relaxed);
                tmh.sender
                    .send(TransmitMsg::PeerMsg(PeerMsg::Reject(addr, req)));
            }
        }
    }
}

async fn run_send_stream<T>(
    mut conn: SendStream<T>,
    cancel: CancellationToken,
    done: oneshot::Sender<()>,
) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    let mut interval = tokio::time::interval(time::Duration::from_secs(120));
    // conn.write_stream.send_interested().await;
    conn.write_stream.maybe_send_pending_msg().await?;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("send stream cancelled");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = conn.write_stream.send_keepalive().await {
                    info!("send keepalive error {e}");
                    // TODO: tell transmit manager this connection is dead
                    break;
                }
            }
            Some(msg) = conn.receiver.recv() => {
                // TODO: maybe use buffer and Notify?
                info!("send stream received {msg:?}");
                conn.handle_cmd(msg).await;
            }
        };
    }
    let _ = done.send(());
    info!("done send stream");
    Ok(())
}

impl<T> SendStream<T>
where
    T: AsyncWrite + Unpin,
{
    async fn handle_cmd(&mut self, msg: Msg) {
        match msg {
            Msg::RequestBlocks(reqs) => {
                let piece_size = reqs.piece_size;
                for rg in reqs.range.iter() {
                    for r in rg.iter(piece_size) {
                        self.n_sent_req.fetch_add(1, Ordering::Relaxed);
                        self.write_stream
                            .send_request(r.index, r.begin, r.len)
                            .await;
                    }
                }
            }
            Msg::Have(i) => {
                self.write_stream.send_have(i).await;
            }
            Msg::Extend(ExtendedMsg::Metadata(m)) => {
                self.write_stream.send_extend_metadata(m).await;
            }
            other => {}
        }
    }
}

/*
struct WriteEnd<T> {
    inner: Arc<WriteEndInner<T>>,
}

struct WriteEndInner<T> {
    wr: Mutex<WriteStream<T>>,
}

impl<T> WriteEndInner<T>
where
    T: AsyncWrite + Unpin,
{
    async fn async_op(&mut self) {
        let lock = self.wr.try_lock();

        if let Ok(ref mut mutex) = lock {
            *mutex.send_keepalive();
        } else {
            println!("try_lock failed");
        }
    }
    fn sync_op(&mut self) {}
    fn send_keepalive(&mut self) {
        // need non-blocking version for recv task
        // and blocking/async version for send task

        // if send task has remaining work
        // call send task to add work(maintain order)

        // if send task does not have remaining work
        // try to non-blocking do all the work
        // maybe half done, keep states and let send task finish the rest
    }
}
*/

async fn handle_extended_msg(
    peer: &SocketAddr,
    tmh: &mut TransmitManagerHandle,
    extended: protocol::ExtendedMsg,
) -> io::Result<()> {
    match extended {
        ExtendedMsg::Handshake(hs) => todo!(),
        ExtendedMsg::Pex(pex) => {
            info!("received pex from {peer}, {pex:?}");
            Ok(())
        }
        ExtendedMsg::Metadata(m) => {
            _ = tmh
                .sender
                .send(TransmitMsg::PeerMsg(PeerMsg::ExtendMetadata(*peer, m)));
            Ok(())
        }
        ExtendedMsg::Unknown(id) => {
            warn!("received unknown extend message: id {id}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::{tests::make_ends_tune, HandshakeOption};
    use crate::transmit_manager::Msg;

    #[tokio::test]
    async fn test_dht_delayed_msg() {
        // opt support dht
        let opt = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .info_hash([0; 20])
            .dht_port(Some(1))
            .build();
        let (end1, end2) = make_ends_tune(opt.clone(), opt).await;

        let (tx, mut rx1) = mpsc::unbounded_channel();
        let tmh1 = TransmitManagerHandle { sender: tx };
        let (tx, mut rx2) = mpsc::unbounded_channel();
        let tmh2 = TransmitManagerHandle { sender: tx };

        let _c1 = ConnectionManagerHandle::new(end1, tmh1.clone());
        let _c2 = ConnectionManagerHandle::new(end2, tmh2.clone());
        let first1 = rx1.recv().await.unwrap();
        let first2 = rx2.recv().await.unwrap();
        let inner1 = match first1 {
            Msg::PeerMsg(PeerMsg::DhtPort(sa, p)) => (sa, p),
            _ => unreachable!(),
        };
        let inner2 = match first2 {
            Msg::PeerMsg(PeerMsg::DhtPort(sa, p)) => (sa, p),
            _ => unreachable!(),
        };
        assert_eq!(inner1, inner2);
        assert_eq!(inner1.1, 1);
    }

    #[tokio::test]
    async fn test_notify_transit() {
        // opt support dht
        let opt = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .info_hash([0; 20])
            .dht_port(None)
            .build();
        let (end1, end2) = make_ends_tune(opt.clone(), opt).await;

        let (tx, mut rx1) = mpsc::unbounded_channel();
        let tmh1 = TransmitManagerHandle { sender: tx };
        let (tx, _rx2) = mpsc::unbounded_channel();
        let tmh2 = TransmitManagerHandle { sender: tx };

        let _c1 = ConnectionManagerHandle::new(end1, tmh1.clone());
        let c2 = ConnectionManagerHandle::new(end2, tmh2.clone());

        drop(c2);
        // after drop, c1 recv should fail, and generate a PeerLeave to transmit handle
        match rx1.recv().await {
            Some(Msg::PeerLeave(_)) => {}
            _ => unreachable!(),
        }
    }
}

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::time;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

use crate::cache::{AbortErr, ArcCache, BufStorage, GetRefErr, PieceBuf, PieceKey, Ref};
use crate::metadata::{self, Metadata};
use crate::picker::{start_receive_piece_block, BlockRequests, HeapPiecePicker};
use crate::protocol::{
    self, BTStream, Capability, CapabilityMap, Conn, ExtendedMetadata, ExtendedMsg, Message, Piece,
    ReadStream, Reader, Split, WriteStream, Writer,
};
use crate::transmit_manager::{Downloading, TransmitManagerHandle};
use crate::transmit_manager::{Msg as TransmitMsg, TorrentState};

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
    pub fn new<T>(conn: BTStream<T>, trh: TransmitManagerHandle, m: Arc<metadata::Metadata>) -> Self
    where
        T: AsyncRead + AsyncWrite + Split + Unpin + Send + 'static,
    {
        let capability = conn.capability();
        let metadata_size = conn.metadata_size();
        use tokio::io::{BufReader, BufWriter};
        let (read_stream, write_stream) = conn.split_buffered();

        let (recv_tx, recv_rx) = mpsc::unbounded_channel();
        let (recv_done_tx, recv_done_rx) = oneshot::channel();
        let recv_cancel = CancellationToken::new();
        let recv_stream = RecvStream::<BufReader<<T as Split>::R>> {
            receiver: recv_rx,
            read_stream,
            transmit_handle: trh,
            blk_recv_count: 0,
        };

        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let send_cancel = CancellationToken::new();
        let (send_done_tx, send_done_rx) = oneshot::channel();
        let send_stream = SendStream::<BufWriter<<T as Split>::W>> {
            receiver: send_rx,
            write_stream,
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

    pub fn new_dyn(conn: BTStream<Box<dyn Conn>>, trh: TransmitManagerHandle) -> Self {
        let capability = conn.capability();
        let metadata_size = conn.metadata_size();
        use tokio::io::{BufReader, BufWriter};
        let (read_stream, write_stream) = conn.split_buffered();

        let (recv_tx, recv_rx) = mpsc::unbounded_channel();
        let (recv_done_tx, recv_done_rx) = oneshot::channel();
        let recv_cancel = CancellationToken::new();
        let recv_stream = RecvStream::<BufReader<Box<dyn Reader>>> {
            receiver: recv_rx,
            read_stream,
            transmit_handle: trh,
            blk_recv_count: 0,
        };

        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let send_cancel = CancellationToken::new();
        let (send_done_tx, send_done_rx) = oneshot::channel();
        let send_stream = SendStream::<BufWriter<Box<dyn Writer>>> {
            receiver: send_rx,
            write_stream,
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

struct RecvStreamHandle {
    sender: mpsc::UnboundedSender<Msg>,
    cancel: DropGuard,
    done: oneshot::Receiver<()>,
}

struct RecvStream<T> {
    receiver: mpsc::UnboundedReceiver<Msg>,
    read_stream: ReadStream<T>,
    transmit_handle: TransmitManagerHandle,

    blk_recv_count: u32,
}

struct SendStreamHandle {
    sender: mpsc::UnboundedSender<Msg>,
    cancel: DropGuard,
    done: oneshot::Receiver<()>,
}

struct SendStream<T> {
    receiver: mpsc::UnboundedReceiver<Msg>,
    write_stream: WriteStream<T>,
    // TODO: do we use this to get blocks to requests?
    // so we can receive requests from recv_handle
    // transmit_handle: TransmitManagerHandle,
}

async fn run_recv_stream<T>(
    mut conn: RecvStream<T>,
    cancel: CancellationToken,
    done: oneshot::Sender<()>,
) where
    T: AsyncRead + Unpin,
{
    info!("in recv stream");
    let mut ticker = tokio::time::interval(time::Duration::from_millis(1000));
    let addr = conn.read_stream.peer_addr();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("recv stream cancelled");
                break;
            }
            Some(msg) = conn.receiver.recv() => {
                // TODO: use buffer and tokio::Notify
                // info!("connection manager recv stream of {} received msg {msg:?}", &manager.conn);
            }
            _ = ticker.tick() => {
                // TODO: many ticks may come together, unfair
                // debug!("recv conn ticker tick {} block received in this epoch", conn.blk_recv_count);
                conn.transmit_handle.sender.send(TransmitMsg::BlockReceived(conn.read_stream.peer_addr(), conn.blk_recv_count));
                conn.blk_recv_count = 0;
            }
            r = conn.read_stream.recv_msg() => {
                // r = receive_peer_msg(&mut conn.read_stream, &mut conn.transmit_handle) => {
                match r {
                    Ok(msg) => {
                        // (handle_peer_hdr(&mut conn, addr, hdr));
                        let n_blk = handle_peer_msg(&mut conn.transmit_handle, addr, msg).await;
                        conn.blk_recv_count += n_blk;
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

// // TODO: change a better name
// async fn handle_peer_hdr<'a, T, U>(
//     tmh: &'a mut TransmitManagerHandle,
//     addr: SocketAddr,
//     hdr: Message<'a, U>,
// ) -> u32
// where
//     T: Split,
//     U: AsyncRead + Unpin,
// {
//     info!("received BT msg hdr {hdr:?}");
//     handle_peer_msg(tmh, addr, hdr).await
// }

// TODO: socketaddr use ref?
// TODO: returns some more meaningful val
// returns if one block is received
async fn handle_peer_msg(tmh: &mut TransmitManagerHandle, addr: SocketAddr, m: Message) -> u32 {
    // TODO: send statistics to transmit handle

    // TODO: shall we use mpsc or just lock the manager and set it
    // since this is generally a sync operation

    // TODO: maybe use bounded channel?
    match m {
        Message::KeepAlive => {
            // do nothing
            info!("ka");
            0
        }
        Message::Choke => {
            info!("ck");
            // TODO: drop all pending requests
            // stop sending all requests
            let r = tmh.sender.send(TransmitMsg::PeerChoke(addr));
            if let Err(e) = r {
                warn!("error send unchoke to transmit manager {e}")
            }
            0
        }
        Message::Unchoke => {
            info!("uck");
            tmh.sender.send(TransmitMsg::PeerUnchoke(addr));
            0
        }
        Message::Interested => {
            // TODO: update peer state
            tmh.sender.send(TransmitMsg::PeerInterested(addr));
            0
        }
        Message::NotInterested => {
            // TODO: update peer state
            tmh.sender.send(TransmitMsg::PeerUninterested(addr));
            0
        }
        Message::Have(i) => {
            tmh.sender.send(TransmitMsg::PeerHave(addr, i));
            0
        }
        Message::BitField(bf) => {
            info!("bf");
            // TODO: handle error
            tmh.sender.send(TransmitMsg::PeerBitField(addr, bf));
            0
        }
        Message::Request(request) => {
            // TODO:
            // if in cache, mark cache in use
            // add to send queue, wake sending task
            // if not in cache, send to background fetch task
            // when block fetched, wake sending task
            0
        }
        Message::Piece(piece) => {
            tmh.sender.send(TransmitMsg::PeerRecvPiece(addr, piece));
            1
        }
        Message::Cancel(request) => {
            // TODO: cancel pending request/fetch task
            // todo!();
            0
        }
        Message::Port(port) => {
            // TODO
            0
        }
        Message::Extended(extend) => {
            handle_extended_msg(&addr, tmh, extend).await;
            1
        }
    }
}

async fn run_send_stream<T>(
    mut conn: SendStream<T>,
    cancel: CancellationToken,
    done: oneshot::Sender<()>,
) where
    T: AsyncWrite + Unpin,
{
    let mut interval = tokio::time::interval(time::Duration::from_secs(120));
    // conn.write_stream.send_interested().await;

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
    info!("handle_extended_msg {extended:?}");

    match extended {
        ExtendedMsg::Handshake(hs) => todo!(),
        ExtendedMsg::Pex(pex) => {
            info!("received pex from {peer}, {pex:?}");
            Ok(())
        }
        ExtendedMsg::Metadata(m) => {
            _ = tmh.sender.send(TransmitMsg::ExtendMetadata(*peer, m));
            Ok(())
        }
        ExtendedMsg::Unknown(id) => {
            warn!("received unknown extend message: id {id}");
            Ok(())
        }
    }
}

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time;
use tokio::sync::{mpsc, oneshot, Notify};

use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

use crate::backfile::WriteJob;
use crate::metadata;
use crate::picker::{BlockRange, BlockRequests};
use crate::protocol::{self, BTStream, Message, ReadStream, Split, WriteStream};
use crate::storage::ArcCache;
use crate::transmit_manager::Msg as TransmitMsg;
use crate::transmit_manager::{self, TransmitManagerHandle};

#[derive(Debug)]
pub(crate) enum WakeUpOption {
    TimeUp(time::Duration),
    NBlock(usize),
}

#[derive(Debug)]
pub(crate) enum Msg {
    RequestBlocks(BlockRequests),
    Have(u32),
    // SendBlocks(BlockRange),
    SetWakeUp(WakeUpOption),
    ResetWakeUp(WakeUpOption),
}

pub(crate) struct ConnectionManagerHandle {
    recv_stream: RecvStreamHandle,
    send_stream: SendStreamHandle,
}

impl ConnectionManagerHandle {
    pub fn new<T>(conn: BTStream<T>, trh: TransmitManagerHandle, m: Arc<metadata::Metadata>) -> Self
    where
        T: AsyncRead + AsyncWrite + Split + Unpin + Send + 'static,
    {
        let (read_stream, write_stream) = conn.split();

        let (recv_tx, recv_rx) = mpsc::unbounded_channel();
        let (recv_cancel_tx, recv_cancel_rx) = oneshot::channel();
        let (recv_done_tx, recv_done_rx) = oneshot::channel();
        let recv_stream_handle = RecvStreamHandle {
            sender: recv_tx,
            cancel: recv_cancel_tx,
            done: recv_done_rx,
        };
        let recv_stream: RecvStream<T> = RecvStream {
            receiver: recv_rx,
            read_stream: read_stream,
            transmit_handle: trh,
            blk_recv_count: 0,
        };

        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let (send_cancel_tx, send_cancel_rx) = oneshot::channel();
        let (send_done_tx, send_done_rx) = oneshot::channel();
        let send_stream_handle = SendStreamHandle {
            sender: send_tx,
            cancel: send_cancel_tx,
            done: send_done_rx,
        };
        let send_stream: SendStream<T> = SendStream {
            receiver: send_rx,
            write_stream: write_stream,
        };
        tokio::spawn(run_recv_stream(recv_stream, recv_cancel_rx, recv_done_tx));
        tokio::spawn(run_send_stream(send_stream, send_cancel_rx, send_done_tx));

        Self {
            recv_stream: recv_stream_handle,
            send_stream: send_stream_handle,
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

    // pub fn request(&self, br: BlockRange) {
    //     let piece_length = self.metadata.info.piece_length;
    //     // send task notify
    // }

    pub async fn stop(self) {
        self.recv_stream.cancel.send(());
        self.recv_stream.done.await;
        self.send_stream.cancel.send(());
        self.send_stream.done.await;
        info!("connection manager cancelled");
    }
}

struct RecvStreamHandle {
    sender: mpsc::UnboundedSender<Msg>,
    cancel: oneshot::Sender<()>,
    done: oneshot::Receiver<()>,
}

struct RecvStream<T>
where
    T: Split,
{
    receiver: mpsc::UnboundedReceiver<Msg>,
    read_stream: ReadStream<<T as Split>::R>,
    transmit_handle: TransmitManagerHandle,

    blk_recv_count: u32,
}

struct SendStreamHandle {
    sender: mpsc::UnboundedSender<Msg>,
    cancel: oneshot::Sender<()>,
    done: oneshot::Receiver<()>,
}

struct SendStream<T>
where
    T: Split,
{
    receiver: mpsc::UnboundedReceiver<Msg>,
    write_stream: WriteStream<<T as Split>::W>,
    // TODO: do we use this to get blocks to requests?
    // so we can receive requests from recv_handle
    // transmit_handle: TransmitManagerHandle,
}

async fn run_recv_stream<T>(
    mut conn: RecvStream<T>,
    mut cancel: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) where
    T: Split,
{
    info!("in recv stream");
    let mut ticker = tokio::time::interval(time::Duration::from_millis(1000));
    let addr = conn.read_stream.peer_addr();
    loop {
        tokio::select! {
            biased;
            _ = &mut cancel => {
                info!("recv stream cancelled");
                break;
            }
            Some(msg) = conn.receiver.recv() => {
                // TODO: use buffer and tokio::Notify
                // info!("connection manager recv stream of {} received msg {msg:?}", &manager.conn);
            }
            _ = ticker.tick() => {
                // TODO: many ticks may come together, unfair
                info!("recv conn ticker tick {} block received in this epoch", conn.blk_recv_count);
                conn.transmit_handle.sender.send(TransmitMsg::BlockReceived(conn.read_stream.peer_addr(), conn.blk_recv_count));
                conn.blk_recv_count = 0;
            }
            r = conn.read_stream.recv_msg_header() => {
                // r = receive_peer_msg(&mut conn.read_stream, &mut conn.transmit_handle) => {
                match r {
                    Ok(hdr) => {
                        // (handle_peer_hdr(&mut conn, addr, hdr));
                        let n_blk = handle_peer_msg(&mut conn.transmit_handle, addr, hdr).await;
                        conn.blk_recv_count += n_blk;
                    }
                    Err(e) => {
                        warn!("recv stream read header error {e}");
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
async fn handle_peer_msg<'a, R>(
    tmh: &'a mut TransmitManagerHandle,
    addr: SocketAddr,
    m: Message<'a, R>,
) -> u32
where
    R: AsyncRead + Unpin,
{
    info!("handle_peer_msg from {addr} {m:?}");
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
            tmh.sender.send(TransmitMsg::PeerInterested);
            0
        }
        Message::NotInterested => {
            // TODO: update peer state
            tmh.sender.send(TransmitMsg::PeerUninterested);
            0
        }
        Message::Have(i) => {
            tmh.sender.send(TransmitMsg::PeerHave(addr, i));
            0
        }
        Message::BitField(mut bf_recv) => {
            info!("bf");
            // TODO: handle error
            let bit_field = bf_recv.read().await.unwrap();
            tmh.sender.send(TransmitMsg::PeerBitField(addr, bit_field));
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
            handle_piece_msg(&addr, tmh, piece).await;
            1
        }
        Message::Cancel(request) => {
            // TODO: cancel pending request/fetch task
            todo!();
            0
        }
    }
}

async fn run_send_stream<T>(
    mut conn: SendStream<T>,
    mut cancel: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) where
    T: Split,
{
    let mut interval = tokio::time::interval(time::Duration::from_secs(120));
    // conn.write_stream.send_interested().await;

    loop {
        tokio::select! {
            _ = &mut cancel => {
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
    T: Split,
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

// TODO: use &mut piece?
async fn handle_piece_msg<T>(
    peer: &SocketAddr,
    tmh: &mut TransmitManagerHandle,
    mut piece: protocol::Piece<'_, T>,
) where
    T: AsyncRead + Unpin,
{
    // TODO:
    // if coming piece have cache, store it in cache
    // if coming piece don't have cache, ???
    // tell manager?
    info!("handle_piece_msg {piece:?}");

    // TODO: if coming block is already received and checked,
    // then discard this block, don't alloc (if needed) piece buffer.
    // And there should be no piece buffer of this piece in pb_map

    let already_have = tmh.picker.lock().unwrap().have_block(&protocol::Request {
        index: piece.index,
        begin: piece.begin,
        len: piece.len,
    });

    if already_have {
        // TODO: make this persistent
        let mut drain = vec![0u8; piece.len as usize];
        piece.read(&mut drain).await;
        // TODO: why this happen (at testing)?
        // seems we are requesting twice for each piece
        warn!(
            "drain PIECE msg {} {} {} block index {}",
            piece.index,
            piece.begin,
            piece.len,
            piece.begin >> 14,
        );
        return;
    }
    warn!(
        "receive PIECE msg {} {} {} block index {}",
        piece.index,
        piece.begin,
        piece.len,
        piece.begin >> 14,
    );

    let (mut block_ref, piece_buf) = {
        // must drop pb_map before await point
        // pb_map is not Send
        let mut pb_map = tmh.piece_buffer.lock().unwrap();
        let pb = match pb_map.get(&piece.index) {
            Some(pb) => pb.clone(),
            None => {
                let pb = ArcCache::new(tmh.piece_size);
                pb_map.insert(piece.index, pb.clone());
                pb
            }
        };

        let block_ref = pb.get_part_ref(
            piece.begin as usize,
            // TODO: change 16384 to const
            (piece.len.next_multiple_of(16384)) as usize,
        );

        if block_ref.is_none() {
            warn!(
                "error get block buf of piece {} block offset {}",
                piece.index, piece.begin
            );
        }
        (block_ref, pb)
    };

    // TODO: need a biglock. What if some peer else is doing operation now?
    // i.e. operation between two locks?
    let block_buf = if let Some(mut bbuf) = block_ref {
        piece.read(bbuf.to_slice_len(piece.len as usize)).await;
        bbuf
    } else {
        let mut drain = vec![0u8; piece.len as usize];
        piece.read(&mut drain).await;
        warn!(
            "drain PIECE msg {} {} {}",
            piece.index, piece.begin, piece.len
        );
        // TODO: what should we do now?
        // we don't have that space
        // maybe reads to supplementary buffer?
        // just return now
        return;
    };

    let received_piece = tmh.picker.lock().unwrap().block_received(
        peer,
        protocol::Request {
            index: piece.index,
            begin: piece.begin,
            len: piece.len,
        },
    );
    if let Some(i) = received_piece {
        info!("piece {i} received");
        // TODO: maybe returns and let upper fn sends this message
        tmh.sender.send(TransmitMsg::PieceReceived(i));

        // block new ref to piece buffer
        // so ref count only decreases
        piece_buf.disable_new_ref();

        let (write_tx, write_rx) = tokio::sync::oneshot::channel::<std::io::Result<()>>();
        let write_job = {
            // TODO: mark piece_buf as ready to write,
            // get_part_ref should stop return new refs
            // TODO: extend_to_entire may fail, if every Ref extend fail,
            // will leaving completed block not written to disk.
            // i.e. some Ref may not call extend_to_entire because the piece
            // did not complete from their views.
            if let Some(mut entire_block) = block_buf.extend_to_entire() {
                let pbuf_s = entire_block.to_slice();

                let bf_copy = tmh.back_file.clone();
                let piece_size = tmh.piece_size;
                tmh.piece_buffer.lock().expect("lock should ok").remove(&i);

                Some(WriteJob {
                    f: bf_copy,
                    offset: (i as usize) * piece_size,
                    buf: pbuf_s,
                    write_tx,
                })
            } else {
                warn!("some one holding block ref in piece {i}, give up writing",);
                None
            }
        };

        if let Some(wj) = write_job {
            if let Err(e) = tmh.write_worker.send(wj) {
                warn!("error sending write job to worker error {e:?}");
            }
            let write_res = write_rx.await;
            // let r = hdl.await;
            info!("write piece {i} result {write_res:?}");
            // TODO: should really check sha1 of piece
            tmh.picker.lock().unwrap().piece_checked(i);
            // TODO: check write succes or fail
            let mut pb_map = tmh.piece_buffer.lock().unwrap();
            pb_map.remove(&i);
            warn!("write piece {i} done ,delete in cache");
            // TODO: still have arc ref to buf
        }
    }
}

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
use crate::transmit_manager::{self, TransmitManagerHandle};

#[derive(Debug)]
pub(crate) enum WakeUpOption {
    TimeUp(time::Duration),
    NBlock(usize),
}

#[derive(Debug)]
pub(crate) enum Msg {
    RequestBlocks(BlockRequests),
    // SendBlocks(BlockRange),
    SetWakeUp(WakeUpOption),
    ResetWakeUp(WakeUpOption),
}

pub(crate) struct ConnectionManagerHandle {
    recv_stream: RecvStreamHandle,
    send_stream: SendStreamHandle,
    metadata: Arc<metadata::Metadata>,
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
        };

        let (send_tx, send_rx) = mpsc::unbounded_channel();
        let (send_cancel_tx, send_cancel_rx) = oneshot::channel();
        let (send_done_tx, send_done_rx) = oneshot::channel();
        let send_stream_handle = SendStreamHandle {
            sender: send_tx,
            notify: Arc::new(Notify::new()),
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
            metadata: m,
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
}

struct SendStreamHandle {
    sender: mpsc::UnboundedSender<Msg>,
    notify: Arc<Notify>,
    cancel: oneshot::Sender<()>,
    done: oneshot::Receiver<()>,
}

struct SendStream<T>
where
    T: Split,
{
    receiver: mpsc::UnboundedReceiver<Msg>,
    write_stream: WriteStream<<T as Split>::W>,
}

async fn run_recv_stream<T>(
    mut conn: RecvStream<T>,
    mut cancel: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) where
    T: Split,
{
    info!("in recv stream");
    loop {
        tokio::select! {
            _ = &mut cancel => {
                info!("recv stream cancelled");
                break;
            }
            Some(msg) = conn.receiver.recv() => {
                // TODO: use buffer and tokio::Notify
                // info!("connection manager of {} received msg {msg:?}", &manager.conn);
            }
            r = receive_peer_msg(&mut conn.read_stream, &mut conn.transmit_handle) => {
            }
        };
    }
    let _ = done.send(());
    info!("done recv stream");
}

async fn receive_peer_msg<T>(
    read_stream: &mut ReadStream<T>,
    transmit_handle: &mut TransmitManagerHandle,
) where
    T: AsyncRead + Unpin,
{
    let peer_addr = read_stream.peer_addr();
    let r = read_stream.recv_msg_header().await;
    match r {
        Ok(m) => {
            info!("received BT msg");
            handle_peer_msg(transmit_handle, peer_addr, m).await;
        }
        Err(e) => {
            info!("receive connection error {e}");
            // TODO: tell transmit manager this connection is dead
        }
    }
}

// TODO: socketaddr use ref?
async fn handle_peer_msg<'a, R>(
    tmh: &'a mut TransmitManagerHandle,
    addr: SocketAddr,
    m: Message<'a, R>,
) where
    R: AsyncRead + Unpin,
{
    // TODO: send statistics to transmit handle

    // TODO: shall we use mpsc or just lock the manager and set it
    // since this is generally a sync operation

    // TODO: maybe use bounded channel?
    match m {
        Message::KeepAlive => {
            // do nothing
        }
        Message::Choke => {
            // TODO: drop all pending requests
            // stop sending all requests
            tmh.sender.send(transmit_manager::Msg::PeerChoke);
        }
        Message::Unchoke => {
            tmh.sender.send(transmit_manager::Msg::PeerUnchoke);
        }
        Message::Interested => {
            // TODO: update peer state
            tmh.sender.send(transmit_manager::Msg::PeerInterested);
        }
        Message::NotInterested => {
            // TODO: update peer state
            tmh.sender.send(transmit_manager::Msg::PeerUninterested);
        }
        Message::Have(_) => {
            // TODO: tell transmit manager
            todo!();
        }
        Message::BitField(bit_field) => {
            // TODO: tell transmit manager
            tmh.sender
                .send(transmit_manager::Msg::PeerBitField(addr, bit_field));
        }
        Message::Request(request) => {
            // TODO:
            // if in cache, mark cache in use
            // add to send queue, wake sending task
            // if not in cache, send to background fetch task
            // when block fetched, wake sending task
        }
        Message::Piece(mut piece) => {
            // TODO:
            // if coming piece have cache, store it in cache
            // if coming piece don't have cache, ???
            // tell manager?
            info!(
                "block received {} {} {}",
                piece.index, piece.begin, piece.len,
            );

            let block_ref = {
                // must drop pb before await point
                // pb is not Send
                let mut pb = tmh.piece_buffer.lock().unwrap();
                if pb.get(&piece.index).is_none() {
                    pb.insert(piece.index, ArcCache::new(tmh.piece_size));
                }

                let block_buf = pb
                    .get(&piece.index)
                    .expect(&format!(
                        "piece {} should exist in piece-buffer",
                        piece.index
                    ))
                    .get_part_ref(
                        piece.begin as usize,
                        // TODO: change 16384 to const
                        (piece.len.next_multiple_of(16384)) as usize,
                    );

                if let None = block_buf {
                    warn!(
                        "error get block buf of piece {} block offset {}",
                        piece.index, piece.begin
                    );
                }
                block_buf
            };

            if let Some(mut bbuf) = block_ref {
                piece.read(bbuf.to_slice_len(piece.len as usize)).await;
            } else {
                let mut drain = vec![0u8; piece.len as usize];
                piece.read(&mut drain).await;
                warn!(
                    "drain PIECE msg {} {} {}",
                    piece.index, piece.begin, piece.len
                );
            }

            let received_piece = tmh
                .picker
                .lock()
                .unwrap()
                .block_received(protocol::Request {
                    index: piece.index,
                    begin: piece.begin,
                    len: piece.len,
                });
            if let Some(i) = received_piece {
                info!("piece {i} received");

                let (write_tx, write_rx) = tokio::sync::oneshot::channel::<std::io::Result<()>>();
                let write_job = {
                    // TODO: dead lock?
                    let mut pb = tmh.piece_buffer.lock().unwrap();

                    let piece_i_buf = pb.get(&i).expect("should exist in piece buffer");

                    // TODO: should mark as ready to write,
                    // get_part_ref should stop return new refs
                    // TODO: last piece
                    if let Some(mut pbuf) = piece_i_buf.get_part_ref(0, tmh.piece_size) {
                        let pbuf_s = pbuf.to_slice();

                        let bf_copy = tmh.back_file.clone();
                        let piece_size = tmh.piece_size;
                        pb.remove(&i);
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
                    tmh.picker.lock().unwrap().piece_checked(i);
                }
            }
        }
        Message::Cancel(request) => {
            // TODO: cancel pending request/fetch task
            todo!();
        }
    };
}

async fn run_send_stream<T>(
    mut conn: SendStream<T>,
    mut cancel: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) where
    T: Split,
{
    let mut interval = tokio::time::interval(time::Duration::from_secs(120));
    loop {
        tokio::select! {
            Some(msg) = conn.receiver.recv() => {
                // TODO: maybe use buffer and Notify?
                info!("send stream received {msg:?}");
                conn.handle_cmd(msg).await;
            }
            _ = interval.tick() => {
                if let Err(e) = conn.write_stream.send_keepalive().await {
                    info!("send keepalive error {e}");
                    // TODO: tell transmit manager this connection is dead
                    break;
                }
            }
            _ = &mut cancel => {
                info!("send stream cancelled");
                break;
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

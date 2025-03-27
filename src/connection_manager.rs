use std::sync::{Arc, Mutex};
use std::time;
use tokio::sync::{mpsc, oneshot};

use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

use crate::protocol::{self, BTStream, ReadStream, Split, WriteStream};

// request a range from peer
// from and to must be aligned with block size (except last block)
// specified by BitTorrent protocol
#[derive(Debug)]
pub(crate) struct BlockRange {
    // from and to are inclusive
    from: protocol::Request,
    to: protocol::Request,
    piece_size: u32,
}

impl Iterator for BlockRange {
    type Item = protocol::Request;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

#[derive(Debug)]
pub(crate) enum WakeUpOption {
    TimeUp(time::Duration),
    NBlock(usize),
}

#[derive(Debug)]
pub(crate) enum Msg {
    RequestBlocks(BlockRange),
    SendBlocks(BlockRange),
    SetWakeUp(WakeUpOption),
    ResetWakeUp(WakeUpOption),
}

pub(crate) struct ConnectionManagerHandle {
    sender: mpsc::UnboundedSender<Msg>,
    cancel: oneshot::Sender<()>,
    done: oneshot::Receiver<()>,
}

impl ConnectionManagerHandle {
    pub fn new<T>(conn: BTStream<T>) -> Self
    where
        T: AsyncRead + AsyncWrite + Split + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let cm = ConnectionManager::new(conn, rx);
        tokio::spawn(run_connection_manager(cm, cancel_rx, done_tx));
        Self {
            sender: tx,
            cancel: cancel_tx,
            done: done_rx,
        }
    }

    pub fn send_cmd(&self, m: Msg) {
        self.sender.send(m);
    }
}

pub(crate) struct ConnectionManager<T>
where
    T: Split,
{
    receiver: mpsc::UnboundedReceiver<Msg>,

    read_stream: ReadStream<<T as Split>::R>,
    write_stream: WriteStream<<T as Split>::W>,
}

impl<T> ConnectionManager<T>
where
    T: AsyncRead + AsyncWrite + Split + Unpin,
{
    fn new(conn: BTStream<T>, receiver: mpsc::UnboundedReceiver<Msg>) -> Self {
        let (rd, wr) = conn.split();
        ConnectionManager {
            read_stream: rd,
            write_stream: wr,
            receiver,
        }
    }
    fn handle_msg(&mut self, m: Msg) {
        match m {
            Msg::RequestBlocks(r) => {
                const BLOCK_SIZE: usize = 16 * 1024;
                for index in r.from.index..=r.to.index {
                    for begin in (0..r.piece_size).step_by(BLOCK_SIZE) {
                        todo!();
                    }
                }
                // TODO: let send task to send data
            }
            Msg::SendBlocks(r) => {
                todo!();
            }
            _ => {
                todo!()
            }
        }
    }
}

// #[inline]
// fn inside_block_range(index: u32, begin: u32, len: u32, m: &BlockRange) -> bool {
//     if m.from.index < index && index < m.to.index {
//         true
//     } else if index == m.from.index {
//         m.from.begin <= begin
//     } else if index == m.to.index {
//         begin +
//     }
// }

pub(crate) async fn run_connection_manager<T>(
    mut manager: ConnectionManager<T>,
    mut cancel: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) where
    T: AsyncRead + AsyncWrite + Split + Unpin,
{
    let (recv_tx, recv_rx) = mpsc::unbounded_channel();
    let (recv_cancel_tx, recv_cancel_rx) = oneshot::channel();
    let (recv_done_tx, recv_done_rx) = oneshot::channel();
    tokio::spawn(run_recv_stream(
        manager.read_stream,
        recv_rx,
        recv_cancel_rx,
        recv_done_tx,
    ));

    loop {
        tokio::select! {
            Some(msg) = manager.receiver.recv() => {
                // info!("connection manager of {} received msg {msg:?}", manager.write_stream);
                info!("connection manager received msg");
                // manager.handle_msg(msg);
            }
            _ = &mut cancel => {
                info!("connection manager cancelled");
                break;
            }
        };
    }
    let _ = done.send(());
    println!("connection manager done");
}

async fn run_recv_stream<T>(
    mut conn: ReadStream<T>,
    mut receiver: mpsc::UnboundedReceiver<()>, // TODO
    mut cancel: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) where
    T: AsyncRead + Unpin,
{
    info!("in recv stream");
    loop {
        tokio::select! {
            Some(msg) = receiver.recv() => {
                // info!("connection manager of {} received msg {msg:?}", &manager.conn);
            }
            r = conn.recv_msg() => {
                match r {
                    Ok(m) => {
                        info!("received BT msg {m:?}");
                    }
                    Err(e) => {
                        info!("receive connection error {e}");
                        break; // just break here. TODO
                    }
                }

            }
            _ = &mut cancel => {
                info!("recv stream cancelled");
                break;
            }
        };
    }
    let _ = done.send(());
    info!("done recv stream");
}

/*
async fn run_send_stream<T>(
    mut conn: WriteEnd<T>,
    receiver: mpsc::UnboundedReceiver<()>, // TODO
    mut cancel: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) where
    T: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            Some(msg) = receiver.recv() => {
                // info!("connection manager of {} received msg {msg:?}", &manager.conn);
                conn.send_keepalive();
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

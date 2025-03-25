use crate::announce_manager::{self, AnnounceManagerHandle};
use crate::connection;
use crate::metadata::{self, AnnounceType, Metadata, TrackerGet};
use crate::protocol::{self, BTStream};
use crate::transmit_manager::{self, run_transmit_manager, TransmitManagerHandle};
use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tracing::{debug_span, info, Instrument, Level, Span};

pub use transmit_manager::TransmitManager;

pub struct TorrentManagerHandle {
    sender: mpsc::UnboundedSender<transmit_manager::Msg>,

    transmit_manager_cancel: oneshot::Sender<()>,
    transmit_manager_done: oneshot::Receiver<()>,

    announce_manager: AnnounceManagerHandle,
}

impl TorrentManagerHandle {
    pub fn new(m: Arc<Metadata>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<transmit_manager::Msg>();
        let tm = TransmitManager::new(m.clone(), tx.clone(), rx);
        let (cancel_transmit, cancel_transmit_rx) = oneshot::channel::<()>();
        let (done_transmit, done_transmit_rx) = oneshot::channel::<()>();
        tokio::spawn(run_transmit_manager(tm, cancel_transmit_rx, done_transmit));

        let am = AnnounceManagerHandle::new(m);
        // let mut am = AnnounceManager::new()
        // tokio::spwan(run_announce_manager());
        Self {
            sender: tx,
            transmit_manager_cancel: cancel_transmit,
            transmit_manager_done: done_transmit_rx,

            announce_manager: am,
        }
    }

    pub fn send_msg(&mut self, m: transmit_manager::Msg) {
        self.sender.send(m); // TODO: preserve result type?
    }

    pub fn send_announce_msg(&mut self, m: announce_manager::Msg) {
        self.announce_manager.send(m); // TODO: preserve result type?
    }

    // TODO: make this Drop trait?
    pub async fn wait_close(self) {
        self.announce_manager.stop().await;

        self.transmit_manager_cancel.send(());
        self.transmit_manager_done.await;
    }
}

// impl Drop for TorrentManagerHandle {
//     fn drop(&mut self) {
//         self.send_msg(Msg::Stop);
//         let _ = self.transmit_manager_done.await;
//     }
// }

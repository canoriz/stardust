use crate::announce_manager::{self, AnnounceManagerHandle};
use crate::metadata::Metadata;
use crate::transmit_manager::{self, run_transmit_manager};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use tokio_util::sync::{CancellationToken, DropGuard};
pub use transmit_manager::TransmitManager;

pub struct TorrentManagerHandle {
    sender: mpsc::UnboundedSender<transmit_manager::Msg>,

    transmit_manager_cancel: DropGuard,
    transmit_manager_done: oneshot::Receiver<()>,

    announce_manager: AnnounceManagerHandle,
}

impl TorrentManagerHandle {
    pub fn new(m: Arc<Metadata>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<transmit_manager::Msg>();

        let tm = TransmitManager::new(m.clone(), tx.clone(), rx);
        let cancel_transmit = CancellationToken::new();
        let (done_transmit, done_transmit_rx) = oneshot::channel::<()>();

        // TODO: convention: let TransmitManager::new call run_transmit_manager
        tokio::spawn(run_transmit_manager(
            tm,
            cancel_transmit.clone(),
            done_transmit,
        ));

        let am = AnnounceManagerHandle::new(m, tx.clone());
        // let mut am = AnnounceManager::new()
        // tokio::spwan(run_announce_manager());
        Self {
            sender: tx,
            transmit_manager_cancel: cancel_transmit.drop_guard(),
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

    pub async fn stop_wait(self) {
        self.transmit_manager_cancel.disarm().cancel();
        self.announce_manager.stop_wait().await;
        _ = self.transmit_manager_done.await;
    }
}

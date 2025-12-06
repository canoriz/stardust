use crate::announce_manager::{self, AnnounceManagerHandle};
use crate::metadata::Metadata;
use crate::transmit_manager::{self, TransmitManager};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct TorrentManagerHandle {
    sender: mpsc::UnboundedSender<transmit_manager::Msg>,

    transmit_manager: TransmitManager,

    announce_manager: AnnounceManagerHandle,
}

impl TorrentManagerHandle {
    pub fn new(m: Arc<Metadata>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<transmit_manager::Msg>();

        let tm = TransmitManager::new(m.clone(), tx.clone(), rx);

        let am = AnnounceManagerHandle::new(m, tx.clone());
        Self {
            sender: tx,
            transmit_manager: tm,
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
        self.transmit_manager.stop_wait().await;
        self.announce_manager.stop_wait().await;
    }
}

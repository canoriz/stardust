use std::sync::Arc;

use crate::announce_manager::{self, AnnounceManagerHandle};
use crate::dht::DHT;
use crate::transmit_manager::{self, TorrentTask, TransmitDump, TransmitManager};
use tokio::sync::{mpsc, oneshot};

pub struct TorrentManagerHandle {
    sender: mpsc::UnboundedSender<transmit_manager::Msg>,

    transmit_manager: TransmitManager,
}

impl TorrentManagerHandle {
    pub fn new(t: TorrentTask, id: [u8; 20], port: u16, dht_client: Option<Arc<DHT>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<transmit_manager::Msg>();

        let info_hash = match &t {
            TorrentTask::Torrent(m) => m.info_hash,
            TorrentTask::Magnet(m) => m.info_hash,
        };

        let am = AnnounceManagerHandle::new(id, port, info_hash, tx.clone());
        let tm = TransmitManager::new(t, id, tx.clone(), rx, dht_client, am);

        Self {
            sender: tx,
            transmit_manager: tm,
        }
    }

    pub fn send_msg(&mut self, m: transmit_manager::Msg) {
        self.sender.send(m); // TODO: preserve result type?
    }

    pub fn send_announce_msg(&mut self, m: announce_manager::Msg) {
        self.sender.send(transmit_manager::Msg::AnnounceMsg(m)); // TODO: preserve result type?
    }

    pub async fn stop_wait(self) {
        self.transmit_manager.stop_wait().await;
    }

    pub async fn check(&mut self) {
        let (tx, rx) = oneshot::channel();
        self.sender.send(transmit_manager::Msg::CheckFile(tx));
        rx.await;
    }

    pub async fn dump_stop(mut self) {
        let (tx, rx) = oneshot::channel();
        self.send_msg(transmit_manager::Msg::DumpStatus(tx));
        let s = serde_json::to_string(&rx.await.unwrap()).unwrap();
        println!("{s}");
    }
}

use std::io;
use std::sync::Arc;

use crate::announce_manager::{self, AnnounceManagerHandle};
use crate::cache::cache_manager::CacheManagerHandle;
use crate::dht::DHT;
use crate::transmit_manager::{self, TorrentTask, TransmitDump, TransmitManager};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct TransmitManagerSender(mpsc::UnboundedSender<transmit_manager::Msg>);

pub struct TorrentManagerHandle {
    pub sender: TransmitManagerSender,

    transmit_manager: TransmitManager,
}

impl TorrentManagerHandle {
    pub fn new(
        t: TorrentTask,
        self_id: [u8; 20],
        port: u16,
        dht_client: Option<Arc<DHT>>,
        cache_handle: CacheManagerHandle,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<transmit_manager::Msg>();

        let info_hash = match &t {
            TorrentTask::Torrent(m) => m.info_hash,
            TorrentTask::Magnet(m) => m.info_hash,
        };

        let am = AnnounceManagerHandle::new(self_id, port, info_hash, tx.clone());
        let tm = TransmitManager::new(
            t,
            self_id,
            port,
            tx.clone(),
            rx,
            dht_client,
            am,
            cache_handle,
        );

        Self {
            sender: TransmitManagerSender(tx),
            transmit_manager: tm,
        }
    }

    pub fn send_msg(&mut self, m: transmit_manager::Msg) -> io::Result<()> {
        self.sender.0.send(m).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("send msg to transmit manager error: {}", e),
            )
        })
    }

    pub fn send_announce_msg(&mut self, m: announce_manager::Msg) {
        self.sender.0.send(transmit_manager::Msg::AnnounceMsg(m)); // TODO: preserve result type?
    }

    pub async fn wait_downloaded(&mut self) -> io::Result<()> {
        self.sender.wait_downloaded().await
    }

    pub async fn stop_wait(self) -> io::Result<TransmitDump> {
        self.transmit_manager.stop_wait().await
    }

    /// Restore a `TorrentManagerHandle` directly from a `TransmitDump`.
    ///
    /// Block-picker state, announce URLs, and previous peers are all restored
    /// inline — no message round-trip needed.
    pub fn restore_from_dump(
        dump: TransmitDump,
        self_id: [u8; 20],
        port: u16,
        dht_client: Option<Arc<DHT>>,
        cache_handle: CacheManagerHandle,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<transmit_manager::Msg>();

        let info_hash = dump.info_hash();
        let am = AnnounceManagerHandle::new(self_id, port, info_hash, tx.clone());
        let tm = TransmitManager::from_dump(
            dump,
            self_id,
            port,
            tx.clone(),
            rx,
            dht_client,
            am,
            cache_handle,
        );

        Self {
            sender: TransmitManagerSender(tx),
            transmit_manager: tm,
        }
    }
}

impl TransmitManagerSender {
    pub async fn wait_downloaded(&mut self) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(transmit_manager::Msg::WaitDownloaded(tx))
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("change state send msg error: {}", e),
                )
            })?;
        let mut has_downloaded = rx.await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("wait downloaded oneshot recv error: {}", e),
            )
        })?;

        has_downloaded.wait_for(|d| *d).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("wait downloaded watch recv error: {}", e),
            )
        })?;
        Ok(())
    }

    pub async fn change_state(&mut self, s: transmit_manager::RunningCmd) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(transmit_manager::Msg::ChangeState(s, tx))
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("change state send msg error: {}", e),
                )
            })?;
        rx.await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("change_state oneshot recv error: {}", e),
            )
        })
    }

    pub fn check(&mut self) -> io::Result<ForceCheck> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(transmit_manager::Msg::CheckFile(tx))
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("check send msg error: {}", e))
            })?;
        Ok(ForceCheck { rx })
    }

    pub async fn query_status(&mut self) -> io::Result<transmit_manager::TorrentRuntimeStatus> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(transmit_manager::Msg::QueryStatus(tx))
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("query status send msg error: {}", e),
                )
            })?;
        rx.await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("query status oneshot recv error: {}", e),
            )
        })
    }
}

pub struct ForceCheck {
    rx: oneshot::Receiver<bool>,
}

impl ForceCheck {
    pub async fn wait(self) -> io::Result<bool> {
        self.rx
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("force check error: {}", e)))
    }
}

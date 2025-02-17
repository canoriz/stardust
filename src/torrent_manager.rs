use crate::metadata::{self, Metadata};
use crate::protocol;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug_span, info, Instrument, Level, Span};

#[derive(Debug)]
#[non_exhaustive]
enum Msg {
    AnnounceFinish(Result<metadata::AnnounceResp, metadata::AnnounceError>),
    NewPeer(protocol::BTStream),
}

pub struct TorrentManager {
    metadata: Metadata,

    change_rx: mpsc::UnboundedReceiver<Msg>,
    change_tx: mpsc::UnboundedSender<Msg>,

    announce_cmd_tx: Option<mpsc::Sender<u32>>,
}

impl TorrentManager {
    pub fn new(m: Metadata) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            metadata: m,
            change_rx: rx,
            change_tx: tx,
            announce_cmd_tx: None,
        }
    }

    pub async fn start(&mut self) {
        self.announce_cmd_tx = Some(self.start_announce_task());
        self.main_loop().await;
    }

    async fn main_loop(&mut self) {
        loop {
            match self.change_rx.recv().await {
                Some(msg) => {
                    info!("main received info {msg:?}");
                    self.handle_msg(msg);
                }
                None => break {},
            }
        }
    }

    fn handle_msg(&mut self, m: Msg) {
        match m {
            Msg::AnnounceFinish(Ok(a)) => {
                self.handle_announce(a);
            }
            Msg::AnnounceFinish(Err(e)) => {
                info!("announce error {}", e);
            }
            other => {
                info!("unhandled other {:?}", other);
            }
        }
    }

    pub fn start_find_peers_task(&self) {
        todo!()
    }

    pub fn start_announce_task(&self) -> mpsc::Sender<u32> {
        let announce_req = metadata::TrackerGet {
            peer_id: "-ZS0405-qwerasdfzxcv".into(),
            uploaded: 0,
            port: 35515,
            downloaded: 0,
            left: 0,
            ip: None,
        };
        let m = self.metadata.clone();
        let main_tx = self.change_tx.clone();
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<u32>(1);

        tokio::spawn(
            async move {
                // TODO: simplify into a struct methods
                let mut interval = Duration::from_secs(1);

                loop {
                    let res =
                        metadata::announce(metadata::AnnounceType::V4, &announce_req, &m).await;
                    // TODO: simplify macro inners
                    tokio::select! {
                        c = cmd_rx.recv() => {
                            info!("announce task received cmd {c:?}");
                        }
                        _ = time::sleep(interval) => {
                            match &res {
                                Ok(r) => {
                                    interval = Duration::from_secs(r.interval as u64);
                                    info!("announce task OK, next announce {}", r.interval);
                                }
                                Err(e) => {
                                    // TODO: send event
                                    info!("announce failed reason {e}");
                                    interval *= 2;
                                }
                            }
                            let send_res = main_tx.send(Msg::AnnounceFinish(res));
                            if send_res.is_err() {
                                info!("send announce res to main error {send_res:?}");
                            }
                        }
                    }
                }
            }
            .instrument(Span::current()),
        );
        cmd_tx
        // TODO: re-announce after period
        // TODO: update downloaded, port, etc
    }
}

async fn handle_announce(mut main_tx: mpsc::Sender<Msg>, a: metadata::AnnounceResp) {
    for p in a.peers {
        if let Ok(ip) = p.ip.parse::<IpAddr>() {
            // TODO: async connect peers
            let mut conn =
                protocol::BTStream::connect_tcp(SocketAddr::from((ip, p.port as u16))).await;
            if let Err(e) = main_tx.send(Msg::NewPeer(conn)) {
                info!("send new peer to main {e}");
            }
        }
    }
}

use crate::metadata::{self, Metadata};
use crate::protocol::{self, BTStream};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug_span, info, Instrument, Level, Span};

#[derive(Debug)]
#[non_exhaustive]
enum Msg {
    AnnounceFinish(Result<metadata::AnnounceResp, metadata::AnnounceError>),

    // TODO: support uTP/proxy
    NewPeer(protocol::BTStream<TcpStream>),
    NewIncomePeer(protocol::BTStream<TcpStream>),
}

pub struct TorrentManager {
    metadata: Metadata,

    change_rx: mpsc::UnboundedReceiver<Msg>,
    change_tx: mpsc::UnboundedSender<Msg>,

    announce_cmd_tx: Option<mpsc::Sender<u32>>,

    // TODO: use a Map instead of Vec?
    connected_peers: HashMap<SocketAddr, BTStream<TcpStream>>,
}

impl TorrentManager {
    pub fn new(m: Metadata) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            metadata: m,
            change_rx: rx,
            change_tx: tx,
            announce_cmd_tx: None,

            connected_peers: HashMap::new(),
        }
    }

    pub async fn start(&mut self) {
        self.announce_cmd_tx = Some(self.start_announce_task(FakeAnnouncer {}));
        self.main_loop().await;
    }

    async fn main_loop(&mut self) {
        loop {
            match self.change_rx.recv().await {
                Some(msg) => {
                    info!("main received msg");
                    self.handle_msg(msg);
                }
                None => break {},
            }
        }
    }

    fn handle_msg(&mut self, m: Msg) {
        match m {
            Msg::AnnounceFinish(Ok(a)) => {
                tokio::spawn(handle_announce(
                    self.change_tx.clone(),
                    a.peers
                        .into_iter()
                        .filter_map(|p| (p.ip).parse().map(|ip: IpAddr| (ip, p.port).into()).ok())
                        .collect(),
                ));
            }
            Msg::AnnounceFinish(Err(e)) => {
                info!("announce error {}", e);
            }
            Msg::NewPeer(bt_conn) => {
                info!("new outcome connection {:?}", bt_conn);
            }
            other => {
                info!("unhandled other {:?}", other);
            }
        }
    }

    pub fn start_find_peers_task(&self) {
        todo!()
    }

    pub fn start_announce_task(
        &self,
        announcer: impl metadata::Announce + Send + 'static,
    ) -> mpsc::Sender<u32> {
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
                    // TODO: what if no announce url?
                    let res = announcer
                        .announce_tier(0, metadata::AnnounceType::V4, &announce_req, &m)
                        .await;
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

async fn handle_announce(main_tx: mpsc::UnboundedSender<Msg>, addrs: Vec<SocketAddr>) {
    for addr in addrs {
        // TODO: async connect peers
        let conn = protocol::BTStream::connect_tcp(addr).await;
        match conn {
            Ok(c) => {
                if let Err(e) = main_tx.send(Msg::NewPeer(c)) {
                    info!("send new peer to main {e}");
                }
            }
            Err(e) => {
                info!("tcp handshake {addr} error {e}");
            }
        }
    }
}

struct FakeAnnouncer {}
impl metadata::Announce for FakeAnnouncer {
    async fn announce_tier<'a>(
        &self,
        tier: u32,
        net_type: metadata::AnnounceType,
        req: &metadata::TrackerGet<'a>,
        torrent: &Metadata,
    ) -> metadata::AnnounceResult {
        Ok(metadata::AnnounceResp {
            interval: 1800,
            peers: vec![metadata::Peer {
                peer_id: "1384".into(),
                ip: "127.0.0.1".into(),
                port: 35515,
            }],
        })
    }
}

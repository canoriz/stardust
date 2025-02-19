use crate::metadata::{self, AnnounceType, Metadata, TrackerGet};
use crate::protocol::{self, BTStream};
use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
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

struct AnnounceCmd(u32);

struct AnnounceManager {
    announce_list: Vec<Vec<String>>,
    cmd_rx: mpsc::Receiver<AnnounceCmd>,
    main_tx: mpsc::UnboundedSender<Msg>,
}
impl AnnounceManager {
    async fn start_worker<'a, T>(&mut self, tg: &TrackerGet<'a>, m: &Metadata)
    where
        T: metadata::Announce,
    {
        for url in self.announce_list.iter().flat_map(|e| e) {
            let res = T::announce_tier(AnnounceType::V4, tg, m, url.clone()).await;
            if let Err(e) = self.main_tx.send(Msg::AnnounceFinish(res)) {
                info!("announce send to main failed {e}");
            }
            let res = T::announce_tier(AnnounceType::V6, tg, m, url.clone()).await;
            if let Err(e) = self.main_tx.send(Msg::AnnounceFinish(res)) {
                info!("announce send to main failed {e}");
            }
        }
    }
}

struct AnnounceManagerHandle {
    cmd_tx: mpsc::Sender<AnnounceCmd>,
}
impl AnnounceManagerHandle {
    fn start_announce_worker(&self) {
        self.cmd_tx.send(AnnounceCmd(0));
    }

    fn stop_all(&self) {
        self.cmd_tx.send(AnnounceCmd(1));
    }
}

pub struct TorrentManager {
    metadata: Metadata,

    change_rx: mpsc::UnboundedReceiver<Msg>,
    change_tx: mpsc::UnboundedSender<Msg>,

    announce_handle: Option<AnnounceManagerHandle>,
    announce_tx: Option<mpsc::Sender<u32>>,

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
            announce_handle: None,
            announce_tx: None,

            connected_peers: HashMap::new(),
        }
    }

    pub fn with_announce_list(mut self, announce_list: Vec<Vec<String>>) -> Self {
        // if let Some(am) = self.announce_handle {
        //     am.stop_all();
        // }
        // let (cmd_tx, cmd_rx) = mpsc::channel(2); // TODO: 2?
        // let mut am = AnnounceManagerHandle { cmd_tx };
        // am.start_announce_worker();
        // self.announce_handle = Some(am);
        self.announce_tx = Some(self.start_announce_task::<FakeAnnouncer>(announce_list));
        self
    }

    pub async fn start(&mut self) {
        // self.announce_cmd_tx = Some(self.start_announce_task(FakeAnnouncer {}));
        self.main_loop().await;
    }

    async fn main_loop(&mut self) {
        loop {
            match self.change_rx.recv().await {
                Some(msg) => {
                    info!("main received msg {msg:?}");
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

    pub fn start_announce_task<T>(&self, announce_list: Vec<Vec<String>>) -> mpsc::Sender<u32>
    where
        T: metadata::Announce + 'static,
    {
        let announce_req = Arc::new(metadata::TrackerGet {
            peer_id: "-ZS0405-qwerasdfzxcv".into(),
            uploaded: 0,
            port: 35515,
            downloaded: 0,
            left: 0,
            ip: None,
        });
        let m = Arc::new(self.metadata.clone());

        let urls: Vec<String> = announce_list
            .into_iter()
            .flat_map(|u| u.into_iter())
            .collect();

        let (cmd_tx, cmd_rx) = mpsc::channel::<u32>(1);
        let main_tx = self.change_tx.clone();
        tokio::spawn(
            announce_tier::<T>(cmd_rx, announce_req.clone(), main_tx, m, urls)
                .instrument(Span::current()),
        );

        cmd_tx
        // TODO: re-announce after period
        // TODO: update downloaded, port, etc
    }
}

async fn announce_tier<T>(
    mut cmd_rx: mpsc::Receiver<u32>,
    announce_req: Arc<metadata::TrackerGet<'static>>,
    main_tx: mpsc::UnboundedSender<Msg>,
    m: Arc<metadata::Metadata>,
    urls: Vec<String>,
) where
    T: metadata::Announce,
{
    announce_url::<T>(
        main_tx.clone(),
        announce_req.clone(),
        m.clone(),
        urls,
        &mut cmd_rx,
    )
    .await;
}

async fn announce_url<'a, T>(
    main_tx: mpsc::UnboundedSender<Msg>,
    announce_req: Arc<metadata::TrackerGet<'a>>,
    m: Arc<metadata::Metadata>,
    urls: Vec<String>,
    input_cmd: &mut mpsc::Receiver<u32>,
) where
    T: metadata::Announce,
{
    // TODO: simplify into a struct methods

    let mut timers = tokio::task::JoinSet::<(AnnounceType, Arc<String>, u64)>::new();

    for url in urls {
        let u = Arc::new(url);
        let u2 = u.clone();
        timers.spawn(async { (AnnounceType::V4, u, 0) });
        timers.spawn(async { (AnnounceType::V6, u2, 0) });
    }

    loop {
        // TODO: simplify macro inners
        tokio::select! {
            c = input_cmd.recv() => {
                info!("announce task received cmd {c:?}");
            }
            join_res = timers.join_next() => {
                let (announce_type, url, sleeped) = match join_res {
                    Some(Ok(timeup)) => {
                        info!("announcing {timeup:?}");
                        timeup
                    },
                    Some(Err(e)) => {
                        info!("join error {e}");
                        continue
                    },
                    None => {break},
                };

                let res = T
                ::announce_tier(
                    announce_type,
                    &announce_req,
                    m.as_ref(),
                    url.as_ref().clone(),
                )
                .await;
                let interval = match &res {
                    Ok(r) => {
                        info!("announce task OK, next announce {}", r.interval);
                        r.interval as u64
                    }
                    Err(e) => {
                        // TODO: send event
                        info!("announce failed reason {e}");
                        if sleeped == 0 {
                            1
                        } else {
                            sleeped * 2
                        }
                    }
                };

                timers.spawn(
                async move {
                    time::sleep(Duration::from_secs(interval)).await;
                    (announce_type, url, interval)
                });

                let send_res = main_tx.send(Msg::AnnounceFinish(res));
                if send_res.is_err() {
                    info!("send announce res to main error {send_res:?}");
                }
            }
        }
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
        net_type: metadata::AnnounceType,
        req: &TrackerGet<'a>,
        torrent: &Metadata,
        url: String,
    ) -> metadata::AnnounceResult {
        return Err(metadata::AnnounceError::ClientErr(
            metadata::ClientErr::Ipv4Err,
        ));
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

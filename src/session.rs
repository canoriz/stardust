use bon::Builder;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::{io, time};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::lookup_host;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

use crate::dht::{self, DHT};
use crate::metadata::Magnet;
use crate::protocol::{AcceptOpt, BTStream, HandshakeOption, InfoHash};
use crate::torrent_manager::{TorrentManagerHandle, TransmitManagerSender};
use crate::transmit_manager::{RunningCmd, TorrentTask};
use crate::{announce_manager, Reunite, Split};

pub struct Session {
    self_id: [u8; 20], // TODO: use randomized self_id

    tasks: Arc<Mutex<HashMap<InfoHash, TorrentManagerHandle>>>,
    port: u16,
    dht_client: Option<Arc<DHT>>,

    _cancel: DropGuard,
}

#[derive(Builder, Clone)]
pub struct SessionOpt {
    self_id: [u8; 20],
    port: u16,

    // TODO: support uTP and dht in same port
    dht_port: Option<u16>,
}

impl Session {
    pub fn new(opt: SessionOpt) -> Self {
        let tasks = Arc::new(Mutex::new(HashMap::new()));
        let cancel = CancellationToken::new();

        let l = Listener {
            self_id: opt.self_id,
            tasks: tasks.clone(),
            cancel: cancel.clone(),
            dht_port: opt.dht_port, // TODO: what if dht client init failed?
        };
        tokio::spawn(run_listener(l, opt.port));

        // TODO: clients connect to us who prefers uTP will be rejected by our DHT handler
        // and not trying to connect with TCP
        // support dual protocol on DHT port, or choose a different dht/tcp port
        let dht_client = if let Some(dht_port) = opt.dht_port {
            let dht_client = Arc::new(DHT::new(opt.self_id, dht_port, "ST01".into()));
            let c = dht_client.clone();

            // TODO: optimize: maybe wait dht bootstrap done then return session
            // TODO: share dht network between sessions?
            // tokio::spawn(async move {
            //     _ = c
            //         .ping_rpc(
            //             dht::RpcAddr::NoID(
            //                 "[240e:b8f:5c11:9f00:560d:1feb:27b8:741]:60981"
            //                     .parse()
            //                     .unwrap(),
            //             ),
            //             time::Duration::from_secs(5),
            //         )
            //         .await;
            //     c.find_closest_node_to(opt.self_id, true).await;
            // });

            tokio::spawn(async move {
                // Ping well-known public bootstrap nodes concurrently to seed the routing tables.
                const BOOTSTRAP_NODES: &[&str] = &[
                    "router.bittorrent.com:6881",
                    "router.utorrent.com:6881",
                    "[2408:820c:5b38:3400:4393:1139:435d:c909]:60416",
                    // "dht.transmissionbt.com:6881",
                    // "dht.libtorrent.org:25401",
                ];
                let timeout = time::Duration::from_secs(5);
                let mut tasks = tokio::task::JoinSet::new();
                for &node in BOOTSTRAP_NODES {
                    let cl = c.clone();
                    tasks.spawn(async move {
                        // Try both families explicitly: one IPv4 and one IPv6 per hostname.
                        let addrs = match lookup_host(node).await {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("dht bootstrap resolve {} failed: {}", node, e);
                                return;
                            }
                        };

                        let mut v4 = None;
                        let mut v6 = None;
                        for addr in addrs {
                            match addr {
                                SocketAddr::V4(_) if v4.is_none() => v4 = Some(addr),
                                SocketAddr::V6(v)
                                    if v.ip().to_ipv4_mapped().is_none() && v6.is_none() =>
                                {
                                    v6 = Some(SocketAddr::V6(v))
                                }
                                _ => {}
                            }
                            if v4.is_some() && v6.is_some() {
                                break;
                            }
                        }

                        if let Some(addr) = v4 {
                            if let Err(e) = cl.ping_rpc(dht::RpcAddr::no_id(addr), timeout).await {
                                warn!("dht bootstrap v4 ping {} failed: {}", addr, e);
                            }
                        }

                        if let Some(addr) = v6 {
                            if let Err(e) = cl.ping_rpc(dht::RpcAddr::no_id(addr), timeout).await {
                                warn!("dht bootstrap v6 ping {} failed: {}", addr, e);
                            }
                        }
                    });
                }
                while tasks.join_next().await.is_some() {}
                // Populate both ipv4 and ipv6 routing tables
                c.get_peers(opt.self_id).await;
            });

            // tokio::spawn(async move {
            //     _ = c
            //         .ping_rpc(
            //             dht::RpcAddr::NoID("[::1]:51774".parse().unwrap()),
            //             time::Duration::from_secs(5),
            //         )
            //         .await;
            //     c.find_closest_node_to(opt.self_id, true).await;
            // });
            Some(dht_client)
        } else {
            None
        };

        Self {
            self_id: opt.self_id,
            tasks,
            port: opt.port,
            dht_client,
            _cancel: cancel.drop_guard(),
        }
    }

    /// add new torrent
    pub async fn add_torrent(&mut self, job: TorrentTask, announce_list: Vec<Vec<String>>) {
        let info_hash = job.info_hash();
        let trackers = if let TorrentTask::Magnet(Magnet { tr, .. }) = &job {
            tr.clone()
        } else {
            None
        };

        let mut tm =
            TorrentManagerHandle::new(job, self.self_id, self.port, self.dht_client.clone());

        for addr in announce_list {
            tm.send_announce_msg(announce_manager::Msg::AddUrl(addr));
        }
        if let Some(addr) = trackers {
            tm.send_announce_msg(announce_manager::Msg::AddUrl(addr));
        }
        _ = tm.sender.change_state(RunningCmd::Resume).await;
        self.tasks.lock().unwrap().insert(info_hash, tm);
    }

    /// remove torrent by info_hash
    pub async fn remove_torrent(&mut self, info_hash: &InfoHash) -> Option<TorrentManagerHandle> {
        self.tasks.lock().unwrap().remove(info_hash)
    }

    pub async fn do_work<F>(&mut self, info_hash: &InfoHash, work: F)
    where
        F: AsyncFnOnce(&mut TransmitManagerSender),
    {
        let mut sender = {
            let mut guard = self.tasks.lock().unwrap();
            if let Some(tm) = guard.get_mut(info_hash) {
                tm.sender.clone()
            } else {
                return;
            }
        };
        work(&mut sender).await
    }
}

struct Listener {
    tasks: Arc<Mutex<HashMap<InfoHash, TorrentManagerHandle>>>,
    cancel: CancellationToken,
    self_id: [u8; 20],
    dht_port: Option<u16>,
}

async fn run_listener(l: Listener, port: u16) -> std::io::Result<()> {
    // TODO: set v6_only to false
    let listener = TcpListener::bind(format!("[::]:{port}")).await?;
    loop {
        tokio::select! {
            _ = l.cancel.cancelled() => {
                info!("session listener cancelled");
                break Ok(());
            }
            Ok((conn, addr)) = listener.accept() => {
                let ic = IncomeConn {
                    conn,
                    addr,
                    dht_port: l.dht_port,
                    self_id: l.self_id,
                    tasks: l.tasks.clone(),
                };
                if let Err(e) = handle_income_connection(ic).await {
                    info!("handle income connection error {e}");
                }
            }
        }
    }
}

struct IncomeConn<T> {
    conn: T,
    dht_port: Option<u16>,
    addr: SocketAddr,
    self_id: [u8; 20],
    tasks: Arc<Mutex<HashMap<InfoHash, TorrentManagerHandle>>>,
}

async fn handle_income_connection<T>(conn: IncomeConn<T>) -> io::Result<()>
where
    T: AsyncRead + AsyncWrite + Split + Unpin + Send + 'static,
    <T as Split>::R: Reunite<W = <T as Split>::W, U = T>,
{
    let opt = HandshakeOption::builder()
        .client_id(conn.self_id)
        .client_version("ST01".into())
        .dht_port(conn.dht_port)
        .build();

    let map = conn.tasks.clone();
    let accept = async move |hash: &InfoHash| -> AcceptOpt {
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = map.lock().unwrap();
            if let Some(tm) = guard.get_mut(hash) {
                match tm.send_msg(crate::transmit_manager::Msg::RequestMetadata(tx)) {
                    Ok(_) => {}
                    Err(e) => {
                        info!("request metadata from transmit manager error: {}", e);
                        return AcceptOpt::Reject;
                    }
                }
            } else {
                return AcceptOpt::Reject;
            }
        };

        match rx.await {
            Ok(Some(m)) => AcceptOpt::HaveMetadata(m),
            Ok(None) => AcceptOpt::NoMetadata,
            Err(e) => {
                info!("request metadata error: {}", e);
                AcceptOpt::Reject
            }
        }
    };

    let bt_conn = match BTStream::accept(conn.conn, accept, opt).await {
        Ok(c) => c,
        Err(e) => {
            info!("accept handshake from {} error: {}", conn.addr, e);
            return Err(e);
        }
    };

    let mut guard = conn.tasks.lock().unwrap();
    if let Some(tm) = guard.get_mut(bt_conn.info().info_hash) {
        tm.send_msg(crate::transmit_manager::Msg::NewPeer(Ok((
            bt_conn.to_dyn(),
            true,
        ))))
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                "task dropped during incoming connection processing",
            )
        })
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "task deleted during incoming connection processing",
        ))
    }
}

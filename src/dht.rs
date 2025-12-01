use bt_bencode::ByteString;
use bt_bencode::Value as BtValue;
use core::time;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

mod routing;
mod wire;
mod wire_serde;
use routing::RoutingTable;
use wire::WireKRPC;

pub type NodeID = [u8; 20];

pub struct DHT {
    id: NodeID,
    port: u16,
    version: String,

    tid: AtomicU64,

    tx: mpsc::Sender<Req>,

    tmap: Arc<Mutex<TransactionMap>>,
    _cancel_token: DropGuard,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(try_from = "WireKRPC")]
#[serde(into = "WireKRPC")]
struct KRPC {
    t: ByteString,
    v: ByteString,
    inner: KRPCInner,
}

#[derive(Debug, PartialEq, Clone)]
enum KRPCInner {
    Request(Arg),
    Response(Resp),
    Err(Vec<(u32, String)>),
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct PingArg {
    #[serde(with = "serde_bytes")]
    id: NodeID,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct FindNodeArg {
    #[serde(with = "serde_bytes")]
    id: NodeID,
    #[serde(with = "serde_bytes")]
    target: NodeID,
}

#[derive(Clone, PartialEq, Debug)]
struct VecNode4(Vec<(NodeID, SocketAddrV4)>);
impl From<Vec<(NodeID, SocketAddrV4)>> for VecNode4 {
    fn from(value: Vec<(NodeID, SocketAddrV4)>) -> Self {
        Self(value)
    }
}

#[derive(Clone, PartialEq, Debug)]
struct VecNode6(Vec<(NodeID, SocketAddrV6)>);
impl From<Vec<(NodeID, SocketAddrV6)>> for VecNode6 {
    fn from(value: Vec<(NodeID, SocketAddrV6)>) -> Self {
        Self(value)
    }
}

#[derive(Clone, PartialEq, Debug)]
struct ByteSocketAddr(SocketAddr);
impl From<SocketAddr> for ByteSocketAddr {
    fn from(value: SocketAddr) -> Self {
        Self(value)
    }
}

#[derive(Serialize, Deserialize)]
struct FindNodeResp {
    #[serde(with = "serde_bytes")]
    id: NodeID,
    token: ByteString,

    #[serde(skip_serializing_if = "Option::is_none")]
    values: Option<Vec<ByteSocketAddr>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    nodes: Option<VecNode6>,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct GetPeersArg {
    #[serde(with = "serde_bytes")]
    id: NodeID,
    #[serde(with = "serde_bytes")]
    info_hash: [u8; 20],
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct AnnouncePeerArg {
    #[serde(with = "serde_bytes")]
    id: NodeID,
    implied_port: u32,
    #[serde(with = "serde_bytes")]
    info_hash: NodeID,
    port: u16,
    token: ByteString,
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
struct AnnouncePeerResp {
    implied_port: u32,
    #[serde(with = "serde_bytes")]
    info_hash: NodeID,
    port: u16,
    token: ByteString,
}

#[derive(Clone, Debug, PartialEq)]
enum Arg {
    Ping(PingArg),
    FindNode(FindNodeArg),
    AnnouncePeer(AnnouncePeerArg),
    GetPeers(GetPeersArg),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Resp {
    #[serde(with = "serde_bytes")]
    id: NodeID,

    /// if find_node/get_peers response
    #[serde(skip_serializing_if = "Option::is_none")]
    nodes: Option<VecNode4>,

    /// if find_node/get_peers response
    #[serde(skip_serializing_if = "Option::is_none")]
    nodes6: Option<VecNode6>,

    /// if get_peers
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<ByteString>,

    // if get_peers
    #[serde(skip_serializing_if = "Option::is_none")]
    values: Option<Vec<ByteSocketAddr>>,
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct NodeAddr {
    id: NodeID,
    addr: SocketAddr,
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum RpcAddr {
    ID(NodeAddr),
    NoID(SocketAddr),
}

impl RpcAddr {
    pub fn no_id(addr: SocketAddr) -> Self {
        Self::NoID(addr)
    }

    pub fn id(id: NodeID, addr: SocketAddr) -> Self {
        Self::ID(NodeAddr { id, addr })
    }
}

struct TransactionGuard {
    tid: Vec<u8>,
    tmap: Arc<Mutex<TransactionMap>>,
}

impl TransactionGuard {
    fn new(
        tid: Vec<u8>,
        tmap: Arc<Mutex<TransactionMap>>,
        tx: oneshot::Sender<io::Result<Resp>>,
    ) -> Self {
        tmap.lock().unwrap().insert(tid.to_vec(), tx);
        Self { tid, tmap }
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        self.tmap.lock().unwrap().remove(&self.tid);
    }
}

impl DHT {
    pub fn new(id: NodeID, port: u16, version: String) -> Self {
        let (tx, rx) = mpsc::channel(2048);
        let tmap = Arc::new(Mutex::new(HashMap::new()));
        let cancel_token = CancellationToken::new();
        if let Err(e) = DHT::run_ipv6(id, port, rx, tmap.clone(), cancel_token.clone()) {
            warn!("dht start server error {e}");
        }
        Self {
            id,
            port,
            version,
            tx,
            tid: 0.into(),
            tmap,
            _cancel_token: cancel_token.drop_guard(),
        }
    }

    fn run_ipv6(
        id: NodeID,
        port: u16,
        rx: mpsc::Receiver<Req>,
        tmap: Arc<Mutex<TransactionMap>>,
        cancel: CancellationToken,
    ) -> io::Result<()> {
        // TODO: multi-homing is common in ipv6
        // should bind to public address, see BEP 32
        let addr = format!("[::]:{}", port);
        let socket = match std::net::UdpSocket::bind(&addr) {
            Ok(s) => {
                s.set_nonblocking(true)?;
                UdpSocket::from_std(s)?
            }
            Err(e) => {
                warn!("error binding dht socket at {addr}, reason {e}");
                return Err(e);
            }
        };

        let server = Server {
            ipv6: true,
            id,
            s: socket,
            route: RoutingTable::new(id),
            tmap,
            nodes_buf: Vec::with_capacity(8),
            nodes4_buf: VecNode4(Vec::with_capacity(8)),
            nodes6_buf: VecNode6(Vec::with_capacity(8)),
        };
        tokio::spawn(server.serve(rx, cancel));
        Ok(())
    }

    async fn remove_route(&self, id: NodeID) -> io::Result<()> {
        let req = Req::RemoveRoute { id };
        self.tx.send(req).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "send remove-route request to worker error",
            )
        })
    }

    async fn do_rpc_req(
        &self,
        addr: RpcAddr,
        krpc: KRPC,
        timeout: time::Duration,
    ) -> io::Result<Resp> {
        let (tx, rx) = oneshot::channel();
        let tid = krpc.t.clone();
        let req = Req::KRPC {
            addr: match addr {
                RpcAddr::ID(na) => na.addr,
                RpcAddr::NoID(a) => a,
            },
            krpc,
        };
        let _drop_guard = TransactionGuard::new(tid.into_vec(), self.tmap.clone(), tx);
        if self.tx.send(req).await.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "send KRPC request to worker error",
            ));
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::Other,
                "recv result from worker error",
            )),
            Err(_) => {
                if let RpcAddr::ID(na) = addr {
                    self.remove_route(na.id).await?;
                }
                Err(io::Error::new(io::ErrorKind::Other, "timeout"))
            }
        }
    }

    pub async fn ping_rpc(&self, addr: RpcAddr, timeout: time::Duration) -> io::Result<Resp> {
        let tid = self.tid.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let tid: ByteString = tid[..].into();
        let krpc = KRPC {
            t: tid.clone(),
            v: self.version.clone().into(),
            inner: KRPCInner::Request(Arg::Ping(PingArg { id: self.id })),
        };
        self.do_rpc_req(addr, krpc, timeout).await
    }

    pub async fn find_node_rpc(
        &self,
        addr: RpcAddr,
        target: NodeID,
        timeout: time::Duration,
    ) -> io::Result<Resp> {
        let tid = self.tid.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let tid: ByteString = tid[..].into();
        let krpc = KRPC {
            t: tid.clone(),
            v: self.version.clone().into(),
            inner: KRPCInner::Request(Arg::FindNode(FindNodeArg {
                id: self.id,
                target,
            })),
        };
        self.do_rpc_req(addr, krpc, timeout).await
    }

    pub async fn get_peers_rpc(
        &self,
        addr: RpcAddr,
        info_hash: NodeID,
        timeout: time::Duration,
    ) -> io::Result<Resp> {
        let tid = self.tid.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let tid: ByteString = tid[..].into();
        let krpc = KRPC {
            t: tid.clone(),
            v: self.version.clone().into(),
            inner: KRPCInner::Request(Arg::GetPeers(GetPeersArg {
                id: self.id,
                info_hash,
            })),
        };
        self.do_rpc_req(addr, krpc, timeout).await
    }

    pub async fn announce_peer(
        &self,
        addr: RpcAddr,
        info_hash: NodeID,
        port: u16,
        implied: bool,
        token: &[u8],
        timeout: time::Duration,
    ) -> io::Result<Resp> {
        let tid = self.tid.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let tid: ByteString = tid[..].into();
        let krpc = KRPC {
            t: tid,
            v: self.version.clone().into(),
            inner: KRPCInner::Request(Arg::AnnouncePeer(AnnouncePeerArg {
                id: self.id,
                implied_port: if implied { 1 } else { 0 },
                info_hash,
                port,
                token: token.into(),
            })),
        };
        self.do_rpc_req(addr, krpc, timeout).await
    }
}

enum Req {
    RemoveRoute { id: NodeID },
    KRPC { addr: SocketAddr, krpc: KRPC },
}

type TransactionMap = HashMap<Vec<u8>, oneshot::Sender<io::Result<Resp>>>;

struct Server {
    ipv6: bool,
    s: UdpSocket,
    id: NodeID,

    route: RoutingTable,

    /// transaction id map
    tmap: Arc<Mutex<TransactionMap>>,

    nodes_buf: Vec<NodeAddr>,
    nodes4_buf: VecNode4,
    nodes6_buf: VecNode6,
}

fn to_nodes64(ns: &[NodeAddr], v4: &mut VecNode4, v6: &mut VecNode6) {
    let r4 = &mut v4.0;
    let r6 = &mut v6.0;
    r4.clear();
    r6.clear();
    for na in ns {
        match na.addr {
            SocketAddr::V4(s4) => r4.push((na.id, s4)),
            SocketAddr::V6(s6) => r6.push((na.id, s6)),
        }
    }
}

const BUF_MAX: usize = 10240;

impl Server {
    async fn serve(mut self, mut out_req: mpsc::Receiver<Req>, cancel_token: CancellationToken) {
        let mut in_buf = vec![0u8; BUF_MAX];
        let mut out_buf = vec![0u8; BUF_MAX];
        loop {
            tokio::select! {
                _ = self.handle_income(&mut in_buf) => {},
                Some(req) = out_req.recv() => {
                    match req {
                        Req::KRPC{ addr, krpc } => self.handle_out_req(
                            addr, krpc, &mut out_buf,
                        ).await,
                        Req::RemoveRoute { id } => self.route.remove_route(&id),
                    }
                },
                _ = cancel_token.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn handle_income(&mut self, buf: &mut [u8]) {
        let (n, peer_addr) = match self.s.recv_from(buf).await {
            Err(e) => {
                info!("dht: receive error {e}");
                return;
            }
            Ok((BUF_MAX, _)) => {
                // TODO: use span to automatically insert "dht:"?
                info!("dht: packet too large");
                return;
            }
            Ok(m) => m,
        };
        let msg: KRPC = match bt_bencode::from_slice(&buf[..n]) {
            Ok(m) => m,
            Err(e) => {
                info!("dht: bdecode error {e}");
                return;
            }
        };
        self.handle_krpc_in(msg, peer_addr).await;
    }

    async fn send_response(&self, addr: SocketAddr, resp: &KRPC) -> io::Result<()> {
        match bt_bencode::to_vec(&resp) {
            Ok(resp_buf) => {
                _ = self.s.send_to(&resp_buf, addr).await?;
            }
            Err(e) => {
                warn!("dht at response, bencode error {e}");
            }
        }
        Ok(())
    }

    async fn handle_krpc_in(&mut self, krpc: KRPC, from_addr: SocketAddr) {
        let version: ByteString = "st01".into();
        match krpc.inner {
            KRPCInner::Request(Arg::Ping(p)) => {
                info!("receive ping from {}", from_addr);
                let resp = KRPC {
                    t: krpc.t,
                    v: version,
                    inner: KRPCInner::Response(Resp {
                        id: self.id,
                        nodes: None,
                        nodes6: None,
                        token: None,
                        values: None,
                    }),
                };
                self.route.add_route(NodeAddr {
                    id: p.id,
                    addr: from_addr,
                });
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Request(Arg::AnnouncePeer(a)) => {
                info!("receive announce_peer from {}", from_addr);
                self.route.add_route(NodeAddr {
                    id: a.id,
                    addr: from_addr,
                });
                // todo!("add data to storage")
            }
            KRPCInner::Request(Arg::FindNode(f)) => {
                info!("receive find_node from {}", from_addr);
                self.route.add_route(NodeAddr {
                    id: f.id,
                    addr: from_addr,
                });
                self.nodes_buf.clear();
                self.route
                    .get_k_closest_nodes(&f.id, 8, &mut self.nodes_buf);
                to_nodes64(&self.nodes_buf, &mut self.nodes4_buf, &mut self.nodes6_buf);
                let resp = KRPC {
                    t: krpc.t,
                    v: version,
                    inner: KRPCInner::Response(Resp {
                        id: self.id,
                        // TODO: optimize clone, use ref or cow
                        nodes: (!self.ipv6).then_some(self.nodes4_buf.clone()),
                        nodes6: (self.ipv6).then_some(self.nodes6_buf.clone()),
                        token: None,
                        values: None,
                    }),
                };
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Request(Arg::GetPeers(gp)) => {
                info!("receive get_peer from {}", from_addr);
                self.route.add_route(NodeAddr {
                    id: gp.id,
                    addr: from_addr,
                });
                self.nodes_buf.clear();
                self.route
                    .get_k_closest_nodes(&gp.id, 8, &mut self.nodes_buf);
                to_nodes64(&self.nodes_buf, &mut self.nodes4_buf, &mut self.nodes6_buf);
                let resp = KRPC {
                    t: krpc.t,
                    v: version,
                    inner: KRPCInner::Response(Resp {
                        id: self.id,
                        nodes: (!self.ipv6).then_some(self.nodes4_buf.clone()),
                        nodes6: (self.ipv6).then_some(self.nodes6_buf.clone()),
                        token: Some("abaaabba".into()), // TODO generate token
                        values: None,                   // TODO: return peers from storage
                    }),
                };
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Response(resp) => {
                self.route.add_route(NodeAddr {
                    id: resp.id,
                    addr: from_addr,
                });
                if let Some(ns) = &resp.nodes6 {
                    if self.ipv6 {
                        for (id, addr) in &ns.0 {
                            self.route.add_route(NodeAddr {
                                id: *id,
                                addr: SocketAddr::V6(*addr),
                            });
                        }
                    }
                }
                if let Some(ns) = &resp.nodes {
                    if !self.ipv6 {
                        for (id, addr) in &ns.0 {
                            self.route.add_route(NodeAddr {
                                id: *id,
                                addr: SocketAddr::V4(*addr),
                            });
                        }
                    }
                }
                if let Some(ret) = self.tmap.lock().unwrap().remove(krpc.t.as_slice()) {
                    _ = ret.send(Ok(resp));
                } else {
                    info!("dht unknown transaction id");
                }
            }
            KRPCInner::Err(items) => todo!(),
        }
    }

    async fn handle_out_req(&mut self, addr: SocketAddr, krpc: KRPC, mut buf: &mut Vec<u8>) {
        buf.clear();
        match bt_bencode::to_writer(&mut buf, &krpc) {
            Ok(b) => b,
            Err(e) => {
                if let Some(v) = self.tmap.lock().unwrap().remove(krpc.t.as_slice()) {
                    _ = v.send(Err(e.into()));
                }
                return;
            }
        };
        match krpc.inner {
            KRPCInner::Request(Arg::Ping(id)) => {}
            KRPCInner::Request(Arg::AnnouncePeer(a)) => {}
            KRPCInner::Request(Arg::FindNode(f)) => {}
            KRPCInner::Request(Arg::GetPeers(gp)) => {}
            KRPCInner::Response(resp) => todo!(),
            KRPCInner::Err(items) => todo!(),
        }
        if let Err(e) = self.s.send_to(buf, addr).await {
            warn!("send udp packet failed error {e}");
        }
    }
}

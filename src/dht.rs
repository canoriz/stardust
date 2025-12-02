use bt_bencode::ByteString;
use bt_bencode::Value as BtValue;
use core::time;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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

use routing::dist;

pub type NodeID = [u8; 20];

pub struct DHT {
    id: NodeID,
    port: u16,
    version: String,

    tid: AtomicU64,

    net_type: NetType,

    // ipv6 request sender
    tx6: mpsc::Sender<Req>,
    // ipv4 request sender
    tx4: mpsc::Sender<Req>,

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

pub struct NetType(u32);

impl NetType {
    pub const V4: NetType = NetType(0b1);
    pub const V6: NetType = NetType(0b10);

    fn enabled(&self, nt: Self) -> bool {
        self.0 & nt.0 > 0
    }
}

impl std::ops::BitOr for NetType {
    type Output = Self;

    #[inline]
    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::fmt::Debug for NetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 & (Self::V4.0 | Self::V6.0) > 0 {
            write!(f, "DUAL STACK")
        } else if self.0 & (Self::V4.0) > 0 {
            write!(f, "IPV4 ONLY")
        } else if self.0 & (Self::V6.0) > 0 {
            write!(f, "IPV6 ONLY")
        } else {
            write!(f, "NONE")
        }
    }
}

impl DHT {
    pub fn new(id: NodeID, port: u16, version: String, nt: NetType) -> Self {
        let mut nt = nt;
        let (tx6, rx6) = mpsc::channel(2048);
        let (tx4, rx4) = mpsc::channel(2048);
        let tmap = Arc::new(Mutex::new(HashMap::new()));
        let cancel_token = CancellationToken::new();

        if nt.enabled(NetType::V6) {
            if let Err(e) = DHT::run_ipv6(id, port, rx6, tmap.clone(), cancel_token.clone()) {
                warn!("dht start v6 server error {e}");
                nt = NetType(nt.0 & !(NetType::V6.0));
            }
        }
        if nt.enabled(NetType::V4) {
            if let Err(e) = DHT::run_ipv4(id, port, rx4, tmap.clone(), cancel_token.clone()) {
                warn!("dht start v4 server error {e}");
                nt = NetType(nt.0 & !(NetType::V4.0));
            }
        }
        Self {
            id,
            port,
            version,
            tx6,
            tx4,
            tid: 0.into(),
            tmap,
            net_type: nt,
            _cancel_token: cancel_token.drop_guard(),
        }
    }

    fn run_ipv4(
        id: NodeID,
        port: u16,
        rx: mpsc::Receiver<Req>,
        tmap: Arc<Mutex<TransactionMap>>,
        cancel: CancellationToken,
    ) -> io::Result<()> {
        let addr = format!("0.0.0.0:{}", port);
        let socket = match std::net::UdpSocket::bind(&addr) {
            Ok(s) => {
                s.set_nonblocking(true)?;
                UdpSocket::from_std(s)?
            }
            Err(e) => {
                warn!("error binding dht v4 socket at {addr}, reason {e}");
                return Err(e);
            }
        };

        let server = Server {
            ipv6: false,
            id,
            s: socket,
            route: RoutingTable::new(id),
            tmap,
            nodes_buf: Vec::with_capacity(8),
            nodes4_buf: VecNode4(Vec::with_capacity(8)),
            nodes6_buf: VecNode6(Vec::with_capacity(8)),
            out_buf: Vec::with_capacity(BUF_MAX),
        };
        tokio::spawn(server.serve(rx, cancel));
        Ok(())
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
                warn!("error binding dht v6 socket at {addr}, reason {e}");
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
            out_buf: Vec::with_capacity(BUF_MAX),
        };
        tokio::spawn(server.serve(rx, cancel));
        Ok(())
    }

    async fn remove_route(&self, id: NodeID) -> io::Result<()> {
        let req = Req::RemoveRoute { id };
        self.tx6.send(req).await.map_err(|_| {
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
        let ip_addr = match addr {
            RpcAddr::ID(na) => na.addr,
            RpcAddr::NoID(a) => a,
        };
        let req = Req::KRPC {
            addr: ip_addr,
            krpc,
        };

        let _drop_guard = TransactionGuard::new(tid.into_vec(), self.tmap.clone(), tx);
        let send_to_worker = match ip_addr {
            SocketAddr::V4(_) => self.tx4.send(req).await,
            SocketAddr::V6(_) => self.tx6.send(req).await,
        };
        if send_to_worker.is_err() {
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

    /// get k closest nodes to id
    async fn get_k_closest(&self, id: NodeID, k: usize, ipv6: bool) -> Vec<NodeAddr> {
        let (tx, rx) = oneshot::channel();

        async fn wait_result(
            rx: oneshot::Receiver<Vec<NodeAddr>>,
        ) -> Result<Vec<NodeAddr>, &'static str> {
            let timeout = time::Duration::from_secs(3);
            match tokio::time::timeout(timeout, rx).await {
                Ok(Ok(r)) => Ok(r),
                Ok(Err(_)) => Err("wait result error"),
                Err(_) => Err("timeout"),
            }
        }

        let mut nodes = if !ipv6 && self.net_type.enabled(NetType::V4) {
            self.tx4.send(Req::GetClosestNodes { id, k, tx: tx }).await;
            match wait_result(rx).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("dht: ipv4 get_k_closest err {}", e);
                    vec![]
                }
            }
        } else if ipv6 && self.net_type.enabled(NetType::V6) {
            self.tx6.send(Req::GetClosestNodes { id, k, tx }).await;
            match wait_result(rx).await {
                Ok(r) => r,
                Err(e) => {
                    warn!("dht: ipv6 get_k_closest err {}", e);
                    vec![]
                }
            }
        } else {
            vec![]
        };

        nodes.sort_by_cached_key(|n| dist(&id, &n.id));
        nodes
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
        self: &Arc<Self>,
        addr: RpcAddr,
        target: NodeID,
        timeout: time::Duration,
    ) -> io::Result<Resp> {
        info!("request find_node {target:?} to {addr:?}");
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

    pub async fn announce_peer_rpc(
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

    /// find closest nodes to target, returns closest IPV4 or IPV6 nodes
    pub async fn find_closest_node_to(
        self: &Arc<Self>,
        target: NodeID,
        ipv6: bool,
    ) -> Vec<NodeAddr> {
        const K: usize = 8;
        const ALPHA: usize = 3;
        let timeout = time::Duration::from_secs(5);

        let send_req = |client: Arc<DHT>,
                        target: NodeID,
                        timeout: time::Duration,
                        addr: RpcAddr,
                        resp: mpsc::Sender<Result<Vec<NodeAddr>, NodeID>>| {
            tokio::spawn(async move {
                let mut ns = Vec::with_capacity(8);
                match client.find_node_rpc(addr, target, timeout).await {
                    Ok(r) => {
                        if !ipv6 {
                            if let Some(n4) = r.nodes {
                                ns.extend(n4.0.into_iter().map(|(id, a)| NodeAddr {
                                    id,
                                    addr: SocketAddr::V4(a),
                                }))
                            }
                        } else if let Some(n6) = r.nodes6 {
                            ns.extend(n6.0.into_iter().map(|(id, a)| NodeAddr {
                                id,
                                addr: SocketAddr::V6(a),
                            }))
                        }
                        ns.sort_by_cached_key(|n| dist(&target, &n.id));
                        _ = resp.send(Ok(ns)).await;
                    }
                    Err(e) => {
                        info!("in find_closest_node, node {target:?} does not respond, error {e}");
                        _ = resp.send(Err(target)).await;
                    }
                };
            })
        };

        struct Dist {
            dist: NodeID,
            addr: NodeAddr,
        }
        impl core::cmp::Ord for Dist {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.dist.cmp(&other.dist)
            }
        }
        impl core::cmp::PartialOrd for Dist {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.dist.partial_cmp(&other.dist)
            }
        }
        impl core::cmp::PartialEq for Dist {
            fn eq(&self, other: &Self) -> bool {
                self.dist == other.dist
            }
        }
        impl core::cmp::Eq for Dist {}

        #[derive(Debug, Eq, PartialEq)]
        enum State {
            Seen,    // known but not queried nodes
            Queried, // queried nodes
            Deleted, // unreachable nodes
        }

        let (resp_tx, mut resp_rx) = mpsc::channel::<Result<Vec<NodeAddr>, NodeID>>(K);
        let mut node_state: HashMap<NodeID, State> = HashMap::new();
        let mut closest_nodes: BTreeSet<Dist> = BTreeSet::new();

        // send the initial node candidates
        _ = resp_tx
            .send(Ok(self.get_k_closest(target, K, ipv6).await))
            .await;

        let mut nodes = vec![];
        while let Some(r) = resp_rx.recv().await {
            match r {
                Ok(nodes) => {
                    for n in nodes {
                        match node_state.get(&n.id) {
                            Some(State::Deleted) => {
                                closest_nodes.retain(|x| x.addr.id != n.id);
                            }
                            None => {
                                node_state.insert(n.id, State::Seen);
                                closest_nodes.insert(Dist {
                                    dist: dist(&target, &n.id),
                                    addr: n,
                                });
                                if closest_nodes.len() > K {
                                    closest_nodes.pop_last();
                                }
                            }
                            Some(_) => {}
                        }
                    }
                }
                Err(id) => {
                    info!("receive response from {id:?} error");
                    node_state.insert(id, State::Deleted);
                    closest_nodes.retain(|x| x.addr.id != id);
                }
            }

            let mut q = 0;
            for Dist { addr, .. } in closest_nodes.iter() {
                match node_state.get(&addr.id) {
                    Some(State::Seen) | None => {
                        node_state.insert(addr.id, State::Queried);
                        send_req(
                            self.clone(),
                            target,
                            timeout,
                            RpcAddr::ID(*addr),
                            resp_tx.clone(),
                        );
                        q += 1;
                        if q >= ALPHA {
                            break;
                        }
                    }
                    Some(s) => assert_eq!(*s, State::Queried),
                }
            }

            if q == 0 {
                // TODO: is the correct, may there any request in flight?
                nodes = closest_nodes.into_iter().map(|x| x.addr).collect();
                break;
            }
        }
        nodes
    }
}

enum Req {
    RemoveRoute {
        id: NodeID,
    },
    GetClosestNodes {
        id: NodeID,
        k: usize,
        tx: oneshot::Sender<Vec<NodeAddr>>,
    },
    KRPC {
        addr: SocketAddr,
        krpc: KRPC,
    },
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
    out_buf: Vec<u8>,
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
        loop {
            tokio::select! {
                _ = self.handle_income(&mut in_buf) => {},
                Some(req) = out_req.recv() => {
                    self.handle_user_req(req).await;
                }
                _ = cancel_token.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn handle_user_req(&mut self, req: Req) {
        match req {
            Req::KRPC { addr, krpc } => self.handle_out_req(addr, krpc).await,
            Req::RemoveRoute { id } => self.route.remove_route(&id),
            Req::GetClosestNodes { id, k, tx } => {
                let mut nodes = Vec::with_capacity(8);
                self.route.get_k_closest_nodes(&id, k, &mut nodes);
                _ = tx.send(nodes);
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

    async fn handle_out_req(&mut self, addr: SocketAddr, krpc: KRPC) {
        self.out_buf.clear();
        match bt_bencode::to_writer(&mut self.out_buf, &krpc) {
            Ok(b) => b,
            Err(e) => {
                if let Some(v) = self.tmap.lock().unwrap().remove(krpc.t.as_slice()) {
                    _ = v.send(Err(e.into()));
                }
                return;
            }
        };
        if let Err(e) = self.s.send_to(&self.out_buf, addr).await {
            warn!("send udp packet failed error {e}");
        }
    }
}

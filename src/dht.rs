use bt_bencode::ByteString;
use bt_bencode::Value as BtValue;
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
use std::time;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, info, warn};

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

    // request sender
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

impl From<ByteSocketAddr> for SocketAddr {
    fn from(value: ByteSocketAddr) -> Self {
        value.0
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
    #[serde(skip_serializing_if = "Option::is_none")]
    implied_port: Option<u32>,
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
    pub id: NodeID,
    pub addr: SocketAddr,
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
            warn!("dht start v6 server error {e}");
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

    /// Run a dual-stack ipv6 socket listening port.
    /// This socket can receive ipv4 packets from a
    /// v4 mapped v6 address.
    fn run_ipv6(
        id: NodeID,
        port: u16,
        rx: mpsc::Receiver<Req>,
        tmap: Arc<Mutex<TransactionMap>>,
        cancel: CancellationToken,
    ) -> io::Result<()> {
        use socket2::{Domain, Protocol, Socket, Type};

        // TODO: multi-homing is common in ipv6
        // should bind to public address, see BEP 32
        let addr: std::net::SocketAddr = format!("[::]:{}", port).parse().unwrap();
        let sock = socket2::Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
        if let Err(e) = sock.set_only_v6(false) {
            warn!("set socket dual-stack error {e}");
            return Err(e);
        }

        let socket = match sock.bind(&addr.into()) {
            Ok(_) => {
                let s: std::net::UdpSocket = sock.into();
                s.set_nonblocking(true)?;
                UdpSocket::from_std(s)?
            }
            Err(e) => {
                warn!("error binding dht v6 dual stack socket at {addr}, reason {e}");
                return Err(e);
            }
        };

        let server = Server {
            id,
            s: socket,
            route4: RoutingTable::new(id),
            route6: RoutingTable::new(id),
            tmap,
            storage: Storage::new(),

            nodes_buf: Vec::with_capacity(8),
            nodes4_buf: VecNode4(Vec::with_capacity(8)),
            nodes6_buf: VecNode6(Vec::with_capacity(8)),
            out_buf: Vec::with_capacity(BUF_MAX),
        };
        tokio::spawn(server.serve(rx, cancel));
        Ok(())
    }

    async fn remove_route(&self, ipv6: bool, id: NodeID) -> io::Result<()> {
        let req = Req::RemoveRoute { ipv6, id };
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
        let sock_addr = match addr {
            RpcAddr::ID(na) => na.addr,
            RpcAddr::NoID(a) => a,
        };
        let ipv6 = is_ipv6(sock_addr);
        let req = Req::KRPC {
            addr: sock_addr,
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
                    self.remove_route(ipv6, na.id).await?;
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

        let mut nodes = {
            _ = self.tx.send(Req::GetClosestNodes { ipv6, id, k, tx }).await;
            match wait_result(rx).await {
                Ok(r) => r,
                Err(e) => {
                    let protocol = if ipv6 { "ipv6" } else { "ipv4" };
                    warn!("dht: {protocol} get_k_closest err {e}");
                    vec![]
                }
            }
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
        debug!("request find_node {target:?} to {addr:?}");
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
                implied_port: if implied { Some(1) } else { Some(0) },
                info_hash,
                port,
                token: token.into(),
            })),
        };
        self.do_rpc_req(addr, krpc, timeout).await
    }

    pub async fn get_peers(self: &Arc<Self>, target: NodeID, ipv6: bool) -> Vec<SocketAddr> {
        let timeout = time::Duration::from_secs(5);
        let ns = self.find_closest_node_to(target, ipv6).await;

        use tokio::task::JoinSet;
        let mut js = JoinSet::new();

        for n in ns {
            let cl = self.clone();
            js.spawn(async move { cl.get_peers_rpc(RpcAddr::ID(n), target, timeout).await });
        }

        let mut ret = vec![];
        for r in js.join_all().await {
            if let Ok(resp) = r {
                if let Some(ps) = resp.values {
                    for addr in ps {
                        ret.push(addr.into());
                    }
                }
            }
        }
        ret
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
                        debug!("in find_closest_node, node {target:?} does not respond, error {e}");
                        _ = resp.send(Err(target)).await;
                    }
                };
            })
        };

        #[derive(Debug, Eq, PartialEq)]
        enum State {
            Seen,    // known but not queried nodes
            Queried, // queried nodes
            Deleted, // unreachable nodes
        }

        use routing::Dist;
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
                    debug!("receive response from {id:?} error");
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
        ipv6: bool,
        id: NodeID,
    },
    GetClosestNodes {
        ipv6: bool,
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
    s: UdpSocket,
    id: NodeID,

    route6: RoutingTable,
    route4: RoutingTable,

    // peer info storage
    storage: Storage,

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

fn is_ipv6(addr: SocketAddr) -> bool {
    match addr {
        SocketAddr::V4(_) => true,
        SocketAddr::V6(v6) => v6.ip().to_ipv4_mapped().is_none(),
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
            Req::RemoveRoute { ipv6, id } => {
                if ipv6 {
                    self.route6.remove_route(&id);
                } else {
                    self.route4.remove_route(&id);
                }
            }
            Req::GetClosestNodes { ipv6, id, k, tx } => {
                let mut nodes = Vec::with_capacity(8);
                if ipv6 {
                    self.route6.get_k_closest_nodes(&id, k, &mut nodes);
                } else {
                    self.route4.get_k_closest_nodes(&id, k, &mut nodes);
                }
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
        let ipv6 = is_ipv6(from_addr);
        let mut add_route = |id: NodeID| {
            if ipv6 {
                self.route6.add_route(NodeAddr {
                    id,
                    addr: from_addr,
                });
            } else {
                self.route4.add_route(NodeAddr {
                    id,
                    addr: from_addr,
                });
            }
        };
        match krpc.inner {
            KRPCInner::Request(Arg::Ping(p)) => {
                debug!("receive ping from {}", from_addr);
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
                add_route(p.id);
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Request(Arg::AnnouncePeer(a)) => {
                debug!("receive announce_peer from {}", from_addr);
                add_route(a.id);
                let port = if let Some(1) = a.implied_port {
                    from_addr.port()
                } else {
                    a.port
                };
                let naddr = NodeAddr {
                    id: a.id,
                    addr: SocketAddr::new(from_addr.ip(), port),
                };
                self.storage.add(a.info_hash, naddr);
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
                _ = self.send_response(from_addr, &resp).await;
                // TODO:("remove old peer entries");
            }
            KRPCInner::Request(Arg::FindNode(f)) => {
                debug!("receive find_node from {}", from_addr);
                add_route(f.id);
                self.nodes_buf.clear();
                if ipv6 {
                    // TODO: support "want" field
                    self.route6
                        .get_k_closest_nodes(&f.target, 8, &mut self.nodes_buf);
                } else {
                    self.route4
                        .get_k_closest_nodes(&f.target, 8, &mut self.nodes_buf);
                }
                to_nodes64(&self.nodes_buf, &mut self.nodes4_buf, &mut self.nodes6_buf);
                let resp = KRPC {
                    t: krpc.t,
                    v: version,
                    inner: KRPCInner::Response(Resp {
                        id: self.id,
                        // TODO: optimize clone, use ref or cow
                        nodes: (!ipv6).then_some(self.nodes4_buf.clone()),
                        nodes6: (ipv6).then_some(self.nodes6_buf.clone()),
                        token: None,
                        values: None,
                    }),
                };
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Request(Arg::GetPeers(gp)) => {
                debug!("receive get_peer from {}", from_addr);
                add_route(gp.id);
                self.nodes_buf.clear();
                if ipv6 {
                    // TODO: support "want" field
                    self.route6
                        .get_k_closest_nodes(&gp.info_hash, 8, &mut self.nodes_buf);
                } else {
                    self.route4
                        .get_k_closest_nodes(&gp.info_hash, 8, &mut self.nodes_buf);
                }
                to_nodes64(&self.nodes_buf, &mut self.nodes4_buf, &mut self.nodes6_buf);
                let peers: Vec<ByteSocketAddr> = self
                    .storage
                    .get(&gp.info_hash, ipv6)
                    .into_iter()
                    .map(|v| v.addr.into())
                    .collect();
                let resp = KRPC {
                    t: krpc.t,
                    v: version,
                    inner: KRPCInner::Response(Resp {
                        id: self.id,
                        nodes: (!ipv6).then_some(self.nodes4_buf.clone()),
                        nodes6: (ipv6).then_some(self.nodes6_buf.clone()),
                        token: Some("abaaabba".into()), // TODO generate token
                        values: if peers.is_empty() { None } else { Some(peers) },
                    }),
                };
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Response(resp) => {
                add_route(resp.id);
                if let Some(ns) = &resp.nodes6 {
                    for (id, addr) in &ns.0 {
                        self.route6.add_route(NodeAddr {
                            id: *id,
                            addr: SocketAddr::V6(*addr),
                        });
                    }
                }
                if let Some(ns) = &resp.nodes {
                    for (id, addr) in &ns.0 {
                        self.route4.add_route(NodeAddr {
                            id: *id,
                            addr: SocketAddr::V4(*addr),
                        });
                    }
                }
                if let Some(ret) = self.tmap.lock().unwrap().remove(krpc.t.as_slice()) {
                    _ = ret.send(Ok(resp));
                } else {
                    debug!("dht unknown transaction id");
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

type ContactInfo = (NodeAddr, time::Instant);
struct Storage {
    peers4: HashMap<NodeID, Vec<ContactInfo>>,
    peers6: HashMap<NodeID, Vec<ContactInfo>>,
}

impl Storage {
    fn new() -> Self {
        Self {
            peers4: HashMap::new(),
            peers6: HashMap::new(),
        }
    }

    fn add(&mut self, id: NodeID, naddr: NodeAddr) {
        let ipv6 = is_ipv6(naddr.addr);

        // the ipv4/v6 aware representation
        // must convert v6 to v4 because the bencode serialization format
        // is different
        let naddr = NodeAddr {
            id: naddr.id,
            addr: SocketAddr::new(naddr.addr.ip().to_canonical(), naddr.addr.port()),
        };

        if ipv6 {
            self.peers6
                .entry(id)
                .and_modify(|v| v.push((naddr, time::Instant::now())))
                .or_insert(vec![(naddr, time::Instant::now())]);
        } else {
            self.peers4
                .entry(id)
                .and_modify(|v| v.push((naddr, time::Instant::now())))
                .or_insert(vec![(naddr, time::Instant::now())]);
        }
    }

    fn get(&self, id: &NodeID, ipv6: bool) -> Vec<NodeAddr> {
        let map = if ipv6 { &self.peers6 } else { &self.peers4 };
        if let Some(v) = map.get(id) {
            v.iter().map(|(n, _)| *n).collect()
        } else {
            vec![]
        }
    }
}

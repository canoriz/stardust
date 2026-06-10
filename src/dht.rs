use bon::Builder;
use bt_bencode::ByteString;
use bt_bencode::Value as BtValue;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time;
use tokio::net::lookup_host;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::instrument;
use tracing::{debug, info, warn};

mod routing;
mod wire;
mod wire_serde;
use routing::RoutingTable;
use wire::WireKRPC;

use routing::dist;

pub type NodeID = [u8; 20];

/// Snapshot of all nodes known to the DHT routing tables at shutdown time.
/// On restore every node is inserted as unreachable so the DHT re-probes it.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DhtDump {
    /// (node_id, addr) pairs from the IPv4/IPv6 routing table.
    pub nodes: Vec<(NodeID, SocketAddr)>,
}

/// Per-bootstrap-node tracking state for on-demand ping logic.
struct BootstrapNodeState {
    /// Number of ping attempts to this node.
    attempt_v4: u32,
    attempt_v6: u32,
    /// When the last ping attempt was sent (for rate-limiting).
    last_v4_ping_attempt: Option<time::Instant>,
    last_v6_ping_attempt: Option<time::Instant>,
}

pub struct DHT {
    id: NodeID,
    port: u16,
    version: String,

    // bootstrap nodes: domain -> state
    bootstrap_nodes: Mutex<HashMap<String, BootstrapNodeState>>,

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

#[derive(Clone, Derivative, PartialEq, Serialize, Deserialize)]
#[derivative(Debug)]
struct PingArg {
    #[serde(with = "serde_bytes")]
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    id: NodeID,
}

#[derive(Clone, Derivative, PartialEq, Serialize, Deserialize)]
#[derivative(Debug)]
struct FindNodeArg {
    #[serde(with = "serde_bytes")]
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    id: NodeID,
    #[serde(with = "serde_bytes")]
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    target: NodeID,
    #[serde(default = "Vec::new")]
    want: Vec<ByteString>,
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

#[derive(Clone, Derivative, PartialEq, Serialize, Deserialize)]
#[derivative(Debug)]
struct GetPeersArg {
    #[serde(with = "serde_bytes")]
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    id: NodeID,

    #[serde(with = "serde_bytes")]
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    info_hash: [u8; 20],

    #[serde(default = "Vec::new")]
    want: Vec<ByteString>,
}

#[derive(Clone, Derivative, PartialEq, Serialize, Deserialize)]
#[derivative(Debug)]
struct AnnouncePeerArg {
    #[serde(with = "serde_bytes")]
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    id: NodeID,

    #[serde(skip_serializing_if = "Option::is_none")]
    implied_port: Option<u32>,

    #[serde(with = "serde_bytes")]
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
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

#[derive(Clone, Derivative, PartialEq, Serialize, Deserialize)]
#[derivative(Debug)]
pub struct Resp {
    #[serde(with = "serde_bytes")]
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
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

#[derive(Copy, Clone, Derivative, Eq, Hash, PartialEq)]
#[derivative(Debug)]
pub struct NodeAddr {
    #[derivative(Debug(format_with = "crate::helper::format_hex"))]
    pub id: NodeID,
    pub addr: SocketAddr,
    // TODO: maybe store both v4 and v6 addr if peer have both
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct RpcAddr<A> {
    id: Option<NodeID>,
    addr: A,
}

impl<A> RpcAddr<A>
where
    A: ToSocketAddrs,
{
    pub fn no_id(addr: A) -> Self {
        Self { id: None, addr }
    }

    pub fn id(id: NodeID, addr: A) -> Self {
        Self { id: Some(id), addr }
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

pub type AnnounceToken = Vec<u8>;

#[derive(Debug)]
pub struct GetPeersResult {
    pub peers: Vec<SocketAddr>,
    pub closest: Vec<(RpcAddr<SocketAddr>, Option<AnnounceToken>)>,
}

#[derive(Builder)]
pub struct DHTOption {
    id: NodeID,
    port: u16,
    version: String,

    #[builder(default)]
    bootstrap_nodes: Vec<String>,
}

impl DHT {
    pub fn new(opt: DHTOption) -> Self {
        let (tx, rx) = mpsc::channel(2048);
        let tmap = Arc::new(Mutex::new(HashMap::new()));
        let cancel_token = CancellationToken::new();

        if let Err(e) = DHT::run_ipv6(opt.id, opt.port, rx, tmap.clone(), cancel_token.clone()) {
            warn!("dht start v6 server error {e}");
        }

        let bootstrap_nodes = opt
            .bootstrap_nodes
            .into_iter()
            .map(|node| {
                (
                    node,
                    BootstrapNodeState {
                        attempt_v4: 0,
                        attempt_v6: 0,
                        last_v4_ping_attempt: None,
                        last_v6_ping_attempt: None,
                    },
                )
            })
            .collect();
        Self {
            id: opt.id,
            port: opt.port,
            version: opt.version,
            tx,
            tid: 0.into(),
            tmap,
            bootstrap_nodes: Mutex::new(bootstrap_nodes),
            _cancel_token: cancel_token.drop_guard(),
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Dump all nodes from both routing tables.
    /// Returns immediately; the result is fetched from the server task via a oneshot.
    pub async fn dump_nodes(&self) -> DhtDump {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Req::DumpNodes { tx }).await.is_err() {
            return DhtDump::default();
        }
        rx.await.unwrap_or_default()
    }

    /// Seed the routing tables with nodes from a previous session dump.
    /// Send a get_peer to each nodes
    pub fn seed_from_dump(self: &Arc<Self>, dump: &DhtDump) {
        let nodes: Vec<NodeAddr> = dump
            .nodes
            .iter()
            .map(|(id, addr)| NodeAddr {
                id: *id,
                addr: *addr,
            })
            .collect();
        let self_id = self.id;
        for node in nodes {
            let dht = self.clone();
            tokio::spawn(async move {
                let _ = dht
                    .get_peers_rpc(
                        RpcAddr::id(node.id, node.addr),
                        self_id,
                        time::Duration::from_secs(10),
                    )
                    .await;
            });
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
        use socket2::{Domain, Protocol, Type};

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

    #[instrument(skip_all, fields(to = ?addr.id, krpc_inner = ?krpc_inner, timeout), ret)]
    async fn do_rpc_req<A>(
        &self,
        addr: RpcAddr<A>,
        krpc_inner: KRPCInner,
        timeout: time::Duration,
    ) -> io::Result<Resp>
    where
        A: ToSocketAddrs,
    {
        use io::Error;

        let mut last_err = None;
        for sock_addr in addr.addr.to_socket_addrs()? {
            let (tx, rx) = oneshot::channel();
            let tid = self.tid.fetch_add(1, Ordering::Relaxed).to_be_bytes();
            let tid: ByteString = tid[..].into();
            let krpc = KRPC {
                t: tid.clone(),
                v: self.version.clone().into(),
                inner: krpc_inner.clone(),
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
                Ok(Ok(r)) => return r,
                Ok(Err(_)) => {
                    last_err = Some(io::Error::new(
                        io::ErrorKind::Other,
                        "recv result from worker error",
                    ))
                }
                Err(_) => {
                    if let Some(id) = addr.id {
                        self.remove_route(ipv6, id).await?;
                    }
                    debug!("dht rpc request to {sock_addr} timeout req: {krpc_inner:?}");
                    last_err = Some(io::Error::new(io::ErrorKind::Other, "timeout"));
                }
            }
        }

        match last_err {
            Some(err) => Err(err),
            None => Err(Error::new(io::ErrorKind::InvalidInput, "can not resolve")),
        }
    }

    /// get k closest nodes to id from our local routing table
    async fn get_k_closest_local(
        self: &Arc<Self>,
        id: NodeID,
        k: usize,
        ipv6: bool,
    ) -> Vec<NodeAddr> {
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

        let nodes = {
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

        if nodes.len() < k {
            self.ping_bootstrap_node(ipv6, id);
        }
        nodes
    }

    /// send bootstrap nodes a GetPeer request for target
    fn ping_bootstrap_node(self: &Arc<Self>, ipv6: bool, target: NodeID) {
        /// Stop bootstrapping a node after this many successful pings.
        const MAX_SUCCESS: u32 = 3;
        /// Minimum interval between ping attempts to the same bootstrap node.
        const MIN_INTERVAL: time::Duration = time::Duration::from_secs(1);

        let mut guard = self.bootstrap_nodes.lock().unwrap();
        let now = time::Instant::now();

        // Pick the first eligible candidate for the requested IP family.
        // "Eligible" means: under the attempt cap and not rate-limited.
        let mut candidates = guard.iter_mut().filter(|(_, state)| {
            let (attempt, last) = if ipv6 {
                (state.attempt_v6, state.last_v6_ping_attempt)
            } else {
                (state.attempt_v4, state.last_v4_ping_attempt)
            };
            attempt < MAX_SUCCESS && last.map_or(true, |t| now.duration_since(t) >= MIN_INTERVAL)
        });

        if let Some((node, state)) = candidates.next() {
            let query_node = node.clone();
            let cl = self.clone();
            if ipv6 {
                state.attempt_v6 += 1;
                state.last_v6_ping_attempt = Some(now);
            } else {
                state.attempt_v4 += 1;
                state.last_v4_ping_attempt = Some(now);
            }
            tokio::spawn(async move {
                // Try candidates in order. Stop at the first node that has a
                // matching address family and accepts a ping.
                let addrs = match lookup_host(&query_node).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("dht bootstrap resolve {} failed: {}", query_node, e);
                        cl.ping_bootstrap_node(ipv6, target);
                        return;
                    }
                };

                debug!("nodes not enough, ping bootstrap node {}", query_node);
                let mut success = false;
                for ping_addr in addrs.filter(|a| match a {
                    SocketAddr::V4(_) => !ipv6,
                    SocketAddr::V6(v) => match v.ip().to_ipv4_mapped() {
                        Some(_) => !ipv6,
                        None => ipv6,
                    },
                }) {
                    if cl
                        .get_peers_rpc(
                            RpcAddr::no_id(ping_addr),
                            target,
                            time::Duration::from_secs(3),
                        )
                        .await
                        .is_ok()
                    {
                        success = true;
                        break;
                    }
                }
                if !success {
                    cl.ping_bootstrap_node(ipv6, target);
                }
            });
        }
    }

    pub async fn ping_rpc<A>(
        self: &Arc<Self>,
        addr: RpcAddr<A>,
        timeout: time::Duration,
    ) -> io::Result<Resp>
    where
        A: ToSocketAddrs,
    {
        let krpc = KRPCInner::Request(Arg::Ping(PingArg { id: self.id }));
        self.do_rpc_req(addr, krpc, timeout).await
    }

    pub async fn find_node_rpc<A>(
        self: &Arc<Self>,
        addr: RpcAddr<A>,
        target: NodeID,
        timeout: time::Duration,
    ) -> io::Result<Resp>
    where
        A: ToSocketAddrs,
    {
        let krpc_inner = KRPCInner::Request(Arg::FindNode(FindNodeArg {
            id: self.id,
            target,
            want: vec![b"n4".as_ref().into(), b"n6".as_ref().into()],
        }));
        self.do_rpc_req(addr, krpc_inner, timeout).await
    }

    #[instrument(skip_all)]
    pub async fn get_peers_rpc<A>(
        self: &Arc<Self>,
        addr: RpcAddr<A>,
        info_hash: NodeID,
        timeout: time::Duration,
    ) -> io::Result<Resp>
    where
        A: ToSocketAddrs,
    {
        if let Some(id) = &addr.id {
            debug!("send get_peers to {:?}", crate::helper::to_hex(id));
        }
        let krpc_inner = KRPCInner::Request(Arg::GetPeers(GetPeersArg {
            id: self.id,
            info_hash,
            want: vec![b"n4".as_ref().into(), b"n6".as_ref().into()],
        }));
        self.do_rpc_req(addr, krpc_inner, timeout).await
    }

    pub async fn announce_peer_rpc<A>(
        self: &Arc<Self>,
        addr: RpcAddr<A>,
        info_hash: NodeID,
        port: u16,
        implied: bool,
        token: &[u8],
        timeout: time::Duration,
    ) -> io::Result<Resp>
    where
        A: ToSocketAddrs,
    {
        let krpc_inner = KRPCInner::Request(Arg::AnnouncePeer(AnnouncePeerArg {
            id: self.id,
            implied_port: if implied { Some(1) } else { Some(0) },
            info_hash,
            port,
            token: token.into(),
        }));
        self.do_rpc_req(addr, krpc_inner, timeout).await
    }

    /// get_peers queries peers of target across both IPv4 and IPv6.
    #[instrument(skip_all, fields(target = crate::helper::to_hex(&target)), ret)]
    pub async fn get_peers(self: &Arc<Self>, target: NodeID) -> GetPeersResult {
        self.get_peers_iterative(target).await
    }

    /// Find the K closest nodes to `target` across both IPv4 and IPv6.
    ///
    /// Returns `(v4_closest, v6_closest)` — up to K nodes each, converged
    /// independently.  Each family runs a full Kademlia iterative lookup:
    /// referrals from either family are fed into the correct candidate set,
    /// and the search terminates only when no family has unqueried candidates
    /// left in its top-K.
    async fn get_peers_iterative(self: &Arc<Self>, target: NodeID) -> GetPeersResult {
        const K: usize = 8;
        const ALPHA: usize = 5;
        let timeout = time::Duration::from_secs(5);

        // Channel message: Ok((v4_referrals, v6_referrals)) on success, Err(id) on timeout.
        type Err = (SocketAddr, Option<NodeID>);
        struct NodeResp {
            id: RpcAddr<SocketAddr>,
            v4_closest: Vec<NodeAddr>,
            v6_closest: Vec<NodeAddr>,
            peers: Vec<SocketAddr>,
            token: Option<Vec<u8>>,
        }
        // v4 closer nodes, v6 closer nodes, peers, optional (responder_addr, token)
        type Msg = Result<NodeResp, Err>;

        // Spawns a FindNode RPC; splits the response into (v4, v6) candidate lists.
        // `target` and `timeout` are Copy and captured from the outer scope.
        let send_req = |client: Arc<DHT>, addr: RpcAddr<SocketAddr>, tx: mpsc::Sender<Msg>| {
            tokio::spawn(async move {
                let node_id = addr.id;

                match client.get_peers_rpc(addr, target, timeout).await {
                    //    match client.find_node_rpc(addr, target, timeout).await {
                    Ok(r) => {
                        let mut v4 = r.nodes.map_or(vec![], |n| {
                            n.0.into_iter()
                                .map(|(id, a)| NodeAddr {
                                    id,
                                    addr: SocketAddr::V4(a),
                                })
                                .collect()
                        });
                        let v6 = r.nodes6.map_or(vec![], |n| {
                            n.0.into_iter()
                                .filter_map(|(id, a)| {
                                    // IPv4-mapped addresses belong in v4
                                    if let Some(v4addr) = a.ip().to_ipv4_mapped() {
                                        v4.push(NodeAddr {
                                            id,
                                            addr: SocketAddr::V4(SocketAddrV4::new(
                                                v4addr,
                                                a.port(),
                                            )),
                                        });
                                        return None;
                                    }
                                    Some(NodeAddr {
                                        id,
                                        addr: SocketAddr::V6(a),
                                    })
                                })
                                .collect()
                        });

                        let peers: Vec<SocketAddr> = r
                            .values
                            .map_or(vec![], |v| v.into_iter().map(|a| a.into()).collect());
                        _ = tx
                            .send(Ok(NodeResp {
                                id: addr,
                                v4_closest: v4,
                                v6_closest: v6,
                                peers,
                                token: r.token.map(|t| t.into_vec()),
                            }))
                            .await;
                    }
                    Err(e) => {
                        debug!("find_closest_node: {addr:?} did not respond: {e}");
                        _ = tx.send(Err((addr.addr, node_id))).await;
                    }
                }
            })
        };

        #[derive(Debug, Eq, PartialEq)]
        enum NodeState {
            Seen,                   // candidate discovered, not yet queried
            Querying,               // query in flight, no response yet
            Queried(AnnounceToken), // query in flight or completed
            Dead,                   // did not respond
        }

        use routing::Dist;
        let (resp_tx, mut resp_rx) = mpsc::channel::<Msg>(8 * K);
        let mut node_state4: HashMap<NodeID, NodeState> = HashMap::new();
        let mut node_state6: HashMap<NodeID, NodeState> = HashMap::new();
        let mut closest_nodes4: BTreeSet<Dist> = BTreeSet::new();
        let mut closest_nodes6: BTreeSet<Dist> = BTreeSet::new();

        // send the initial node candidates
        // in_flight tracks responses still pending; start at 1 for the initial seed message.
        let mut in_flight: usize = 1;
        _ = resp_tx
            .send(Ok(NodeResp {
                // the first "response" is synthetic, so we can put anything in it
                id: RpcAddr::no_id(SocketAddr::from(([0, 0, 0, 0], 0))),
                v4_closest: self.get_k_closest_local(target, K, false).await,
                v6_closest: self.get_k_closest_local(target, K, true).await,
                peers: vec![],
                token: None,
            }))
            .await;

        let mut peers = vec![];
        while let Some(r) = resp_rx.recv().await {
            in_flight -= 1;
            match r {
                Ok(NodeResp {
                    id,
                    v4_closest: n4,
                    v6_closest: n6,
                    peers: p,
                    token,
                }) => {
                    peers.extend(p);
                    for n in n4 {
                        if matches!(node_state4.get(&n.id), None) {
                            node_state4.insert(n.id, NodeState::Seen);
                            closest_nodes4.insert(Dist {
                                dist: dist(&target, &n.id),
                                addr: n,
                            });
                            if closest_nodes4.len() > K {
                                closest_nodes4.pop_last();
                            }
                        }
                    }
                    for n in n6 {
                        if matches!(node_state6.get(&n.id), None) {
                            node_state6.insert(n.id, NodeState::Seen);
                            closest_nodes6.insert(Dist {
                                dist: dist(&target, &n.id),
                                addr: n,
                            });
                            if closest_nodes6.len() > K {
                                closest_nodes6.pop_last();
                            }
                        }
                    }
                    if let (Some(token), Some(nid)) = (token, id.id) {
                        match id.addr {
                            SocketAddr::V4(_) => {
                                node_state4.insert(nid, NodeState::Queried(token.clone()))
                            }
                            SocketAddr::V6(_) => {
                                node_state6.insert(nid, NodeState::Queried(token.clone()))
                            }
                        };
                    }
                }
                Err((addr, id)) => {
                    debug!("receive response from {id:?} error");
                    if let Some(id) = id {
                        if addr.is_ipv4() {
                            node_state4.insert(id, NodeState::Dead);
                            closest_nodes4.retain(|Dist { addr, .. }| addr.id != id);
                        } else {
                            node_state6.insert(id, NodeState::Dead);
                            closest_nodes6.retain(|Dist { addr, .. }| addr.id != id);
                        }
                    }
                }
            }

            // q: how many closest nodes we know
            let mut q = 0;
            for Dist { addr, .. } in closest_nodes4.iter() {
                match node_state4.get(&addr.id) {
                    Some(NodeState::Seen) | None => {
                        node_state4.insert(addr.id, NodeState::Querying);
                        send_req(
                            self.clone(),
                            RpcAddr::id(addr.id, addr.addr),
                            resp_tx.clone(),
                        );
                        in_flight += 1;
                        q += 1;
                        if q >= ALPHA {
                            break;
                        }
                    }
                    Some(NodeState::Querying | NodeState::Queried(_)) => {}
                    Some(NodeState::Dead) => unreachable!("dead node in closest_nodes4"),
                }
            }

            // q: how many closest nodes we know
            let mut q = 0;
            for Dist { addr, .. } in closest_nodes6.iter() {
                match node_state6.get(&addr.id) {
                    Some(NodeState::Seen) | None => {
                        node_state6.insert(addr.id, NodeState::Querying);
                        send_req(
                            self.clone(),
                            RpcAddr::id(addr.id, addr.addr),
                            resp_tx.clone(),
                        );
                        in_flight += 1;
                        q += 1;
                        if q >= ALPHA {
                            break;
                        }
                    }
                    Some(NodeState::Querying | NodeState::Queried(_)) => {}
                    Some(NodeState::Dead) => unreachable!("dead node in closest_nodes6"),
                }
            }

            if in_flight == 0 {
                // we did not found any closer nodes
                break;
            }
        }
        let closest4 = closest_nodes4.iter().map(|Dist { addr, .. }| {
            (
                RpcAddr::id(addr.id, addr.addr),
                match node_state4.get(&addr.id) {
                    Some(NodeState::Queried(token)) => Some(token.clone()),
                    _ => None,
                },
            )
        });
        let closest6 = closest_nodes6.iter().map(|Dist { addr, .. }| {
            (
                RpcAddr::id(addr.id, addr.addr),
                match node_state6.get(&addr.id) {
                    Some(NodeState::Queried(token)) => Some(token.clone()),
                    _ => None,
                },
            )
        });

        GetPeersResult {
            peers,
            closest: closest4.chain(closest6).collect(),
        }
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
    DumpNodes {
        tx: oneshot::Sender<DhtDump>,
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

/// Convert an IPv4-mapped IPv6 address (::ffff:x.x.x.x) to a plain IPv4 address.
/// All other addresses are returned unchanged.
fn normalize_addr(addr: SocketAddr) -> SocketAddr {
    if let SocketAddr::V6(v6) = addr {
        if let Some(ipv4) = v6.ip().to_ipv4_mapped() {
            return SocketAddr::V4(SocketAddrV4::new(ipv4, v6.port()));
        }
    }
    addr
}

/// Parse the `want` list from a KRPC request into `(want_v4, want_v6)`.
/// Falls back to the requesting address family when `want` is absent or empty.
fn parse_want(want: &[ByteString], from_ipv6: bool) -> (bool, bool) {
    if want.is_empty() {
        return (!from_ipv6, from_ipv6);
    }
    let want_v4 = want.iter().any(|w| w.as_ref() == b"n4");
    let want_v6 = want.iter().any(|w| w.as_ref() == b"n6");
    (want_v4, want_v6)
}

fn is_ipv6(addr: SocketAddr) -> bool {
    match addr {
        SocketAddr::V4(_) => false,
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
            Req::DumpNodes { tx } => {
                let mut nodes = self.route4.all_nodes();
                nodes.extend(self.route6.all_nodes());
                let dump = DhtDump { nodes };
                _ = tx.send(dump);
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
                info!("dht: bdecode error {e} raw: {:?}", &buf[..n]);
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
        let from_ipv6 = is_ipv6(from_addr);
        let mut add_route = |id: NodeID, addr: SocketAddr, reachable: bool| {
            if addr.port() == 0 || addr.ip().is_unspecified() {
                return;
            }
            // Normalize IPv4-mapped-IPv6 addresses so they go into route4 with a true IPv4 addr.
            let addr = normalize_addr(addr);
            let ipv6 = is_ipv6(addr);
            if ipv6 {
                self.route6.add_route(NodeAddr { id, addr }, reachable);
            } else {
                self.route4.add_route(NodeAddr { id, addr }, reachable);
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
                add_route(p.id, from_addr, false);
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Request(Arg::AnnouncePeer(a)) => {
                debug!("receive announce_peer from {}", from_addr);
                add_route(a.id, from_addr, false);
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
                add_route(f.id, from_addr, false);
                let (want_v4, want_v6) = parse_want(&f.want, from_ipv6);
                let nodes4 = if want_v4 {
                    self.nodes_buf.clear();
                    self.route4
                        .get_k_closest_nodes(&f.target, 8, &mut self.nodes_buf);
                    self.nodes4_buf.0.clear();
                    for na in &self.nodes_buf {
                        if let SocketAddr::V4(s4) = na.addr {
                            self.nodes4_buf.0.push((na.id, s4));
                        }
                    }
                    Some(self.nodes4_buf.clone())
                } else {
                    None
                };
                let nodes6 = if want_v6 {
                    self.nodes_buf.clear();
                    self.route6
                        .get_k_closest_nodes(&f.target, 8, &mut self.nodes_buf);
                    self.nodes6_buf.0.clear();
                    for na in &self.nodes_buf {
                        if let SocketAddr::V6(s6) = na.addr {
                            self.nodes6_buf.0.push((na.id, s6));
                        }
                    }
                    Some(self.nodes6_buf.clone())
                } else {
                    None
                };
                let resp = KRPC {
                    t: krpc.t,
                    v: version,
                    inner: KRPCInner::Response(Resp {
                        id: self.id,
                        nodes: nodes4,
                        nodes6,
                        token: None,
                        values: None,
                    }),
                };
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Request(Arg::GetPeers(gp)) => {
                debug!("receive get_peer from {} {gp:?}", from_addr);
                add_route(gp.id, from_addr, false);
                let (want_v4, want_v6) = parse_want(&gp.want, from_ipv6);
                let nodes4 = if want_v4 {
                    self.nodes_buf.clear();
                    self.route4
                        .get_k_closest_nodes(&gp.info_hash, 8, &mut self.nodes_buf);
                    self.nodes4_buf.0.clear();
                    for na in &self.nodes_buf {
                        if let SocketAddr::V4(s4) = na.addr {
                            self.nodes4_buf.0.push((na.id, s4));
                        }
                    }
                    Some(self.nodes4_buf.clone())
                } else {
                    None
                };
                let nodes6 = if want_v6 {
                    self.nodes_buf.clear();
                    self.route6
                        .get_k_closest_nodes(&gp.info_hash, 8, &mut self.nodes_buf);
                    self.nodes6_buf.0.clear();
                    for na in &self.nodes_buf {
                        if let SocketAddr::V6(s6) = na.addr {
                            self.nodes6_buf.0.push((na.id, s6));
                        }
                    }
                    Some(self.nodes6_buf.clone())
                } else {
                    None
                };

                // values always correspond to the requesting address family, not the want field
                let peers: Vec<ByteSocketAddr> = self
                    .storage
                    .get(&gp.info_hash, from_ipv6)
                    .into_iter()
                    .map(|v| v.addr.into())
                    .collect();
                let resp = KRPC {
                    t: krpc.t,
                    v: version,
                    inner: KRPCInner::Response(Resp {
                        id: self.id,
                        nodes: nodes4,
                        nodes6,
                        token: Some("abaaabba".into()), // TODO generate token
                        values: if peers.is_empty() { None } else { Some(peers) },
                    }),
                };
                _ = self.send_response(from_addr, &resp).await;
            }
            KRPCInner::Response(resp) => {
                // This node replied to our outgoing request — it is proven reachable.
                add_route(resp.id, from_addr, true);
                if let Some(ns) = &resp.nodes6 {
                    for (id, addr) in &ns.0 {
                        if addr.port() == 0 || addr.ip().is_unspecified() {
                            let v = std::str::from_utf8(krpc.v.as_ref())
                                .map(|s| s.to_owned())
                                .unwrap_or_else(|_| format!("{:02x?}", krpc.v.as_ref()));
                            warn!(
                                "peer {} (client v={:?}) sent bogus v6 node {:02x?} -> {}",
                                from_addr, v, id, addr
                            );
                        } else {
                            add_route(*id, SocketAddr::V6(*addr), false);
                        }
                    }
                }
                if let Some(ns) = &resp.nodes {
                    for (id, addr) in &ns.0 {
                        if addr.port() == 0 || addr.ip().is_unspecified() {
                            let v = std::str::from_utf8(krpc.v.as_ref())
                                .map(|s| s.to_owned())
                                .unwrap_or_else(|_| format!("{:02x?}", krpc.v.as_ref()));
                            warn!(
                                "peer {} (client v={:?}) sent bogus v4 node {:02x?} -> {}",
                                from_addr, v, id, addr
                            );
                        } else {
                            add_route(*id, SocketAddr::V4(*addr), false);
                        }
                    }
                }
                if let Some(ret) = self.tmap.lock().unwrap().remove(krpc.t.as_slice()) {
                    _ = ret.send(Ok(resp));
                } else {
                    debug!("dht unknown transaction id");
                }
            }
            KRPCInner::Err(items) => {
                // TODO: properly handle this
                debug!("dht received error from {}: {:?}", from_addr, items);
            }
        }
    }

    #[instrument(skip_all, fields(to = ?addr))]
    async fn handle_out_req(&mut self, addr: SocketAddr, krpc: KRPC) {
        self.out_buf.clear();
        // TODO: add logs
        match bt_bencode::to_writer(&mut self.out_buf, &krpc) {
            Ok(b) => b,
            Err(e) => {
                if let Some(v) = self.tmap.lock().unwrap().remove(krpc.t.as_slice()) {
                    _ = v.send(Err(e.into()));
                }
                return;
            }
        };
        // On Windows, a dual-stack IPv6 socket requires IPv4 destinations in
        // IPv4-mapped IPv6 form (::ffff:a.b.c.d). Passing a raw sockaddr_in
        // to an AF_INET6 socket gives WSAEFAULT (os error 10014).
        let addr = match addr {
            SocketAddr::V4(v4) => {
                SocketAddr::V6(SocketAddrV6::new(v4.ip().to_ipv6_mapped(), v4.port(), 0, 0))
            }
            v6 => v6,
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

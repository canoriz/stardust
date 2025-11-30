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

type NodeID = [u8; 20];

pub struct DHT {
    id: NodeID,
    port: u16,
    version: String,

    tid: AtomicU64,

    tx: mpsc::Sender<OutReq>,

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

#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct NodeAddr {
    pub id: NodeID,
    pub addr: SocketAddr,
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
        rx: mpsc::Receiver<OutReq>,
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
            id,
            s: socket,
            route: RoutingTable::new(id),
            tmap,
        };
        tokio::spawn(server.serve(rx, cancel));
        Ok(())
    }

    fn delete_route(&self, id: NodeID) {
        todo!()
    }

    async fn do_req(
        &self,
        addr: NodeAddr,
        krpc: KRPC,
        timeout: time::Duration,
    ) -> io::Result<Resp> {
        let (tx, rx) = oneshot::channel();
        let tid = krpc.t.clone();
        let req = OutReq {
            addr: addr.addr,
            krpc,
        };
        let _drop_guard = TransactionGuard::new(tid.into_vec(), self.tmap.clone(), tx);
        if self.tx.send(req).await.is_err() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "send request to worker error",
            ));
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::Other,
                "recv result from worker error",
            )),
            Err(_) => {
                self.delete_route(addr.id);
                Err(io::Error::new(io::ErrorKind::Other, "timeout"))
            }
        }
    }

    pub async fn ping(&self, addr: NodeAddr, timeout: time::Duration) -> io::Result<Resp> {
        let tid = self.tid.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        let tid: ByteString = tid[..].into();
        let krpc = KRPC {
            t: tid.clone(),
            v: self.version.clone().into(),
            inner: KRPCInner::Request(Arg::Ping(PingArg { id: self.id })),
        };
        self.do_req(addr, krpc, timeout).await
    }

    pub async fn find_node(
        &self,
        addr: NodeAddr,
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
        self.do_req(addr, krpc, timeout).await
    }

    pub async fn get_peers(
        &self,
        addr: NodeAddr,
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
        self.do_req(addr, krpc, timeout).await
    }

    pub async fn announce_peer(
        &self,
        addr: NodeAddr,
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
        self.do_req(addr, krpc, timeout).await
    }
}

struct OutReq {
    addr: SocketAddr,
    krpc: KRPC,
}

type TransactionMap = HashMap<Vec<u8>, oneshot::Sender<io::Result<Resp>>>;

struct Server {
    s: UdpSocket,
    id: NodeID,

    route: RoutingTable,

    /// transaction id map
    tmap: Arc<Mutex<TransactionMap>>,
}

const BUF_MAX: usize = 10240;

impl Server {
    async fn serve(mut self, mut out_req: mpsc::Receiver<OutReq>, cancel_token: CancellationToken) {
        let mut buf = vec![0u8; BUF_MAX];
        let mut out_buf = vec![0u8; BUF_MAX];
        loop {
            tokio::select! {
                _ = self.handle_income(&mut buf) => {},
                Some(OutReq{addr, krpc}) = out_req.recv() => {
                    self.handle_out_req(addr, krpc,  &mut out_buf).await
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

    async fn handle_krpc_in(&mut self, krpc: KRPC, from_addr: SocketAddr) {
        let version: ByteString = "st01".into();
        match krpc.inner {
            KRPCInner::Request(Arg::Ping(p)) => {
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
                if let Ok(resp_buf) = bt_bencode::to_vec(&resp) {
                    _ = self.s.send_to(&resp_buf, from_addr).await;
                }
                self.route.add(NodeAddr {
                    id: p.id,
                    addr: from_addr,
                })
            }
            KRPCInner::Request(Arg::AnnouncePeer(a)) => self.route.add(NodeAddr {
                id: a.id,
                addr: from_addr,
            }),
            KRPCInner::Request(Arg::FindNode(f)) => self.route.add(NodeAddr {
                id: f.id,
                addr: from_addr,
            }),
            KRPCInner::Request(Arg::GetPeers(gp)) => self.route.add(NodeAddr {
                id: gp.id,
                addr: from_addr,
            }),
            KRPCInner::Response(resp) => {
                if let Some(ret) = self.tmap.lock().unwrap().remove(krpc.t.as_slice()) {
                    _ = ret.send(Ok(resp));
                } else {
                    info!("dht unknown transaction id");
                }
                // TODO: do we refresh routing table?
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

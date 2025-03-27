use crate::connection_manager::ConnectionManagerHandle;
use crate::metadata::{self, AnnounceType, Metadata, TrackerGet};
use crate::protocol::{self, BTStream};
use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tracing::{debug_span, info, Instrument, Level, Span};

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Msg {
    AnnounceFinish(Result<metadata::AnnounceResp, metadata::AnnounceError>),

    // TODO: support uTP/proxy
    NewPeer(protocol::BTStream<TcpStream>),
    NewIncomePeer(protocol::BTStream<TcpStream>),
}

#[derive(Clone)]
pub(crate) struct TransmitManagerHandle(pub mpsc::UnboundedSender<Msg>);

pub struct TransmitManager {
    metadata: Arc<Metadata>,

    receiver: mpsc::UnboundedReceiver<Msg>,

    self_handle: TransmitManagerHandle,

    // change_rx: mpsc::UnboundedReceiver<Msg>,
    // change_tx: mpsc::UnboundedSender<Msg>,

    // announce_handle: Option<AnnounceManagerHandle>,
    // announce_tx: Option<mpsc::Sender<u32>>,

    // TODO: use a Map instead of Vec?
    // TODO: change V type
    connected_peers: HashMap<SocketAddr, ConnectionManagerHandle>,
}

impl TransmitManager {
    pub fn new(
        m: Arc<Metadata>,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        Self {
            metadata: m,
            receiver: cmd_receiver,
            self_handle: TransmitManagerHandle(cmd_sender),
            // announce_handle: None,
            // announce_tx: None,
            connected_peers: HashMap::new(),
        }
    }

    // pub fn with_announce_list(mut self, announce_list: Vec<Vec<String>>) -> Self {
    //     // if let Some(am) = self.announce_handle {
    //     //     am.stop_all();
    //     // }
    //     // let (cmd_tx, cmd_rx) = mpsc::channel(2); // TODO: 2?
    //     // let mut am = AnnounceManagerHandle { cmd_tx };
    //     // am.start_announce_worker();
    //     // self.announce_handle = Some(am);
    //     self.announce_tx = Some(self.start_announce_task::<FakeAnnouncer>(announce_list));
    //     self
    // }

    fn handle_msg(&mut self, m: Msg) {
        match m {
            Msg::AnnounceFinish(Ok(a)) => {
                // self.handle_announce(
                //     a.peers
                //         .into_iter()
                //         .filter_map(|p| (p.ip).parse().map(|ip: IpAddr| (ip, p.port).into()).ok())
                //         .collect(),
                // );
                // TODO
            }
            Msg::AnnounceFinish(Err(e)) => {
                info!("announce error {}", e);
            }
            Msg::NewPeer(bt_conn) => {
                info!("new outward connection {:?}", bt_conn);
                if self.connected_peers.get(&bt_conn.local_addr()).is_none() {
                    let peer_addr = bt_conn.peer_addr();
                    let cm = ConnectionManagerHandle::new(bt_conn);
                    self.connected_peers.insert(peer_addr, cm);
                }
            }
            other => {
                info!("unhandled other {:?}", other);
                todo!()
            }
        }
    }

    pub fn start_find_peers_task(&self) {
        todo!()
    }

    // pub fn start_announce_task<T>(&self, announce_list: Vec<Vec<String>>) -> mpsc::Sender<u32>
    // where
    //     T: metadata::Announce + 'static,
    // {
    //     let announce_req = Arc::new(metadata::TrackerGet {
    //         peer_id: "-ZS0405-qwerasdfzxcv".into(),
    //         uploaded: 0,
    //         port: 35515,
    //         downloaded: 0,
    //         left: 0,
    //         ip: None,
    //     });
    //     let m = Arc::new(self.metadata.clone());

    //     let urls: Vec<String> = announce_list
    //         .into_iter()
    //         .flat_map(|u| u.into_iter())
    //         .collect();

    //     let (cmd_tx, cmd_rx) = mpsc::channel::<u32>(1);
    //     let main_tx = self.change_tx.clone();
    //     tokio::spawn(
    //         announce_url::<T>(main_tx, announce_req.clone(), m, urls, cmd_rx)
    //             .instrument(Span::current()),
    //     );

    //     cmd_tx
    //     // TODO: re-announce after period
    //     // TODO: update downloaded, port, etc
    // }

    fn handle_announce(&mut self, addrs: Vec<SocketAddr>) {
        todo!("use a connect tool to convert SocketAddr to BTConn");
        // for addr in addrs {
        //     if self.connected_peers.get(&addr).is_none() {
        //         self.connected_peers.insert(addr, ());
        //         tokio::spawn(connect_peer(self.self_handle.clone(), addr));
        //     }
        // }
    }
}

pub(crate) async fn run_transmit_manager(
    mut transmit: TransmitManager,
    mut cancel: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) {
    loop {
        tokio::select! {
            Some(msg) = transmit.receiver.recv() => {
                info!("main received msg {msg:?}");
                transmit.handle_msg(msg);
            }
            _ = &mut cancel => {
                info!("transmit manager cancelled");
                break;
            }
        };
    }
    let _ = done.send(());
    println!("done done");
}

async fn connect_peer(main_tx: TransmitManagerHandle, addr: SocketAddr) {
    let conn = protocol::BTStream::connect_tcp(addr).await;
    match conn {
        Ok(c) => {
            if let Err(e) = main_tx.0.send(Msg::NewPeer(c)) {
                info!("send new peer to main {e}");
            }
        }
        Err(e) => {
            info!("tcp handshake {addr} error {e}");
        }
    }
}

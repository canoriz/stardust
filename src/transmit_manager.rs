use crate::backfile;
use crate::backfile::BackFile;
use crate::backfile::WriteJob;
use crate::connection_manager::ConnectionManagerHandle;
use crate::connection_manager::Msg as ConnMsg;
use crate::metadata::{self, AnnounceType, Metadata, TrackerGet};
use crate::picker::{BlockRequests, HeapPiecePicker};
use crate::protocol::{self, BTStream, BitField};
use crate::storage::ArcCache;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tracing::{debug_span, info, warn, Instrument, Level, Span};

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Msg {
    AnnounceFinish(Result<metadata::AnnounceResp, metadata::AnnounceError>),

    // TODO: support uTP/proxy
    NewPeer(protocol::BTStream<TcpStream>),
    NewIncomePeer(protocol::BTStream<TcpStream>),

    PeerChoke,
    PeerUnchoke,
    PeerInterested,
    PeerUninterested,
    PeerBitField(SocketAddr, BitField),
}

#[derive(Clone)]
pub(crate) struct TransmitManagerHandle {
    pub sender: mpsc::UnboundedSender<Msg>,

    // TODO: maybe not use Arc<Mutex<..>> but use a splitted lock structure to
    // reduce contention?
    // TODO: using dyn <trait Picker>?
    pub picker: Arc<Mutex<HeapPiecePicker>>,
    pub piece_buffer: Arc<Mutex<HashMap<u32, ArcCache>>>,
    pub piece_size: usize,
    pub back_file: Arc<Mutex<BackFile>>,
    pub write_worker: std::sync::mpsc::Sender<WriteJob<'static>>,
}

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

    piece_picker: Arc<Mutex<HeapPiecePicker>>,
    piece_buffer: Arc<Mutex<HashMap<u32, ArcCache>>>,
}

impl TransmitManager {
    pub fn new(
        m: Arc<Metadata>,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        let piece_size = m.info.piece_length;
        let total_length = m.len();
        let piece_picker = Arc::new(Mutex::new(HeapPiecePicker::new(total_length, piece_size)));
        let piece_buffer = Arc::new(Mutex::new(HashMap::new()));

        let (job_tx, job_rx) = std::sync::mpsc::channel();
        tokio::task::spawn_blocking(move || backfile::write_worker(job_rx));
        Self {
            metadata: m.clone(),
            receiver: cmd_receiver,
            self_handle: TransmitManagerHandle {
                sender: cmd_sender,
                picker: piece_picker.clone(),
                piece_buffer: piece_buffer.clone(),
                piece_size: piece_size as usize,
                back_file: Arc::new(Mutex::new(BackFile::new(m))),
                write_worker: job_tx,
            },
            // announce_handle: None,
            // announce_tx: None,
            connected_peers: HashMap::new(),
            piece_picker,
            piece_buffer,
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
    fn pick_blocks_for_peer(&mut self, n_blocks: usize) {
        for (addr, h) in &mut self.connected_peers {
            let reqs = self
                .piece_picker
                .lock()
                .unwrap() // TODO: fix unwrap
                .pick_blocks(addr, n_blocks);
            h.send_stream_cmd(ConnMsg::RequestBlocks(reqs));
        }
    }

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
                    let cm = ConnectionManagerHandle::new(
                        bt_conn,
                        self.self_handle.clone(),
                        self.metadata.clone(),
                    );
                    self.connected_peers.insert(peer_addr, cm);
                }
            }
            Msg::PeerBitField(addr, bitfield) => {
                info!("new BitField msg from peer {addr}");
                self.piece_picker.lock().unwrap().peer_add(addr, bitfield);
                self.pick_blocks_for_peer(15);
            }
            other => {
                info!("unhandled other {:?}", other);
                // todo!()
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
    let mut ticker = tokio::time::interval(time::Duration::from_millis(500));
    loop {
        // TODO: lets use notify?
        tokio::select! {
            Some(msg) = transmit.receiver.recv() => {
                info!("main received msg {msg:?}");
                transmit.handle_msg(msg);
            }
            _ = ticker.tick() => {
                info!("transmit ticker tick");
                transmit.pick_blocks_for_peer(300);
                let pbl = transmit.self_handle.piece_buffer.lock().unwrap().len();
                warn!("pbl remains {pbl}");
            }
            _ = &mut cancel => {
                for (addr, handle) in transmit.connected_peers.drain() {
                    info!("stopping connection to {addr}");
                    handle.stop().await;
                };
                info!("transmit manager cancelled");
                break;
            }
        };
    }
    let _ = done.send(());
    println!("transmit manager done");
}

async fn connect_peer(main_tx: TransmitManagerHandle, addr: SocketAddr) {
    let conn = protocol::BTStream::connect_tcp(addr).await;
    match conn {
        Ok(c) => {
            if let Err(e) = main_tx.sender.send(Msg::NewPeer(c)) {
                info!("send new peer to main {e}");
            }
        }
        Err(e) => {
            info!("tcp handshake {addr} error {e}");
        }
    }
}

use crate::backfile::BackFile;
use crate::backfile::NormalFile;
use crate::cache::BufStorage;
use crate::connection_manager::ConnectionManagerHandle;
use crate::connection_manager::Msg as ConnMsg;
use crate::metadata::{self, Magnet, Metadata};
use crate::picker::HeapPiecePicker;
use crate::protocol::Conn;
use crate::protocol::ExtendedMetadata;
use crate::protocol::FuncBits;
use crate::protocol::{self, BitField};

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio_util::sync::{CancellationToken, DropGuard as CancelDropGuard};
use tracing::{info, warn};

pub enum TorrentTask {
    Torrent(Metadata),
    Magnet(Magnet),
}

type PeerAddr = SocketAddr;

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Msg {
    AnnounceFinish(Result<metadata::AnnounceResp, metadata::AnnounceError>),

    NewPeer(Result<protocol::BTStream<Box<dyn Conn>>, SocketAddr>),
    NewIncomePeer(protocol::BTStream<Box<dyn Conn>>),

    // TODO: use a structure ptr to connection_peer struct
    // to replace SocketAddr
    // which removes the HashMap cost
    PeerChoke(PeerAddr),
    PeerUnchoke(PeerAddr),
    PeerInterested(PeerAddr),
    PeerUninterested(PeerAddr),
    PeerBitField(PeerAddr, BitField),
    PeerHave(PeerAddr, u32),

    PieceReceived(u32),

    BlockReceived(PeerAddr, u32),

    ExtendMetadata(ExtendedMetadata),
}

#[derive(Debug, PartialEq, Eq)]
enum ChokeStatus {
    Choked,
    Unchoked,
    Unknown,
}

#[derive(Debug, PartialEq, Eq)]
enum InterestStatus {
    Interested,
    Uninterested,
    Unknown,
}

#[derive(Debug, PartialEq, Eq)]
struct PeerStatus {
    our_choke_status: ChokeStatus,
    our_interest_status: InterestStatus,
    peer_choke_status: ChokeStatus,
    peer_interest_status: InterestStatus,
}

struct PeerConn {
    conn: ConnectionManagerHandle,
    state: PeerStatus,

    last_pick_time: time::Instant,

    n_block_in_flight: u32, // TODO: remove this
}

#[derive(Clone)]
pub(crate) struct TransmitManagerHandle {
    pub sender: mpsc::UnboundedSender<Msg>,

    pub torrent_state: Arc<TorrentState>,
}

pub(crate) struct TransmitManager {
    cancel: CancelDropGuard,
    worker_stop: oneshot::Receiver<()>,
}

impl TransmitManager {
    pub fn new(
        t: TorrentTask,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        let worker = TransmitWorker::new(t, cmd_sender, cmd_receiver);
        let cancel_transmit = CancellationToken::new();
        let (done_transmit, done_transmit_rx) = oneshot::channel::<()>();
        tokio::spawn(run_transmit_worker(
            worker,
            cancel_transmit.clone(),
            done_transmit,
        ));
        Self {
            cancel: cancel_transmit.drop_guard(),
            worker_stop: done_transmit_rx,
        }
    }

    pub async fn stop_wait(self) {
        self.cancel.disarm().cancel();
        _ = self.worker_stop.await;
    }
}

pub enum TorrentState {
    Metadata(Downloading),
    Fetching(FetchingMetadata),
}

/// The fetching information of a torrent
/// still downloading metadata
pub struct FetchingMetadata {
    pub metadata: Vec<u8>,
    pub magnet: Magnet,
}

/// The concrete download information of a
/// torrent with full(verified) metadata
pub struct Downloading {
    pub metadata: Arc<Metadata>,

    // TODO: maybe not use Arc<Mutex<..>> but use a splitted lock structure to
    // reduce contention?
    // TODO: using dyn <trait Picker>?
    pub piece_picker: Arc<Mutex<HeapPiecePicker>>,
    pub storage: Arc<BufStorage>,
}

pub struct TransmitWorker {
    /// The state of the torrent
    /// whether have metadata or not
    torrent_state: Arc<TorrentState>,

    receiver: mpsc::UnboundedReceiver<Msg>,

    self_handle: TransmitManagerHandle,

    // change_rx: mpsc::UnboundedReceiver<Msg>,
    // change_tx: mpsc::UnboundedSender<Msg>,

    // announce_handle: Option<AnnounceManagerHandle>,
    // announce_tx: Option<mpsc::Sender<u32>>,

    // TODO: use a Map instead of Vec?
    // TODO: change V type
    connected_peers: HashMap<SocketAddr, PeerConn>,
    connecting_peers: HashSet<SocketAddr>,
}

impl TransmitWorker {
    pub fn new(
        t: TorrentTask,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        match t {
            TorrentTask::Torrent(m) => Self::new_with_metadata(m, cmd_sender, cmd_receiver),
            TorrentTask::Magnet(m) => Self::new_without_metadata(m, cmd_sender, cmd_receiver),
        }
    }

    fn new_without_metadata(
        m: Magnet,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        let state = Arc::new(TorrentState::Fetching(FetchingMetadata {
            metadata: Vec::new(),
            magnet: m,
        }));
        Self {
            torrent_state: state.clone(),
            receiver: cmd_receiver,
            self_handle: TransmitManagerHandle {
                torrent_state: state,
                sender: cmd_sender,
            },
            // announce_handle: None,
            // announce_tx: None,
            connected_peers: HashMap::new(),
            connecting_peers: HashSet::new(),
        }
    }

    fn metadata_into_downloading(m: Metadata) -> Downloading {
        let m = Arc::new(m);
        let piece_size = m.regular_piece_size() as u32;
        let total_length = m.len();
        let piece_picker = Arc::new(Mutex::new(HeapPiecePicker::new(total_length, piece_size)));

        let (piece_total, last_piece_size) = (
            m.total_pieces(),
            m.piece_size_of(m.total_pieces() as u32 - 1),
        );

        let back_file = BackFile::new::<NormalFile>().metadata(m.clone()).build();
        let buf_storage = Arc::new(BufStorage::new(
            total_length,
            m.regular_piece_size(),
            back_file,
        ));

        Downloading {
            metadata: m,
            piece_picker,
            storage: buf_storage,
        }
    }

    fn new_with_metadata(
        m: Metadata,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        let state = Arc::new(TorrentState::Metadata(Self::metadata_into_downloading(m)));
        Self {
            torrent_state: state.clone(),
            receiver: cmd_receiver,
            self_handle: TransmitManagerHandle {
                torrent_state: state,
                sender: cmd_sender,
            },
            // announce_handle: None,
            // announce_tx: None,
            connected_peers: HashMap::new(),
            connecting_peers: HashSet::new(),
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
    fn pick_blocks_for_all_peers(&mut self, n_blocks: usize) {
        let piece_picker = match &*self.torrent_state {
            TorrentState::Metadata(d) => &d.piece_picker,
            TorrentState::Fetching(_) => {
                return;
            }
        };

        let now = std::time::Instant::now();
        for (addr, h) in &mut self.connected_peers {
            info!("peer status {addr}: {:?}", h.state);
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let (reqs, n) = piece_picker
                    .lock()
                    .unwrap() // TODO: fix unwrap
                    .pick_blocks(addr, n_blocks, now);
                info!("aaa {n_blocks} {reqs:?}");
                h.conn.send_stream_cmd(ConnMsg::RequestBlocks(reqs));
            }
        }
    }

    // fn pick_blocks_for_peer(&mut self, addr: &SocketAddr, n_blocks: usize) {
    // Fn: n_blk_received, n_blk_in_flight -> n_this_time_pick
    fn pick_blocks_for_peer(&mut self, addr: &SocketAddr, n_received: u32) {
        let piece_picker = match &*self.torrent_state {
            TorrentState::Metadata(d) => &d.piece_picker,
            TorrentState::Fetching(_) => {
                return;
            }
        };

        // n_block_in_flight = estimated_bandwidth * response_time
        // response_time = RTT + process_time
        // estimated_bandwidth = ALPHA * n_received_per_second
        let now = std::time::Instant::now();
        if let Some(h) = self.connected_peers.get_mut(addr) {
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let mut picker = piece_picker.lock().unwrap(); // TODO: fix unwrap

                h.n_block_in_flight = picker
                    .get_status(addr)
                    .map(|s| s.n_in_flight as u32)
                    .unwrap_or(0);
                dbg!(h.n_block_in_flight);

                let mut n_blk = 1;
                if n_received > 0 {
                    let period_duration = h.last_pick_time.elapsed();
                    h.last_pick_time = time::Instant::now();
                    let estm_bw_bps = if let Some(status) = picker.get_status(addr) {
                        (status.bandwidth.count(period_duration) as f32)
                            / period_duration.div_duration_f32(time::Duration::from_secs(1))
                    } else {
                        0.0
                    };

                    // TODO: now send 10 senconds in batch
                    // maybe calculate this with response time
                    let batch_seconds = 10.0;
                    let mut optimal_n_in_flight = (estm_bw_bps * batch_seconds / 16384.0) as u32;
                    optimal_n_in_flight = optimal_n_in_flight.min(1500).max(16);

                    dbg!(n_received, estm_bw_bps, optimal_n_in_flight);

                    dbg!(h.n_block_in_flight);
                    // let n_blk = pick_fn(n_received, h.n_block_in_flight);
                    n_blk = if optimal_n_in_flight > h.n_block_in_flight {
                        optimal_n_in_flight - h.n_block_in_flight
                    } else {
                        0
                    };
                }
                warn!(
                    "peer {addr} picking {n_blk} block in next period, {} in flight",
                    h.n_block_in_flight
                );
                let (reqs, n) = picker.pick_blocks(addr, n_blk as usize, now);
                h.conn.send_stream_cmd(ConnMsg::RequestBlocks(reqs));
            }
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
                info!("announce finish");
                for p in a.peers {
                    use std::str::FromStr;
                    if let Ok(ip) = std::net::IpAddr::from_str(&p.ip) {
                        let h_clone = self.self_handle.clone();
                        let info_hash = match &*self.torrent_state {
                            TorrentState::Metadata(d) => d.metadata.info_hash,
                            TorrentState::Fetching(f) => f.magnet.info_hash,
                        };
                        // TODO: store peers in a map, if cannot connect this time
                        // try re-connect later
                        // TODO: if we already connected to a lot of active peers,
                        // maybe store available peers in a pool, connect to them when
                        // running out of peers
                        let s = SocketAddr::new(ip, p.port);
                        if !self.connected_peers.contains_key(&s)
                            && !self.connecting_peers.contains(&s)
                        {
                            tokio::spawn(connect_peer(h_clone, s, info_hash));
                        }
                    }
                }
            }
            Msg::AnnounceFinish(Err(e)) => {
                info!("announce error {}", e);
            }
            Msg::NewPeer(Ok(bt_conn)) => {
                info!("new outward connection {:?}", bt_conn);
                let peer_addr = bt_conn.peer_addr();
                let cm = ConnectionManagerHandle::new_dyn(bt_conn, self.self_handle.clone());
                self.connected_peers.insert(
                    peer_addr,
                    PeerConn {
                        conn: cm,
                        state: PeerStatus {
                            our_choke_status: ChokeStatus::Unknown,
                            our_interest_status: InterestStatus::Unknown,
                            peer_choke_status: ChokeStatus::Unknown,
                            peer_interest_status: InterestStatus::Unknown,
                        },
                        last_pick_time: time::Instant::now(),
                        n_block_in_flight: 0,
                    },
                );
                self.connecting_peers.remove(&peer_addr);
            }
            Msg::NewPeer(Err(addr)) => {
                info!("err connect to {:?}", addr);
                self.connecting_peers.remove(&addr);
            }
            Msg::PeerBitField(addr, bitfield) => {
                info!("new BitField msg from peer {addr}");
                let piece_picker = match &*self.torrent_state {
                    TorrentState::Metadata(d) => &d.piece_picker,
                    TorrentState::Fetching(_) => return,
                };
                piece_picker.lock().unwrap().peer_add(addr, bitfield);
            }
            Msg::PeerHave(peer, i) => {
                info!("peer {peer} have piece {i}");
                let piece_picker = match &*self.torrent_state {
                    TorrentState::Metadata(d) => &d.piece_picker,
                    TorrentState::Fetching(_) => return,
                };
                piece_picker.lock().unwrap().peer_have(&peer, i);
            }
            Msg::PeerChoke(peer) => {
                warn!("{peer} choked us");
                let piece_picker = match &*self.torrent_state {
                    TorrentState::Metadata(d) => &d.piece_picker,
                    TorrentState::Fetching(_) => return,
                };
                piece_picker.lock().unwrap().peer_mark_not_requested(&peer);
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_choke_status = ChokeStatus::Choked;
                });
                assert_eq!(
                    self.connected_peers[&peer].state.peer_choke_status,
                    ChokeStatus::Choked
                );
                // TODO: record the ?stable transmit rate/ i.e. how many packets is in flight
                // so we can recover to max speed (hopefully) once they unchoked us
            }
            Msg::PeerUnchoke(peer) => {
                let n_first_pick = 3;
                warn!("{peer} unchoked us");
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_choke_status = ChokeStatus::Unchoked;
                });
                assert_eq!(
                    self.connected_peers[&peer].state.peer_choke_status,
                    ChokeStatus::Unchoked
                );
                warn!(
                    "nblock in flight {}",
                    self.connected_peers[&peer].n_block_in_flight
                );
                // TODO: are we interested in this peer?
                self.pick_blocks_for_peer(&peer, 0);
            }
            Msg::NewIncomePeer(btstream) => todo!(),
            Msg::PeerInterested(peer) => {
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_interest_status = InterestStatus::Interested;
                });
            }
            Msg::PeerUninterested(peer) => {
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_interest_status = InterestStatus::Uninterested;
                });
            }
            Msg::PieceReceived(i) => {
                for (_, h) in self.connected_peers.iter() {
                    h.conn.send_stream_cmd(ConnMsg::Have(i));
                }
            }
            Msg::BlockReceived(peer, n) => {
                // optimally
                // n_packet_in_flight = (bandwidth * response_time) / packet_size
                // response_time can be measured
                // packet_size is known
                // bandwitdh is unknown and ?difficult to measure
                let conn_stat = self.connected_peers.get_mut(&peer).expect("should exist");
                warn!(
                    "peer {peer} received {n} block in prev period, in flight {}",
                    conn_stat.n_block_in_flight
                );

                // TODO: peer may take longer than period to process,
                // we need to estimate bandwidth
                //
                // n_block_in_flight = estimated_bandwidth * response_time
                // response_time = RTT + process_time
                // estimated_bandwidth = ALPHA * n_received_per_second
                //
                // so we can estimate response_time

                if conn_stat.state.peer_choke_status == ChokeStatus::Unchoked {
                    self.pick_blocks_for_peer(&peer, n);
                }

                // TODO: if peer is choking us?
            }
            Msg::ExtendMetadata(m) => {
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

pub(crate) async fn run_transmit_worker(
    mut transmit: TransmitWorker,
    cancel: CancellationToken,
    done: oneshot::Sender<()>,
) {
    // let mut ticker = tokio::time::interval(time::Duration::from_millis(1000));
    loop {
        // TODO: lets use notify?
        tokio::select! {
            Some(msg) = transmit.receiver.recv() => {
                info!("transmit manager received msg {msg:?}");
                transmit.handle_msg(msg);
            }
            // _ = ticker.tick() => {
            //     info!("transmit ticker tick");
            //     transmit.pick_blocks_for_all_peers(2);
            //     let pbl = transmit.self_handle.piece_buffer.lock().unwrap().len();
            //     warn!("piece buffer pending remains {pbl}");
            // }
            _ = cancel.cancelled() => {
                info!("transmit manager cancelled");
                break;
            }
        };
    }
    let _ = done.send(());
    println!("transmit manager done");
}

async fn connect_peer(
    main_tx: TransmitManagerHandle,
    addr: SocketAddr,
    info_hash: [u8; 20],
) -> Result<(), std::io::Error> {
    let tcp_stream = TcpStream::connect(addr).await?;
    let func = FuncBits::default().set_dht().set_extension();
    func.set_extension();
    let conn = protocol::BTStream::connect(
        tcp_stream,
        &protocol::Handshake {
            reserved: func,
            client_id: [
                0x54, 0x42, 0x54, 0x69, 0x21, 0x58, 0x21, 0x58, 0x68, 0x69, 0x93, 0x51, 0x54, 0x42,
                0x54, 0x69, 0x21, 0x58, 0x21, 0x58,
            ],
            torrent_hash: info_hash,
        },
        &protocol::ExtendedHandshake {
            m: protocol::EXTENSION_IDS_MAP.clone(), // supported extensions and id number
            p: 14351,                               // TCP listen port
            v: "stardust 0.1.0".into(),             // client name and version

            yourip: None,

            ipv6: None,
            ipv4: None,
            reqq: None,          // request queue limit before drop any message
            metadata_size: None, // TODO: FIXME: send correct metadata size
        },
    )
    .await;
    match conn {
        Ok(c) => {
            if let Err(e) = main_tx.sender.send(Msg::NewPeer(Ok(c.to_dyn()))) {
                info!("send new peer to main {e}");
            }
            Ok(())
        }
        Err(e) => {
            info!("tcp handshake {addr} error {e}");
            _ = main_tx.sender.send(Msg::NewPeer(Err(addr)));
            Err(e)
        }
    }
}

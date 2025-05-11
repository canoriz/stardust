use crate::backfile;
use crate::backfile::BackFile;
use crate::backfile::WriteJob;
use crate::connection_manager::ConnectionManagerHandle;
use crate::connection_manager::Msg as ConnMsg;
use crate::metadata::{self, AnnounceType, Metadata, TrackerGet};
use crate::picker::{BlockRequests, HeapPiecePicker};
use crate::protocol::{self, BTStream, BitField};
use crate::storage::ArcCache;

use reqwest::header::OccupiedEntry;
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

    // TODO: use a structure ptr to connection_peer struct
    // to replace SocketAddr
    // which removes the HashMap cost
    PeerChoke(SocketAddr),
    PeerUnchoke(SocketAddr),
    PeerInterested,
    PeerUninterested,
    PeerBitField(SocketAddr, BitField),
    PeerHave(SocketAddr, u32),

    PieceReceived(u32),

    BlockReceived(SocketAddr, u32),
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

    n_block_in_flight: u32,
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
    connected_peers: HashMap<SocketAddr, PeerConn>,

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
    fn pick_blocks_for_all_peers(&mut self, n_blocks: usize) {
        let now = std::time::Instant::now();
        for (addr, h) in &mut self.connected_peers {
            info!("peer status {addr}: {:?}", h.state);
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let (reqs, n) = self
                    .piece_picker
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
    fn pick_blocks_for_peer<F>(&mut self, addr: &SocketAddr, n_received: u32, pick_fn: F)
    where
        F: FnOnce(u32, u32) -> u32,
    {
        // n_block_in_flight = estimated_bandwidth * response_time
        // response_time = RTT + process_time
        // estimated_bandwidth = ALPHA * n_received_per_second
        let now = std::time::Instant::now();
        if let Some(h) = self.connected_peers.get_mut(addr) {
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let mut picker = self.piece_picker.lock().unwrap(); // TODO: fix unwrap
                let n_timeout = picker.get_and_reset_n_timeout(addr) as u32;

                let mut n_blk = 1;
                if n_received > 0 {
                    // bandwidth = 16k * estm_bandwidth / period_duration
                    let period_duration = time::Duration::from_secs(1);
                    let curr_bw = n_received as f32;
                    let mut bw = picker.get_bw_status(addr);
                    bw.estm_bandwidth = if bw.estm_bandwidth <= curr_bw * 1.1 + 0.3 {
                        curr_bw * 1.3 + 0.3 // TODO: now only count this time, maybe use a weighted avg
                    } else {
                        bw.estm_bandwidth * 0.6
                    };
                    let optimal_n_in_flight = if bw.response_time > period_duration {
                        let mut rt = time::Duration::from_secs(1);
                        if rt > bw.response_time {
                            rt = bw.response_time;
                        }
                        (rt.div_duration_f32(period_duration) * bw.estm_bandwidth).ceil() as u32
                    } else {
                        bw.estm_bandwidth.ceil() as u32
                    };
                    picker.update_bw_status(*addr, bw);
                    dbg!(n_received, bw, optimal_n_in_flight);

                    dbg!(h.n_block_in_flight, n_timeout);
                    if h.n_block_in_flight < n_timeout {
                        h.n_block_in_flight = 0;
                    } else {
                        h.n_block_in_flight -= n_timeout;
                    }
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
                h.n_block_in_flight += n as u32;
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
                    let sock = format!("{}:{}", p.ip, p.port);
                    let sockv6 = format!("[{}]:{}", p.ip, p.port);
                    if let Ok(s) = sock.parse() {
                        let h_clone = self.self_handle.clone();
                        let m_clone = self.metadata.clone();
                        // TODO: only try connect not-connected peer
                        // TODO: store peers in a map, if cannot connect this time
                        // try re-connect later
                        tokio::spawn(connect_peer(h_clone, s, m_clone));
                    } else if let Ok(s) = sockv6.parse() {
                        let h_clone = self.self_handle.clone();
                        let m_clone = self.metadata.clone();
                        // TODO: only try connect not-connected peer
                        // TODO: store peers in a map, if cannot connect this time
                        // try re-connect later
                        tokio::spawn(connect_peer(h_clone, s, m_clone));
                    }
                }
            }
            Msg::AnnounceFinish(Err(e)) => {
                info!("announce error {}", e);
            }
            Msg::NewPeer(bt_conn) => {
                info!("new outward connection {:?}", bt_conn);
                if !self.connected_peers.contains_key(&bt_conn.peer_addr()) {
                    let peer_addr = bt_conn.peer_addr();
                    let cm = ConnectionManagerHandle::new(
                        bt_conn,
                        self.self_handle.clone(),
                        self.metadata.clone(),
                    );
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
                            n_block_in_flight: 0,
                        },
                    );
                }
            }
            Msg::PeerBitField(addr, bitfield) => {
                info!("new BitField msg from peer {addr}");
                self.piece_picker.lock().unwrap().peer_add(addr, bitfield);
            }
            Msg::PeerHave(peer, i) => {
                info!("peer {peer} have piece {i}");
                self.piece_picker.lock().unwrap().peer_have(&peer, i);
            }
            Msg::PeerChoke(peer) => {
                warn!("{peer} choked us");
                self.piece_picker
                    .lock()
                    .unwrap()
                    .peer_mark_not_requested(&peer);
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_choke_status = ChokeStatus::Choked;
                    st.n_block_in_flight = 0;
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
                self.pick_blocks_for_peer(&peer, 0, |_, _| n_first_pick);
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

                // TODO: this should not have a fix point (now is 5)
                let n_blk_fn = |n_received, n_in_flight| {
                    if n_received == 1 {
                        2
                    } else if n_in_flight <= n {
                        (n_received as f32 * 1.2) as u32 + 1
                    } else {
                        (n_received as f32 * 0.8) as u32 + 1
                    }
                };
                // TODO: peer may take longer than period to process,
                // we need to estimate bandwidth
                //
                // n_block_in_flight = estimated_bandwidth * response_time
                // response_time = RTT + process_time
                // estimated_bandwidth = ALPHA * n_received_per_second
                //
                // so we can estimate response_time

                // TODO: peer may sends us more piece than requested, causing downflow
                conn_stat.n_block_in_flight = if n >= conn_stat.n_block_in_flight {
                    0
                } else {
                    dbg!(conn_stat.n_block_in_flight);
                    conn_stat.n_block_in_flight - n
                };

                // conn_stat.n_block_in_flight -= n;
                if conn_stat.state.peer_choke_status == ChokeStatus::Unchoked {
                    self.pick_blocks_for_peer(&peer, n, n_blk_fn);
                }

                // TODO: if peer is choking us?

                let pbl = self.self_handle.piece_buffer.lock().unwrap().len();
                info!("piece buffer pending remains {pbl}");
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
            _ = &mut cancel => {
                for (addr, conn) in transmit.connected_peers.drain() {
                    info!("stopping connection to {addr}");
                    conn.conn.stop().await;
                };
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
    m: Arc<Metadata>,
) -> Result<(), std::io::Error> {
    let mut conn = protocol::BTStream::connect_tcp(addr).await;
    match conn {
        Ok(mut c) => {
            c.send_handshake(&protocol::Handshake {
                reserved: [0u8; 8],
                client_id: [
                    0x54, 0x42, 0x54, 0x69, 0x21, 0x58, 0x21, 0x58, 0x68, 0x69, 0x93, 0x51, 0x54,
                    0x42, 0x54, 0x69, 0x21, 0x58, 0x21, 0x58,
                ],
                torrent_hash: m.info_hash,
            })
            .await?;
            c.recv_handshake().await?;
            if let Err(e) = main_tx.sender.send(Msg::NewPeer(c)) {
                info!("send new peer to main {e}");
            }
            Ok(())
        }
        Err(e) => {
            info!("tcp handshake {addr} error {e}");
            Err(e)
        }
    }
}

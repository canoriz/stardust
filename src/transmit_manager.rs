use crate::backfile::{BackFile, NormalFile};
use crate::cache::simple_buffer::BufStorage;
use crate::cache::simple_buffer::{GetPieceErr, PieceBuf};
use crate::connection_manager::{ConnectionManagerHandle, Msg as ConnMsg};
use crate::dht::DHT;
use crate::metadata::{self, Magnet, Metadata};
use crate::picker::{start_receive_piece_block, HeapPiecePicker};
use crate::protocol::{self, BitField, Conn, ExtendedMetadata, ExtendedMsg, FuncBits, Piece};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio_util::sync::{CancellationToken, DropGuard as CancelDropGuard};
use tracing::{debug, info, warn};

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
    PeerRecvPiece(PeerAddr, Piece),

    PieceBufReady {
        index: usize,
        buf: io::Result<PieceBuf>,
    },

    PieceReceived(u32),

    BlockReceived(PeerAddr, u32),

    ExtendMetadata(PeerAddr, ExtendedMetadata),
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
    bitmap: Option<BitField>,

    last_pick_time: time::Instant,

    n_block_in_flight: u32, // TODO: remove this
}

#[derive(Clone)]
pub(crate) struct TransmitManagerHandle {
    pub sender: mpsc::UnboundedSender<Msg>,
}

pub(crate) struct TransmitManager {
    cancel: CancelDropGuard,
    worker_stop: oneshot::Receiver<()>,
}

impl TransmitManager {
    pub fn new(
        t: TorrentTask,
        id: [u8; 20],
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
        dht_client: Option<Arc<DHT>>,
    ) -> Self {
        let worker = TransmitWorker::new(t, id, dht_client, cmd_sender, cmd_receiver);
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
    meta_buf: MetadataBuffer,
    pub magnet: Magnet,
}

struct MetadataBuffer {
    // stores total_piece map
    // any honest peers should sends same size
    // if not, we chose the most frequent one
    // hashmap(size, count), most frequent size
    size_bucket: HashMap<usize, usize>,

    // 0 if unknown
    most_frequent_size: usize,

    // buffer for metadata
    pub metadata: Vec<u8>,

    // records which parts of metadata are not requested yet
    not_requested: BTreeSet<u32>,

    // requests for parts of metadata sent, but no response yet
    requesting: BTreeMap<u32, time::Instant>,
}

impl MetadataBuffer {
    fn new() -> Self {
        Self {
            size_bucket: HashMap::new(),
            most_frequent_size: 0,
            metadata: Vec::with_capacity(16384),
            not_requested: BTreeSet::new(),
            requesting: BTreeMap::new(),
        }
    }

    fn add_size_to_bucket(&mut self, sz: usize) {
        self.size_bucket
            .entry(sz)
            .and_modify(|c| *c += 1)
            .or_insert(1);
        if let Some(cc) = self.size_bucket.get(&sz) {
            if *cc > self.most_frequent_size {
                if sz > self.most_frequent_size {
                    for p in (self.most_frequent_size / 16384)..=((sz - 1) / 16384) {
                        self.not_requested.insert(p as u32);
                    }
                } else {
                    for p in (sz / 16384)..=((self.most_frequent_size - 1) / 16384) {
                        self.not_requested.insert(p as u32);
                    }
                }
                self.most_frequent_size = sz;
            }
        }

        let buf = &mut self.metadata;

        // TODO: filter out malicious very large size
        let expand_to = buf.len().max(self.most_frequent_size);
        buf.resize(expand_to, 0);
    }

    fn probable_total_size(&self) -> usize {
        self.most_frequent_size
    }
}

/// The concrete download information of a
/// torrent with full(verified) metadata
pub struct Downloading {
    pub metadata: Arc<Metadata>,

    pub piece_picker: HeapPiecePicker,
    pub storage: BufStorage,
}

pub struct TransmitWorker {
    // our peer ID
    id: [u8; 20],
    info_hash: [u8; 20],

    dht_client: Option<Arc<DHT>>,

    /// The state of the torrent
    /// whether have metadata or not
    torrent_state: TorrentState,

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

    waiting_for_piecebuf: HashMap<u32, Vec<Piece>>,
}

impl TransmitWorker {
    pub fn new(
        t: TorrentTask,
        id: [u8; 20],
        dht_client: Option<Arc<DHT>>,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        match t {
            TorrentTask::Torrent(m) => {
                Self::new_with_metadata(m, id, dht_client, cmd_sender, cmd_receiver)
            }
            TorrentTask::Magnet(m) => {
                Self::new_without_metadata(m, id, dht_client, cmd_sender, cmd_receiver)
            }
        }
    }

    fn new_without_metadata(
        m: Magnet,
        id: [u8; 20],
        dht_client: Option<Arc<DHT>>,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        let info_hash = m.info_hash;
        let state = TorrentState::Fetching(FetchingMetadata {
            meta_buf: MetadataBuffer::new(),
            magnet: m,
        });
        Self {
            id,
            info_hash,
            dht_client,
            torrent_state: state,
            receiver: cmd_receiver,
            self_handle: TransmitManagerHandle { sender: cmd_sender },
            // announce_handle: None,
            // announce_tx: None,
            connected_peers: HashMap::new(),
            connecting_peers: HashSet::new(),
            waiting_for_piecebuf: HashMap::new(),
        }
    }

    fn metadata_into_downloading(m: Metadata) -> Downloading {
        let m = Arc::new(m);
        let piece_size = m.regular_piece_size() as u32;
        let total_length = m.len();
        let piece_picker = HeapPiecePicker::new(total_length, piece_size);

        let (piece_total, last_piece_size) = (
            m.total_pieces(),
            m.piece_size_of(m.total_pieces() as u32 - 1),
        );

        let back_file = BackFile::new::<NormalFile>().metadata(m.clone()).build();
        let buf_storage = BufStorage::new(total_length, m.regular_piece_size(), back_file);

        Downloading {
            metadata: m,
            piece_picker,
            storage: buf_storage,
        }
    }

    fn new_with_metadata(
        m: Metadata,
        id: [u8; 20],
        dht_client: Option<Arc<DHT>>,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        let info_hash = m.info_hash;
        let state = TorrentState::Metadata(Self::metadata_into_downloading(m));
        Self {
            id,
            info_hash,
            dht_client,
            torrent_state: state,
            receiver: cmd_receiver,
            self_handle: TransmitManagerHandle { sender: cmd_sender },
            // announce_handle: None,
            // announce_tx: None,
            connected_peers: HashMap::new(),
            connecting_peers: HashSet::new(),
            waiting_for_piecebuf: HashMap::new(),
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
        let piece_picker = match &mut self.torrent_state {
            TorrentState::Metadata(d) => &mut d.piece_picker,
            TorrentState::Fetching(_) => {
                return;
            }
        };

        let now = std::time::Instant::now();
        for (addr, h) in &mut self.connected_peers {
            info!("peer status {addr}: {:?}", h.state);
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let (reqs, n) = piece_picker.pick_blocks(addr, n_blocks, now);
                h.conn.send_stream_cmd(ConnMsg::RequestBlocks(reqs));
            }
        }
    }

    // fn pick_blocks_for_peer(&mut self, addr: &SocketAddr, n_blocks: usize) {
    // Fn: n_blk_received, n_blk_in_flight -> n_this_time_pick
    fn pick_blocks_for_peer(&mut self, addr: &SocketAddr, n_received: u32) {
        let piece_picker = match &mut self.torrent_state {
            TorrentState::Metadata(d) => &mut d.piece_picker,
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
                h.n_block_in_flight = piece_picker
                    .get_status(addr)
                    .map(|s| s.n_in_flight as u32)
                    .unwrap_or(0);
                dbg!(h.n_block_in_flight);

                let mut n_blk = 1;
                if n_received > 0 {
                    let period_duration = h.last_pick_time.elapsed();
                    h.last_pick_time = time::Instant::now();
                    let estm_bw_bps = if let Some(status) = piece_picker.get_status(addr) {
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
                let (reqs, n) = piece_picker.pick_blocks(addr, n_blk as usize, now);
                h.conn.send_stream_cmd(ConnMsg::RequestBlocks(reqs));
            }
        }
    }

    fn handle_msg(&mut self, m: Msg) -> io::Result<()> {
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
                let info_hash = {
                    match &self.torrent_state {
                        TorrentState::Metadata(d) => d.metadata.info_hash,
                        TorrentState::Fetching(f) => f.magnet.info_hash,
                    }
                };
                for p in a.peers {
                    use std::str::FromStr;
                    if let Ok(ip) = std::net::IpAddr::from_str(&p.ip) {
                        let h_clone = self.self_handle.clone();
                        // TODO: store peers in a map, if cannot connect this time
                        // try re-connect later
                        // TODO: if we already connected to a lot of active peers,
                        // maybe store available peers in a pool, connect to them when
                        // running out of peers
                        let s = SocketAddr::new(ip, p.port);
                        if !self.connected_peers.contains_key(&s)
                            && !self.connecting_peers.contains(&s)
                        {
                            self.connecting_peers.insert(s);
                            tokio::spawn(connect_peer(h_clone, self.id, s, info_hash));
                        }
                    }
                }
                Ok(())
            }
            Msg::AnnounceFinish(Err(e)) => {
                info!("announce error {}", e);
                Ok(())
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
                        bitmap: None,
                        last_pick_time: time::Instant::now(),
                        n_block_in_flight: 0,
                    },
                );
                self.connecting_peers.remove(&peer_addr);
                Ok(())
            }
            Msg::NewPeer(Err(addr)) => {
                info!("err connect to {:?}", addr);
                self.connecting_peers.remove(&addr);
                Ok(())
            }
            Msg::PeerBitField(addr, bitfield) => {
                let piece_picker = match &mut self.torrent_state {
                    TorrentState::Metadata(d) => &mut d.piece_picker,
                    TorrentState::Fetching(_) => {
                        let mut pc = self
                            .connected_peers
                            .get_mut(&addr)
                            .expect("connection should in map");
                        pc.bitmap = Some(bitfield);
                        return Ok(());
                    }
                };
                piece_picker.peer_add(addr, bitfield);
                Ok(())
            }
            Msg::PeerHave(peer, i) => {
                info!("peer {peer} have piece {i}");
                let piece_picker = match &mut self.torrent_state {
                    TorrentState::Metadata(d) => &mut d.piece_picker,
                    TorrentState::Fetching(_) => {
                        return Ok(());
                    }
                };

                piece_picker.peer_have(&peer, i);
                Ok(())
            }
            Msg::PeerChoke(peer) => {
                warn!("{peer} choked us");
                let piece_picker = match &mut self.torrent_state {
                    TorrentState::Metadata(d) => &mut d.piece_picker,
                    TorrentState::Fetching(_) => {
                        return Ok(());
                    }
                };
                piece_picker.peer_mark_not_requested(&peer);
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_choke_status = ChokeStatus::Choked;
                });
                assert_eq!(
                    self.connected_peers[&peer].state.peer_choke_status,
                    ChokeStatus::Choked
                );
                // TODO: record the ?stable transmit rate/ i.e. how many packets is in flight
                // so we can recover to max speed (hopefully) once they unchoked us
                Ok(())
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
                // self.pick_blocks_for_peer(&peer, 0);
                Ok(())
            }
            Msg::NewIncomePeer(btstream) => todo!(),
            Msg::PeerInterested(peer) => {
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_interest_status = InterestStatus::Interested;
                });
                Ok(())
            }
            Msg::PeerUninterested(peer) => {
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_interest_status = InterestStatus::Uninterested;
                });
                Ok(())
            }
            Msg::PieceReceived(i) => {
                for (_, h) in self.connected_peers.iter() {
                    h.conn.send_stream_cmd(ConnMsg::Have(i));
                }
                Ok(())
            }
            Msg::BlockReceived(peer, n) => {
                // optimally
                // n_packet_in_flight = (bandwidth * response_time) / packet_size
                // response_time can be measured
                // packet_size is known
                // bandwitdh is unknown and ?difficult to measure
                let conn_stat = self.connected_peers.get_mut(&peer).expect("should exist");
                debug!(
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
                Ok(())

                // TODO: if peer is choking us?
            }
            Msg::PeerRecvPiece(addr, piece) => {
                debug!("recv {piece:?} from {addr:?}");
                self.handle_piece_msg(&addr, piece)?;
                Ok(())
            }
            Msg::PieceBufReady { index, buf } => match buf {
                Ok(mut buf) => {
                    let pending = self.waiting_for_piecebuf.remove(&(index as u32));
                    if let Some(ps) = pending {
                        for piece in ps {
                            copy_to_piecebuf(&piece, &mut buf);
                        }
                    }
                    match &mut self.torrent_state {
                        TorrentState::Metadata(d) => {
                            d.storage.add_piece(buf);
                        }
                        TorrentState::Fetching(_) => {
                            info!("piece buf ready when fetching, maybe unreachable");
                            unreachable!()
                        }
                    }
                    Ok(())
                }
                Err(e) => return Err(e),
            },
            Msg::ExtendMetadata(pa, m) => {
                self.handle_extend_metadata(pa, m);
                Ok(())
            }
        }
    }

    pub fn start_find_peers_task(&self) {
        todo!()
    }

    fn handle_announce(&mut self, addrs: Vec<SocketAddr>) {
        todo!("use a connect tool to convert SocketAddr to BTConn");
        // for addr in addrs {
        //     if self.connected_peers.get(&addr).is_none() {
        //         self.connected_peers.insert(addr, ());
        //         tokio::spawn(connect_peer(self.self_handle.clone(), addr));
        //     }
        // }
    }

    /// handle PIECE message
    // TODO: fix the return type
    fn handle_piece_msg(
        &mut self,
        peer: &SocketAddr,
        mut piece: protocol::Piece,
    ) -> io::Result<()> {
        let blk = protocol::Request {
            index: piece.index,
            begin: piece.begin,
            len: piece.len,
        };

        let (piece_picker, metadata, storage) = match &mut self.torrent_state {
            TorrentState::Metadata(d) => (&mut d.piece_picker, d.metadata.clone(), &mut d.storage),
            TorrentState::Fetching(_) => {
                info!(
                    "receive PIECE msg {} {} {} block index {} before having metadata",
                    piece.index,
                    piece.begin,
                    piece.len,
                    piece.begin >> 14,
                );
                return Ok(());
            }
        };
        let mut receiving_guard =
            if let Some(g) = start_receive_piece_block(piece_picker, peer, &blk) {
                g
            } else {
                // TODO: why this happen (at testing)?
                // seems we are requesting twice for each piece
                warn!(
                    "drain PIECE msg {} {} {} block index {}",
                    piece.index,
                    piece.begin,
                    piece.len,
                    piece.begin >> 14,
                );
                return Ok(());
            };

        debug!(
            "receive PIECE msg {} {} {} block index {}",
            piece.index,
            piece.begin,
            piece.len,
            piece.begin >> 14,
        );

        let sender = self.self_handle.sender.clone();
        let index = piece.index as usize;
        let on_ready = move |p| {
            _ = sender.send(Msg::PieceBufReady { index, buf: p });
        };
        match storage.get_piece(piece.index as usize, on_ready) {
            Ok(piecebuf) => copy_to_piecebuf(&piece, piecebuf),
            Err(GetPieceErr::InvalidPiece) => {
                info!("invalid piece {piece:?}");
            }
            Err(e) => {
                // TODO: maybe set some unblock_conn upper limit
                // piece.unblock_conn();
                let index = piece.index;
                match self.waiting_for_piecebuf.get_mut(&index) {
                    Some(v) => v.push(piece),
                    None => {
                        self.waiting_for_piecebuf.insert(index, vec![piece]);
                    }
                }
                info!("piecebuf not present err {e:?}");
            }
        }
        if let Some(p) = receiving_guard.piece_received() {
            // TODO: if using bounded channel, don't do this as this might
            // dead lock the loop.
            // instead, deal with piece received at here.
            _ = self.self_handle.sender.send(Msg::PieceReceived(p));
        }
        Ok(())
    }

    fn handle_extend_metadata(&mut self, addr: PeerAddr, m: ExtendedMetadata) {
        // TODO: maybe send peer conn in parameter instead of from
        // hashmap.
        let c = if let Some(c) = self.connected_peers.get_mut(&addr) {
            c
        } else {
            warn!("receive extend metadata from unknown peer {addr:?}");
            return;
        };
        match m {
            ExtendedMetadata::Request { piece } => match &self.torrent_state {
                TorrentState::Fetching(_) => {
                    // we don't have the data, we can't give them
                    let msg = ExtendedMsg::Metadata(ExtendedMetadata::Reject { piece });
                    _ = c.conn.send_stream_cmd(ConnMsg::Extend(msg));
                }
                TorrentState::Metadata(m) => {
                    todo!("sends them metadata")
                }
            },
            ExtendedMetadata::Data {
                piece,
                data,
                total_size,
            } => match &mut self.torrent_state {
                TorrentState::Fetching(f) => {
                    // Don't use a shared buffer like PIECE message, since
                    // the data has already been received. Copying should be
                    // relatively fast.

                    let mbuf = &mut f.meta_buf;
                    if let Some(sz) = total_size {
                        mbuf.add_size_to_bucket(sz);
                    }
                    let probably_tot_size = mbuf.probable_total_size();
                    let buf = &mut mbuf.metadata;
                    let offset = (piece * 16384) as usize;
                    buf[offset..offset + data.len()].copy_from_slice(&data);
                    mbuf.requesting.remove(&piece);
                    mbuf.not_requested.remove(&piece);

                    Self::fetching_metadata_from_peer_addr(&self.connected_peers, &addr, 3, mbuf);

                    let have_metadata = if probably_tot_size > 0
                        && mbuf.not_requested.len() == 0
                        && mbuf.requesting.len() == 0
                    {
                        match check_received_metadata(mbuf, f.magnet.info_hash) {
                            Ok(m) => {
                                warn!("metadata received");
                                Some(m)
                            }
                            Err(_) => {
                                warn!("metadata verify failed, needs re-download");
                                for p in 0..=((mbuf.most_frequent_size - 1) / 16384) {
                                    let p = p as u32;
                                    if !mbuf.requesting.contains_key(&p) {
                                        mbuf.not_requested.insert(p);
                                    }
                                }
                                None
                            }
                        }
                    } else {
                        None
                    };

                    if let Some(m) = have_metadata {
                        let mut metadata = Self::metadata_into_downloading(m);
                        let piece_picker = &mut metadata.piece_picker;
                        for (addr, pc) in &mut self.connected_peers {
                            if let Some(map) = pc.bitmap.take() {
                                piece_picker.peer_add(*addr, map);
                            }
                        }
                        self.torrent_state = TorrentState::Metadata(metadata);
                    }
                }
                TorrentState::Metadata(m) => {
                    // simply ignore them
                }
            },
            ExtendedMetadata::Reject { piece } => match &mut self.torrent_state {
                TorrentState::Fetching(f) => {
                    let mbuf = &mut f.meta_buf;
                    mbuf.requesting.remove(&piece);
                    mbuf.not_requested.insert(piece);
                    info!("metadata request of piece {piece} to {addr:?} is rejected");
                }
                TorrentState::Metadata(_) => {
                    // simply ignore them
                }
            },
        }
    }

    fn fetching_metadata(&mut self) {
        let meta_buf = match &mut self.torrent_state {
            TorrentState::Metadata(_) => {
                return;
            }
            TorrentState::Fetching(f) => &mut f.meta_buf,
        };

        let now = tokio::time::Instant::now();
        let timeout = tokio::time::Duration::from_secs(5);
        let mut timeout_pieces = vec![];
        meta_buf.requesting.retain(|k, v| {
            if v.elapsed() > timeout {
                timeout_pieces.push(*k);
                false
            } else {
                true
            }
        });
        for p in timeout_pieces {
            meta_buf.not_requested.insert(p);
        }

        for (_, h) in &mut self.connected_peers {
            if h.conn.support_metadata_extension() && h.conn.metadata_size() > 0 {
                // TODO: adaptively set value of n
                meta_buf.add_size_to_bucket(h.conn.metadata_size());
                Self::fetching_metadata_from_peer(h, 2, meta_buf, now);
            }
        }
    }

    fn fetching_metadata_from_peer_addr(
        peers: &HashMap<PeerAddr, PeerConn>,
        addr: &PeerAddr,
        n: usize,
        meta_buf: &mut MetadataBuffer,
    ) {
        if let Some(c) = peers.get(addr) {
            Self::fetching_metadata_from_peer(c, n, meta_buf, time::Instant::now());
        }
    }

    fn fetching_metadata_from_peer(
        conn: &PeerConn,
        n: usize,
        meta_buf: &mut MetadataBuffer,
        now: time::Instant,
    ) {
        for _ in 0..n {
            if let Some(piece) = meta_buf.not_requested.pop_first() {
                conn.conn
                    .send_stream_cmd(ConnMsg::Extend(ExtendedMsg::Metadata(
                        ExtendedMetadata::Request { piece },
                    )));
                meta_buf.requesting.insert(piece, now);
            }
        }
    }
}

fn check_received_metadata(mbuf: &mut MetadataBuffer, info_hash: [u8; 20]) -> io::Result<Metadata> {
    use crate::metadata::Info;
    let info: Info = bt_bencode::from_slice(&mbuf.metadata)?;
    let meta = info.to_metadata(info_hash);
    if let Ok(true) = meta.verify_info_hash() {
        Ok(meta)
    } else {
        Err(io::Error::new(io::ErrorKind::Other, "verify failed"))
    }
}

pub(crate) async fn run_transmit_worker(
    mut transmit: TransmitWorker,
    cancel: CancellationToken,
    done: oneshot::Sender<()>,
) {
    let mut ticker = tokio::time::interval(time::Duration::from_millis(1000));
    let mut dht_ticker = tokio::time::interval(time::Duration::from_secs(60));
    loop {
        // TODO: lets use notify?
        tokio::select! {
            Some(msg) = transmit.receiver.recv() => {
                debug!("transmit manager received msg {msg:?}");
                transmit.handle_msg(msg); // TODO: handle result
            }
            _ = dht_ticker.tick() => {
                info!("dht ticker tick");
                run_dht(&mut transmit);
            }
            _ = ticker.tick() => {
                // transmit.pick_blocks_for_all_peers(2);
                transmit.fetching_metadata();
            }
            _ = cancel.cancelled() => {
                info!("transmit manager cancelled");
                break;
            }
        };
    }
    let _ = done.send(());
    println!("transmit manager done");
}

fn copy_to_piecebuf(piece: &Piece, piecebuf: &mut PieceBuf) {
    let begin = piece.begin as usize;
    let end = begin + piece.len as usize;
    piecebuf.as_mut()[begin..end].copy_from_slice(piece.buf().expect("received piece should be OK"))
}

fn run_dht(transmit: &mut TransmitWorker) {
    if let Some(c) = &transmit.dht_client {
        let cl = c.clone();
        tokio::spawn(dht_get_peers(
            cl,
            transmit.id,
            transmit.info_hash,
            transmit.self_handle.clone(),
        ));
    }
}

async fn dht_get_peers(
    client: Arc<DHT>,
    self_id: [u8; 20],
    target: [u8; 20],
    tmh: TransmitManagerHandle,
) {
    let mut addrs = client.get_peers(target, false).await;
    addrs.extend_from_slice(&client.get_peers(target, true).await);
    for a in addrs {
        let t = tmh.clone();
        tokio::spawn(connect_peer(t, self_id, a, target));
    }
}

async fn connect_peer(
    main_tx: TransmitManagerHandle,
    id: [u8; 20],
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
            client_id: id,
            torrent_hash: info_hash,
        },
        &protocol::ExtendedHandshake {
            m: protocol::EXTENSION_IDS_MAP.clone(), // supported extensions and id number
            p: None,                                // TCP listen port
            v: Some("stardust 0.1.0".into()),       // client name and version

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

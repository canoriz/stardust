use crate::announce_manager::{self, AnnounceManagerHandle};
use crate::backfile::{BackFile, NormalFile};
use crate::bandwidth::{self, Bandwidth, RTT};
use crate::cache::simple_buffer::{BufStorage, FlushErr};
use crate::cache::simple_buffer::{GetPieceErr, PieceBuf};
use crate::connection_manager::{
    ConnectionManagerHandle, CtrlOfRecv, CtrlOfSend, CtrlOfSend as ConnMsg,
};
use crate::dht::DHT;
use crate::metadata::{self, Magnet, Metadata};
use crate::picker::{BlockPicker, BlockPickerDump, BlockStatus, PieceState, RarestPicker};
use crate::protocol::{
    self, BTStream, BitField, Conn, ExtendedMetadata, ExtendedMsg, ExtendedPex, HandshakeOption,
    InfoHash, Piece, Request,
};

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio_util::sync::{CancellationToken, DropGuard as CancelDropGuard};
use tracing::{debug, info, warn};

pub enum TorrentTask {
    Torrent(Metadata),
    Magnet(Magnet),
}

impl TorrentTask {
    pub fn info_hash(&self) -> [u8; 20] {
        match self {
            TorrentTask::Torrent(m) => m.info_hash,
            TorrentTask::Magnet(m) => m.info_hash,
        }
    }
}

type PeerAddr = SocketAddr;

#[derive(Debug)]
#[non_exhaustive]
pub enum PeerMsg {
    // TODO: use a structure ptr to connection_peer struct
    // to replace SocketAddr
    // which removes the HashMap cost
    Choke(PeerAddr),
    Unchoke(PeerAddr),
    Interested(PeerAddr),
    Uninterested(PeerAddr),
    PieceState(PeerAddr, PieceState),
    Have(PeerAddr, u32),
    Piece(PeerAddr, Piece),
    DhtPort(PeerAddr, u16),
    SuggestPiece(PeerAddr, u32),
    AllowedFast(PeerAddr, u32),
    Cancel(PeerAddr, Request),
    Reject(PeerAddr, Request),
    Request(PeerAddr, Request),
    ExtendMetadata(PeerAddr, ExtendedMetadata),
    ExtendPex(PeerAddr, ExtendedPex),
    BlockReceived {
        peer: PeerAddr,
        n_req_in_flight: usize,

        // how many blocks we received in this period
        n_recv_in_period: usize,
    },
}

/// (conn, is_income)
pub type NewPeerConn = (protocol::BTStream<Box<dyn Conn>>, bool);

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Msg {
    AnnounceFinish(Result<metadata::AnnounceResp, metadata::AnnounceError>),
    AnnounceMsg(announce_manager::Msg),

    NewPeer(Result<NewPeerConn, SocketAddr>),
    NewDiscoveredPeer(SocketAddr),
    PeerLeave(PeerAddr),

    PieceBufReady {
        index: usize,
        buf: io::Result<PieceBuf>,
    },

    /// A message received from peer
    PeerMsg(PeerMsg),
    FlushError(FlushErr),

    /// A peer reaches it's rtt limit, check if any
    /// request are timeout
    PeerTimeoutCheck(PeerAddr),

    RequestMetadata(oneshot::Sender<Option<Arc<Metadata>>>),
    DumpStatus(oneshot::Sender<TransmitDump>),
    LoadProgress(TransmitDump, oneshot::Sender<()>),
    CheckFile(oneshot::Sender<bool>),
    ChangeState(RunningCmd, oneshot::Sender<()>),
    WaitDownloaded(oneshot::Sender<watch::Receiver<bool>>),
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

/// modes for bandwidth control
/// we want a balanced max-bandwidth and min rtt
enum BandwidthMode {
    /// adaptively increase requests
    Auto {
        since: time::Instant,
        min_rtt: time::Duration,
    },

    /// slow down to this in-flight
    SlowDown {
        since_auto: time::Instant,
        min_rtt: time::Duration,

        // slow down to target inflight
        inflight_target: usize,
        expire: time::Instant,

        rtt_before: time::Duration,

        // if rtt is smaller
        faster: bool,
    },

    /// probe rtt
    ProbeRTT {
        since_auto: time::Instant,
        min_rtt: time::Duration,

        expire: time::Instant,
        n_to_receive: usize,
    },
}

struct PeerConn {
    conn: ConnectionManagerHandle,
    state: PeerStatus,
    bitmap: Option<PieceState>,
    bw: Bandwidth<16>,
    bw_mode: BandwidthMode,
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
        announce_manager: AnnounceManagerHandle,
    ) -> Self {
        let worker = TransmitWorker::new(
            t,
            id,
            dht_client,
            announce_manager,
            cmd_sender,
            cmd_receiver,
        );
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
        // TODO: dump status
        self.cancel.disarm().cancel();
        _ = self.worker_stop.await;
    }
}

pub enum TorrentState {
    Metadata(Downloading),
    Fetching(FetchingMetadata),
}

type CheckResult = u8;

#[derive(Serialize, Deserialize)]
struct CheckState {
    state: Vec<CheckResult>,
    known: usize,
}
impl CheckState {
    const UNKNOWN: CheckResult = 0;
    const VERIFIED: CheckResult = 1;
    const CORRUPT: CheckResult = 2;

    fn new(n: usize) -> Self {
        Self {
            state: vec![Self::UNKNOWN; n],
            known: 0,
        }
    }

    fn get(&self, i: usize) -> CheckResult {
        self.state[i]
    }

    fn check(&mut self, index: usize, ok: bool) {
        let s = &mut self.state[index];
        match *s {
            Self::UNKNOWN => {
                *s = if ok { Self::VERIFIED } else { Self::CORRUPT };
                self.known += 1;
            }
            _ => {}
        }
    }

    fn known(&self) -> usize {
        self.known
    }
}

#[derive(Serialize, Deserialize)]
enum RunningState {
    Downloading,
    Paused,  // maintains connection but do not download
    Stopped, // all stopped
    Seeding,
    Checking {
        prev_state: RunningCmd,
        selected: BitField,
        checked: CheckState,
        #[serde(skip)]
        waiter: Vec<oneshot::Sender<bool>>,
    }, // checking local file
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RunningCmd {
    Resume,
    Pause,
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TorrentStateDump {
    Metadata(BlockPickerDump),
    Fetching(FetchingMetadata),
}

/// The fetching information of a torrent
/// still downloading metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchingMetadata {
    meta_buf: MetadataBuffer,
    pub magnet: Magnet,
}

impl FetchingMetadata {
    /// Receive a new metadata part.
    /// Returns: if metadata is complete and verified
    fn receive_metadata_part(
        &mut self,
        piece: u32,
        data: Vec<u8>,
        total_size: Option<usize>,
    ) -> Option<Metadata> {
        let mbuf = &mut self.meta_buf;
        if let Some(sz) = total_size {
            mbuf.add_size_to_bucket(sz);
        }
        let probably_tot_size = mbuf.probable_total_size();
        let buf = &mut mbuf.metadata;
        let offset = (piece * 16384) as usize;
        buf[offset..offset + data.len()].copy_from_slice(&data);
        mbuf.requesting.remove(&piece);
        mbuf.not_requested.remove(&piece);

        if probably_tot_size > 0 && mbuf.not_requested.len() == 0 && mbuf.requesting.len() == 0 {
            // received full metadata
            match check_received_metadata(mbuf, self.magnet.info_hash) {
                Ok(m) => Some(m),
                Err(_) => {
                    warn!("metadata verify failed, needs re-download");
                    for p in 0..=((probably_tot_size - 1) / 16384) {
                        let p = p as u32;
                        if !mbuf.requesting.contains_key(&p) {
                            mbuf.not_requested.insert(p);
                        }
                    }
                    None
                }
            }
        } else {
            // did not receive full metadata yet
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[serde(skip)]
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

    pub block_picker: BlockPicker,
    pub storage: BufStorage,
}

pub struct TransmitWorker {
    // our peer ID
    id: [u8; 20],
    info_hash: InfoHash,

    handshake_opt: HandshakeOption,

    dht_client: Option<Arc<DHT>>,

    announce_manager: AnnounceManagerHandle,

    /// The state of the torrent
    /// whether have metadata or not
    torrent_state: TorrentState,

    /// The running state
    running_state: RunningState,

    /// receives various events
    receiver: mpsc::UnboundedReceiver<Msg>,

    /// contains sender of receiver
    self_handle: TransmitManagerHandle,

    connected_peers: HashMap<PeerAddr, PeerConn>,
    connecting_peers: HashSet<PeerAddr>,

    /// received blocks waiting writing to piece buf once
    /// piece buf is ready
    waiting_for_piecebuf: HashMap<u32, Vec<BlockWaitingBuf>>,

    downloaded: watch::Sender<bool>,
}

struct BlockWaitingBuf {
    piece: Piece,

    /// if this piece is all_received
    full_received: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransmitDump {
    pub state: TorrentStateDump,
    pub peers: Vec<SocketAddr>,
}

impl TransmitWorker {
    const DHT_TIMEOUT: time::Duration = time::Duration::from_secs(3);
    pub fn new(
        t: TorrentTask,
        id: [u8; 20],
        dht_client: Option<Arc<DHT>>,
        announce_manager: AnnounceManagerHandle,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
    ) -> Self {
        let (info_hash, state) = match t {
            TorrentTask::Torrent(m) => {
                let info_hash = m.info_hash;
                let state = TorrentState::Metadata(Self::metadata_into_downloading(m));
                (info_hash, state)
            }
            TorrentTask::Magnet(m) => {
                let info_hash = m.info_hash;
                let state = TorrentState::Fetching(FetchingMetadata {
                    meta_buf: MetadataBuffer::new(),
                    magnet: m,
                });
                (info_hash, state)
            }
        };
        let opt = HandshakeOption::builder()
            .client_id(id)
            .dht_port(dht_client.as_ref().map(|c| c.port()))
            .build();
        let downloaded = watch::channel(false).0;
        Self {
            id,
            info_hash,
            handshake_opt: opt,
            dht_client,
            announce_manager,
            torrent_state: state,
            receiver: cmd_receiver,
            self_handle: TransmitManagerHandle { sender: cmd_sender },
            // announce_handle: None,
            // announce_tx: None,
            connected_peers: HashMap::new(),
            connecting_peers: HashSet::new(),
            waiting_for_piecebuf: HashMap::new(),
            downloaded,
            running_state: RunningState::Stopped,
        }
    }

    fn metadata_into_downloading(m: Metadata) -> Downloading {
        let m = Arc::new(m);
        let piece_size = m.regular_piece_size() as u32;
        let total_length = m.len();
        let piece_picker = Box::new(RarestPicker::new(total_length, piece_size as usize));
        let mut block_picker = BlockPicker::new(
            total_length,
            piece_size as usize,
            piece_picker,
            time::Duration::from_secs(120),
        );

        for i in 0..block_picker.n_pieces() {
            block_picker.select(i as u32, true);
        }

        let back_file = BackFile::new::<NormalFile>().metadata(m.clone()).build();
        let buf_storage = BufStorage::new(total_length, m.regular_piece_size(), back_file);

        Downloading {
            metadata: m,
            block_picker,
            storage: buf_storage,
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
        let block_picker = match &mut self.torrent_state {
            TorrentState::Metadata(d) => &mut d.block_picker,
            TorrentState::Fetching(_) => {
                return;
            }
        };

        // TODO: optimize this, do not clone every time
        let rtts = self
            .connected_peers
            .iter()
            .map(|(k, v)| (*k, v.bw.get_rtt_4var(time::Duration::from_secs(10))))
            .collect();

        for (addr, h) in &self.connected_peers {
            info!("peer status {addr}: {:?}", h.state);
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let in_flight = h.conn.get_n_in_flight();
                let (reqs, n) = block_picker.pick_blocks(addr, &rtts, in_flight as usize, n_blocks);
                h.conn.send_stream_cmd(ConnMsg::RequestBlocks(reqs));
            }
        }
    }

    // fn pick_blocks_for_peer(&mut self, addr: &SocketAddr, n_blocks: usize) {
    // Fn: n_blk_received, n_blk_in_flight -> n_this_time_pick
    fn pick_blocks_for_peer(&mut self, addr: &SocketAddr, pick_n: usize) {
        warn!("pick {pick_n} blocks from {addr:?}");
        let block_picker = match &mut self.torrent_state {
            TorrentState::Metadata(d) => &mut d.block_picker,
            TorrentState::Fetching(_) => {
                return;
            }
        };

        // TODO: optimize this, do not clone every time
        let rtts = self
            .connected_peers
            .iter()
            .map(|(k, v)| (*k, v.bw.get_rtt_4var(time::Duration::from_secs(10))))
            .collect();

        if let Some(h) = self.connected_peers.get_mut(addr) {
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let n_in_flight = h.conn.get_n_in_flight();
                let (reqs, _n) =
                    block_picker.pick_blocks(addr, &rtts, pick_n, n_in_flight as usize);
                h.conn.send_stream_cmd(ConnMsg::RequestBlocks(reqs));
            }
        }
    }

    fn handle_msg(&mut self, m: Msg) -> io::Result<()> {
        match m {
            Msg::NewDiscoveredPeer(addr) => {
                self.handle_new_discovered_peer(addr);
                Ok(())
            }
            Msg::AnnounceMsg(m) => {
                self.announce_manager.send(m);
                Ok(())
            }
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
                        let addr = SocketAddr::new(ip, p.port);
                        let addr = to_canonical_addr(addr);
                        self.handle_new_discovered_peer(addr);
                    }
                }
                Ok(())
            }
            Msg::AnnounceFinish(Err(e)) => {
                info!("announce error {}", e);
                Ok(())
            }
            Msg::NewPeer(Ok((bt_conn, is_income))) => {
                info!(
                    "new {} connection {:?}",
                    if is_income { "income" } else { "outward" },
                    bt_conn
                );
                let peer_addr = to_canonical_addr(bt_conn.peer_addr());
                if !self.connected_peers.contains_key(&peer_addr) {
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
                            bw: Bandwidth::new(),
                            bw_mode: BandwidthMode::Auto {
                                since: time::Instant::now(),
                                min_rtt: time::Duration::MAX,
                            },
                        },
                    );

                    // ensure piece picker has a record for this peer (assume HaveNone until
                    // the peer sends its bitfield/have messages). This prevents
                    // pick_blocks from panicking when called before we received a bitfield.
                    match &mut self.torrent_state {
                        TorrentState::Metadata(d) => {
                            d.block_picker.peer_add(peer_addr, PieceState::HaveNone);
                        }
                        TorrentState::Fetching(_) => {}
                    }

                    self.self_handle
                        .sender
                        .send(Msg::PeerTimeoutCheck(peer_addr));
                }
                self.connecting_peers.remove(&peer_addr);
                // TODO: if is income, send bitfield
                Ok(())
            }
            Msg::NewPeer(Err(addr)) => {
                info!("err connect to {:?}", addr);
                self.connecting_peers.remove(&addr);
                Ok(())
            }
            Msg::PeerLeave(addr) => {
                info!("peer leave {addr}");
                // notify piece picker this peer left so it can update rarity and internal state
                match &mut self.torrent_state {
                    TorrentState::Metadata(d) => {
                        d.block_picker.peer_leave(&to_canonical_addr(addr))
                    }
                    TorrentState::Fetching(_) => {}
                }
                self.connected_peers.remove(&to_canonical_addr(addr));
                Ok(())
            }
            Msg::PeerMsg(pm) => self.handle_peer_msg(pm),
            Msg::FlushError(_) => {
                todo!()
            }
            Msg::PeerTimeoutCheck(addr) => self.handle_peer_request_timeout(addr),
            Msg::PieceBufReady { index, buf } => self.handle_piecebuf_ready(index, buf),
            Msg::DumpStatus(sender) => {
                self.handle_dump_status(sender);
                Ok(())
            }
            Msg::RequestMetadata(sender) => {
                self.handle_request_metadata(sender);
                Ok(())
            }
            Msg::LoadProgress(dump, sender) => {
                self.handle_load_progress(dump, sender);
                Ok(())
            }
            Msg::CheckFile(sender) => {
                self.handle_check_file(sender);
                Ok(())
            }
            Msg::ChangeState(cmd, sender) => {
                // TODO: FIXME: should pause announce task as well
                match cmd {
                    RunningCmd::Resume => {
                        self.running_state = RunningState::Downloading;
                        self.pick_blocks_for_all_peers(10);
                    }
                    RunningCmd::Pause => {
                        self.running_state = RunningState::Paused;
                    }
                    RunningCmd::Stop => {
                        self.running_state = RunningState::Stopped;
                    }
                }
                _ = sender.send(());
                Ok(())
            }
            Msg::WaitDownloaded(sender) => {
                _ = sender.send(self.downloaded.subscribe());
                Ok(())
            }
        }
    }

    fn handle_peer_msg(&mut self, m: PeerMsg) -> io::Result<()> {
        match m {
            PeerMsg::PieceState(addr, state) => {
                info!("peer {addr} sends state {state:?}");
                let block_picker = match &mut self.torrent_state {
                    TorrentState::Metadata(d) => &mut d.block_picker,
                    TorrentState::Fetching(_) => {
                        let pc = self
                            .connected_peers
                            .get_mut(&addr)
                            .expect("connection should in map");
                        pc.bitmap = Some(state);
                        return Ok(());
                    }
                };
                block_picker.peer_add(addr, state);
                Ok(())
            }
            PeerMsg::Have(peer, i) => {
                info!("peer {peer} have piece {i}");
                let block_picker = match &mut self.torrent_state {
                    TorrentState::Metadata(d) => &mut d.block_picker,
                    TorrentState::Fetching(_) => {
                        if let Some(pc) = self.connected_peers.get_mut(&peer) {
                            pc.bitmap.as_mut().map(|bm| bm.set_have2(i));
                        }
                        return Ok(());
                    }
                };

                block_picker.peer_new_have(&peer, i);
                Ok(())
            }
            PeerMsg::Choke(peer) => {
                warn!("{peer} choked us");
                let piece_picker = match &mut self.torrent_state {
                    TorrentState::Metadata(d) => &mut d.block_picker,
                    TorrentState::Fetching(_) => {
                        return Ok(());
                    }
                };
                piece_picker.peer_choke(&peer);
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
            PeerMsg::Unchoke(peer) => {
                let n_first_pick = 3;
                warn!("{peer} unchoked us");
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_choke_status = ChokeStatus::Unchoked;
                });
                assert_eq!(
                    self.connected_peers[&peer].state.peer_choke_status,
                    ChokeStatus::Unchoked
                );
                // TODO: are we interested in this peer?
                // self.pick_blocks_for_peer(&peer, 0);
                Ok(())
            }
            PeerMsg::Interested(peer) => {
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_interest_status = InterestStatus::Interested;
                });
                Ok(())
            }
            PeerMsg::Uninterested(peer) => {
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_interest_status = InterestStatus::Uninterested;
                });
                Ok(())
            }
            PeerMsg::BlockReceived {
                peer,
                n_req_in_flight,
                n_recv_in_period,
            } => {
                // TODO: OPTIMIZE: return connection handle to reduce map search
                let conn = self.connected_peers.get_mut(&peer).expect("should exist");

                const TEN_SECS: time::Duration = time::Duration::from_secs(10);
                // TODO: this 10 is set randomly, choose a good value value instead
                let (max_bw, min_rtt) = conn.bw.count_max_bw_and_min_rtt(TEN_SECS);
                let rtt = conn.bw.get_rtt(TEN_SECS);
                warn!("peer {peer} estimated max bandwidth {max_bw}, min rtt {min_rtt:?} req in flight: {n_req_in_flight}");

                let optimum_bdp = 2.0 * min_rtt.as_secs_f32() * max_bw;

                let n_to_pick = match conn.bw_mode {
                    BandwidthMode::Auto {
                        ref mut since,
                        ref mut min_rtt,
                    } => {
                        let since_auto = *since;
                        let min_rtt2 = *min_rtt;
                        let slow_down_to = |optimum_inflight: usize, mode: &mut BandwidthMode| {
                            let expire = time::Instant::now()
                                + rtt.mul_f32(
                                    (n_req_in_flight.saturating_sub(optimum_inflight)) as f32 * 1.3,
                                );
                            *mode = BandwidthMode::SlowDown {
                                since_auto,
                                min_rtt: min_rtt2,
                                inflight_target: optimum_inflight,
                                expire,
                                rtt_before: rtt,
                                faster: false,
                            };
                        };

                        if since.elapsed() > TEN_SECS {
                            info!("{peer} 10 sec no smaller rtt to SlowDown mode");
                            slow_down_to(0, &mut conn.bw_mode);
                            0
                        } else {
                            if rtt < *min_rtt {
                                *since = time::Instant::now();
                                *min_rtt = rtt;
                            }
                            let (rtt_slope, rtt_correlation) =
                                conn.bw.get_rtt_slope_and_correlation();
                            info!(
                                "peer {peer} rtt slope {rtt_slope} correlation {rtt_correlation} points {}",
                                conn.bw.get_rtt_n_points()
                            );

                            let mut optimum_inflight = optimum_bdp as usize / 16384;
                            if conn.bw.get_rtt_n_points() >= 7 {
                                if rtt_slope > 0.1 && rtt_correlation > 0.7 {
                                    optimum_inflight = (conn.bw.get_prev_in_flight() as f32 * 0.5)
                                        as usize
                                        / 16384;
                                    info!("{peer} slow down to {optimum_bdp}");
                                    slow_down_to(optimum_inflight, &mut conn.bw_mode);
                                } else if rtt_slope > 0.5 {
                                    // non-linear rtt increase, maybe a new app level speed limit is
                                    // applied on peer
                                    info!(
                                        "{peer} rtt non-linear drastically increase slow down to 2"
                                    );
                                    slow_down_to(2, &mut conn.bw_mode);
                                }
                            }

                            // Only can pick more blocks if we received some or no requests in flight.
                            // For peers with small bandwidth, we don't request too much from them
                            // to avoid mark these blocks as in-flight and not requesting from other peers.
                            // preventing accumulating too much partial downloaded pieces.
                            const MIN_IN_FLIGHT: usize = 2;
                            if n_recv_in_period > 0 || n_req_in_flight < MIN_IN_FLIGHT {
                                if optimum_inflight > n_req_in_flight {
                                    optimum_inflight - n_req_in_flight
                                } else if n_req_in_flight < MIN_IN_FLIGHT {
                                    MIN_IN_FLIGHT - n_req_in_flight
                                } else {
                                    0
                                }
                            } else {
                                0
                            }
                        }
                    }
                    BandwidthMode::SlowDown {
                        ref mut since_auto,
                        ref mut min_rtt,
                        expire,
                        inflight_target,
                        rtt_before,
                        ref mut faster,
                    } => {
                        let (_, rtt) = conn.bw.count_max_bw_and_min_rtt(TEN_SECS);
                        if rtt < *min_rtt {
                            *since_auto = time::Instant::now();
                            *min_rtt = rtt;
                            *faster = true;
                        }

                        if n_req_in_flight <= inflight_target || time::Instant::now() > expire {
                            const TEST_RTT_BURST: usize = 5;
                            let now = time::Instant::now();
                            let timeout = rtt_before.mul_f32(TEST_RTT_BURST as f32 * 1.2);
                            if *faster {
                                conn.bw_mode = BandwidthMode::Auto {
                                    since: *since_auto,
                                    min_rtt: *min_rtt,
                                };
                                TEST_RTT_BURST // some random value
                            } else {
                                info!("probe timeout {timeout:?}");
                                conn.bw_mode = BandwidthMode::ProbeRTT {
                                    since_auto: *since_auto,
                                    min_rtt: *min_rtt,
                                    expire: now + timeout,
                                    n_to_receive: TEST_RTT_BURST,
                                };
                                TEST_RTT_BURST
                            }
                        } else {
                            info!(
                                "slow down mode {} to recv",
                                n_req_in_flight.saturating_sub(inflight_target)
                            );
                            0
                        }
                    }
                    BandwidthMode::ProbeRTT {
                        ref mut since_auto,
                        ref mut min_rtt,
                        expire,
                        ref mut n_to_receive,
                    } => {
                        if *n_to_receive > 0 {
                            let (_, rtt) = conn.bw.count_max_bw_and_min_rtt(TEN_SECS);
                            if rtt < *min_rtt {
                                *since_auto = time::Instant::now();
                                *min_rtt = rtt;
                            }

                            *n_to_receive = n_to_receive.saturating_sub(n_recv_in_period);
                            info!(
                                "received {}, {} to receive in probeRTT testing mode",
                                n_recv_in_period, *n_to_receive,
                            );
                        }
                        if *n_to_receive == 0 || time::Instant::now() > expire {
                            conn.bw_mode = BandwidthMode::Auto {
                                since: *since_auto,
                                min_rtt: rtt,
                            };
                        }
                        n_recv_in_period
                    }
                };

                if matches!(self.running_state, RunningState::Downloading) {
                    if conn.state.peer_choke_status == ChokeStatus::Unchoked {
                        self.pick_blocks_for_peer(&peer, n_to_pick);
                    }
                }
                Ok(())
            }
            PeerMsg::Piece(addr, piece) => self.handle_piece_msg(&addr, piece),
            PeerMsg::DhtPort(addr, port) => self.handle_dht_port_msg(addr, port),
            PeerMsg::ExtendMetadata(pa, m) => {
                self.handle_extend_metadata(pa, m);
                Ok(())
            }
            PeerMsg::ExtendPex(pa, pex) => {
                self.handle_extend_pex(pa, pex);
                Ok(())
            }
            PeerMsg::SuggestPiece(addr, index) => {
                info!("{addr} suggest piece {index}");
                Ok(())
            }
            PeerMsg::AllowedFast(addr, index) => {
                info!("{addr} allowed fast {index}");
                Ok(())
            }
            PeerMsg::Cancel(addr, req) => {
                info!("{addr} cancel {req:?}");
                Ok(())
            }
            PeerMsg::Reject(addr, req) => {
                self.handle_reject_msg(addr, req);
                Ok(())
            }
            PeerMsg::Request(addr, req) => {
                // TODO: optimize: handle can be passed so avoid map search overhead
                if let Some(conn) = self.connected_peers.get_mut(&addr) {
                    if conn.conn.capability().contains(&protocol::Capability::Fast) {
                        conn.conn.send_stream_cmd(ConnMsg::Reject(req));
                    }
                }
                Ok(())
            }
        }
    }

    fn handle_peer_request_timeout(&mut self, peer: PeerAddr) -> io::Result<()> {
        // call pick 0 to revoke timeout requests
        self.pick_blocks_for_peer(&peer, 0);
        if let Some(pc) = self.connected_peers.get(&peer) {
            pc.conn.recv_stream_cmd(CtrlOfRecv::ReportStat);

            // TODO: set a alarm at some clock instead of using tokio task?
            let next_alarm_wait = (pc.bw.get_rtt_4var(time::Duration::from_secs(10)) * 2)
                .max(time::Duration::from_millis(10));
            let s = self.self_handle.sender.clone();
            info!("check timeout for {peer:?}, next check after {next_alarm_wait:?}");
            tokio::spawn(async move {
                time::sleep(next_alarm_wait).await;
                s.send(Msg::PeerTimeoutCheck(peer));
            });
        }
        Ok(())
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

    fn verify_piece(p: &PieceBuf, metadata: &Metadata) -> bool {
        use std::io::Write;
        let target = &metadata.info.pieces[p.index() * 20..p.index() * 20 + 20];
        let mut hasher = Sha1::new();
        _ = hasher.write_all(p.as_ref());
        let res: [u8; 20] = hasher.finalize().into();
        res == target
    }

    /// called when a full piece received
    /// return verify result of this piece
    fn handle_full_piece_received(
        p: &mut PieceBuf,
        metadata: &Metadata,
        connected_peers: &mut HashMap<PeerAddr, PeerConn>,
        block_picker: &mut BlockPicker,
    ) {
        info!("piece {} full received, verifying...", p.index());
        if Self::verify_piece(p, metadata) {
            // flush error is not fatal
            // we always catch drop error
            p.flush(|_| {});
            for (_, h) in connected_peers.iter() {
                h.conn.send_stream_cmd(ConnMsg::Have(p.index() as u32));
            }
            block_picker.piece_verified(p.index() as u32, true);
        } else {
            info!("piece {} verify failed", p.index());
            block_picker.piece_verified(p.index() as u32, false);
        }
    }

    fn get_piecebuf(
        storage: &mut BufStorage,
        sender: mpsc::UnboundedSender<Msg>,
        index: usize,
    ) -> Result<&mut PieceBuf, GetPieceErr> {
        let err_sender = sender.clone();
        let on_ready = move |p| {
            _ = sender.send(Msg::PieceBufReady { index, buf: p });
        };
        let on_err = move |e| {
            _ = err_sender.send(Msg::FlushError(e));
        };
        storage.get_piece(index, on_ready, Box::new(on_err))
    }

    /// NOTE: CRITICAL: if concurrently get same index, only one of them may return
    /// others may block indefinitely
    async fn get_piecebuf_now<'a>(
        storage: &'a mut BufStorage,
        index: usize,
    ) -> io::Result<&'a mut PieceBuf> {
        let (tx, rx) = oneshot::channel();
        let on_ready = move |p| {
            _ = tx.send(p);
        };
        let on_err = move |_| {};

        match storage.get_piece(index, on_ready, Box::new(on_err)) {
            Ok(p) => {
                // return Ok(p);
            }
            Err(GetPieceErr::InvalidPiece) => panic!("wrong index {}", index),
            Err(GetPieceErr::Returned) => panic!("already returned piece {}", index),
            Err(GetPieceErr::Loading) => {
                let p = rx.await.map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "get piecebuf_now oneshot recv error")
                })??;
                storage.add_piece(p);
            }
        }
        Ok(storage.get_piece(index, |_| {}, Box::new(|_| {})).unwrap())
    }

    /// handle PIECE message
    // TODO: fix the return type
    fn handle_piece_msg(
        &mut self,
        peer: &SocketAddr,
        mut piece: protocol::Piece,
    ) -> io::Result<()> {
        debug!("recv {piece:?} from {peer:?}");

        let (block_picker, metadata, storage) = match &mut self.torrent_state {
            TorrentState::Metadata(d) => (&mut d.block_picker, &d.metadata, &mut d.storage),
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

        let req = Request {
            index: piece.index,
            begin: piece.begin,
            len: piece.len,
        };

        if let Some(pc) = self.connected_peers.get_mut(peer) {
            let rtt = block_picker.get_rtt(peer, &req);
            let inflight_when_sent = block_picker.get_inflight_when_sent(peer, &req);
            pc.bw
                .add_sample(piece.len as usize, rtt, inflight_when_sent);
        }

        if !block_picker.want_block(req) {
            warn!(
                "discard PIECE msg {} {} {} block index {}",
                piece.index,
                piece.begin,
                piece.len,
                piece.begin >> 14,
            );
            return Ok(());
        }

        let (piece_received, peers_requested) = block_picker.receive_block(req);
        for addr in peers_requested {
            if addr != *peer {
                // if this block come from peer we did not request, cancel old request
                // TODO: remove pending requests if not sent
                if let Some(conn) = self.connected_peers.get(&addr) {
                    if conn.conn.capability().contains(&protocol::Capability::Fast) {
                        conn.conn.send_stream_cmd(ConnMsg::Cancel(req));
                    }
                }
            }
        }

        match Self::get_piecebuf(
            storage,
            self.self_handle.sender.clone(),
            piece.index as usize,
        ) {
            Ok(piecebuf) => {
                copy_to_piecebuf(&piece, piecebuf);
                if let Some(_) = piece_received {
                    Self::handle_full_piece_received(
                        piecebuf,
                        &metadata,
                        &mut self.connected_peers,
                        block_picker,
                    );
                }
            }
            Err(GetPieceErr::InvalidPiece) => {
                info!("invalid piece {piece:?}");
            }
            Err(e) => {
                // TODO: maybe set some unblock_conn upper limit
                // piece.unblock_conn();
                let index = piece.index;
                let full_received = piece_received.is_some();
                match self.waiting_for_piecebuf.get_mut(&index) {
                    Some(v) => v.push(BlockWaitingBuf {
                        piece,
                        full_received,
                    }),
                    None => {
                        self.waiting_for_piecebuf.insert(
                            index,
                            vec![BlockWaitingBuf {
                                piece,
                                full_received,
                            }],
                        );
                    }
                }
                info!("piecebuf not present err {e:?}");
            }
        }
        Ok(())
    }

    fn handle_piecebuf_ready(&mut self, index: usize, buf: io::Result<PieceBuf>) -> io::Result<()> {
        let (block_picker, metadata) = match &mut self.torrent_state {
            TorrentState::Metadata(d) => (&mut d.block_picker, &d.metadata),
            TorrentState::Fetching(_) => {
                unreachable!();
            }
        };
        match buf {
            Ok(mut buf) => {
                let pending = self.waiting_for_piecebuf.remove(&(index as u32));
                if let Some(ps) = pending {
                    let mut full_received = false;
                    info!(
                        "piecebuf {index} now ready, flushing {} blocks into it",
                        ps.len()
                    );
                    for p in ps {
                        // full_received should be set at most once
                        assert!(!full_received);
                        copy_to_piecebuf(&p.piece, &mut buf);
                        if p.full_received {
                            full_received = true;
                            info!("flush new ready piecebuf {index}");
                            Self::handle_full_piece_received(
                                &mut buf,
                                &metadata,
                                &mut self.connected_peers,
                                block_picker,
                            );
                        }
                    }
                }

                match &mut self.running_state {
                    RunningState::Checking {
                        prev_state,
                        selected,
                        checked,
                        waiter,
                    } => {
                        let mut notify_waiter = |r: bool| {
                            for w in waiter.drain(0..) {
                                w.send(r);
                            }
                        };
                        let index = buf.index() as u32;
                        if selected.get(index) && checked.get(index as usize) == CheckState::UNKNOWN
                        {
                            let r = Self::verify_piece(&buf, metadata);
                            checked.check(index as usize, r);
                            block_picker.set_have(index, r);

                            assert!(checked.get(index as usize) != CheckState::UNKNOWN);
                            if selected.count_ones() as usize == checked.known() {
                                let r = checked.state.iter().all(|s| *s != CheckState::CORRUPT);
                                notify_waiter(r);
                                match prev_state {
                                    RunningCmd::Resume => {
                                        if block_picker.is_finished() {
                                            self.running_state = RunningState::Seeding
                                        } else {
                                            self.running_state = RunningState::Downloading
                                        }
                                    }
                                    RunningCmd::Pause => self.running_state = RunningState::Paused,
                                    RunningCmd::Stop => self.running_state = RunningState::Stopped,
                                }
                            }
                        }
                    }
                    _ => {}
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
            Err(e) => {
                // TODO: why that's error
                // shall we reload?
                return Err(e);
            }
        }
    }

    fn handle_reject_msg(&mut self, peer: SocketAddr, req: Request) {
        debug!("{peer} rejects {req:?}");

        let block_picker = match &mut self.torrent_state {
            TorrentState::Metadata(d) => &mut d.block_picker,
            TorrentState::Fetching(_) => return,
        };
        block_picker.peer_reject_block(&peer, req);
    }

    fn handle_dump_status(&mut self, sender: oneshot::Sender<TransmitDump>) {
        // TODO: dump announce
        let peers: Vec<_> = self.connected_peers.keys().cloned().collect();
        let state = match &mut self.torrent_state {
            TorrentState::Metadata(d) => TorrentStateDump::Metadata(d.block_picker.dump()),
            TorrentState::Fetching(f) => TorrentStateDump::Fetching(f.clone()),
        };
        let dump = TransmitDump { peers, state };
        _ = sender.send(dump);
    }

    fn handle_request_metadata(&mut self, sender: oneshot::Sender<Option<Arc<Metadata>>>) {
        let metadata = match &mut self.torrent_state {
            TorrentState::Metadata(d) => Some(d.metadata.clone()),
            TorrentState::Fetching(_) => None,
        };
        _ = sender.send(metadata);
    }

    fn handle_load_progress(&mut self, progress: TransmitDump, sender: oneshot::Sender<()>) {
        // TODO: dump announce
        match progress.state {
            TorrentStateDump::Metadata(block_picker_dump) => match &mut self.torrent_state {
                TorrentState::Metadata(d) => {
                    d.block_picker.load_progress(block_picker_dump);
                }
                TorrentState::Fetching(_) => {
                    info!("load progress when fetching metadata, maybe unreachable");
                    todo!()
                }
            },
            TorrentStateDump::Fetching(f) => {
                self.torrent_state = TorrentState::Fetching(f);
            }
        }

        for p in progress.peers {
            self.handle_new_discovered_peer(to_canonical_addr(p));
        }
        _ = sender.send(());
    }

    fn is_downloaded(&mut self) -> bool {
        let block_picker = match &mut self.torrent_state {
            TorrentState::Metadata(d) => &mut d.block_picker,
            TorrentState::Fetching(_) => return false,
        };
        block_picker.is_finished()
    }

    fn handle_dht_port_msg(&mut self, mut addr: PeerAddr, port: u16) -> io::Result<()> {
        use crate::dht::RpcAddr;
        if let Some(c) = &self.dht_client {
            let c = c.clone();
            addr.set_port(port);
            tokio::spawn(async move {
                _ = c.ping_rpc(RpcAddr::NoID(addr), Self::DHT_TIMEOUT).await;
            });
        }
        Ok(())
    }

    fn handle_new_discovered_peer(&mut self, addr: SocketAddr) {
        if !self.connected_peers.contains_key(&addr) && !self.connecting_peers.contains(&addr) {
            self.connecting_peers.insert(addr);
            let h_clone = self.self_handle.clone();
            let opt = self.handshake_opt.clone();
            tokio::spawn(connect_peer(h_clone, addr, opt, self.info_hash));
        }
    }

    fn handle_check_file(&mut self, sender: oneshot::Sender<bool>) {
        let (block_picker, storage, metadata) = match &mut self.torrent_state {
            TorrentState::Metadata(d) => (&mut d.block_picker, &mut d.storage, &d.metadata),
            TorrentState::Fetching(_) => {
                info!("check file when fetching metadata, maybe unreachable");
                sender.send(false);
                return;
            }
        };
        let selected = block_picker.selected_pieces().clone();
        let mut checked = CheckState::new(block_picker.n_pieces());
        let total_pieces = block_picker.n_pieces();

        // check pieces in buffer
        for (i, piecebuf) in storage.iter_buffered() {
            if selected.get(*i as u32) {
                let r = Self::verify_piece(piecebuf, metadata);
                checked.check(*i, r);
            }
        }

        // arrange for loading pieces not in buffer
        for i in 0..total_pieces {
            if selected.get(i as u32) && checked.get(i) == CheckState::UNKNOWN {
                let res = Self::get_piecebuf(storage, self.self_handle.sender.clone(), i);
                assert!(!res.is_ok())
            }
        }

        if checked.known() == selected.count_ones() as usize {
            info!("file check complete");
            let _ = sender.send(true);
        } else {
            match &mut self.running_state {
                RunningState::Checking { waiter, .. } => {
                    waiter.push(sender);
                }
                s => {
                    let prev_state = match s {
                        RunningState::Downloading => RunningCmd::Resume,
                        RunningState::Paused => RunningCmd::Pause,
                        RunningState::Stopped => RunningCmd::Stop,
                        RunningState::Seeding => RunningCmd::Resume,
                        _ => unreachable!(),
                    };
                    let waiter = vec![sender];
                    self.running_state = RunningState::Checking {
                        prev_state,
                        checked,
                        selected,
                        waiter,
                    };
                }
            }
        }
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
                    let reject = ExtendedMsg::Metadata(ExtendedMetadata::Reject { piece });
                    let metadata = match bt_bencode::to_vec(&m.metadata.info) {
                        Ok(m) => m,
                        Err(e) => {
                            info!("respond metadata bt-bencode failed {e}");
                            _ = c.conn.send_stream_cmd(ConnMsg::Extend(reject));
                            return;
                        }
                    };

                    let begin = (piece as usize) * 16384;
                    let end = (piece as usize + 1) * 16384;
                    if begin < metadata.len() {
                        // TODO: OPTIMIZE: only reference to metadata.info should be
                        // enough, no need to serialize and to_vec().
                        let part = &metadata[begin..end.min(metadata.len())];
                        let data = ExtendedMsg::Metadata(ExtendedMetadata::Data {
                            total_size: Some(metadata.len()),
                            piece,
                            data: part.to_vec(),
                        });
                        _ = c.conn.send_stream_cmd(ConnMsg::Extend(data));
                    } else {
                        _ = c.conn.send_stream_cmd(ConnMsg::Extend(reject));
                    }
                }
            },
            ExtendedMetadata::Data {
                piece,
                data,
                total_size,
            } => match &mut self.torrent_state {
                TorrentState::Fetching(f) => {
                    if let Some(m) = f.receive_metadata_part(piece, data, total_size) {
                        let mut downloading = Self::metadata_into_downloading(m);
                        let piece_picker = &mut downloading.block_picker;
                        for (addr, pc) in &mut self.connected_peers {
                            if let Some(mut ps) = pc.bitmap.take() {
                                // bit field map got in `Fetching` state may be the same length
                                // as piece number.
                                // Resize BitField map to the exact size of pieces
                                match &mut ps {
                                    PieceState::Bitfield(b) => {
                                        b.resize(downloading.metadata.total_pieces() as u32)
                                    }
                                    _ => {}
                                }
                                piece_picker.peer_add(*addr, ps);
                            } else {
                                piece_picker.peer_add(*addr, PieceState::HaveNone);
                            }
                        }
                        self.torrent_state = TorrentState::Metadata(downloading);
                    } else {
                        // if we don't have metadata yet, fetch more from this peer
                        Self::fetching_metadata_from_peer_addr(
                            &self.connected_peers,
                            &addr,
                            3,
                            &mut f.meta_buf,
                        );
                    }
                }
                TorrentState::Metadata(_) => {
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
                    // already have metadata, simply ignore them
                }
            },
        }
    }

    fn handle_extend_pex(&mut self, addr: PeerAddr, pex: ExtendedPex) {
        info!("receive pex from {addr:?}: {:?}", pex);
        for (addr, flags) in pex.added {
            self.handle_new_discovered_peer(addr);
        }
        for (addr, flags) in pex.added6 {
            self.handle_new_discovered_peer(addr);
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
                // debug!("transmit manager received msg {msg:?}");
                transmit.handle_msg(msg); // TODO: handle result
                if transmit.is_downloaded() {
                    transmit.downloaded.send(true);
                    break;
                }
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
            transmit.handshake_opt.clone(),
            transmit.self_handle.clone(),
        ));
    }
}

async fn dht_get_peers(
    client: Arc<DHT>,
    self_id: [u8; 20],
    target: [u8; 20],
    handshake_opt: HandshakeOption,
    tmh: TransmitManagerHandle,
) {
    let mut addrs = client.get_peers(target, false).await;
    addrs.extend_from_slice(&client.get_peers(target, true).await);
    for a in addrs {
        let t = tmh.clone();
        let opt = handshake_opt.clone();
        tmh.sender.send(Msg::NewDiscoveredPeer(a));
    }
}

async fn connect_peer(
    main_tx: TransmitManagerHandle,
    addr: SocketAddr,
    opt: HandshakeOption,
    info_hash: InfoHash,
) -> Result<(), std::io::Error> {
    let tcp_stream = TcpStream::connect(addr).await?;
    let conn = protocol::BTStream::connect(tcp_stream, opt, info_hash).await;
    match conn {
        Ok(c) => {
            if let Err(e) = main_tx.sender.send(Msg::NewPeer(Ok((c.to_dyn(), false)))) {
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

fn to_canonical_addr(s: SocketAddr) -> SocketAddr {
    SocketAddr::new(s.ip().to_canonical(), s.port())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_dump_and_load_fetching() {
        // construct a simple TransmitDump with Fetching state
        let mb = MetadataBuffer::new();
        let magnet = Magnet {
            info_hash: [1; 20],
            dn: None,
            pe: None,
            tr: None,
        };
        let fetching = FetchingMetadata {
            meta_buf: mb,
            magnet,
        };
        let dump = TransmitDump {
            state: TorrentStateDump::Fetching(fetching),
            peers: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                6881,
            )],
        };

        let ser = serde_json::to_string(&dump).expect("serialize dump");
        let de: TransmitDump = serde_json::from_str(&ser).expect("deserialize dump");
        assert_eq!(dump.peers, de.peers);

        // ensure state variant matches
        match de.state {
            TorrentStateDump::Fetching(f) => {
                assert_eq!(f.magnet.info_hash, [1; 20]);
            }
            _ => panic!("expected Fetching state"),
        }
    }
}

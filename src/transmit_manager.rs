use crate::announce_manager::{self, AnnounceManagerHandle};
use crate::backfile::{BackFile, NormalFile, VoidFile};
use crate::bandwidth::Bandwidth;
use crate::buffer_pool::BlockBuf;
use crate::cache::cache_manager::{CacheManagerHandle, GlobalPieceKey, PieceLease};
use crate::cache::simple_buffer::{FlushErr, JointIndex, PieceBuf, SUB_PIECE_SIZE};
use crate::connection_manager::{
    ConnectionManagerHandle, CtrlOfRecv, CtrlOfSend, CtrlOfSend as ConnMsg, ReceivedBlocks,
};
use crate::dht::DHT;
use crate::hasher::HashState;
use crate::metadata::{self, Magnet, Metadata};
use crate::picker::{BlockPicker, BlockPickerDump, PieceState, RarestPicker};
use crate::protocol::{
    self, Capability, Conn, ExtendedMetadata, ExtendedMsg, ExtendedPex, HandshakeOption, InfoHash,
    PexFlag, Piece, Request,
};

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;
use tokio_util::sync::{CancellationToken, DropGuard as CancelDropGuard};
use tracing::{debug, error, info, instrument, span, trace, warn, Level};

mod bandwidth_mode;
mod inflight;
use bandwidth_mode::BandwidthMode;
use inflight::Inflight;

const PROBE_TO_AUTO_NORMAL_RTT_LIMIT: u32 = 2;
const PROBE_TO_SLOWDOWN_SLOW_RTT_LIMIT: u32 = 2;

/// 95% 置信度 (双侧检验) 的 t 分布临界值表
/// 用于判定 RTT 是否显著偏离均值（两侧）
/// 索引 0 对应 df=1 (n=2), 索引 9 对应 df=10 (n=11)
const T_TABLE_95_TWO_TAIL_DF_1_10: [f32; 10] = [
    12.706, // df = 1: 极度保守，防止只有两个包时的随机抖动
    4.303,  // df = 2
    3.182,  // df = 3
    2.776,  // df = 4
    2.571,  // df = 5
    2.447,  // df = 6
    2.365,  // df = 7
    2.306,  // df = 8
    2.262,  // df = 9
    2.228,  // df = 10
];

/// 获取双侧 t 临界值
fn get_t_critical_two_tail(df: usize) -> f32 {
    if df == 0 {
        return T_TABLE_95_TWO_TAIL_DF_1_10[0];
    }
    if df <= 10 {
        T_TABLE_95_TWO_TAIL_DF_1_10[df - 1]
    } else {
        // 当 df > 10 时，t 值继续向 1.960 收敛（95% 双侧）
        1.960
    }
}

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

const INIT_RTT_STARTUP: time::Duration = time::Duration::from_secs(50);

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
    Pieces2(PeerAddr, Option<ReceivedBlocks>),
    Piece {
        addr: PeerAddr,
        piece: Piece,
        buf: BlockBuf,
        recv_time: std::time::Instant,
    },
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
    },
    /// Peer is stalled waiting for a block buffer from the pool; mark app_limited.
    BufferWaiting {
        peer: PeerAddr,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum PeerFrom {
    DHT,
    Tracker,
    PEX,
}

/// (conn, is_income)
pub type NewPeerConn = (protocol::BTStream<Box<dyn Conn>>, bool);

#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum Msg {
    AnnounceFinish(Result<metadata::AnnounceResp, metadata::AnnounceError>),
    AnnounceMsg(announce_manager::Msg),

    NewPeer(Result<NewPeerConn, SocketAddr>),
    NewDiscoveredPeer {
        addr: SocketAddr,
        from: PeerFrom,
    },
    PeerLeave(PeerAddr),

    PieceBufReady {
        index: JointIndex,
        buf: io::Result<PieceLease>,
    },

    /// A message received from peer
    PeerMsg(PeerMsg),
    FlushError(FlushErr),

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

struct PeerConn {
    conn: ConnectionManagerHandle,
    /// PEX state: peers we have already advertised to this peer.
    pex_known_peers: HashMap<SocketAddr, Option<PexFlag>>,
    state: PeerStatus,
    bitmap: Option<PieceState>,
    bw: Bandwidth<50>,
    min_rtt: time::Duration,
    since_min_rtt: time::Instant,
    inflight: Inflight,
    bw_mode: BandwidthMode,

    app_limited: bool,
    // TODO: optimize: use integer type
    max_bw_unlimited: f32, // the max bandwidth when not app-limited
}

impl PeerConn {
    /// returns max_bw, min_rtt_in_period, min_rtt_in_period_at
    fn get_max_bw(
        &self,
        look_back_duration: time::Duration,
    ) -> (f32, time::Duration, time::Instant) {
        if self.app_limited {
            let (_, min_rtt_in_period, min_rtt_in_period_at) =
                self.bw.count_max_bw_and_min_rtt(look_back_duration);
            (
                self.max_bw_unlimited,
                min_rtt_in_period,
                min_rtt_in_period_at,
            )
        } else {
            self.bw.count_max_bw_and_min_rtt(look_back_duration)
        }
    }
}

fn compute_probe_bdp_rtt(min_rtt: time::Duration) -> time::Duration {
    (min_rtt * 3 / 2).min(min_rtt + time::Duration::from_millis(50))
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
        port: u16,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
        dht_client: Option<Arc<DHT>>,
        announce_manager: AnnounceManagerHandle,
        cache_handle: CacheManagerHandle,
    ) -> Self {
        let worker = TransmitWorker::new(
            t,
            id,
            port,
            dht_client,
            announce_manager,
            cmd_sender,
            cmd_receiver,
            cache_handle,
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
        to_check: BTreeSet<u32>,
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
    pub cache_handle: CacheManagerHandle,
    pub hasher: HashMap<u32, HashState<Sha1>>,
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
    waiting_for_piecebuf: HashMap<JointIndex, PieceWaitState>,

    downloaded: watch::Sender<bool>,

    /// Handle to the global CacheManager actor.
    cache_handle: CacheManagerHandle,
}

// impl std::fmt::Debug for TransmitWorker {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         use crate::helper::to_hex;
//         f.debug_struct("TransmitWorker")
//             .field("id", &to_hex(&self.id))
//             .field("info_hash", &to_hex(&self.info_hash))
//             // .field("handshake_opt", &self.handshake_opt)
//             // .field("dht_client", &self.dht_client)
//             // .field("announce_manager", &self.announce_manager)
//             // .field("torrent_state", &self.torrent_state)
//             // .field("running_state", &self.running_state)
//             // .field("receiver", &self.receiver)
//             // .field("self_handle", &self.self_handle)
//             // .field("connected_peers", &self.connected_peers)
//             // .field("connecting_peers", &self.connecting_peers)
//             // .field("waiting_for_piecebuf", &self.waiting_for_piecebuf)
//             // .field("downloaded", &self.downloaded)
//             .finish()
//     }
// }

struct BlockWaitingBuf {
    piece: Piece,
    buf: BlockBuf,
}

struct PieceWaitState {
    blocks: Vec<BlockWaitingBuf>,
    /// Whether GetPiece has already been sent to CacheManager for this sub-piece.
    requested: bool,
    /// Whether the sub-piece is complete (all blocks received).
    sub_piece_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransmitDump {
    pub state: TorrentStateDump,
    pub peers: Vec<SocketAddr>,
}

fn bw_look_back_window(probe_bdp_rtt: time::Duration) -> time::Duration {
    (10 * probe_bdp_rtt).max(time::Duration::from_millis(1500))
}

impl TransmitWorker {
    const DHT_TIMEOUT: time::Duration = time::Duration::from_secs(3);

    fn broadcast_have(&self, index: u32) {
        for (_, h) in self.connected_peers.iter() {
            h.conn.send_stream_cmd(ConnMsg::Have(index));
        }
    }

    pub fn new(
        t: TorrentTask,
        id: [u8; 20],
        port: u16,
        dht_client: Option<Arc<DHT>>,
        announce_manager: AnnounceManagerHandle,
        cmd_sender: mpsc::UnboundedSender<Msg>,
        cmd_receiver: mpsc::UnboundedReceiver<Msg>,
        cache_handle: CacheManagerHandle,
    ) -> Self {
        let (info_hash, state) = match t {
            TorrentTask::Torrent(m) => {
                let info_hash = m.info_hash;
                let state = TorrentState::Metadata(Self::metadata_into_downloading(
                    m,
                    &cache_handle,
                    cmd_sender.clone(),
                ));
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
            .port(port)
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
            cache_handle,
        }
    }

    fn metadata_into_downloading(
        m: Metadata,
        cache_handle: &CacheManagerHandle,
        error_sender: mpsc::UnboundedSender<Msg>,
    ) -> Downloading {
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

        let back_file = Arc::new(std::sync::Mutex::new(
            // TODO: maybe only send metadata to cache manager, and let cache manager create back file when needed?
            BackFile::new::<NormalFile>().metadata(m.clone()).build(),
        ));
        cache_handle.register_torrent(
            m.info_hash,
            m.regular_piece_size(),
            total_length,
            back_file,
            error_sender,
        );

        Downloading {
            metadata: m,
            block_picker,
            cache_handle: cache_handle.clone(),
            hasher: HashMap::new(),
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
        let Downloading { block_picker, .. } = match &mut self.torrent_state {
            TorrentState::Metadata(d) => d,
            TorrentState::Fetching(_) => {
                return;
            }
        };

        let mut revoked = HashMap::new();
        for (addr, h) in &mut self.connected_peers {
            info!("peer status {addr}: {:?}", h.state);
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let in_flight = h.inflight.inflight();
                let probe_bdp_rtt = compute_probe_bdp_rtt(h.min_rtt);
                let look_back_duration = bw_look_back_window(probe_bdp_rtt);
                let avg_bw = h.bw.count_avg_bw_in(look_back_duration);
                let (reqs, pick_n) = block_picker.pick_blocks(
                    addr,
                    n_blocks,
                    in_flight,
                    avg_bw,
                    h.min_rtt,
                    &mut revoked,
                    self.cache_handle.vacant_count(),
                );
                for rg in reqs.range.iter() {
                    for req in rg.iter(reqs.piece_size) {
                        h.inflight.request(req);
                    }
                }

                h.conn.send_stream_cmd(ConnMsg::RequestBlocks(reqs));
            }
        }

        for (peer, reqs) in revoked {
            for req in reqs {
                if let Some(h) = self.connected_peers.get_mut(&peer) {
                    h.inflight.timeout(req);
                }
            }
        }
    }

    /// pick blocks for a peer, return the number of blocks picked
    fn pick_blocks_for_peer(&mut self, addr: &SocketAddr, pick_n: usize) -> usize {
        if pick_n > 0 {
            warn!("pick {pick_n} blocks from {addr:?}");
        }
        let Downloading { block_picker, .. } = match &mut self.torrent_state {
            TorrentState::Metadata(d) => d,
            TorrentState::Fetching(_) => {
                return 0;
            }
        };

        let mut revoked = HashMap::new();
        let picked_n = if let Some(h) = self.connected_peers.get_mut(addr) {
            if h.state.peer_choke_status == ChokeStatus::Unchoked {
                let n_in_flight = h.inflight.inflight();
                let probe_bdp_rtt = compute_probe_bdp_rtt(h.min_rtt);
                let look_back_duration = bw_look_back_window(probe_bdp_rtt);
                let avg_bw = h.bw.count_avg_bw_in(look_back_duration);
                let (reqs, picked_n) = block_picker.pick_blocks(
                    addr,
                    pick_n,
                    n_in_flight,
                    avg_bw,
                    h.min_rtt,
                    &mut revoked,
                    self.cache_handle.vacant_count(),
                );
                for rg in reqs.range.iter() {
                    for req in rg.iter(reqs.piece_size) {
                        h.inflight.request(req);
                        debug!("{addr} pick block request {req:?}");
                    }
                }

                h.conn.send_stream_cmd(ConnMsg::RequestBlocks(reqs));

                if pick_n > 0 {
                    trace!("really picked {picked_n} blocks");
                    if picked_n < pick_n {
                        h.max_bw_unlimited = h.max_bw_unlimited.max({
                            // TODO: FIXME: optimize
                            let probe_bdp_rtt = compute_probe_bdp_rtt(h.min_rtt);
                            let look_back_duration = bw_look_back_window(probe_bdp_rtt);
                            let (max_bw, _, _) = h.get_max_bw(look_back_duration);
                            max_bw
                        });
                        if !h.app_limited {
                            debug!(
                                "{addr} only picked {picked_n} blocks from {addr:?} app limited"
                            );
                        }
                        h.app_limited = true;
                    } else if matches!(
                        h.bw_mode,
                        BandwidthMode::ProbeBW { .. } | BandwidthMode::Startup { .. }
                    ) {
                        // in slowdown mode or probeRTT mode, pipe is easily full, only reset app_limited if
                        // if we are in startup|probeBW mode
                        if h.app_limited {
                            debug!("{addr} exit app limited");
                        }
                        h.app_limited = false;
                    }

                    match h.bw_mode {
                        BandwidthMode::ProbeBW {
                            ref mut capacity, ..
                        } => {
                            *capacity = capacity.saturating_sub(picked_n);
                        }
                        _ => {}
                    }
                }
                picked_n
            } else {
                0
            }
        } else {
            0
        };

        for (peer, reqs) in revoked {
            for req in reqs {
                debug!("{peer} revoke block request {req:?}");
                if let Some(h) = self.connected_peers.get_mut(&peer) {
                    h.inflight.timeout(req);
                }
            }
        }

        picked_n
    }

    fn handle_msg(&mut self, m: Msg) -> io::Result<()> {
        match m {
            Msg::NewDiscoveredPeer { addr, from } => {
                info!("new discovered peer {addr} from {from:?}");
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
                info!("announce finish, get peers {:?}", a.peers);
                for p in a.peers {
                    use std::str::FromStr;
                    if let Ok(ip) = std::net::IpAddr::from_str(&p.ip) {
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
                    let state = match &mut self.torrent_state {
                        TorrentState::Metadata(d) => {
                            Some((d.block_picker.our_state(), d.block_picker.n_pieces()))
                        }
                        TorrentState::Fetching(_) => None,
                    };

                    if let Some((piece_state, n)) = state {
                        if cm.capability().have(protocol::Capability::Fast) {
                            match piece_state {
                                PieceState::HaveAll => {
                                    cm.send_stream_cmd(CtrlOfSend::HaveAll);
                                }
                                PieceState::HaveNone => {
                                    cm.send_stream_cmd(CtrlOfSend::HaveNone);
                                }
                                PieceState::Bitfield(bit_field) => {
                                    cm.send_stream_cmd(CtrlOfSend::BitField(bit_field));
                                }
                            }
                        } else {
                            cm.send_stream_cmd(CtrlOfSend::BitField(piece_state.as_bitfield(n)));
                        }
                    } else {
                        if cm.capability().have(protocol::Capability::Fast) {
                            cm.send_stream_cmd(CtrlOfSend::HaveNone);
                        }
                    }

                    if cm.capability().have(protocol::Capability::DHT) {
                        if let Some(port) = self.handshake_opt.dht_port {
                            cm.send_stream_cmd(CtrlOfSend::DHTPort(port));
                        }
                    }

                    cm.send_stream_cmd(CtrlOfSend::Interested);
                    self.connected_peers.insert(
                        peer_addr,
                        PeerConn {
                            conn: cm,
                            pex_known_peers: HashMap::new(),
                            state: PeerStatus {
                                our_choke_status: ChokeStatus::Unknown,
                                our_interest_status: InterestStatus::Unknown,
                                peer_choke_status: ChokeStatus::Unknown,
                                peer_interest_status: InterestStatus::Unknown,
                            },
                            bitmap: None,
                            bw: Bandwidth::new(),
                            min_rtt: INIT_RTT_STARTUP,
                            since_min_rtt: time::Instant::now(),
                            bw_mode: BandwidthMode::new_choked(),
                            inflight: Inflight::new(time::Duration::from_secs(90)),

                            app_limited: false,
                            max_bw_unlimited: 0.0,
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
                }
                self.connecting_peers.remove(&peer_addr);
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
                // TODO: FIXME
                // self.remove_unreachable_pieces_from_buf();
                Ok(())
            }
            Msg::PeerMsg(pm) => self.handle_peer_msg(pm),
            Msg::FlushError(_) => {
                todo!()
            }
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
                    st.bw_mode = BandwidthMode::Choked;
                });
                assert_eq!(
                    self.connected_peers[&peer].state.peer_choke_status,
                    ChokeStatus::Choked
                );
                // TODO: record the ?stable transmit rate/ i.e. how many packets is in flight
                // so we can recover to max speed (hopefully) once they unchoked us
                // TODO: shall we remove immediately, or wait for a while in case peer
                // unchokes us again soon?
                // TODO: NOTE: we did not category choke as unreachable peer
                // self.remove_unreachable_pieces_from_buf();
                Ok(())
            }
            PeerMsg::Unchoke(peer) => {
                let n_first_pick = 3;
                warn!("{peer} unchoked us");
                self.connected_peers.entry(peer).and_modify(|st| {
                    st.state.peer_choke_status = ChokeStatus::Unchoked;
                    st.min_rtt = INIT_RTT_STARTUP;
                    st.since_min_rtt = time::Instant::now();
                    st.app_limited = false;
                    st.bw_mode = BandwidthMode::new_auto();
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
            PeerMsg::BlockReceived { peer } => {
                // TODO: FIXME: add backlog:
                // if waiting_for_piece has too many pending pieces, slow down picking
                // and mark app_limited
                if let Some(pc) = self.connected_peers.get_mut(&peer) {
                    pc.conn.recv_stream_cmd(CtrlOfRecv::GetBlocks);
                }
                Ok(())
            }
            PeerMsg::BufferWaiting { peer } => {
                if let Some(h) = self.connected_peers.get_mut(&to_canonical_addr(peer)) {
                    if !h.app_limited {
                        info!("peer {peer} waiting for block buffer — marking app_limited");
                    }
                    h.max_bw_unlimited = h.max_bw_unlimited.max({
                        // TODO: FIXME: optimize
                        let probe_bdp_rtt = compute_probe_bdp_rtt(h.min_rtt);
                        let look_back_duration = bw_look_back_window(probe_bdp_rtt);
                        let (max_bw, _, _) = h.get_max_bw(look_back_duration);
                        max_bw
                    });
                    h.app_limited = true;
                }
                Ok(())
            }
            PeerMsg::Piece {
                addr,
                piece,
                buf,
                recv_time,
            } => self.handle_piece_msg(&addr, piece, buf, recv_time),
            PeerMsg::Pieces2(peer, receive_blks) => {
                let mut count = 0;
                if let Some(blks) = receive_blks {
                    for piece in blks.blocks {
                        count += 1;
                        self.handle_piece_msg(&peer, piece.piece, piece.buf, piece.recv_time)?;
                    }
                }
                debug!("handled {count} piece messages");
                self.handle_blocks_receieved(peer)
            }
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
                    if conn.conn.capability().have(protocol::Capability::Fast) {
                        conn.conn.send_stream_cmd(ConnMsg::Reject(req));
                    }
                }
                Ok(())
            }
        }
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

    fn verify_piece(index: usize, metadata: &Metadata, hasher: HashState<Sha1>) -> bool {
        let target = &metadata.info.pieces[index * 20..index * 20 + 20];
        let res: [u8; 20] = hasher.finalize().finalize().into();
        res == target
    }

    /// Hash a single sub-piece into the piece hasher.
    /// Returns true if all sub-pieces for this piece have now been hashed.
    fn advance_hash(
        &mut self,
        ji: JointIndex,
        piecebuf: &mut PieceLease,
        force_check: bool,
    ) -> io::Result<bool> {
        let index = ji.index();
        let Downloading {
            block_picker,
            hasher: piece_hasher,
            ..
        } = match &mut self.torrent_state {
            TorrentState::Metadata(d) => d,
            TorrentState::Fetching(_) => panic!("should not receive piece before metadata"),
        };
        let piece_size = block_picker.piece_size(index);

        let hasher = piece_hasher
            .entry(index)
            .or_insert_with(|| HashState::new(Sha1::new()));

        let req = Request {
            index,
            begin: hasher.next_offset() as u32,
            len: 0,
        };
        let next_to_hash = JointIndex::from(req);
        if ji == next_to_hash && (block_picker.have_sub(ji) || force_check) {
            info!("write to hasher: {ji:?}");
            hasher.write(&piecebuf)?;
            piecebuf.flush(|_| {});
        }

        let total_sub_pieces =
            (piece_size as usize + SUB_PIECE_SIZE as usize - 1) / SUB_PIECE_SIZE as usize;
        let next_sub_piece =
            (hasher.next_offset() + SUB_PIECE_SIZE as usize - 1) / SUB_PIECE_SIZE as usize;
        if next_sub_piece < total_sub_pieces {
            let next_ji = {
                let req = Request {
                    index,
                    begin: hasher.next_offset() as u32,
                    len: 0,
                };
                JointIndex::from(req)
            };
            if block_picker.have_sub(next_ji) || force_check {
                let key = GlobalPieceKey {
                    info_hash: self.info_hash,
                    index: next_ji,
                };
                self.cache_handle
                    .send_get_piece(key, self.self_handle.sender.clone());
            }
        }
        info!("advance hash of piece {index} from sub piece {next_sub_piece} / {total_sub_pieces}");
        Ok(next_sub_piece >= total_sub_pieces)
    }

    /// called when a full piece received
    /// return verify result of the full piece
    /// index: piece index
    /// piecebuf: the piece buffer, should be exactly the size of the sub-piece
    /// force_check: load pieces regardless of state of piece picker
    /// used in file recheck, where piece picker may not be accurate, piece picker
    fn handle_sub_piece_received(
        &mut self,
        ji: JointIndex,
        piecebuf: &mut PieceLease,
        force_check: bool,
    ) -> io::Result<Option<bool>> {
        info!("sub piece of {:?} received, hashing...", ji);
        let full_hashed = self.advance_hash(ji, piecebuf, force_check)?;

        if !full_hashed {
            return Ok(None);
        }

        let Downloading {
            metadata,
            block_picker,
            hasher: piece_hasher,
            ..
        } = match self.torrent_state {
            TorrentState::Metadata(ref mut d) => d,
            TorrentState::Fetching(_) => panic!("should not receive piece before metadata"),
        };
        let hasher = piece_hasher.remove(&ji.index()).unwrap();
        if Self::verify_piece(ji.index() as usize, metadata, hasher) {
            info!("piece {} verify pass", ji.index());
            block_picker.piece_verified(ji.index() as u32, true);
            Ok(Some(true))
        } else {
            info!("piece {} verify failed", ji.index());
            block_picker.piece_verified(ji.index() as u32, false);
            Ok(Some(false))
        }
    }

    /// called when checking file and a piece is checked
    fn handle_checkfile_on_piece_verified(&mut self, index: u32, passed: bool) -> io::Result<()> {
        info!(
            "piece {index} check {}",
            if passed { "passed" } else { "failed" }
        );
        match &mut self.running_state {
            RunningState::Checking {
                prev_state,
                to_check,
                checked,
                waiter,
            } => {
                let mut notify_waiter = |r: bool| {
                    for w in waiter.drain(0..) {
                        w.send(r);
                    }
                };
                checked.check(index as usize, passed);
                to_check.remove(&index);
                if to_check.is_empty() {
                    let r = checked.state.iter().all(|s| *s != CheckState::CORRUPT);
                    notify_waiter(r);
                    match prev_state {
                        RunningCmd::Resume => {
                            let Downloading { block_picker, .. } = match &mut self.torrent_state {
                                TorrentState::Metadata(d) => d,
                                TorrentState::Fetching(_) => {
                                    unreachable!();
                                }
                            };
                            if block_picker.is_finished() {
                                self.running_state = RunningState::Seeding;
                            } else {
                                self.running_state = RunningState::Downloading;
                            }
                        }
                        RunningCmd::Pause => {
                            self.running_state = RunningState::Paused;
                        }
                        RunningCmd::Stop => {
                            self.running_state = RunningState::Stopped;
                        }
                    }
                } else {
                    // load next piece to check
                    let next_piece = to_check.pop_first().unwrap();
                    self.cache_handle.send_get_piece(
                        GlobalPieceKey {
                            info_hash: self.info_hash,
                            index: JointIndex::new(next_piece, 0),
                        },
                        self.self_handle.sender.clone(),
                    );
                }
                Ok(())
            }
            _ => unreachable!("called from not checking state"),
        }
    }

    // fn remove_unreachable_pieces_from_buf(&mut self) {
    //     let state = match &mut self.torrent_state {
    //         TorrentState::Metadata(d) => d,
    //         TorrentState::Fetching(_) => return,
    //     };
    //     // state.block_picker.piece_availability(index)
    //     let n_pieces = state.block_picker.n_pieces();
    //     for index in 0..n_pieces {
    //         if state.block_picker.piece_availability(index as u32) == 0 {
    //             state.storage.remove_piece(index);
    //         }
    //     }
    // }

    #[instrument(skip_all)]
    fn handle_blocks_receieved(&mut self, peer: PeerAddr) -> io::Result<()> {
        let peer = to_canonical_addr(peer);
        info!("get BlockReceived from {peer}");
        // TODO: OPTIMIZE: return connection handle to reduce map search
        if !self.connected_peers.contains_key(&peer) {
            use tracing::error;
            error!(
                "block received from unknown peer {peer} connected peers: {:?}",
                self.connected_peers.keys()
            );
        }
        let conn = self.connected_peers.get_mut(&peer).expect("should exist");
        match &self.torrent_state {
            TorrentState::Fetching(_) => {
                return Ok(());
            }
            TorrentState::Metadata(_) => {}
        }

        // For Probe mode BDP estimation, use a conservative RTT baseline under jitter:
        let probe_bdp_rtt = compute_probe_bdp_rtt(conn.min_rtt);

        let look_back_duration = bw_look_back_window(probe_bdp_rtt);
        let (max_bw, min_rtt_in_period, min_rtt_in_period_at) = conn.get_max_bw(look_back_duration);
        let probe_rtt_interval = (probe_bdp_rtt * 10).max(time::Duration::from_secs(6));

        const TEN_SECS: time::Duration = time::Duration::from_secs(10);
        let bytes_10sec = conn.bw.count_bytes_within_period(TEN_SECS).0;
        let avg_bw_10s = conn.bw.count_avg_bw_in(TEN_SECS);
        let avg_bw = conn.bw.count_avg_bw_in(look_back_duration);

        let rtt = conn.bw.get_rtt().max(time::Duration::from_millis(10));
        let likely_respond_within = conn
            .bw
            .get_rtt_4var()
            .max(time::Duration::from_millis(1500))
            .min(time::Duration::from_secs(6));
        let likely_recv_next_within = if bytes_10sec > 0 && avg_bw > 0.0 {
            time::Duration::from_secs_f32((7.0 * 16384.0 / avg_bw).min(6.0))
                .max(time::Duration::from_millis(1500))
        } else {
            time::Duration::from_secs(6)
        };
        info!(
            "{peer} rtt {rtt:?} var {:?}, probably respond {:?}, interval within {:?}",
            conn.bw.get_var(),
            likely_respond_within,
            likely_recv_next_within
        );
        info!("peer {peer} estimated max bandwidth {max_bw}, min rtt {:?}, min rtt in period {:?} req in flight: {}", conn.min_rtt, min_rtt_in_period, conn.inflight.inflight());

        let n_req_in_flight = conn.inflight.inflight();

        match conn.bw_mode {
            BandwidthMode::Startup {
                ref mut cwnd,
                ref mut max_bw,
                ref mut cwnd_since,
                ref mut limit_count,
            } => {
                const NO_MORE_GAIN_LIMIT: u32 = 2;
                let cwnd_probe_interval =
                    (probe_bdp_rtt * 10).max(time::Duration::from_millis(1500));
                let cwnd_duration = cwnd_since.elapsed().max(time::Duration::from_millis(1500));
                if cwnd_since.elapsed() > cwnd_probe_interval {
                    // TODO: call count_max_bw_and_min_rtt or get_max_bw?
                    // get_max_bw auto deals with app_limited
                    let (max_bw_in_rtt, _, _) = conn.bw.count_max_bw_and_min_rtt(cwnd_duration);
                    info!(
                        "{peer} in Startup mode, cwnd {}, inflight {} prev max bw {}, avg-bw {} avg_bw_10s {} new max bw {}, limit count {} app limited {}",
                        *cwnd, n_req_in_flight, *max_bw, avg_bw, avg_bw_10s, max_bw_in_rtt, *limit_count, conn.app_limited,
                    );
                    info!("{cwnd_probe_interval:?}, {:?}", cwnd_since.elapsed());
                    *cwnd_since = time::Instant::now();

                    if !conn.app_limited {
                        if max_bw_in_rtt <= *max_bw * 1.2 {
                            *limit_count += 1;
                            info!(
                            "{peer} Startup stagnation {}, elapsed {:?} max bw in rtt {}, current max bw {}",
                            *limit_count,
                            cwnd_since.elapsed(),
                            max_bw_in_rtt,
                            max_bw,
                        );
                        } else {
                            *limit_count = 0;
                        }

                        *max_bw = (*max_bw).max(max_bw_in_rtt);
                        let bdp =
                            (conn.min_rtt.as_secs_f32() * (*max_bw).max(0.0) / 16384.0) as usize;
                        let startup_cwnd_cap = 4usize.max(bdp.saturating_mul(3));
                        *cwnd = (*cwnd * 2).min(startup_cwnd_cap);

                        if *limit_count >= NO_MORE_GAIN_LIMIT {
                            let optimum = (*max_bw * conn.min_rtt.as_secs_f32() / 16384.0) as usize;
                            info!(
                            "{peer} change from Startup to Slowdown mode, {} {:?} slow down to {optimum}",
                            *max_bw, conn.min_rtt,
                        );
                            conn.bw_mode = BandwidthMode::SlowDown {
                                last_piece_time: time::Instant::now(),
                                inflight_target: optimum,
                            }
                        }
                    }
                }
            }
            BandwidthMode::ProbeBW {
                ref mut since_cycle,
                ref mut last_piece_time,
                slow_down_to,
                ref mut slow_count,
                ref mut probe_df,
                ref mut cycle_index,
                ref mut capacity,
            } => {
                let last_recv_time = *last_piece_time;
                *last_piece_time = time::Instant::now();

                if since_cycle.elapsed() > probe_bdp_rtt {
                    if *capacity > 0 {
                        info!("{peer} ProbeBW capacity remains {capacity}");
                    }

                    *cycle_index = (*cycle_index + 1) & 0x7;
                    *since_cycle = time::Instant::now();

                    *capacity = BandwidthMode::compute_probe_bw_capacity(
                        probe_bdp_rtt,
                        max_bw,
                        *cycle_index,
                        6, // 1.5
                    );
                    info!(
                        "{peer} ProbeBW cycle update {}, probe df {}, max bw {}, avg bw {}, avg_bw_10s {}, min rtt {:?}, probe rtt {:?} capacity {}",
                        *cycle_index, *probe_df, max_bw, avg_bw, avg_bw_10s, conn.min_rtt, probe_bdp_rtt, *capacity,
                    );
                }

                const SLOW_LIMIT: u32 = 2;
                if conn.since_min_rtt.elapsed() > probe_rtt_interval {
                    info!(
                        "{peer} change from ProbeBW to SlowDown mode because no smaller rtt after {:?}",
                        probe_rtt_interval,
                    );
                    conn.bw_mode = BandwidthMode::SlowDown {
                        last_piece_time: *last_piece_time,
                        inflight_target: 0,
                    }
                    // } else if *slow_count >= SLOW_LIMIT {
                    //     info!("{peer} change from ProbeBW to Slowdown mode because {SLOW_LIMIT} times of slow rtt");
                    //     conn.bw_mode = BandwidthMode::SlowDown {
                    //         since_min_rtt,
                    //         last_piece_time: *last_piece_time,
                    //         min_rtt,
                    //         inflight_target: n_req_in_flight / 2,
                    //     };
                    // } else if last_piece_time.elapsed() > likely_recv_next_within {
                    //     // either it's a small probability event
                    //     // or the peer is slowing the send rate
                    //     info!(
                    //         "{peer} change from ProbeBW to Slowdown mode because no packet in {:?}",
                    //         likely_recv_next_within,
                    //     );
                    //     conn.bw_mode = BandwidthMode::SlowDown {
                    //         since_min_rtt,
                    //         last_piece_time: *last_piece_time,
                    //         min_rtt,
                    //         inflight_target: 0,
                    //     };
                }
            }
            BandwidthMode::SlowDown {
                last_piece_time,
                inflight_target,
            } => {
                if n_req_in_flight <= inflight_target {
                    if inflight_target > 0 {
                        let (max_bw, _, _) = conn.get_max_bw(look_back_duration);
                        let cap =
                            BandwidthMode::compute_probe_bw_capacity(probe_bdp_rtt, max_bw, 0, 4);
                        info!("{peer} change from Slowdown to ProbeBW mode, because inflight target {inflight_target} > 0");
                        conn.bw_mode = BandwidthMode::ProbeBW {
                            slow_count: 0,
                            probe_df: 1,
                            slow_down_to: 0,
                            last_piece_time,
                            cycle_index: 0,
                            since_cycle: time::Instant::now(),
                            capacity: cap,
                        };
                    } else {
                        info!("{peer} change from Slowdown to ProbeRTT mode");
                        conn.bw.reset_var();
                        conn.bw_mode = BandwidthMode::ProbeRTT {
                            since: time::Instant::now(),
                            cnt: 0,
                            last_piece_time,
                            inflight_target: 4,
                            from_clear: inflight_target == 0,
                            normal_count: 0,
                            slow_count: 0,
                        };
                    }
                }
            }
            BandwidthMode::ProbeRTT {
                since,
                last_piece_time,
                normal_count,
                slow_count,
                from_clear,
                inflight_target,
                cnt,
                ..
            } => {
                info!(
                    "{peer} in ProbeRTT mode cnt {} normal count {} min rtt {:?} avg bw {} avg_bw_10s {} req in flight {}",
                    cnt, normal_count,
                    conn.min_rtt, avg_bw, avg_bw_10s, n_req_in_flight
                );

                if cnt > 4 {
                    conn.min_rtt = min_rtt_in_period;
                    conn.since_min_rtt = min_rtt_in_period_at;
                    let cap = BandwidthMode::compute_probe_bw_capacity(probe_bdp_rtt, max_bw, 0, 4);
                    info!("{peer} change from ProbeRTT to ProbeBW mode");
                    conn.bw_mode = BandwidthMode::ProbeBW {
                        since_cycle: time::Instant::now(),
                        slow_count: 0,
                        probe_df: 1,
                        slow_down_to: 0,
                        last_piece_time,
                        cycle_index: 0,
                        capacity: cap,
                    }
                }
            }
            BandwidthMode::Choked => {}
        };

        let n_to_pick = match conn.bw_mode {
            BandwidthMode::Startup { cwnd, .. } => cwnd.saturating_sub(n_req_in_flight),
            BandwidthMode::ProbeBW {
                cycle_index,
                ref mut capacity,
                ..
            } => {
                // Only can pick more blocks if we received some or no requests in flight.
                // For peers with small bandwidth, we don't request too much from them
                // to avoid mark these blocks as in-flight and not requesting from other peers.
                // preventing accumulating too much partial downloaded pieces.
                const MIN_IN_FLIGHT: usize = 5;
                const MAX_IN_FLIGHT: usize = 500;
                let max_in_flight = if conn.conn.info().reqq_limit > 0 {
                    conn.conn.info().reqq_limit
                } else {
                    MAX_IN_FLIGHT
                };

                // TODO: OPTIMIZE: pre-calculate, do not calculate every time
                let limit = {
                    let bdp = BandwidthMode::compute_probe_bw_capacity(probe_bdp_rtt, max_bw, 3, 4);
                    bdp + bdp / 2
                };
                info!(
                    "{peer} ProbeBW mode max bw {}, avg bw {}, avg_bw_10s {}, limit {}",
                    max_bw, avg_bw, avg_bw_10s, limit
                );

                let n_to_pick = (*capacity)
                    .min(max_in_flight.saturating_sub(n_req_in_flight))
                    .min(limit.saturating_sub(n_req_in_flight))
                    .max(MIN_IN_FLIGHT.saturating_sub(n_req_in_flight));
                info!(
                    "{peer} ProbeBW mode cycle {} capacity {} min rtt {:?} probe rtt {:?} avg bw {} avg_bw_10s {} max_bw {} req in flight {}",
                    cycle_index, *capacity, conn.min_rtt, probe_bdp_rtt, avg_bw, avg_bw_10s, max_bw, n_req_in_flight
                );
                n_to_pick
            }
            BandwidthMode::SlowDown {
                ref mut last_piece_time,
                ref mut inflight_target,
                ..
            } => {
                info!(
                    "{peer} slow down mode {} to recv",
                    n_req_in_flight.saturating_sub(*inflight_target)
                );
                info!(
                    "{peer} Slowdown mode min rtt {:?} avg bw {} avg_bw_10s {} req in flight {}",
                    conn.min_rtt, avg_bw, avg_bw_10s, n_req_in_flight
                );
                0
            }
            BandwidthMode::ProbeRTT {
                inflight_target, ..
            } => {
                info!(
                    "{peer} ProbeRTT mode min rtt {:?} avg bw {} avg_bw_10s {} inflight_target {} req in flight {}",
                    conn.min_rtt, avg_bw, avg_bw_10s, inflight_target, n_req_in_flight
                );
                inflight_target.saturating_sub(n_req_in_flight)
            }
            BandwidthMode::Choked => 0,
        };

        if matches!(self.running_state, RunningState::Downloading) {
            if conn.state.peer_choke_status == ChokeStatus::Unchoked {
                let really_picked = self.pick_blocks_for_peer(&peer, n_to_pick);
            }
        }
        Ok(())
    }

    /// handle PIECE message
    // TODO: fix the return type
    #[instrument(skip(self, buf, recv_time), fields(delay = ?recv_time.elapsed()))]
    fn handle_piece_msg(
        &mut self,
        peer: &SocketAddr,
        piece: protocol::Piece,
        buf: BlockBuf,
        recv_time: std::time::Instant,
    ) -> io::Result<()> {
        let queue_delay = recv_time.elapsed();
        debug!("recv {piece:?} from {peer:?}, queue_delay {queue_delay:?}");

        let req = Request {
            index: piece.index,
            begin: piece.begin,
            len: piece.len,
        };

        if let Some(conn) = self.connected_peers.get_mut(peer) {
            conn.inflight.receive(req);
        }

        let Downloading { block_picker, .. } = match &mut self.torrent_state {
            TorrentState::Metadata(d) => d,
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

        let conn = self
            .connected_peers
            .get_mut(&to_canonical_addr(*peer))
            .expect("should exist");
        let rtt = block_picker.get_rtt(peer, &req, recv_time);
        let inflight_when_sent = block_picker.get_inflight_when_sent(peer, &req);
        conn.bw
            .add_sample(piece.len as usize, rtt, inflight_when_sent);
        trace!(
            "{peer} add bw sample {rtt:?}, inflight {} inflight when sent {inflight_when_sent:?}",
            conn.inflight.inflight()
        );

        // let expected_response_time = block_picker.get_expected_response_time(peer, &req);
        // if let (Some(real_recv_time), Some(expected_recv_time)) = (rtt, expected_response_time) {
        //     let (sign, delta) = if real_recv_time >= expected_recv_time {
        //         ('+', real_recv_time - expected_recv_time)
        //     } else {
        //         ('-', expected_recv_time - real_recv_time)
        //     };
        //     trace!(
        //         "{peer} piece timing {req:?} expected {expected_recv_time:?} real {real_recv_time:?} {sign}{delta:?}",
        //     );
        // }

        if let Some(rtt) = rtt {
            let mean = conn.bw.get_rtt();
            let sigma = conn.bw.get_var();

            if rtt < conn.min_rtt {
                conn.min_rtt = rtt;
                conn.since_min_rtt = time::Instant::now();
                trace!("{peer} new min rtt = {:?}", conn.min_rtt);
            }
        }

        match &mut conn.bw_mode {
            BandwidthMode::Startup { .. } => {
                if let Some(rtt) = rtt {
                    conn.bw.add_rtt(rtt);
                }
            }
            BandwidthMode::ProbeBW {
                last_piece_time,
                slow_count,
                probe_df,
                ..
            } => {
                trace!("{peer} add rtt sample {rtt:?}, inflight when sent {inflight_when_sent:?}");
                *last_piece_time = time::Instant::now();
                if let Some(rtt) = rtt {
                    conn.bw.add_rtt(rtt);
                }
            }
            BandwidthMode::SlowDown {
                last_piece_time, ..
            } => {
                *last_piece_time = time::Instant::now();
            }
            BandwidthMode::ProbeRTT {
                cnt,
                last_piece_time,
                ..
            } => {
                *cnt += 1;
                *last_piece_time = time::Instant::now();
            }
            BandwidthMode::Choked => {}
        };

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

        let (complete, peers_revoked) = block_picker.receive_block(req);
        for addr in peers_revoked {
            if addr != *peer {
                // if this block come from peer we did not request, cancel old request
                // TODO: remove pending requests if not sent
                if let Some(conn) = self.connected_peers.get_mut(&addr) {
                    conn.inflight.cancel(req);
                    if conn.conn.capability().have(protocol::Capability::Fast) {
                        conn.conn.send_stream_cmd(ConnMsg::Cancel(req));
                    }
                }
            }
        }

        let ji = JointIndex::from(piece.to_request());
        {
            let state = self
                .waiting_for_piecebuf
                .entry(ji)
                .or_insert_with(|| PieceWaitState {
                    blocks: Vec::new(),
                    requested: false,
                    sub_piece_complete: false,
                });
            state.blocks.push(BlockWaitingBuf { piece, buf });
            if complete.sub_piece {
                state.sub_piece_complete = true;
            }
            if !state.requested {
                let key = GlobalPieceKey {
                    info_hash: self.info_hash,
                    index: ji,
                };
                self.cache_handle
                    .send_get_piece(key, self.self_handle.sender.clone());
                state.requested = true;
            }
        }
        Ok(())
    }

    fn handle_piecebuf_ready(
        &mut self,
        ji: JointIndex,
        buf: io::Result<PieceLease>,
    ) -> io::Result<()> {
        let Downloading { block_picker, .. } = match &mut self.torrent_state {
            TorrentState::Metadata(d) => d,
            TorrentState::Fetching(_) => {
                unreachable!();
            }
        };
        match buf {
            Ok(mut lease) => {
                let pending = self.waiting_for_piecebuf.remove(&ji);
                let sub_piece_complete = pending
                    .as_ref()
                    .map(|s| s.sub_piece_complete)
                    .unwrap_or(false);
                if let Some(ps) = pending {
                    info!(
                        "piecebuf {ji:?} now ready, flushing {} blocks into it",
                        ps.blocks.len()
                    );
                    for p in ps.blocks {
                        copy_to_piecebuf(&p.piece, &p.buf, &mut lease);
                    }
                }

                // TODO: remove immediate if that piece is no longer available among peers
                // if block_picker.piece_availability(ji.index()) > 0 {
                //     storage.add_piece(buf);
                // } else {
                //     // storage.forget_piece(buf);
                // }
                let is_checking = match &self.running_state {
                    RunningState::Checking { .. } => true,
                    _ => false,
                };
                if is_checking || block_picker.have_sub(ji) {
                    info!("sub piece {ji:?} piece loaded",);
                    match self.handle_sub_piece_received(ji, &mut lease, is_checking)? {
                        Some(passed) => {
                            if is_checking {
                                // TODO: FIXME: if we verified to have a piece we previously not,
                                // we should notify peers, sending them a HAVE
                                // and only send HAVE if we have NOT sent them one before!
                                self.handle_checkfile_on_piece_verified(ji.index(), passed)?;
                                if passed {
                                    self.broadcast_have(ji.index() as u32);
                                }
                            } else if passed {
                                self.broadcast_have(ji.index() as u32);
                            }
                        }
                        None => {}
                    }
                }
                Ok(())
            }
            Err(e) => {
                // TODO: why that's error
                // shall we reload?
                // TODO: what to do about the remaing waiting blocks?
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
        if let Some(h) = self.connected_peers.get_mut(&peer) {
            h.inflight.reject(req);
        }
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
                _ = c.ping_rpc(RpcAddr::no_id(addr), Self::DHT_TIMEOUT).await;
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

    fn handle_check_file(&mut self, sender: oneshot::Sender<bool>) -> io::Result<()> {
        let Downloading { block_picker, .. } = match &mut self.torrent_state {
            TorrentState::Metadata(d) => d,
            TorrentState::Fetching(_) => {
                info!("check file when fetching metadata, maybe unreachable");
                let _ = sender.send(false);
                return Ok(());
            }
        };
        match &mut self.running_state {
            RunningState::Checking { waiter, .. } => {
                waiter.push(sender);
                return Ok(());
            }
            _ => (),
        };

        // arrange for loading pieces not in buffer
        let mut selected = block_picker
            .selected_pieces()
            .iter()
            .enumerate()
            .filter_map(|(i, selected)| selected.then(|| i as u32));
        if let Some(first) = selected.next() {
            info!("piece {first} scheduled for checking",);
            match &mut self.running_state {
                RunningState::Checking { .. } => {
                    unreachable!("should not be checking when handle check file")
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
                    let to_check = selected.collect::<BTreeSet<_>>();
                    let checked = CheckState::new(block_picker.n_pieces());
                    *s = RunningState::Checking {
                        prev_state,
                        checked,
                        to_check,
                        waiter,
                    };
                }
            }
            self.cache_handle.send_get_piece(
                GlobalPieceKey {
                    info_hash: self.info_hash,
                    index: JointIndex::new(first, 0),
                },
                self.self_handle.sender.clone(),
            );
        } else {
            info!("no piece selected for checking, check file complete");
            // TODO: MAYBE FIXME: we did not select any piece, but we return true here
            let _ = sender.send(true);
            return Ok(());
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
                    let reject = ExtendedMsg::Metadata(ExtendedMetadata::Reject { piece });
                    let metadata = m.metadata.raw_info.get();

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
                        let mut downloading = Self::metadata_into_downloading(
                            m,
                            &self.cache_handle,
                            self.self_handle.sender.clone(),
                        );
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
        for (paddr, flags) in pex.added {
            info!("new discovered peer {paddr:?} from pex of {addr:?}");
            self.handle_new_discovered_peer(paddr);
        }
        for (paddr, flags) in pex.added6 {
            info!("new discovered peer {paddr:?} from pex of {addr:?}");
            self.handle_new_discovered_peer(paddr);
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
            if h.conn.capability().have(protocol::Capability::Metadata)
                && h.conn.metadata_size() > 0
            {
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
    use crate::metadata::InfoWithRaw;
    let info: InfoWithRaw = bt_bencode::from_slice(&mbuf.metadata)?;
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
    let mut pex_ticker = tokio::time::interval(time::Duration::from_secs(30));
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
            _ = pex_ticker.tick() => {
                info!("pex ticker tick");
                run_pex(&mut transmit);
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
    info!("transmit manager done");
}

/// copy data to a sub-piece buffer
fn copy_to_piecebuf(piece: &Piece, data: &[u8], piecebuf: &mut [u8]) {
    // `begin` is the byte offset of this block within its sub-piece buffer.
    // SUB_PIECE_SIZE is a multiple of BLOCK_SIZE, so no block ever straddles
    // a sub-piece boundary and `begin + data.len()` is always within bounds.
    let begin = piece.begin as usize % SUB_PIECE_SIZE as usize;
    let end = begin + data.len();
    debug_assert!(
        end <= piecebuf.as_ref().len(),
        "copy_to_piecebuf: end {end} exceeds piecebuf len {} (begin={begin}, piece.len={})",
        piecebuf.as_ref().len(),
        piece.len,
    );
    piecebuf[begin..end].copy_from_slice(data);
}

fn run_dht(transmit: &mut TransmitWorker) {
    if let Some(c) = &transmit.dht_client {
        let cl = c.clone();
        // TODO: why port field is an option
        let listen_port = transmit.handshake_opt.port.unwrap_or(0);
        tokio::spawn(dht_get_peers(
            cl,
            transmit.id,
            transmit.info_hash,
            transmit.handshake_opt.clone(),
            transmit.self_handle.clone(),
            listen_port,
        ));
    }
}

fn run_pex(transmit: &mut TransmitWorker) {
    // Build the current set of advertised addresses once (all connected peers, correct listen port).
    let now_peers: HashMap<SocketAddr, Option<PexFlag>> = transmit
        .connected_peers
        .iter()
        .filter_map(|(addr, conn)| {
            let advertised = if conn.conn.info().is_income {
                conn.conn
                    .info()
                    .peer_listen_port
                    .map(|port| SocketAddr::new(addr.ip(), port))
            } else {
                Some(*addr)
            };
            // TODO: FIXME: set correct Pex flags
            advertised.map(|a| (a, None))
        })
        .collect();

    for (peer_addr, conn) in transmit
        .connected_peers
        .iter_mut()
        .filter(|(_, conn)| conn.conn.capability().have(Capability::Pex))
    {
        let conn_info = conn.conn.info();
        let peer_self = if conn_info.is_income {
            conn_info
                .peer_listen_port
                .map(|port| SocketAddr::new(peer_addr.ip(), port))
                .unwrap_or(*peer_addr)
        } else {
            *peer_addr
        };
        if let Some(pex_msg) = protocol::pex_delta(peer_self, &now_peers, &mut conn.pex_known_peers)
        {
            conn.conn
                .send_stream_cmd(CtrlOfSend::Extend(ExtendedMsg::Pex(pex_msg)));
        }
    }
}

async fn dht_get_peers(
    client: Arc<DHT>,
    self_id: [u8; 20],
    target: [u8; 20],
    handshake_opt: HandshakeOption,
    tmh: TransmitManagerHandle,
    listen_port: u16,
) {
    let result = client.get_peers(target).await;
    for a in result.peers {
        tmh.sender.send(Msg::NewDiscoveredPeer {
            addr: a,
            from: PeerFrom::DHT,
        });
    }
    // Announce ourselves to the closest nodes that issued us a token.
    let announce_timeout = time::Duration::from_secs(5);
    for (node, token) in result
        .closest
        .into_iter()
        .filter_map(|(node, token)| token.map(|t| (node, t)))
    {
        let c = client.clone();
        tokio::spawn(async move {
            c.announce_peer_rpc(
                node.clone(),
                target,
                listen_port,
                false,
                &token,
                announce_timeout,
            )
            .await
        });
    }
}

async fn connect_peer(
    main_tx: TransmitManagerHandle,
    addr: SocketAddr,
    opt: HandshakeOption,
    info_hash: InfoHash,
) -> Result<(), std::io::Error> {
    let do_connect = async || -> io::Result<_> {
        let tcp_stream = TcpStream::connect(addr).await?;
        protocol::BTStream::connect(tcp_stream, opt, info_hash).await
    };

    match do_connect().await {
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

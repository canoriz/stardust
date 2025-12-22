use crate::cache::{AbortErr, AsyncAbortRead, Ref};
use crate::metadata::Metadata;
use bon::Builder;
use bt_bencode::ByteIpAddr;
use bt_bencode::ByteString;

use bytes::BytesMut;
use core::fmt;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, LazyLock};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net;
use tokio::net::tcp;
use tokio::sync::oneshot;
use tracing::{info, warn};

const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);

pub type InfoHash = [u8; 20];

pub trait Reader: AsyncRead + Send + Unpin + 'static {}
pub trait Writer: AsyncWrite + Send + Unpin + 'static {}
impl<T> Reader for T where T: AsyncRead + Send + Unpin + 'static {}
impl<T> Writer for T where T: AsyncWrite + Send + Unpin + 'static {}

pub trait Split {
    type R: Reader;
    type W: Writer;

    /// split connection to two individual read end and write end
    fn split(self) -> (Self::R, Self::W);

    /// the remote address of this connection
    fn remote_addr(&self) -> SocketAddr;

    /// the underlying protocol
    fn protocol() -> &'static str;
}

/// Dynamic connection
/// BTStream can operate with any connection implemented this.
// prepared for utp/tcp/proxy support
pub trait Conn: Send {
    fn split(self: Box<Self>) -> (Box<dyn Reader>, Box<dyn Writer>);
    fn remote_addr(&self) -> SocketAddr;
    fn protocol(&self) -> &'static str;
}

impl fmt::Debug for dyn Conn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("dyn Connection")
            .field("remote_addr", &self.remote_addr())
            .field("protocol", &self.protocol())
            .finish()
    }
}

pub enum AcceptOpt {
    HaveMetadata(Arc<Metadata>),
    NoMetadata,
    Reject,
}

impl<T> Conn for T
where
    T: Split + Send,
{
    fn split(self: Box<Self>) -> (Box<dyn Reader>, Box<dyn Writer>) {
        let (r, w) = Split::split(*self);
        (Box::new(r), Box::new(w))
    }

    fn remote_addr(&self) -> SocketAddr {
        Split::remote_addr(self)
    }

    fn protocol(&self) -> &'static str {
        T::protocol()
    }
}

pub trait Reunite {
    type W;
    type U;
    fn reunite(self, w: Self::W) -> Result<Self::U, ReuniteError>;
}

#[derive(Debug)]
pub struct ReuniteError;
impl fmt::Display for ReuniteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "reunite error, maybe write end and read end from different connection?"
        )
    }
}

impl Split for net::TcpStream {
    type R = tcp::OwnedReadHalf;
    type W = tcp::OwnedWriteHalf;

    fn split(self) -> (Self::R, Self::W) {
        self.into_split()
    }

    fn remote_addr(&self) -> SocketAddr {
        const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
        self.peer_addr().unwrap_or(DEFAULT_ADDR)
    }

    fn protocol() -> &'static str {
        "tcp"
    }
}

impl Reunite for tcp::OwnedReadHalf {
    type U = net::TcpStream;
    type W = tcp::OwnedWriteHalf;

    fn reunite(self, w: Self::W) -> Result<Self::U, ReuniteError> {
        self.reunite(w).map_err(|_| ReuniteError)
    }
}

const EMPTY_PARTIAL_HEADER: PartialHeader = PartialHeader {
    field_len: [0; 4],
    field_ty: 0,
    field1: [0; 4],
    field2: [0; 4],
    field3: [0; 4],
    filled: 0,
    discard_remain: 0,
};
const INITIAL_PARTIAL_READ: PartialRead = PartialRead::Header(EMPTY_PARTIAL_HEADER);

const EXTENSION_NAME_PEX: &str = "ut_pex";
const EXTENSION_NAME_METADATA: &str = "ut_metadata";
const EXTENSION_ID_PEX: u8 = 1;
const EXTENSION_ID_METADATA: u8 = 3;

pub static EXTENSION_IDS_MAP: LazyLock<HashMap<String, u8>> = LazyLock::new(|| {
    HashMap::from([
        (EXTENSION_NAME_METADATA.into(), 3),
        (EXTENSION_NAME_PEX.into(), 1),
    ])
});

fn extension_type(ext_name: &str) -> Option<ExtensionType> {
    match ext_name {
        EXTENSION_NAME_METADATA => Some(ExtensionType::Metadata),
        EXTENSION_NAME_PEX => Some(ExtensionType::Pex),
        _ => {
            // TODO: use leveled logger? span?
            warn!("received unknown extension {ext_name}");
            None
        }
    }
}

#[derive(Hash, Debug, Eq, PartialEq)]
pub enum ExtensionType {
    Metadata,
    Pex,
}

impl ExtensionType {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Metadata => EXTENSION_NAME_METADATA,
            Self::Pex => EXTENSION_NAME_PEX,
        }
    }
}

pub struct BTStream<T> {
    inner: T,
    partial_read: PartialRead,
    extension_id: HashMap<ExtensionType, u8>,

    metadata_size: usize,

    reserved: FuncBits,
    peer_id: [u8; 20],
    info_hash: [u8; 20],
    // TODO: maybe add a torrent hash Arc<>
    // torrent_hash: [u8; 20],

    // what this peer knows about our connected peers
    pex_peers: HashMap<IpAddr, Option<PexFlag>>,

    // buffer for incoming piece
    // this might be transferred to other place for further
    // processing and returns back when done
    piece_buf: Option<BytesMut>,

    // pending PORT message to be sent later by splitted writer
    // not sending it in connect(), because not sure if both ends
    // sends and no one receives.
    pending_dht_port: Option<u16>,
}

impl<T> BTStream<T>
where
    T: Split + Send + 'static,
{
    pub fn peer_addr(&self) -> SocketAddr {
        self.inner.remote_addr()
    }

    pub fn to_dyn(self) -> BTStream<Box<dyn Conn>> {
        BTStream {
            inner: Box::new(self.inner),
            partial_read: self.partial_read,
            extension_id: self.extension_id,
            reserved: self.reserved,
            peer_id: self.peer_id,
            info_hash: self.info_hash,
            pex_peers: self.pex_peers,
            metadata_size: self.metadata_size,
            piece_buf: Some(BytesMut::new()),
            pending_dht_port: self.pending_dht_port,
        }
    }
}

impl BTStream<Box<dyn Conn>> {
    pub fn peer_addr(&self) -> SocketAddr {
        self.inner.remote_addr()
    }
}

impl<T> fmt::Debug for BTStream<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTStream")
            .field("connection", &self.inner)
            .field("extension_id", &self.extension_id)
            .field("peer_id", &self.peer_id)
            .finish()
    }
}

#[derive(Debug)]
pub struct ReadStream<T> {
    inner: T,
    peer_addr: SocketAddr,

    // required to implement Cancel Safe for read_msg
    partial_read: PartialRead,

    metadata_size: usize,

    peer_id: [u8; 20],
    info_hash: [u8; 20],
    reserved: FuncBits,

    // buffer for incoming piece
    piece_buf: Option<BytesMut>,
}

#[derive(Debug)]
enum PartialRead {
    Header(PartialHeader),
    BitField(PartialExtend),
    Extend(PartialExtend),
    Piece(PartialPiece),
    Discard(PartialExtend),
    Processing(oneshot::Receiver<BytesMut>),
}

// store partial received header,
// Cancel Safe for read_msg_header
#[derive(Debug)]
struct PartialHeader {
    field_len: [u8; 4],
    field_ty: u8,
    field1: [u8; 4],
    field2: [u8; 4],
    field3: [u8; 4],
    filled: usize,

    discard_remain: usize,
}

#[derive(Debug)]
struct PartialPiece {
    index: u32,
    begin: u32,
    len: usize,
    remain: usize,
}

#[derive(Debug)]
struct PartialExtend {
    id: u8,
    remain: usize,
    buf: BytesMut,
}

#[derive(Debug)]
pub struct WriteStream<T> {
    inner: T,
    peer_addr: SocketAddr,

    extension_id: HashMap<ExtensionType, u8>,

    metadata_size: usize,

    // what this peer knows about our connected peers
    pex_peers: HashMap<IpAddr, Option<PexFlag>>,

    peer_id: [u8; 20],
    info_hash: [u8; 20],
    reserved: FuncBits,

    pending_dht_port: Option<u16>,
}

impl BTStream<net::TcpStream> {
    // pub async fn connect_tcp(peer_addr: SocketAddr) -> io::Result<BTStream<net::TcpStream>> {
    //     // TODO: fix type of peer_addr
    //     // TODO: add timeout
    //     let tcp_stream = net::TcpStream::connect(peer_addr).await?;
    //     Ok(BTStream::<net::TcpStream> {
    //         // inner: BufStream::new(tcp_stream),
    //         inner: tcp_stream,
    //         partial_header: PartialHeader {
    //             field_len: [0; 4],
    //             field_ty: 0,
    //             field1: [0; 4],
    //             field2: [0; 4],
    //             field3: [0; 4],
    //             filled: 0,
    //             discard_remain: 0,
    //         },
    //         extension_id: HashMap::new(),
    //     })
    // }

    pub fn local_addr(&self) -> SocketAddr {
        // TODO: is this possible to be error?
        // self.inner.get_ref().local_addr().expect("expect ok")
        self.inner.local_addr().expect("expect ok")
    }
}

pub type CapabilityMap = HashSet<Capability>;

#[derive(Eq, Hash, PartialEq)]
pub enum Capability {
    DHT,
    Fast,
    Metadata,
    Pex,
}

pub struct ConnInfo<'a> {
    pub func_bits: &'a FuncBits,
    pub peer_id: &'a [u8; 20],
    pub info_hash: &'a InfoHash,
}

impl<T> BTStream<T> {
    pub fn info(&self) -> ConnInfo {
        ConnInfo {
            func_bits: &self.reserved,
            peer_id: &self.peer_id,
            info_hash: &self.info_hash,
        }
    }

    pub fn capability(&self) -> CapabilityMap {
        let mut ret = CapabilityMap::new();
        if self.reserved.have_dht() {
            ret.insert(Capability::DHT);
        }
        if self.reserved.have_fast() {
            ret.insert(Capability::Fast);
        }
        for id in self.extension_id.keys() {
            match id {
                ExtensionType::Metadata => ret.insert(Capability::Metadata),
                ExtensionType::Pex => ret.insert(Capability::Pex),
            };
        }
        ret
    }

    pub fn metadata_size(&self) -> usize {
        self.metadata_size
    }
}

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Split,
{
    pub fn split(self) -> (ReadStream<<T as Split>::R>, WriteStream<<T as Split>::W>) {
        let peer_addr = self.inner.remote_addr();
        let (read_end, write_end) = self.inner.split();
        (
            ReadStream {
                inner: read_end,
                peer_addr,
                partial_read: self.partial_read,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                piece_buf: self.piece_buf,
            },
            WriteStream {
                inner: write_end,
                peer_addr,
                extension_id: self.extension_id,
                pex_peers: self.pex_peers,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                pending_dht_port: self.pending_dht_port,
            },
        )
    }

    pub fn reunite(
        r: ReadStream<<T as Split>::R>,
        w: WriteStream<<T as Split>::W>,
    ) -> Result<Self, ReuniteError>
    where
        <T as Split>::R: Reunite<W = <T as Split>::W, U = T>,
    {
        if r.peer_id != w.peer_id {
            return Err(ReuniteError);
        }
        Ok(Self {
            inner: r.inner.reunite(w.inner)?,
            partial_read: r.partial_read,
            extension_id: w.extension_id,
            reserved: r.reserved,
            peer_id: r.peer_id,
            info_hash: r.info_hash,
            pex_peers: w.pex_peers,
            metadata_size: r.metadata_size,
            piece_buf: r.piece_buf,
            pending_dht_port: w.pending_dht_port,
        })
    }

    pub fn split_buffered(
        self,
    ) -> (
        ReadStream<BufReader<<T as Split>::R>>,
        WriteStream<BufWriter<<T as Split>::W>>,
    ) {
        let peer_addr = self.inner.remote_addr();
        let (read_end, write_end) = self.inner.split();
        (
            ReadStream {
                inner: BufReader::with_capacity(32768, read_end),
                peer_addr,
                partial_read: self.partial_read,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                piece_buf: self.piece_buf,
            },
            WriteStream {
                inner: BufWriter::with_capacity(32768, write_end),
                peer_addr,
                extension_id: self.extension_id,
                pex_peers: self.pex_peers,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                pending_dht_port: self.pending_dht_port,
            },
        )
    }
}

impl BTStream<Box<dyn Conn>> {
    pub fn split(self) -> (ReadStream<Box<dyn Reader>>, WriteStream<Box<dyn Writer>>) {
        let peer_addr = self.inner.remote_addr();
        let (read_end, write_end) = self.inner.split();
        (
            ReadStream {
                inner: read_end,
                peer_addr,
                partial_read: self.partial_read,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                piece_buf: self.piece_buf,
            },
            WriteStream {
                inner: write_end,
                peer_addr,
                extension_id: self.extension_id,
                pex_peers: self.pex_peers,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                pending_dht_port: self.pending_dht_port,
            },
        )
    }

    pub fn split_buffered(
        self,
    ) -> (
        ReadStream<BufReader<Box<dyn Reader>>>,
        WriteStream<BufWriter<Box<dyn Writer>>>,
    ) {
        let peer_addr = self.inner.remote_addr();
        let (read_end, write_end) = (self.inner).split();
        (
            ReadStream {
                inner: BufReader::with_capacity(32768, read_end),
                peer_addr,
                partial_read: self.partial_read,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                piece_buf: self.piece_buf,
            },
            WriteStream {
                inner: BufWriter::with_capacity(32768, write_end),
                peer_addr,
                extension_id: self.extension_id,
                pex_peers: self.pex_peers,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                pending_dht_port: self.pending_dht_port,
            },
        )
    }
}

pub(crate) struct WriteHandle<'a, T>
where
    T: AsyncWrite + Unpin,
{
    wr: &'a mut WriteStream<T>,
    flushed: bool,
}

impl<T> Drop for WriteHandle<'_, T>
where
    T: AsyncWrite + Unpin,
{
    fn drop(&mut self) {
        if !self.flushed {
            panic!("write handle not flushed but dropped")
        }
    }
}

impl<T> WriteHandle<'_, T>
where
    T: AsyncWrite + Unpin,
{
    async fn flush(&mut self) -> io::Result<()> {
        self.wr.inner.flush().await
    }
}

impl<T> WriteStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    fn write_handle(&mut self) -> WriteHandle<T> {
        WriteHandle {
            wr: self,
            flushed: false,
        }
    }
}

#[derive(Builder, Clone)]
pub struct HandshakeOption {
    #[builder(default = true)]
    pex: bool,
    #[builder(default = true)]
    metadata: bool,
    port: Option<u16>,

    #[builder(required)]
    dht_port: Option<u16>, // dht port
    // fast: bool,       // fast extension
    client_id: [u8; 20],
    client_version: Option<String>, // used in extension
}

impl HandshakeOption {
    fn handshake(self) -> (Handshake, Option<ExtendedHandshake>) {
        let mut func = FuncBits::none();
        let mut map = HashMap::new();
        if self.metadata {
            func = func.set_extension();
            map.insert(EXTENSION_NAME_METADATA.into(), EXTENSION_ID_METADATA);
        }
        if self.pex {
            func = func.set_extension();
            map.insert(EXTENSION_NAME_PEX.into(), EXTENSION_ID_PEX);
        }
        if self.dht_port.is_some() {
            func = func.set_dht();
        }
        func = func.set_fast();
        let exth = ExtendedHandshake {
            m: map,
            p: self.port,
            v: self.client_version,
            yourip: None,
            ipv6: None,
            ipv4: None,
            reqq: None,
            metadata_size: None,
        };
        let h = Handshake {
            reserved: func,
            client_id: self.client_id,
        };
        (
            h,
            if func.have_extension() {
                Some(exth)
            } else {
                None
            },
        )
    }
}

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Split + Unpin,
{
    pub async fn connect(mut t: T, opt: HandshakeOption, info_hash: InfoHash) -> io::Result<Self>
    where
        <T as Split>::R: Reunite<W = <T as Split>::W, U = T>,
    {
        let dht_port = opt.dht_port;
        let (h, eh) = opt.handshake();
        send_handshake(&mut t, &h, &info_hash).await?;
        let (peer_info_hash, peer_handshake) = recv_handshake(&mut t).await?;
        if peer_info_hash != info_hash {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "peer's info_hash differs from ours",
            ));
        }

        let reserved = peer_handshake.reserved.common(&h.reserved);
        // TODO: check peer_handshake's client id
        let s = BTStream {
            inner: t,
            partial_read: INITIAL_PARTIAL_READ,
            extension_id: HashMap::new(),
            peer_id: peer_handshake.client_id,
            info_hash,
            reserved,
            pex_peers: HashMap::new(),
            metadata_size: 0,
            piece_buf: Some(BytesMut::new()),
            pending_dht_port: if reserved.have_dht() { dht_port } else { None },
        };

        if reserved.have_extension() {
            // Maybe over engineering.
            // If both ends are sends handshake before recv handshake,
            // and if both of OS buffer are full,
            // both will wait other end recv data first, and both will blocks
            // Resolve this by recv and send handshake in parallel,
            // and later rejoins two ends.
            let (mut read_end, mut write_end) = s.split();
            let send_ext_handshake = tokio::spawn(async move {
                // this can not fail, this is the only Arc
                send_extension_handshake(
                    &mut write_end.inner,
                    &eh.expect("if negotiates extension, the extension from builder must be Some"),
                )
                .await?;
                io::Result::Ok(write_end)
            });
            let recv_ext_handshake = recv_extend_handshake(
                &mut read_end.inner,
                &mut read_end.partial_read,
                &mut read_end.piece_buf,
            );
            let (write_end, exth) = {
                let (w, exth) = tokio::join!(send_ext_handshake, recv_ext_handshake);
                (w??, exth?)
            };

            let mut s =
                BTStream::<T>::reunite(read_end, write_end).expect("reunite BTStream should OK");
            s.extension_id = exth
                .m
                .iter()
                .filter_map(|(s, id)| extension_type(s).map(|ss| (ss, *id)))
                .filter(|(_, id)| *id != 0)
                .collect();
            s.metadata_size = exth.metadata_size.unwrap_or(0) as usize;
            return Ok(s);
        }
        Ok(s)
    }

    pub async fn accept<F>(mut t: T, accept: F, opt: HandshakeOption) -> io::Result<Self>
    where
        <T as Split>::R: Reunite<W = <T as Split>::W, U = T>,
        F: AsyncFnOnce(&InfoHash) -> AcceptOpt,
    {
        let dht_port = opt.dht_port;
        let (h, mut eh) = opt.handshake();
        let (peer_info_hash, peer_handshake) = recv_handshake(&mut t).await?;

        // TODO: let accept return metadata size and send to peer
        match accept(&peer_info_hash).await {
            AcceptOpt::HaveMetadata(m) => {
                let b = match bt_bencode::to_vec(&m.info) {
                    Ok(b) => b,
                    Err(e) => {
                        // TODO: optimize, don't encode every time
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "handshake metadata bencode failed",
                        ));
                    }
                };
                if let Some(eh) = &mut eh {
                    eh.metadata_size = Some(b.len() as u32);
                }
            }
            AcceptOpt::NoMetadata => {}
            AcceptOpt::Reject => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "handshake rejected by accept function",
                ));
            }
        }

        send_handshake(&mut t, &h, &peer_info_hash).await?;

        let reserved = peer_handshake.reserved.common(&h.reserved);
        let support_dht = reserved.have_dht();
        let s = BTStream {
            inner: t,
            partial_read: INITIAL_PARTIAL_READ,
            extension_id: HashMap::new(),
            peer_id: peer_handshake.client_id,
            info_hash: peer_info_hash,
            reserved,
            pex_peers: HashMap::new(),
            metadata_size: 0,
            piece_buf: Some(BytesMut::new()),
            pending_dht_port: if support_dht { dht_port } else { None },
        };

        let support_extension = reserved.have_extension();
        if support_extension {
            let (mut read_end, mut write_end) = s.split();
            let send_ext_handshake = tokio::spawn(async move {
                // this can not fail, this is the only Arc
                send_extension_handshake(
                    &mut write_end.inner,
                    &eh.expect("if negotiates extension, the extension from builder must be Some"),
                )
                .await?;
                io::Result::Ok(write_end)
            });
            let recv_ext_handshake = recv_extend_handshake(
                &mut read_end.inner,
                &mut read_end.partial_read,
                &mut read_end.piece_buf,
            );
            let (write_end, extend_received) = {
                let (w, exth) = tokio::join!(send_ext_handshake, recv_ext_handshake);
                (w??, exth?)
            };

            let mut s =
                BTStream::<T>::reunite(read_end, write_end).expect("reunite BTStream should OK");
            s.extension_id = extend_received
                .m
                .iter()
                .filter_map(|(s, id)| extension_type(s).map(|ss| (ss, *id)))
                .filter(|(_, id)| *id != 0)
                .collect();
            s.metadata_size = extend_received.metadata_size.unwrap_or(0) as usize;
            return Ok(s);
        }
        Ok(s)
    }

    pub async fn send_keepalive(&mut self) -> io::Result<()> {
        send_keepalive(&mut self.inner).await
    }

    pub async fn send_choke(&mut self) -> io::Result<()> {
        send_choke(&mut self.inner).await
    }

    pub async fn send_unchoke(&mut self) -> io::Result<()> {
        send_unchoke(&mut self.inner).await
    }

    pub async fn send_interested(&mut self) -> io::Result<()> {
        send_interested(&mut self.inner).await
    }

    pub async fn send_notinterested(&mut self) -> io::Result<()> {
        send_notinterested(&mut self.inner).await
    }

    pub async fn send_have(&mut self, index: u32) -> io::Result<()> {
        send_have(&mut self.inner, index).await
    }

    pub async fn send_bitfield(&mut self, b: &BitField) -> io::Result<()> {
        send_bitfield(&mut self.inner, b).await
    }

    pub async fn send_request(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        send_request(&mut self.inner, index, begin, len).await
    }

    pub async fn send_piece(&mut self, index: u32, begin: u32, piece: &[u8]) -> io::Result<()> {
        send_piece(&mut self.inner, index, begin, piece).await
    }

    pub async fn send_cancel(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        send_cancel(&mut self.inner, index, begin, len).await
    }

    pub async fn send_port(&mut self, port: u16) -> io::Result<()> {
        send_port(&mut self.inner, port).await
    }

    pub async fn send_reject(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        send_reject(&mut self.inner, index, begin, len).await
    }

    pub async fn send_allowed_fast(&mut self, index: u32) -> io::Result<()> {
        send_allowed_fast(&mut self.inner, index).await
    }

    pub async fn send_suggest_piece(&mut self, index: u32) -> io::Result<()> {
        send_suggest_piece(&mut self.inner, index).await
    }

    pub async fn send_have_all(&mut self) -> io::Result<()> {
        send_have_all(&mut self.inner).await
    }

    pub async fn send_have_none(&mut self) -> io::Result<()> {
        send_have_none(&mut self.inner).await
    }

    pub async fn send_extend_pex(
        &mut self,
        now_connected: &HashMap<IpAddr, Option<PexFlag>>,
    ) -> io::Result<()> {
        let (added, dropped) = make_added_and_dropped(now_connected, &self.pex_peers);
        match send_extend_pex(
            &mut self.inner,
            added.iter(),
            dropped.iter(),
            &self.extension_id,
        )
        .await
        {
            Ok(_) => {
                update_pex_map(&mut self.pex_peers, &added, &dropped);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn send_extend_metadata(&mut self, meta: ExtendedMetadata) -> io::Result<()> {
        send_extend_metadata(&mut self.inner, meta, &self.extension_id).await
    }
}

fn make_added_and_dropped(
    now_connected: &HashMap<IpAddr, Option<PexFlag>>,
    old_connected: &HashMap<IpAddr, Option<PexFlag>>,
) -> (Vec<(IpAddr, Option<PexFlag>)>, Vec<IpAddr>) {
    // to send added and dropped
    // what we now have, but peer don't know comes to added
    // what we don't have, but peer thinks we have, come to dropped

    const MAX_PEERS: usize = 50; // spec says no more than 50 peer in one message

    let added: Vec<_> = now_connected
        .iter()
        .filter(|(ip, _)| !old_connected.contains_key(ip))
        .take(MAX_PEERS)
        .map(|(ip, pex)| (*ip, *pex))
        .collect();
    let dropped: Vec<_> = old_connected
        .iter()
        .filter(|(ip, _)| !now_connected.contains_key(ip))
        .take(MAX_PEERS)
        .map(|(ip, _)| *ip)
        .collect();
    (added, dropped)
}

fn update_pex_map(
    pex_map: &mut HashMap<IpAddr, Option<PexFlag>>,
    added: &Vec<(IpAddr, Option<PexFlag>)>,
    dropped: &Vec<IpAddr>,
) {
    for (ip, pex) in added {
        pex_map.insert(*ip, *pex);
    }
    for ip in dropped {
        pex_map.remove(ip);
    }
}

// TODO: write returns 0 means EOF, should return error
impl<T> WriteStream<T>
where
    T: AsyncWrite + Unpin,
{
    pub async fn send_keepalive(&mut self) -> io::Result<()> {
        send_keepalive(&mut self.inner).await
    }

    pub async fn send_choke(&mut self) -> io::Result<()> {
        send_choke(&mut self.inner).await
    }

    pub async fn send_unchoke(&mut self) -> io::Result<()> {
        send_unchoke(&mut self.inner).await
    }

    pub async fn send_interested(&mut self) -> io::Result<()> {
        send_interested(&mut self.inner).await
    }

    pub async fn send_notinterested(&mut self) -> io::Result<()> {
        send_notinterested(&mut self.inner).await
    }

    pub async fn send_have(&mut self, index: u32) -> io::Result<()> {
        send_have(&mut self.inner, index).await
    }

    pub async fn send_bitfield(&mut self, b: &BitField) -> io::Result<()> {
        send_bitfield(&mut self.inner, b).await
    }

    pub async fn send_request(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        send_request(&mut self.inner, index, begin, len).await
    }

    pub async fn send_piece(&mut self, index: u32, begin: u32, piece: &[u8]) -> io::Result<()> {
        send_piece(&mut self.inner, index, begin, piece).await
    }

    pub async fn send_cancel(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        send_cancel(&mut self.inner, index, begin, len).await
    }

    pub async fn send_port(&mut self, port: u16) -> io::Result<()> {
        send_port(&mut self.inner, port).await
    }

    pub async fn send_reject(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        send_reject(&mut self.inner, index, begin, len).await
    }

    pub async fn send_allowed_fast(&mut self, index: u32) -> io::Result<()> {
        send_allowed_fast(&mut self.inner, index).await
    }

    pub async fn send_suggest_piece(&mut self, index: u32) -> io::Result<()> {
        send_suggest_piece(&mut self.inner, index).await
    }

    pub async fn send_have_all(&mut self) -> io::Result<()> {
        send_have_all(&mut self.inner).await
    }

    pub async fn send_have_none(&mut self) -> io::Result<()> {
        send_have_none(&mut self.inner).await
    }

    pub async fn send_extend_pex(
        &mut self,
        now_connected: &HashMap<IpAddr, Option<PexFlag>>,
    ) -> io::Result<()> {
        let (added, dropped) = make_added_and_dropped(now_connected, &self.pex_peers);
        match send_extend_pex(
            &mut self.inner,
            added.iter(),
            dropped.iter(),
            &self.extension_id,
        )
        .await
        {
            Ok(_) => {
                update_pex_map(&mut self.pex_peers, &added, &dropped);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn send_extend_metadata(&mut self, meta: ExtendedMetadata) -> io::Result<()> {
        send_extend_metadata(&mut self.inner, meta, &self.extension_id).await
    }

    /// called after split reader and writer
    /// the pending DHT PORT message can now be sent
    pub async fn maybe_send_pending_msg(&mut self) -> io::Result<()> {
        if let Some(port) = self.pending_dht_port.take() {
            send_port(&mut self.inner, port).await?;
        }
        Ok(())
    }
}

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// # Cancel Safety
    /// this is safe
    pub async fn recv_msg(&mut self) -> io::Result<Message> {
        recv_msg(&mut self.inner, &mut self.partial_read, &mut self.piece_buf).await
    }
}

impl<T> ReadStream<T>
where
    T: AsyncRead + Unpin,
{
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// receive one message
    /// # Cancel safety
    /// this is cancel safe
    pub async fn recv_msg(&mut self) -> io::Result<Message> {
        recv_msg(&mut self.inner, &mut self.partial_read, &mut self.piece_buf).await
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FuncBits([u8; 8]);
impl FuncBits {
    pub const fn have_extension(&self) -> bool {
        self.0[5] & 0x10 > 0
    }

    pub const fn set_extension(mut self) -> Self {
        self.0[5] |= 0x10;
        self
    }

    pub const fn have_dht(&self) -> bool {
        self.0[7] & 0x1 > 0
    }

    pub const fn set_dht(mut self) -> Self {
        self.0[7] |= 0x1;
        self
    }

    pub const fn set_fast(mut self) -> Self {
        self.0[7] |= 0x4;
        self
    }

    pub const fn have_fast(&self) -> bool {
        self.0[7] & 0x1 > 0
    }

    pub const fn new(b: [u8; 8]) -> Self {
        Self(b)
    }

    pub const fn none() -> Self {
        Self::new([0; 8])
    }

    pub const fn basic() -> Self {
        Self::none().set_extension().set_dht()
    }

    fn common(mut self, other: &Self) -> Self {
        for i in 0..8 {
            self.0[i] &= other.0[i];
        }
        self
    }
}

impl From<[u8; 8]> for FuncBits {
    fn from(t: [u8; 8]) -> Self {
        Self::new(t)
    }
}

impl Default for FuncBits {
    fn default() -> Self {
        Self::basic()
    }
}

// TODO: make this a builder pattern
#[derive(Debug, Eq, PartialEq)]
pub struct Handshake {
    pub reserved: FuncBits,
    pub client_id: [u8; 20],
}

struct MsgTy {}
impl MsgTy {
    const CHOKE: u8 = 0;
    const UNCHOKE: u8 = 1;
    const INTERESTED: u8 = 2;
    const NOTINTERESTED: u8 = 3;
    const HAVE: u8 = 4;
    const BITFIELD: u8 = 5;
    const REQUEST: u8 = 6;
    const PIECE: u8 = 7;
    const CANCEL: u8 = 8;
    const PORT: u8 = 9;
    const EXTENDED: u8 = 20;
    const HAVE_ALL: u8 = 0x0e;
    const HAVE_NONE: u8 = 0x0f;
    const SUGGEST_PIECE: u8 = 0x0d;
    const REJECT: u8 = 0x10;
    const ALLOWED_FAST: u8 = 0x11;

    const KEEPALIVE_LEN: u32 = 0;
    const CHOKE_LEN: u32 = 1;
    const UNCHOKE_LEN: u32 = 1;
    const INTERESTED_LEN: u32 = 1;
    const NOTINTERESTED_LEN: u32 = 1;
    const HAVE_LEN: u32 = 5;
    // const  BITFIELD_LEN(_) : u32= unimplemented!(), //1 + ((3 + b.len()) >> 2);
    const REQUEST_LEN: u32 = 13;
    // const  PIECE_LEN(_) : u32= unimplemented!();
    const CANCEL_LEN: u32 = 13;
    const PORT_LEN: u32 = 3;
    const HAVE_ALL_LEN: u32 = 1;
    const HAVE_NONE_LEN: u32 = 1;
    const SUGGEST_PIECE_LEN: u32 = 5;
    const REJECT_LEN: u32 = 13;
    const ALLOWED_FAST_LEN: u32 = 5;
}

#[derive(Eq, PartialEq)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    BitField(BitField),
    Request(Request),
    Piece(Piece),
    Cancel(Request),
    Port(u16),
    SuggestPiece(u32),
    AllowedFast(u32),
    HaveAll,
    HaveNone,
    Reject(Request),
    Extended(ExtendedMsg),
}

#[derive(Eq, PartialEq)]
pub enum MessageHeader {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    BitField { capacity: usize },
    Request(Request),
    Piece { index: u32, begin: u32, len: u32 },
    Cancel(Request),
    Port(u16),
    SuggestPiece(u32),
    AllowedFast(u32),
    HaveAll,
    HaveNone,
    Reject(Request),
    Extended { id: u8, len: usize },
    Discard { len: usize },
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::KeepAlive => f.write_str("KeepAlive"),
            Message::Choke => f.write_str("Choke"),
            Message::Unchoke => f.write_str("Unchoke"),
            Message::Interested => f.write_str("Interested"),
            Message::NotInterested => f.write_str("NotInterested"),
            Message::Have(h) => f.write_str(&format!("Have: {}", *h)),
            Message::BitField(bit_field) => f
                .debug_struct("BitField")
                .field("byte length", &bit_field.u8_len())
                .finish(),
            Message::Request(request) => f.debug_struct("Request").field("inner", request).finish(),
            Message::Piece(piece) => f
                .debug_struct("Piece")
                .field("index", &piece.index)
                .field("begin", &piece.begin)
                .field("len", &piece.len)
                .finish(),
            Message::Cancel(request) => f.debug_struct("Cancel").field("inner", request).finish(),
            Message::Port(p) => f.write_str(&format!("Port: {}", *p)),
            Message::Extended(extend) => extend.fmt(f),
            Message::SuggestPiece(p) => f.write_str(&format!("SuggestPiece: {}", *p)),
            Message::AllowedFast(p) => f.write_str(&format!("AllowedFast: {}", *p)),
            Message::HaveAll => f.write_str("HaveAll"),
            Message::HaveNone => f.write_str("HaveNone"),
            Message::Reject(request) => f.debug_struct("Reject").field("inner", request).finish(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct BitField {
    bitfield: Vec<u8>, // use array?

    /// how many (1)s in there
    count: u32,
}

impl BitField {
    const TYPE: u8 = 5;

    pub fn new(bitfield: Vec<u8>) -> Self {
        let mut count = 0;
        for b in bitfield.iter() {
            count += b.count_ones();
        }
        Self { bitfield, count }
    }

    pub fn with_bit_len(len: usize) -> Self {
        Self::new(vec![0; (len + 7) / 8])
    }

    pub fn bitfield_bytes(&self) -> &[u8] {
        self.bitfield.as_ref()
    }

    fn u8_len(&self) -> u32 {
        self.bitfield.len() as u32
    }

    pub fn set(&mut self, bit_index: u32, set: bool) {
        if bit_index as usize > self.bitfield.len() * 8 {
            self.resize(bit_index);
        }
        let u8_index = bit_index >> 3;
        let bit_offset = 7 - (bit_index % 8);
        let ptr = &mut self.bitfield[u8_index as usize];
        let old = *ptr & (1 << bit_offset) > 0;
        if set {
            *ptr |= 1 << bit_offset;
            self.count += (!old) as u32;
        } else {
            *ptr &= !(1 << bit_offset);
            self.count -= (old) as u32;
        }
    }

    /// expand or shrink BitField, expand with zero
    /// shrink clears out bits in shrink section
    pub fn resize(&mut self, new_size: u32) {
        let new_u8_size = ((new_size + 7) / 8) as usize;
        if new_size as usize > self.bitfield.len() * 8 {
            self.bitfield.resize(new_u8_size, 0);
        } else {
            for i in new_size..((self.bitfield.len() * 8) as u32) {
                self.set(i as u32, false);
            }
            self.bitfield.resize(new_u8_size, 0);
        }
    }

    pub fn unset(&mut self, bit_index: u32) {
        self.set(bit_index, false)
    }

    pub fn get(&self, bit_index: u32) -> bool {
        let u8_index = bit_index >> 3;
        let bit_offset = 7 - (bit_index % 8);
        self.bitfield[u8_index as usize] & (1 << bit_offset) != 0
    }

    pub fn count_ones(&self) -> u32 {
        self.count
    }

    pub fn iter<'a>(&'a self) -> BitFieldIter<'a> {
        let iter = self.bitfield.iter();
        BitFieldIter {
            u: 0,
            iter,
            bit_offset: 0,
        }
    }
}

impl<T> From<T> for BitField
where
    T: AsRef<[bool]>,
{
    fn from(v: T) -> Self {
        let s = v.as_ref();
        let bitfield = s
            .chunks(8)
            .map(|bs| {
                let mut ret = 0u8;
                for (i, b) in bs.iter().enumerate() {
                    ret |= (*b as u8) << (7 - i);
                }
                ret
            })
            .collect();
        Self::new(bitfield)
    }
}

pub struct BitFieldIter<'a> {
    u: u8,
    bit_offset: u8,
    iter: core::slice::Iter<'a, u8>,
}

impl Iterator for BitFieldIter<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bit_offset > 0 {
            self.bit_offset -= 1;
            Some(self.u & (1 << self.bit_offset) != 0)
        } else if let Some(u) = self.iter.next() {
            self.u = *u;
            self.bit_offset = 7;
            Some(self.u & (1 << 7) != 0)
        } else {
            None
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Request {
    pub index: u32,
    pub begin: u32,
    pub len: u32,
}

pub struct Piece {
    pub index: u32,
    pub begin: u32,
    pub len: u32,
    pub piece: Option<(BytesMut, oneshot::Sender<BytesMut>)>,
}

impl Piece {
    pub fn buf(&self) -> Option<&BytesMut> {
        self.piece.as_ref().map(|(p, _)| p)
    }

    /// Drops the sender so that the connection receiver may
    /// unblock. The bytes used is not recycled
    pub fn unblock_conn(&mut self) {
        self.piece.as_mut().map(|(_, s)| {
            let (mut ns, _) = oneshot::channel();
            std::mem::swap(s, &mut ns);
        });
    }
}

impl Eq for Piece {}
impl PartialEq for Piece {
    fn eq(&self, other: &Self) -> bool {
        let range_ok =
            self.index == other.index && self.begin == other.begin && self.len == other.len;
        let all_none = self.piece.is_none() && other.piece.is_none();
        let all_same = self
            .buf()
            .is_some_and(|b| other.buf().is_some_and(|ob| b.as_ref() == ob.as_ref()));
        range_ok && (all_none || all_same)
    }
}

impl fmt::Debug for Piece {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Piece")
            .field("index", &self.index)
            .field("begin", &self.begin)
            .field("len", &self.len)
            .field("buffer len", &self.buf().map_or(0, |b| b.len()))
            .finish()
    }
}

impl Drop for Piece {
    fn drop(&mut self) {
        if let Some((buf, tx)) = self.piece.take() {
            _ = tx.send(buf);
        }
    }
}

#[derive(Eq, PartialEq)]
pub struct PieceHeader {
    pub index: u32,
    pub begin: u32,
    pub len: u32,

    read: u32,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExtendedMsg {
    Handshake(ExtendedHandshake),
    Pex(ExtendedPex),
    Metadata(ExtendedMetadata),
    Unknown(u8),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExtendedHandshake {
    // TODO: when sending can use 'static ref
    pub m: HashMap<String, u8>, // supported extensions and id number

    #[serde(skip_serializing_if = "Option::is_none")]
    pub p: Option<u16>, // TCP listen port

    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: Option<String>, // client name and version

    // A string containing the compact representation of the ip address this peer
    // sees you as. i.e. this is the receiver's external ip address (no port is
    // included). This may be either an IPv4 (4 bytes) or an IPv6 (16 bytes) address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub yourip: Option<ByteIpAddr>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv6: Option<ByteIpAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv4: Option<ByteIpAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reqq: Option<u32>, // request queue limit before drop any message

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_size: Option<u32>, // size of metadata
}

type MetadataMsgType = u8;
const METADATA_MSG_TYPE_REQUEST: MetadataMsgType = 0;
const METADATA_MSG_TYPE_DATA: MetadataMsgType = 1;
const METADATA_MSG_TYPE_REJECT: MetadataMsgType = 2;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ExtendedMetadataWire {
    msg_type: MetadataMsgType,
    piece: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_size: Option<usize>,
}

fn bytes_to_metadata(mut data: BytesMut) -> io::Result<ExtendedMetadata> {
    let mut de = bt_bencode::Deserializer::from_slice(data.as_ref());
    let bencode_meta = <ExtendedMetadataWire>::deserialize(&mut de)?;
    match bencode_meta {
        ExtendedMetadataWire {
            msg_type: METADATA_MSG_TYPE_REJECT,
            piece,
            ..
        } => {
            de.end()?;
            Ok(ExtendedMetadata::Reject { piece })
        }
        ExtendedMetadataWire {
            msg_type: METADATA_MSG_TYPE_REQUEST,
            piece,
            ..
        } => {
            de.end()?;
            Ok(ExtendedMetadata::Request { piece })
        }
        ExtendedMetadataWire {
            msg_type: METADATA_MSG_TYPE_DATA,
            piece,
            total_size,
        } => {
            let rest = data.split_off(de.byte_offset());
            Ok(ExtendedMetadata::Data {
                piece,
                data: rest.to_vec(),
                total_size,
            })
        }
        ExtendedMetadataWire { msg_type, .. } => {
            warn!("received unknown metadata type {msg_type}");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("received unknown metadata type {msg_type}"),
            ));
        }
    }
}

fn empty_bytestring() -> ByteString {
    ByteString::from("")
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ExtendedPexWire {
    // some implementation (Transmission) won't send fields if no change
    // although BEP10 says it's a required field
    #[serde(rename = "added")]
    #[serde(default = "empty_bytestring")]
    added: ByteString,
    #[serde(rename = "added.f")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    addedf: Option<ByteString>,
    #[serde(rename = "added6")]
    #[serde(default = "empty_bytestring")]
    added6: ByteString,
    #[serde(rename = "added6.f")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    added6f: Option<ByteString>,

    #[serde(rename = "dropped")]
    #[serde(default = "empty_bytestring")]
    dropped: ByteString,
    #[serde(rename = "dropped6")]
    #[serde(default = "empty_bytestring")]
    dropped6: ByteString,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct PexFlag(u8);

impl From<u8> for PexFlag {
    fn from(value: u8) -> Self {
        PexFlag(value)
    }
}

#[derive(Debug, Eq, PartialEq, Default)]
pub struct ExtendedPex {
    pub added: Vec<(IpAddr, Option<PexFlag>)>,
    pub added6: Vec<(IpAddr, Option<PexFlag>)>,
    pub dropped: Vec<IpAddr>,
    pub dropped6: Vec<IpAddr>,
}

impl From<ExtendedPexWire> for ExtendedPex {
    // Required method
    fn from(v: ExtendedPexWire) -> Self {
        let mut r = Self::default();
        for (i, bip) in v.added.chunks_exact(4).enumerate() {
            let ip = IpAddr::from(Ipv4Addr::from([bip[0], bip[1], bip[2], bip[3]]));
            let f = if let Some(ref fs) = v.addedf {
                if fs.len() > i {
                    let f = PexFlag::from(fs[i]);
                    Some(f)
                } else {
                    None
                }
            } else {
                None
            };
            r.added.push((ip, f))
        }

        for (i, bip) in v.added6.chunks_exact(16).enumerate() {
            let ip = IpAddr::from(Ipv6Addr::from([
                bip[0], bip[1], bip[2], bip[3], bip[4], bip[5], bip[6], bip[7], bip[8], bip[9],
                bip[10], bip[11], bip[12], bip[13], bip[14], bip[15],
            ]));
            let f = if let Some(ref fs) = v.added6f {
                if fs.len() > i {
                    let f = PexFlag::from(fs[i]);
                    Some(f)
                } else {
                    None
                }
            } else {
                None
            };
            r.added6.push((ip, f))
        }

        r.dropped = v
            .dropped
            .chunks_exact(4)
            .map(|bip| {
                let ip = IpAddr::from(Ipv4Addr::from([bip[0], bip[1], bip[2], bip[3]]));
                ip
            })
            .collect();

        r.dropped6 = v
            .dropped6
            .chunks_exact(16)
            .map(|bip| {
                let ip = IpAddr::from(Ipv6Addr::from([
                    bip[0], bip[1], bip[2], bip[3], bip[4], bip[5], bip[6], bip[7], bip[8], bip[9],
                    bip[10], bip[11], bip[12], bip[13], bip[14], bip[15],
                ]));
                ip
            })
            .collect();
        r
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExtendedMetadata {
    Request {
        piece: u32,
    },
    Data {
        piece: u32,
        data: Vec<u8>,
        total_size: Option<usize>,
    },
    Reject {
        piece: u32,
    },
}

async fn send_handshake<T: AsyncWrite + Unpin>(
    handle: &mut T,
    h: &Handshake,
    info_hash: &InfoHash,
) -> io::Result<()> {
    handle.write_u8(19).await?;
    handle.write_all(b"BitTorrent protocol").await?;
    handle.write_all(&h.reserved.0).await?;
    handle.write_all(info_hash).await?;
    handle.write_all(&h.client_id).await?;
    handle.flush().await
}

async fn send_extension_handshake<T: AsyncWrite + Unpin>(
    handle: &mut T,
    h: &ExtendedHandshake,
) -> io::Result<()> {
    let mut buf = Vec::new();
    bt_bencode::to_writer(&mut buf, h)?;
    let len = buf.len() + 2;
    handle.write_u32(len as u32).await?;
    handle.write_u8(MsgTy::EXTENDED).await?;
    handle.write_u8(0).await?;

    handle.write_all(&mut buf).await?;

    handle.flush().await
}

async fn send_keepalive<T: AsyncWrite + Unpin>(handle: &mut T) -> io::Result<()> {
    handle.write_u32(MsgTy::KEEPALIVE_LEN).await?;
    handle.flush().await
}

async fn send_choke<T: AsyncWrite + Unpin>(handle: &mut T) -> io::Result<()> {
    handle.write_u32(MsgTy::CHOKE_LEN).await?;
    handle.write_u8(MsgTy::CHOKE).await?;
    handle.flush().await
}

async fn send_unchoke<T: AsyncWrite + Unpin>(handle: &mut T) -> io::Result<()> {
    handle.write_u32(MsgTy::UNCHOKE_LEN).await?;
    handle.write_u8(MsgTy::UNCHOKE).await?;
    handle.flush().await
}

async fn send_interested<T: AsyncWrite + Unpin>(handle: &mut T) -> io::Result<()> {
    handle.write_u32(MsgTy::INTERESTED_LEN).await?;
    handle.write_u8(MsgTy::INTERESTED).await?;
    handle.flush().await
}

async fn send_notinterested<T: AsyncWrite + Unpin>(handle: &mut T) -> io::Result<()> {
    handle.write_u32(MsgTy::NOTINTERESTED_LEN).await?;
    handle.write_u8(MsgTy::NOTINTERESTED).await?;
    handle.flush().await
}

async fn send_have<T: AsyncWrite + Unpin>(handle: &mut T, index: u32) -> io::Result<()> {
    handle.write_u32(MsgTy::HAVE_LEN).await?;
    handle.write_u8(MsgTy::HAVE).await?;
    handle.write_u32(index).await?;
    handle.flush().await
}

async fn send_bitfield<T: AsyncWrite + Unpin>(handle: &mut T, b: &BitField) -> io::Result<()> {
    handle.write_u32(1 + b.u8_len()).await?;
    handle.write_u8(BitField::TYPE).await?;
    handle.write_all(b.bitfield_bytes()).await?;
    handle.flush().await
}

async fn send_request<T: AsyncWrite + Unpin>(
    handle: &mut T,
    index: u32,
    begin: u32,
    len: u32,
) -> io::Result<()> {
    // TODO: len must be 16KiB unless end of file
    handle.write_u32(MsgTy::REQUEST_LEN).await?;
    handle.write_u8(MsgTy::REQUEST).await?;
    handle.write_u32(index).await?;
    handle.write_u32(begin).await?;
    handle.write_u32(len).await?;
    handle.flush().await
}

async fn send_piece<T: AsyncWrite + Unpin>(
    handle: &mut T,
    index: u32,
    begin: u32,
    piece: &[u8],
) -> io::Result<()> {
    // TODO: len must be 16KiB unless end of file
    handle.write_u32(1 + 4 + 4 + piece.len() as u32).await?; // length
    handle.write_u8(MsgTy::PIECE).await?;
    handle.write_u32(index).await?;
    handle.write_u32(begin).await?;
    handle.write_all(piece).await?;
    handle.flush().await
}

async fn send_extend_metadata<T: AsyncWrite + Unpin>(
    handle: &mut T,
    meta: ExtendedMetadata,
    extend_idmap: &HashMap<ExtensionType, u8>,
) -> io::Result<()> {
    // TODO: len must be 16KiB unless end of file
    let extension_id = if let Some(id) = extend_idmap.get(&ExtensionType::Metadata) {
        *id
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata extension id not in map",
        ));
    };

    let mut bencode_part = Vec::new();
    let data_part = match meta {
        ExtendedMetadata::Request { piece } => {
            bt_bencode::to_writer(
                &mut bencode_part,
                &ExtendedMetadataWire {
                    msg_type: METADATA_MSG_TYPE_REQUEST,
                    piece,
                    total_size: None,
                },
            )?;
            None
        }
        ExtendedMetadata::Data {
            piece,
            data,
            total_size,
        } => {
            bt_bencode::to_writer(
                &mut bencode_part,
                &ExtendedMetadataWire {
                    msg_type: METADATA_MSG_TYPE_DATA,
                    piece,
                    total_size,
                },
            )?;
            Some(data)
        }
        ExtendedMetadata::Reject { piece } => {
            bt_bencode::to_writer(
                &mut bencode_part,
                &ExtendedMetadataWire {
                    msg_type: METADATA_MSG_TYPE_REJECT,
                    piece,
                    total_size: None,
                },
            )?;
            None
        }
    };

    let len = 2
        + bencode_part.len()
        + if let Some(ref d) = data_part {
            d.len()
        } else {
            0
        };
    handle.write_u32(len as u32).await?; // length
    handle.write_u8(MsgTy::EXTENDED).await?;
    handle.write_u8(extension_id).await?;
    handle.write_all(&bencode_part).await?;
    if let Some(data) = data_part {
        handle.write_all(&data).await?;
    }
    handle.flush().await
}

async fn send_extend_pex<T: AsyncWrite + Unpin>(
    handle: &mut T,
    added: impl Iterator<Item = &(IpAddr, Option<PexFlag>)>,
    dropped: impl Iterator<Item = &IpAddr>,
    extend_idmap: &HashMap<ExtensionType, u8>,
) -> io::Result<()> {
    let extension_id = if let Some(id) = extend_idmap.get(&ExtensionType::Pex) {
        *id
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pex extension id not in map",
        ));
    };

    // TODO: len must be 16KiB unless end of file
    let mut added_bin = empty_bytestring();
    let mut addedf_bin = empty_bytestring();
    let mut added6_bin = empty_bytestring();
    let mut added6f_bin = empty_bytestring();
    let mut dropped_bin = empty_bytestring();
    let mut dropped6_bin = empty_bytestring();
    for (a, f) in added {
        match a {
            IpAddr::V4(v4) => {
                added_bin.extend_from_slice(&v4.octets());
                addedf_bin.push(f.unwrap_or(PexFlag::from(0)).0);
            }
            IpAddr::V6(v6) => {
                added6_bin.extend_from_slice(&v6.octets());
                added6f_bin.push(f.unwrap_or(PexFlag::from(0)).0);
            }
        }
    }

    for a in dropped {
        match a {
            IpAddr::V4(v4) => dropped_bin.extend_from_slice(&v4.octets()),
            IpAddr::V6(v6) => dropped6_bin.extend_from_slice(&v6.octets()),
        }
    }

    let mut data = Vec::new();
    bt_bencode::to_writer(
        &mut data,
        &ExtendedPexWire {
            added: added_bin,
            added6: added6_bin,
            addedf: Some(addedf_bin),
            added6f: Some(added6f_bin),
            dropped: dropped_bin,
            dropped6: dropped6_bin,
        },
    )?;

    let len = 2 + data.len();
    handle.write_u32(len as u32).await?; // length
    handle.write_u8(MsgTy::EXTENDED).await?;
    handle.write_u8(extension_id).await?;
    handle.write_all(&data).await?;
    handle.flush().await
}

async fn send_cancel<T: AsyncWrite + Unpin>(
    handle: &mut T,
    index: u32,
    begin: u32,
    len: u32,
) -> io::Result<()> {
    handle.write_u32(MsgTy::CANCEL_LEN).await?; // length
    handle.write_u8(MsgTy::CANCEL).await?;
    handle.write_u32(index).await?;
    handle.write_u32(begin).await?;
    handle.write_u32(len).await?;
    handle.flush().await
}

async fn send_port<T: AsyncWrite + Unpin>(handle: &mut T, port: u16) -> io::Result<()> {
    handle.write_u32(MsgTy::PORT_LEN).await?; // length
    handle.write_u8(MsgTy::PORT).await?;
    handle.write_u16(port).await?;
    handle.flush().await
}

async fn send_have_all<T: AsyncWrite + Unpin>(handle: &mut T) -> io::Result<()> {
    handle.write_u32(MsgTy::HAVE_ALL_LEN).await?;
    handle.write_u8(MsgTy::HAVE_ALL).await?;
    handle.flush().await
}

async fn send_have_none<T: AsyncWrite + Unpin>(handle: &mut T) -> io::Result<()> {
    handle.write_u32(MsgTy::HAVE_NONE_LEN).await?;
    handle.write_u8(MsgTy::HAVE_NONE).await?;
    handle.flush().await
}

async fn send_reject<T: AsyncWrite + Unpin>(
    handle: &mut T,
    index: u32,
    begin: u32,
    len: u32,
) -> io::Result<()> {
    handle.write_u32(MsgTy::REJECT_LEN).await?;
    handle.write_u8(MsgTy::REJECT).await?;
    handle.write_u32(index).await?;
    handle.write_u32(begin).await?;
    handle.write_u32(len).await?;
    handle.flush().await
}

async fn send_suggest_piece<T: AsyncWrite + Unpin>(handle: &mut T, index: u32) -> io::Result<()> {
    handle.write_u32(MsgTy::SUGGEST_PIECE_LEN).await?;
    handle.write_u8(MsgTy::SUGGEST_PIECE).await?;
    handle.write_u32(index).await?;
    handle.flush().await
}

async fn send_allowed_fast<T: AsyncWrite + Unpin>(handle: &mut T, index: u32) -> io::Result<()> {
    handle.write_u32(MsgTy::ALLOWED_FAST_LEN).await?;
    handle.write_u8(MsgTy::ALLOWED_FAST).await?;
    handle.write_u32(index).await?;
    handle.flush().await
}

async fn discard_remain<T>(reader: &mut T, state: &mut PartialExtend) -> io::Result<()>
where
    T: AsyncRead + Unpin,
{
    let buf = &mut state.buf;
    while state.remain > 0 {
        let mut limit_reader = reader.take(16384);
        state.remain -= match limit_reader.read_buf(buf).await? {
            0 => {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
            }
            n => n,
        };
        buf.clear();
    }
    Ok(())
}

async fn recv_msg<'a, T>(
    reader: &'a mut T,
    partial_read: &'a mut PartialRead,
    piece_buf: &'a mut Option<BytesMut>,
) -> io::Result<Message>
where
    T: AsyncRead + Unpin,
{
    loop {
        match partial_read {
            PartialRead::Header(partial_header) => {
                match recv_msg_header(reader, partial_header).await? {
                    MessageHeader::BitField { capacity } => {
                        *partial_read = PartialRead::BitField(PartialExtend {
                            id: 0,
                            remain: capacity,
                            buf: BytesMut::new(),
                        });
                    }
                    MessageHeader::Piece { index, begin, len } => {
                        piece_buf
                            .as_mut()
                            .expect("when receiving a PIECE, piece_buf should be Some")
                            .clear();
                        *partial_read = PartialRead::Piece(PartialPiece {
                            index,
                            begin,
                            len: len as usize,
                            remain: len as usize,
                        })
                    }
                    MessageHeader::Extended { id, len } => {
                        *partial_read = PartialRead::Extend(PartialExtend {
                            id,
                            remain: len,
                            buf: BytesMut::new(),
                        })
                    }
                    MessageHeader::Discard { len } => {
                        *partial_read = PartialRead::Discard(PartialExtend {
                            id: 0,
                            remain: len,
                            buf: BytesMut::new(),
                        })
                    }
                    MessageHeader::KeepAlive => return Ok(Message::KeepAlive),
                    MessageHeader::Choke => return Ok(Message::Choke),
                    MessageHeader::Unchoke => return Ok(Message::Unchoke),
                    MessageHeader::Interested => return Ok(Message::Interested),
                    MessageHeader::NotInterested => return Ok(Message::NotInterested),
                    MessageHeader::Have(have) => return Ok(Message::Have(have)),
                    MessageHeader::Request(req) => return Ok(Message::Request(req)),
                    MessageHeader::Cancel(req) => return Ok(Message::Cancel(req)),
                    MessageHeader::Port(port) => return Ok(Message::Port(port)),
                    MessageHeader::HaveAll => return Ok(Message::HaveAll),
                    MessageHeader::HaveNone => return Ok(Message::HaveNone),
                    MessageHeader::SuggestPiece(index) => return Ok(Message::SuggestPiece(index)),
                    MessageHeader::AllowedFast(index) => return Ok(Message::AllowedFast(index)),
                    MessageHeader::Reject(req) => return Ok(Message::Reject(req)),
                }
            }
            PartialRead::BitField(p) => {
                let res = Ok(Message::BitField(recv_bitfield_msg(reader, p).await?));
                *partial_read = INITIAL_PARTIAL_READ;
                return res;
            }
            PartialRead::Extend(p) => {
                let res = Ok(Message::Extended(recv_extend_msg(reader, p).await?));
                *partial_read = INITIAL_PARTIAL_READ;
                return res;
            }
            PartialRead::Piece(p) => {
                recv_piece_msg(
                    reader,
                    p,
                    piece_buf
                        .as_mut()
                        .expect("when receiving a PIECE, piece_buf should be Some"),
                )
                .await?;
                let piece = piece_buf
                    .take()
                    .expect("when receiving a PIECE, piece_buf should be Some");
                let (tx, rx) = oneshot::channel();
                let ret = Ok(Message::Piece(Piece {
                    index: p.index,
                    begin: p.begin,
                    len: p.len as u32,
                    piece: Some((piece, tx)),
                }));
                *partial_read = PartialRead::Processing(rx);
                return ret;
            }
            PartialRead::Discard(p) => {
                discard_remain(reader, p).await?;
                *partial_read = INITIAL_PARTIAL_READ;
            }
            PartialRead::Processing(done) => {
                let timeout = tokio::time::Duration::from_secs(1);
                match tokio::time::timeout(timeout, done).await {
                    Ok(Ok(buf)) => *piece_buf = Some(buf),
                    Ok(_) => {
                        // sender not sends buffer back, make a new one
                        warn!("receive BytesMut failed");
                        *piece_buf = Some(BytesMut::new());
                    }
                    Err(_) => {
                        // for any reason buffer are not returned
                        // make a new one
                        warn!("receive BytesMut time elapsed, make a new one");
                        *piece_buf = Some(BytesMut::new());
                    }
                }
                *partial_read = INITIAL_PARTIAL_READ;
            }
        }
    }
}

/// read header and output msg header
async fn recv_msg_header<'a, T>(
    reader: &'a mut T,
    state: &mut PartialHeader,
) -> io::Result<MessageHeader>
where
    T: AsyncRead + Unpin,
{
    // TODO: what to do if some malicious peer sends a long len data
    // and a lot of garbage data? use timeout
    // TODO: what if some bug happens in peer and peer shutdown connection
    // leaving data unsend?
    // TODO: what if peer claims to send data, but does not really send?

    while state.filled < 4 {
        let n = reader.read(&mut state.field_len[state.filled..4]).await?;
        state.filled += n;
        if n == 0 {
            // Go has ZeroReadIsEof, in TCP, this should be true
            // TODO: use custom error
            warn!("closed conn");
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
        }
    }

    let len = u32::from_be_bytes(state.field_len);
    if len == 0 {
        state.filled = 0;
        return Ok(MessageHeader::KeepAlive);
    }

    if state.filled <= 4 {
        // read_u8 is cancel safe
        state.field_ty = reader.read_u8().await?;
        state.filled += 1;
    }

    match state.field_ty {
        MsgTy::CHOKE => {
            state.filled = 0;
            Ok(MessageHeader::Choke)
        }
        MsgTy::UNCHOKE => {
            state.filled = 0;
            Ok(MessageHeader::Unchoke)
        }
        MsgTy::INTERESTED => {
            state.filled = 0;
            Ok(MessageHeader::Interested)
        }
        MsgTy::NOTINTERESTED => {
            state.filled = 0;
            Ok(MessageHeader::NotInterested)
        }
        MsgTy::HAVE_ALL => {
            state.filled = 0;
            Ok(MessageHeader::HaveAll)
        }
        MsgTy::HAVE_NONE => {
            state.filled = 0;
            Ok(MessageHeader::HaveNone)
        }
        m @ MsgTy::HAVE | m @ MsgTy::ALLOWED_FAST | m @ MsgTy::SUGGEST_PIECE => {
            // TODO: check length match, absorb remain length in case
            // unimplemented extension
            assert!(state.filled >= 5);
            let mut filled_len = state.filled - 5;
            while filled_len < 4 {
                let n = reader.read(&mut state.field1[filled_len..4]).await?;
                state.filled += n;
                if n == 0 {
                    // Go has ZeroReadIsEof, in TCP, this should be true
                    // TODO: use custom error
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
                }
                filled_len += n;
            }
            state.filled = 0;

            let field1 = u32::from_be_bytes(state.field1);
            match m {
                MsgTy::HAVE => Ok(MessageHeader::Have(field1)),
                MsgTy::ALLOWED_FAST => Ok(MessageHeader::AllowedFast(field1)),
                MsgTy::SUGGEST_PIECE => Ok(MessageHeader::SuggestPiece(field1)),
                _ => unreachable!(),
            }
        }
        MsgTy::BITFIELD => {
            let capacity = (len - 1) as usize;
            state.filled = 0;
            Ok(MessageHeader::BitField { capacity })
        }
        m @ MsgTy::REQUEST | m @ MsgTy::CANCEL | m @ MsgTy::REJECT => {
            // TODO: check length match
            assert!(state.filled >= 5);
            let mut filled_len = state.filled - 5;
            while filled_len < 4 {
                let n = reader.read(&mut state.field1[filled_len..4]).await?;
                state.filled += n;
                if n == 0 {
                    // Go has ZeroReadIsEof, in TCP, this should be true
                    // TODO: use custom error
                    warn!("closed conn");
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
                }
                filled_len += n;
            }
            let index = u32::from_be_bytes(state.field1);

            assert!(state.filled >= 9);
            let mut filled_len = state.filled - 9;
            while filled_len < 4 {
                let n = reader.read(&mut state.field2[filled_len..4]).await?;
                state.filled += n;
                if n == 0 {
                    // Go has ZeroReadIsEof, in TCP, this should be true
                    // TODO: use custom error
                    warn!("closed conn");
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
                }
                filled_len += n;
            }
            let begin = u32::from_be_bytes(state.field2);

            assert!(state.filled >= 13);
            let mut filled_len = state.filled - 13;
            while filled_len < 4 {
                let n = reader.read(&mut state.field3[filled_len..4]).await?;
                state.filled += n;
                if n == 0 {
                    // Go has ZeroReadIsEof, in TCP, this should be true
                    // TODO: use custom error
                    warn!("closed conn");
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
                }
                filled_len += n;
            }
            let len = u32::from_be_bytes(state.field3);
            state.filled = 0;
            match m {
                MsgTy::REQUEST => Ok(MessageHeader::Request(Request { index, begin, len })),
                MsgTy::CANCEL => Ok(MessageHeader::Cancel(Request { index, begin, len })),
                MsgTy::REJECT => Ok(MessageHeader::Reject(Request { index, begin, len })),
                _ => unreachable!(),
            }
        }
        MsgTy::PIECE => {
            // TODO: check length match
            let capacity = (len - 4 - 4 - 1) as usize;

            assert!(state.filled >= 5);
            let mut filled_len = state.filled - 5;
            while filled_len < 4 {
                let n = reader.read(&mut state.field1[filled_len..4]).await?;
                state.filled += n;
                if n == 0 {
                    // Go has ZeroReadIsEof, in TCP, this should be true
                    // TODO: use custom error
                    warn!("closed conn");
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
                }
                filled_len += n;
            }
            let index = u32::from_be_bytes(state.field1);

            assert!(state.filled >= 9);
            let mut filled_len = state.filled - 9;
            while filled_len < 4 {
                let n = reader.read(&mut state.field2[filled_len..4]).await?;
                state.filled += n;
                if n == 0 {
                    // Go has ZeroReadIsEof, in TCP, this should be true
                    // TODO: use custom error
                    warn!("closed conn");
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
                }
                filled_len += n;
            }
            let begin = u32::from_be_bytes(state.field2);

            state.filled = 0;
            Ok(MessageHeader::Piece {
                index,
                begin,
                len: capacity as u32,
            })
        }
        MsgTy::PORT => {
            assert!(state.filled >= 5);
            let mut filled_len = state.filled - 5;
            while filled_len < 2 {
                let n = reader.read(&mut state.field1[filled_len..2]).await?;
                state.filled += n;
                filled_len += n;
                if n == 0 {
                    // Go has ZeroReadIsEof, in TCP, this should be true
                    // TODO: use custom error
                    warn!("closed conn");
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
                }
            }
            state.filled = 0;
            let port = u16::from_be_bytes([state.field1[0], state.field1[1]]);
            Ok(MessageHeader::Port(port))
        }
        MsgTy::EXTENDED => {
            let capacity = (len - 2) as usize;

            assert!(state.filled >= 5);

            // this is cancel safe, and it's last field, so not update
            // state.filled more
            let ext_id = reader.read_u8().await?;
            state.filled = 0;

            Ok(MessageHeader::Extended {
                id: ext_id,
                len: capacity,
            })
        }
        other => {
            warn!("received unknown Msg type {other}, length {len}");
            Ok(MessageHeader::Discard { len: len as usize })
        }
    }
}

async fn recv_piece_msg<'a, T>(
    reader: &'a mut T,
    state: &'a mut PartialPiece,
    piece_buf: &'a mut BytesMut,
) -> io::Result<()>
where
    T: AsyncRead + Unpin,
{
    let mut limit_reader = reader.take(state.remain as u64);

    while state.remain > 0 {
        state.remain -= match limit_reader.read_buf(piece_buf).await? {
            0 => {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
            }
            n => n,
        };
    }

    Ok(())
}

async fn recv_extend_msg<'a, T>(
    reader: &'a mut T,
    state: &mut PartialExtend,
) -> io::Result<ExtendedMsg>
where
    T: AsyncRead + Unpin,
{
    let mut limit_reader = reader.take(state.remain as u64);

    while state.remain > 0 {
        state.remain -= match limit_reader.read_buf(&mut state.buf).await? {
            0 => {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
            }
            n => n,
        };
    }

    match state.id {
        0 => {
            // 0 is handshake
            let handshake: ExtendedHandshake = bt_bencode::from_reader(state.buf.as_ref())?;
            Ok(ExtendedMsg::Handshake(handshake))
        }
        EXTENSION_ID_METADATA => {
            let mut new_buf = BytesMut::new();
            std::mem::swap(&mut state.buf, &mut new_buf);
            Ok(ExtendedMsg::Metadata(bytes_to_metadata(new_buf)?))
        }
        EXTENSION_ID_PEX => {
            let pex: ExtendedPexWire = bt_bencode::from_reader(state.buf.as_ref())?;
            Ok(ExtendedMsg::Pex(pex.into()))
        }
        other => {
            warn!("received unknown extension id {other}");
            // TODO: maybe store the unknown extension's name?
            Ok(ExtendedMsg::Unknown(other))
        }
    }
}

async fn recv_bitfield_msg<'a, T>(
    reader: &'a mut T,
    state: &mut PartialExtend,
) -> io::Result<BitField>
where
    T: AsyncRead + Unpin,
{
    let mut limit_reader = reader.take(state.remain as u64);
    while state.remain > 0 {
        state.remain -= match limit_reader.read_buf(&mut state.buf).await? {
            0 => {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "!"));
            }
            n => n,
        };
    }

    let mut new_buf = BytesMut::new();
    std::mem::swap(&mut state.buf, &mut new_buf);

    Ok(BitField::new(new_buf.into()))
}

async fn recv_extend_handshake<'a, T>(
    reader: &'a mut T,
    pr: &mut PartialRead,
    buf: &mut Option<BytesMut>, // not used here
) -> io::Result<ExtendedHandshake>
where
    T: AsyncRead + Unpin,
{
    let msg = recv_msg(reader, pr, buf).await?;
    match msg {
        Message::Extended(ExtendedMsg::Handshake(e)) => Ok(e),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expecting extension handshake, receive {other:?}"),
        )),
    }
}

async fn recv_handshake<T: AsyncRead + Unpin>(handle: &mut T) -> io::Result<(InfoHash, Handshake)> {
    let first = handle.read_u8().await?;
    if first != 19 {
        todo!();
    }
    let mut header = [0u8; 19];
    handle.read_exact(&mut header).await?;
    if header != *b"BitTorrent protocol" {
        todo!();
    }
    let mut reserved = FuncBits::default();
    let mut info_hash = [0u8; 20];
    let mut client_id = [0u8; 20];
    handle.read_exact(&mut reserved.0).await?;
    handle.read_exact(&mut info_hash).await?;
    handle.read_exact(&mut client_id).await?;

    Ok((
        info_hash,
        Handshake {
            reserved,
            client_id,
        },
    ))
}

#[cfg(test)]
pub mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake};

    use super::*;
    use tokio::io::{duplex, split, DuplexStream, ReadHalf, WriteHalf};

    impl Split for DuplexStream {
        type R = ReadHalf<DuplexStream>;
        type W = WriteHalf<DuplexStream>;

        fn split(self) -> (Self::R, Self::W) {
            tokio::io::split(self)
        }

        fn remote_addr(&self) -> SocketAddr {
            DEFAULT_ADDR
        }

        fn protocol() -> &'static str {
            "tokio-duplex"
        }
    }

    impl Reunite for ReadHalf<DuplexStream> {
        type U = DuplexStream;
        type W = WriteHalf<DuplexStream>;
        fn reunite(self, w: Self::W) -> Result<Self::U, ReuniteError> {
            Ok(self.unsplit(w))
        }
    }

    macro_rules! extract_enum {
        ($expression:expr, $pattern:path) => {
            match $expression {
                $pattern(value) => value,
                e => panic!(
                    "cannot convert {} of {:?} to {}",
                    stringify!($expression),
                    e,
                    stringify!($pattern)
                ),
            }
        };
    }

    #[test]
    fn test_bitfield() {
        let test_bits = [false, true, false, true, false, false, false, false, true];
        let mut a = BitField::from(&test_bits);
        assert_eq!(a.count, 3);
        assert_eq!(a.bitfield, [0b01010000, 0b10000000]);
        for (b1, b2) in a.iter().zip(test_bits.iter()) {
            assert_eq!(b1, *b2);
        }

        a.set(5, true);
        assert_eq!(a.count, 4);
        a.set(5, true);
        assert_eq!(a.count, 4);
        a.set(5, false);
        assert_eq!(a.count, 3);
        a.set(5, false);
        assert_eq!(a.count, 3);
    }

    #[test]
    fn test_bitfield_set_resize() {
        let test_bits = [false, true, false, true, false, true, false, false];
        let mut a = BitField::from(&test_bits);
        assert_eq!(a.count, 3);
        assert_eq!(a.bitfield, [0b01010100]);
        a.resize(4);
        assert_eq!(a.count, 2);
        assert_eq!(a.bitfield, [0b01010000]);

        a.set(9, true);
        assert_eq!(a.count, 3);
        assert_eq!(a.bitfield, [0b01010000, 0b01000000]);
    }

    pub async fn make_ends_tune(
        opt: HandshakeOption,
        opt2: HandshakeOption,
    ) -> (BTStream<DuplexStream>, BTStream<DuplexStream>) {
        let (peer1, peer2) = duplex(1024 * 1024);
        let p1 = tokio::spawn(async move { BTStream::connect(peer1, opt, [0; 20]).await.unwrap() });
        let p2 = tokio::spawn(async move {
            BTStream::accept(peer2, async |_| AcceptOpt::NoMetadata, opt2)
                .await
                .unwrap()
        });

        (p1.await.unwrap(), p2.await.unwrap())
    }

    async fn make_ends() -> (BTStream<DuplexStream>, BTStream<DuplexStream>) {
        let opt = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .pex(true)
            .metadata(true)
            .dht_port(None)
            .build();
        make_ends_tune(opt.clone(), opt).await
    }

    impl BTStream<DuplexStream> {
        pub fn into_split(
            self,
        ) -> (
            ReadStream<ReadHalf<DuplexStream>>,
            WriteStream<WriteHalf<DuplexStream>>,
        ) {
            let (read_end, write_end) = split(self.inner);
            (
                ReadStream {
                    inner: read_end,
                    peer_addr: DEFAULT_ADDR,
                    partial_read: INITIAL_PARTIAL_READ,
                    peer_id: [0; 20],
                    info_hash: [0; 20],
                    reserved: [0; 8].into(),
                    metadata_size: 0,
                    piece_buf: Some(BytesMut::new()),
                },
                WriteStream {
                    inner: write_end,
                    peer_addr: DEFAULT_ADDR,
                    extension_id: self.extension_id,
                    pex_peers: HashMap::new(),
                    peer_id: [0; 20],
                    info_hash: [0; 20],
                    reserved: [0; 8].into(),
                    metadata_size: 0,
                    pending_dht_port: None,
                },
            )
        }
    }

    async fn make_ends_split() -> (
        (
            ReadStream<ReadHalf<DuplexStream>>,
            WriteStream<WriteHalf<DuplexStream>>,
        ),
        (
            ReadStream<ReadHalf<DuplexStream>>,
            WriteStream<WriteHalf<DuplexStream>>,
        ),
    ) {
        let (end1, end2) = make_ends().await;
        (end1.into_split(), end2.into_split())
    }

    #[tokio::test]
    async fn handshake() {
        // TODO: change value of hash, id and reserved

        // opt1 does not support extension
        let opt1 = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .pex(false)
            .metadata(false)
            .dht_port(None)
            .build();
        // opt2 supports extension
        let opt2 = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .pex(true)
            .metadata(true)
            .dht_port(None)
            .build();
        make_ends_tune(opt1, opt2).await;
        // TODO: test info
    }

    #[tokio::test]
    async fn test_handshake_accept_ok() {
        let opt1 = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .pex(false)
            .metadata(false)
            .dht_port(None)
            .build();
        let opt2 = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .pex(true)
            .metadata(true)
            .dht_port(None)
            .build();
        let (peer1, peer2) = duplex(1024 * 1024);
        let p1 =
            tokio::spawn(async move { BTStream::connect(peer1, opt1, [1; 20]).await.unwrap() });
        let p2 = tokio::spawn(async move {
            BTStream::accept(
                peer2,
                async |h| {
                    if *h == [1; 20] {
                        AcceptOpt::NoMetadata
                    } else {
                        AcceptOpt::Reject
                    }
                },
                opt2,
            )
            .await
            .unwrap()
        });
        p1.await.unwrap();
        p2.await.unwrap();
    }

    #[tokio::test]
    async fn test_handshake_accept_reject() {
        let opt1 = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .pex(false)
            .metadata(false)
            .dht_port(None)
            .build();
        let opt2 = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .pex(true)
            .metadata(true)
            .dht_port(None)
            .build();
        let (peer1, peer2) = duplex(1024 * 1024);

        // because accept end rejects, connect end will also error
        let p1 =
            tokio::spawn(async move { BTStream::connect(peer1, opt1, [1; 20]).await.unwrap_err() });
        let p2 = tokio::spawn(async move {
            BTStream::accept(
                peer2,
                async |h| {
                    if *h == [0; 20] {
                        AcceptOpt::NoMetadata
                    } else {
                        AcceptOpt::Reject
                    }
                },
                opt2,
            )
            .await
            .unwrap_err() // should fail because info hash not match
        });
        p1.await.unwrap();
        p2.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_extend() {
        // TODO: change value of hash, id and reserved
        let opt = HandshakeOption::builder()
            .client_id([0; 20])
            .client_version("1".into())
            .pex(true)
            .metadata(true)
            .dht_port(None)
            .build();
        make_ends_tune(opt.clone(), opt.clone()).await;
        // TODO: test info
    }

    #[tokio::test]
    async fn keepalive() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_keepalive().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::KeepAlive));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_keepalive().await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::KeepAlive));
    }

    #[tokio::test]
    async fn keepalive_split() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_keepalive().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::KeepAlive));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_keepalive().await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::KeepAlive));
    }

    #[tokio::test]
    async fn choke() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_choke().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::Choke));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_choke().await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::Choke));
    }

    #[tokio::test]
    async fn unchoke() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_unchoke().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::Unchoke));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_unchoke().await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::Unchoke));
    }

    #[tokio::test]
    async fn intrested() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_interested().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::Interested));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_interested().await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::Interested));
    }

    #[tokio::test]
    async fn notintrested() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_notinterested().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::NotInterested));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_notinterested().await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::NotInterested));
    }

    #[tokio::test]
    async fn have() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_have(533).await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::Have(533)));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_have(533).await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::Have(533)));
    }

    #[tokio::test]
    async fn bitfield() {
        let (mut peer1, mut peer2) = make_ends().await;
        let fields = rand::random::<[u8; 143]>();
        peer1
            .send_bitfield(&BitField::new(fields.into()))
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        let b = extract_enum!(received, Message::BitField);
        assert_eq!(b, BitField::new(fields.into()));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        let fields = rand::random::<[u8; 143]>();
        p1w.send_bitfield(&BitField::new(fields.into()))
            .await
            .expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        let b = extract_enum!(received, Message::BitField);
        assert_eq!(b, BitField::new(fields.into()));
    }

    #[tokio::test]
    async fn request() {
        let (mut peer1, mut peer2) = make_ends().await;
        let index = rand::random::<u32>();
        let begin = rand::random::<u32>();
        peer1
            .send_request(index, begin, 4)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        let r = extract_enum!(received, Message::Request);
        assert_eq!(
            r,
            Request {
                index,
                begin,
                len: 4,
            }
        );
        // TODO: test long request

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_request(index, begin, 4)
            .await
            .expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        let r = extract_enum!(received, Message::Request);
        assert_eq!(
            r,
            Request {
                index,
                begin,
                len: 4,
            }
        );
        // TODO: test long request
    }

    #[tokio::test]
    async fn piece() {
        let (mut peer1, mut peer2) = make_ends().await;
        let random_bytes = rand::random::<[u8; 143]>();
        let index = rand::random::<u32>();
        let begin = rand::random::<u32>();
        peer1
            .send_piece(index, begin, &random_bytes)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        let piece = extract_enum!(received, Message::Piece);
        assert_eq!(piece.index, index);
        assert_eq!(piece.begin, begin);
        assert_eq!(piece.len, random_bytes.len() as u32);
        assert_eq!(piece.buf().unwrap().as_ref(), random_bytes);

        // TODO: test long piece are dropped

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        let random_bytes = rand::random::<[u8; 143]>();
        p1w.send_piece(index, begin, &random_bytes)
            .await
            .expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");

        let piece = extract_enum!(received, Message::Piece);
        assert_eq!(piece.index, index);
        assert_eq!(piece.begin, begin);
        assert_eq!(piece.len, random_bytes.len() as u32);
        assert_eq!(piece.buf().unwrap().as_ref(), random_bytes);
        // TODO: test long piece are dropped
    }

    #[tokio::test]
    async fn many_piece() {
        let (mut peer1, mut peer2) = make_ends().await;
        let random_bytes = rand::random::<[u8; 143]>();
        let index = rand::random::<u32>();
        let begin = rand::random::<u32>();
        peer1
            .send_piece(index, begin, &random_bytes)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        let piece = extract_enum!(received, Message::Piece);
        assert_eq!(piece.index, index);
        assert_eq!(piece.begin, begin);
        assert_eq!(piece.len, random_bytes.len() as u32);
        assert_eq!(piece.buf().unwrap().as_ref(), random_bytes);

        // drop this piece so the buffer can be returned
        drop(piece);

        peer1
            .send_piece(index, begin, &random_bytes)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        let piece = extract_enum!(received, Message::Piece);
        assert_eq!(piece.index, index);
        assert_eq!(piece.begin, begin);
        assert_eq!(piece.len, random_bytes.len() as u32);
        assert_eq!(piece.buf().unwrap().as_ref(), random_bytes);
    }

    #[tokio::test]
    async fn cancel() {
        let (mut peer1, mut peer2) = make_ends().await;
        let index = rand::random::<u32>();
        let begin = rand::random::<u32>();
        peer1
            .send_cancel(index, begin, 4)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");

        let msg = extract_enum!(received, Message::Cancel);
        assert_eq!(msg.begin, begin);
        assert_eq!(msg.index, index);
        assert_eq!(msg.len, 4);
        // TODO: test long request

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_cancel(index, begin, 4)
            .await
            .expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        let msg = extract_enum!(received, Message::Cancel);
        assert_eq!(msg.begin, begin);
        assert_eq!(msg.index, index);
        assert_eq!(msg.len, 4);
        // TODO: test long request
    }

    #[tokio::test]
    async fn port() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_port(4133).await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");

        let msg = extract_enum!(received, Message::Port);
        assert_eq!(msg, 4133);

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_port(4133).await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        let msg = extract_enum!(received, Message::Port);
        assert_eq!(msg, 4133);
    }

    #[tokio::test]
    async fn reject() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1
            .send_reject(1, 0, 16384)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");

        let msg = extract_enum!(received, Message::Reject);
        assert_eq!(
            msg,
            Request {
                index: 1,
                begin: 0,
                len: 16384
            }
        );

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_reject(1, 0, 16384).await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        let msg = extract_enum!(received, Message::Reject);
        assert_eq!(
            msg,
            Request {
                index: 1,
                begin: 0,
                len: 16384
            }
        );
    }

    #[tokio::test]
    async fn suggest_piece() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_suggest_piece(4).await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");

        let msg = extract_enum!(received, Message::SuggestPiece);
        assert_eq!(msg, 4);

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_suggest_piece(4).await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        let msg = extract_enum!(received, Message::SuggestPiece);
        assert_eq!(msg, 4);
    }

    #[tokio::test]
    async fn allowed_fast() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_allowed_fast(4).await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");

        let msg = extract_enum!(received, Message::AllowedFast);
        assert_eq!(msg, 4);

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_allowed_fast(4).await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        let msg = extract_enum!(received, Message::AllowedFast);
        assert_eq!(msg, 4);
    }

    #[tokio::test]
    async fn have_all() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_have_all().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");

        assert!(matches!(received, Message::HaveAll));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_have_all().await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::HaveAll));
    }

    #[tokio::test]
    async fn have_none() {
        let (mut peer1, mut peer2) = make_ends().await;
        peer1.send_have_none().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");

        assert!(matches!(received, Message::HaveNone));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_have_none().await.expect("should send ok");
        let received = p2r.recv_msg().await.expect("should recv ok");
        assert!(matches!(received, Message::HaveNone));
    }

    #[tokio::test]
    async fn bi_direction() {
        let ((mut p1r, mut p1w), (mut p2r, mut p2w)) = make_ends_split().await;
        p1w.send_interested().await.expect("p1 should send ok");
        let p2_recv = p2r.recv_msg().await.expect("p2 should recv ok");
        p2w.send_choke().await.expect("p2 should send ok");
        let p1_recv = p1r.recv_msg().await.expect("p1 should recv ok");
        assert!(matches!(p2_recv, Message::Interested));
        assert!(matches!(p1_recv, Message::Choke));
    }

    #[tokio::test]
    async fn extend_pex() {
        let (mut peer1, mut peer2) = make_ends().await;
        let initial: Vec<_> = vec![
            ("1.2.3.4".parse().unwrap(), Some(PexFlag(1))),
            ("::9".parse().unwrap(), Some(PexFlag(2))),
        ];

        peer1
            .send_extend_pex(&HashMap::from_iter(initial.clone().into_iter()))
            .await
            .expect("should send ok");
        let hdr = peer2.recv_msg().await.unwrap();
        let extend_recv = extract_enum!(hdr, Message::Extended);
        let pex_msg = extract_enum!(extend_recv, ExtendedMsg::Pex);
        assert_eq!(
            pex_msg,
            ExtendedPex {
                added: vec![("1.2.3.4".parse().unwrap(), Some(PexFlag(1))),],
                added6: vec![("::9".parse().unwrap(), Some(PexFlag(2)))],
                dropped: vec![],
                dropped6: vec![],
            }
        );

        let then: Vec<_> = vec![
            ("4.3.2.1".parse().unwrap(), Some(PexFlag(1))),
            ("::9".parse().unwrap(), Some(PexFlag(2))),
        ];

        peer1
            .send_extend_pex(&HashMap::from_iter(then.clone().into_iter()))
            .await
            .expect("should send ok");
        let hdr = peer2.recv_msg().await.unwrap();
        let extend_recv = extract_enum!(hdr, Message::Extended);
        let pex_msg = extract_enum!(extend_recv, ExtendedMsg::Pex);
        assert_eq!(
            pex_msg,
            ExtendedPex {
                added: vec![("4.3.2.1".parse().unwrap(), Some(PexFlag(1))),],
                added6: vec![],
                dropped: vec!["1.2.3.4".parse().unwrap()],
                dropped6: vec![],
            }
        );

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_extend_pex(&HashMap::from_iter(initial.clone().into_iter()))
            .await
            .expect("should send ok");
        let hdr = p2r.recv_msg().await.unwrap();
        let extend_recv = extract_enum!(hdr, Message::Extended);
        let pex_msg = extract_enum!(extend_recv, ExtendedMsg::Pex);
        assert_eq!(
            pex_msg,
            ExtendedPex {
                added: vec![("1.2.3.4".parse().unwrap(), Some(PexFlag(1))),],
                added6: vec![("::9".parse().unwrap(), Some(PexFlag(2)))],
                dropped: vec![],
                dropped6: vec![],
            }
        )
    }

    #[tokio::test]
    async fn extend_metadata() {
        let (mut peer1, mut peer2) = make_ends().await;

        peer1
            .send_extend_metadata(ExtendedMetadata::Data {
                piece: 0,
                data: [1, 2, 3, 4, 5].into(),
                total_size: Some(5),
            })
            .await
            .expect("should send ok");

        let hdr = peer2.recv_msg().await.unwrap();
        let extend_recv = extract_enum!(hdr, Message::Extended);
        let meta_msg = extract_enum!(extend_recv, ExtendedMsg::Metadata);
        assert_eq!(
            meta_msg,
            ExtendedMetadata::Data {
                piece: 0,
                total_size: Some(5),
                data: vec![1, 2, 3, 4, 5]
            }
        );

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        p1w.send_extend_metadata(ExtendedMetadata::Data {
            piece: 0,
            data: [1, 2, 3, 4, 5].into(),
            total_size: Some(5),
        })
        .await
        .expect("should send ok");

        let hdr = p2r.recv_msg().await.unwrap();
        let extend_recv = extract_enum!(hdr, Message::Extended);
        let meta_msg = extract_enum!(extend_recv, ExtendedMsg::Metadata);
        assert_eq!(
            meta_msg,
            ExtendedMetadata::Data {
                piece: 0,
                total_size: Some(5),
                data: vec![1, 2, 3, 4, 5]
            }
        );
    }

    #[tokio::test]
    async fn read_msg_header_cancel_safe() {
        let ((p1r, mut p1w), (mut p2r, p2w)) = make_ends_split().await;
        let request_msg = [
            0u8, 0, 0, 13, 6, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc,
        ];
        // this requests a Request message with piece 0x01020304
        // begin 0x05060708
        // len 0x090a0b0c

        // TODO: test all types of message

        struct MockWaker;
        impl Wake for MockWaker {
            fn wake(self: Arc<Self>) {}
        }

        // TODO: maybe using tokio_test's Future
        let waker = Arc::new(MockWaker).into();
        let mut cx = Context::from_waker(&waker);

        let mut split = vec![];
        for i in 0..request_msg.len() - 2 {
            for j in i + 1..request_msg.len() - 1 {
                split.push((i, j));
            }
        }

        for (i, j) in split {
            let p2_recv_fut = p2r.recv_msg();
            let mut fut_pin = Box::pin(p2_recv_fut);

            // write 0..i
            let _ = p1w.inner.write(&request_msg[0..i]).await;
            let _ = p1w.inner.flush().await;

            let res1 = fut_pin.as_mut().poll(&mut cx);
            assert!(matches!(res1, Poll::Pending));

            // write i..j
            let _ = p1w.inner.write(&request_msg[i..j]).await;
            let _ = p1w.inner.flush().await;
            let res2 = fut_pin.as_mut().poll(&mut cx);
            assert!(matches!(res2, Poll::Pending));

            // write j..
            let _ = p1w.inner.write(&request_msg[j..]).await;
            let _ = p1w.inner.flush().await;
            let res3 = fut_pin.as_mut().await;
            assert!(matches!(
                res3,
                Ok(Message::Request(Request {
                    index: 0x01020304,
                    begin: 0x05060708,
                    len: 0x090a0b0c
                }))
            ));
        }
    }
}

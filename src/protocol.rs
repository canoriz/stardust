use crate::metadata::Metadata;
use bon::Builder;
use bt_bencode::ByteIpAddr;
use bt_bencode::ByteString;
use bytes::{BufMut, BytesMut};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, LazyLock};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net;
use tokio::net::tcp;
use tracing::{info, warn};

// Hex-encode bytes (lowercase, no prefix). Efficient: avoids per-byte
// temporary strings by pushing characters directly.
fn to_hex(bs: &[u8]) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bs.len() * 2);
    for &b in bs {
        s.push(HEX_CHARS[(b >> 4) as usize] as char);
        s.push(HEX_CHARS[(b & 0x0f) as usize] as char);
    }
    s
}

const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
const READBUF_CAP: usize = 16 * 16384;
const WRITEBUF_CAP: usize = 16 * 16384;

pub type InfoHash = [u8; 20];

pub trait Reader: AsyncRead + Send + Unpin + 'static {}
pub trait Writer: AsyncWrite + Send + Unpin + 'static {}
impl<T> Reader for T where T: AsyncRead + Send + Unpin + 'static {}
impl<T> Writer for T where T: AsyncWrite + Send + Unpin + 'static {}

pub trait Split: AsyncRead + AsyncWrite + Send + Unpin + 'static {
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
pub trait Conn: AsyncRead + AsyncWrite + Send + Unpin + 'static {
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
};
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

    peer_addr: SocketAddr,

    metadata_size: usize,

    reserved: FuncBits,
    peer_id: [u8; 20],
    info_hash: [u8; 20],
    // TODO: maybe add a torrent hash Arc<>
    // torrent_hash: [u8; 20],

    // Received messages during handshake phase (after handshake and before extend handshake).
    // To be sent to upper layer.
    // Some implementations send Port and BitField messages between handshake and extend handshake.
    pending_recvs: Vec<Message>,

    reqq_limit: usize,
    peer_listen_port: Option<u16>,
    is_income: bool,
}

impl<T> BTStream<T>
where
    T: Split + Send + 'static,
{
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub fn reqq_limit(&self) -> usize {
        self.reqq_limit
    }

    pub fn to_dyn(self) -> BTStream<Box<dyn Conn>> {
        BTStream {
            inner: Box::new(self.inner),
            peer_addr: self.peer_addr,
            partial_read: self.partial_read,
            extension_id: self.extension_id,
            reserved: self.reserved,
            peer_id: self.peer_id,
            info_hash: self.info_hash,
            metadata_size: self.metadata_size,
            pending_recvs: self.pending_recvs,
            reqq_limit: self.reqq_limit,
            peer_listen_port: self.peer_listen_port,
            is_income: self.is_income,
        }
    }
}

impl BTStream<Box<dyn Conn>> {
    pub fn peer_addr(&self) -> SocketAddr {
        self.inner.remote_addr()
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

    pub async fn send_extend_metadata(&mut self, meta: ExtendedMetadata) -> io::Result<()> {
        send_extend_metadata(&mut self.inner, meta, &self.extension_id).await
    }

    pub async fn send_extend_pex(&mut self, pex: &ExtendedPex) -> io::Result<()> {
        send_extend_pex(&mut self.inner, pex, &self.extension_id).await
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

    pending_recvs: Vec<Message>,
    is_income: bool,
}

#[derive(Debug)]
enum PartialRead {
    Header { partial_header: PartialHeader },
    BitField(PartialExtend),
    Extend(PartialExtend),
    Piece(PartialPiece),
    Discard(PartialExtend),
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
}

#[derive(Debug)]
struct PartialPiece {
    index: u32,
    begin: u32,
    len: usize,
    remain: usize,
    /// Buffer for the cancel-safe `recv_msg` path. Lazily allocated on first entry;
    /// preserved across cancellation so resumption can append to partial data.
    /// `None` on the external-buffer (`recv_piece_body`) path.
    buf: Option<BytesMut>,
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

    peer_id: [u8; 20],
    info_hash: [u8; 20],
    reserved: FuncBits,

    reqq_limit: usize,
    is_income: bool,
}

impl BTStream<net::TcpStream> {
    pub fn local_addr(&self) -> SocketAddr {
        // TODO: is this possible to be error?
        // self.inner.get_ref().local_addr().expect("expect ok")
        self.inner.local_addr().expect("expect ok")
    }
}

#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct Capability {
    cap: u32,
}

impl Capability {
    pub const DHT: Self = Self { cap: 1 };
    pub const Fast: Self = Self { cap: 1 << 1 };
    pub const Metadata: Self = Self { cap: 1 << 2 };
    pub const Pex: Self = Self { cap: 1 << 3 };

    pub fn have(&self, cap: Capability) -> bool {
        self.cap & cap.cap > 0
    }
}

pub struct ConnInfo {
    pub func_bits: FuncBits,
    pub peer_id: [u8; 20],
    pub info_hash: InfoHash,
    pub metadata_size: usize,
    pub capability: Capability,
    pub reqq_limit: usize,
    pub is_income: bool,
    pub peer_listen_port: Option<u16>,
}

impl<T> BTStream<T> {
    pub fn info(&self) -> ConnInfo {
        ConnInfo {
            func_bits: self.reserved,
            peer_id: self.peer_id,
            info_hash: self.info_hash,
            metadata_size: self.metadata_size,
            capability: self.capability(),
            reqq_limit: self.reqq_limit,
            is_income: self.is_income,
            peer_listen_port: self.peer_listen_port,
        }
    }

    pub fn capability(&self) -> Capability {
        let mut ret = Capability { cap: 0 };
        if self.reserved.have_dht() {
            ret.cap |= Capability::DHT.cap;
        }
        if self.reserved.have_fast() {
            ret.cap |= Capability::Fast.cap;
        }
        for id in self.extension_id.keys() {
            match id {
                ExtensionType::Metadata => ret.cap |= Capability::Metadata.cap,
                ExtensionType::Pex => ret.cap |= Capability::Pex.cap,
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
                pending_recvs: self.pending_recvs,
                is_income: self.is_income,
            },
            WriteStream {
                inner: write_end,
                peer_addr,
                extension_id: self.extension_id,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                reqq_limit: self.reqq_limit,
                is_income: self.is_income,
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
            peer_addr: r.peer_addr,
            partial_read: r.partial_read,
            extension_id: w.extension_id,
            reserved: r.reserved,
            peer_id: r.peer_id,
            info_hash: r.info_hash,
            metadata_size: r.metadata_size,
            pending_recvs: r.pending_recvs,
            reqq_limit: w.reqq_limit,
            peer_listen_port: None,
            is_income: r.is_income,
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
                inner: BufReader::with_capacity(READBUF_CAP, read_end),
                peer_addr,
                partial_read: self.partial_read,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                pending_recvs: self.pending_recvs,
                is_income: self.is_income,
            },
            WriteStream {
                inner: BufWriter::with_capacity(WRITEBUF_CAP, write_end),
                peer_addr,
                extension_id: self.extension_id,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                reqq_limit: self.reqq_limit,
                is_income: self.is_income,
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
                pending_recvs: self.pending_recvs,
                is_income: self.is_income,
            },
            WriteStream {
                inner: write_end,
                peer_addr,
                extension_id: self.extension_id,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                reqq_limit: self.reqq_limit,
                is_income: self.is_income,
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
                pending_recvs: self.pending_recvs,
                is_income: self.is_income,
            },
            WriteStream {
                inner: BufWriter::with_capacity(32768, write_end),
                peer_addr,
                extension_id: self.extension_id,
                peer_id: self.peer_id,
                info_hash: self.info_hash,
                reserved: self.reserved,
                metadata_size: self.metadata_size,
                reqq_limit: self.reqq_limit,
                is_income: self.is_income,
            },
        )
    }
}

/// Wraps any `AsyncWrite + Unpin` and makes `flush()` a no-op at the poll level.
///
/// Used by [`BufWrite`] so that individual `send_x` calls accumulate bytes into
/// the underlying `BufWriter` without flushing at the end of each send.
/// A single explicit [`BufWrite::flush`] drains the buffer once.
struct NoFlush<T>(T);

impl<T: AsyncWrite + Unpin> AsyncWrite for NoFlush<T> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        // Intentional no-op: flush is deferred to BufWrite::flush().
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

/// A batched-write handle over [`WriteStream`] and [`BTStream`].
///
/// Obtain one via `buf_write()`. Call any number of `send_x`
/// methods — each serialises the message into the underlying `BufWriter` without
/// flushing at the end. Call [`BufWrite::flush`] when done to push buffered
/// bytes to the socket.
///
/// # Panic
/// Dropping a `BufWrite` without calling `flush()` panics, catching forgotten
/// flushes immediately.
pub struct BufWrite<'a, T>
where
    T: AsyncWrite + Unpin,
{
    inner: &'a mut T,
    extension_id: &'a HashMap<ExtensionType, u8>,
    flushed: bool,
}

impl<T> Drop for BufWrite<'_, T>
where
    T: AsyncWrite + Unpin,
{
    fn drop(&mut self) {
        if !self.flushed {
            panic!("BufWrite dropped without calling flush()");
        }
    }
}

impl<T> BufWrite<'_, T>
where
    T: AsyncWrite + Unpin,
{
    /// Flush all buffered bytes to the socket.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.flushed = true;
        self.inner.flush().await
    }

    pub async fn send_keepalive(&mut self) -> io::Result<()> {
        self.flushed = false;
        send_keepalive(&mut NoFlush(&mut *self.inner)).await
    }

    pub async fn send_choke(&mut self) -> io::Result<()> {
        self.flushed = false;
        send_choke(&mut NoFlush(&mut *self.inner)).await
    }

    pub async fn send_unchoke(&mut self) -> io::Result<()> {
        self.flushed = false;
        send_unchoke(&mut NoFlush(&mut *self.inner)).await
    }

    pub async fn send_interested(&mut self) -> io::Result<()> {
        self.flushed = false;
        send_interested(&mut NoFlush(&mut *self.inner)).await
    }

    pub async fn send_notinterested(&mut self) -> io::Result<()> {
        self.flushed = false;
        send_notinterested(&mut NoFlush(&mut *self.inner)).await
    }

    pub async fn send_have(&mut self, index: u32) -> io::Result<()> {
        self.flushed = false;
        send_have(&mut NoFlush(&mut *self.inner), index).await
    }

    pub async fn send_bitfield(&mut self, b: &BitField) -> io::Result<()> {
        self.flushed = false;
        send_bitfield(&mut NoFlush(&mut *self.inner), b).await
    }

    pub async fn send_request(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        self.flushed = false;
        send_request(&mut NoFlush(&mut *self.inner), index, begin, len).await
    }

    pub async fn send_piece(&mut self, index: u32, begin: u32, piece: &[u8]) -> io::Result<()> {
        self.flushed = false;
        send_piece(&mut NoFlush(&mut *self.inner), index, begin, piece).await
    }

    pub async fn send_cancel(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        self.flushed = false;
        send_cancel(&mut NoFlush(&mut *self.inner), index, begin, len).await
    }

    pub async fn send_port(&mut self, port: u16) -> io::Result<()> {
        self.flushed = false;
        send_port(&mut NoFlush(&mut *self.inner), port).await
    }

    pub async fn send_reject(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        self.flushed = false;
        send_reject(&mut NoFlush(&mut *self.inner), index, begin, len).await
    }

    pub async fn send_allowed_fast(&mut self, index: u32) -> io::Result<()> {
        self.flushed = false;
        send_allowed_fast(&mut NoFlush(&mut *self.inner), index).await
    }

    pub async fn send_suggest_piece(&mut self, index: u32) -> io::Result<()> {
        self.flushed = false;
        send_suggest_piece(&mut NoFlush(&mut *self.inner), index).await
    }

    pub async fn send_have_all(&mut self) -> io::Result<()> {
        self.flushed = false;
        send_have_all(&mut NoFlush(&mut *self.inner)).await
    }

    pub async fn send_have_none(&mut self) -> io::Result<()> {
        self.flushed = false;
        send_have_none(&mut NoFlush(&mut *self.inner)).await
    }

    pub async fn send_extend_metadata(&mut self, meta: ExtendedMetadata) -> io::Result<()> {
        self.flushed = false;
        send_extend_metadata(&mut NoFlush(&mut *self.inner), meta, self.extension_id).await
    }
}

impl<T> WriteStream<T> {
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// The request queue limit of this peer
    /// in flight request should never exceed this limit
    pub fn reqq_limit(&self) -> usize {
        self.reqq_limit
    }
}

impl<T> WriteStream<T>
where
    T: AsyncWrite + Unpin,
{
    /// Obtain a [`BufWrite`] for batched writes.
    ///
    /// Send multiple messages via the handle's `send_x` methods without flushing
    /// after each send. Call [`BufWrite::flush`] once when done.
    pub fn buf_write(&mut self) -> BufWrite<'_, T> {
        BufWrite {
            inner: &mut self.inner,
            extension_id: &self.extension_id,
            flushed: false,
        }
    }
}

impl<T> BTStream<T>
where
    T: AsyncWrite + Unpin,
{
    /// Obtain a [`BufWrite`] for batched writes on `BTStream`.
    ///
    /// Send multiple messages via the handle's `send_x` methods without flushing
    /// after each send. Call [`BufWrite::flush`] once when done.
    pub fn buf_write(&mut self) -> BufWrite<'_, T> {
        BufWrite {
            inner: &mut self.inner,
            extension_id: &self.extension_id,
            flushed: false,
        }
    }
}

#[derive(Builder, Clone, Debug)]
pub struct HandshakeOption {
    #[builder(default = true)]
    pub pex: bool,
    #[builder(default = true)]
    pub metadata: bool,
    pub port: Option<u16>,

    #[builder(required)]
    pub dht_port: Option<u16>, // dht port
    // fast: bool,       // fast extension
    pub client_id: [u8; 20],
    pub client_version: Option<String>, // used in extension
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
        let (h, eh) = opt.handshake();
        // Log outgoing handshake (peer first, then handshake detail)
        info!(
            "{} sending handshake reserved={:?} client_id={} info_hash={}",
            t.remote_addr(),
            h.reserved,
            to_hex(&h.client_id),
            to_hex(&info_hash)
        );
        send_handshake(&mut t, &h, &info_hash).await?;
        let (peer_info_hash, peer_handshake) = recv_handshake(&mut t).await?;
        // Log received handshake (peer first, then handshake detail)
        info!(
            "{} recv handshake reserved={:?} client_id={} info_hash={}",
            t.remote_addr(),
            peer_handshake.reserved,
            to_hex(&peer_handshake.client_id),
            to_hex(&peer_info_hash)
        );
        if peer_info_hash != info_hash {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "peer's info_hash differs from ours",
            ));
        }

        let reserved = peer_handshake.reserved.common(&h.reserved);
        let peer_addr = t.remote_addr();
        // TODO: check peer_handshake's client id
        let s = BTStream {
            inner: t,
            peer_addr,
            partial_read: init_partial_read(),
            extension_id: HashMap::new(),
            peer_id: peer_handshake.client_id,
            info_hash,
            reserved,
            metadata_size: 0,
            pending_recvs: vec![],
            reqq_limit: 0,
            peer_listen_port: None,
            is_income: false,
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
            let recv_ext_handshake =
                recv_extend_handshake(&mut read_end.inner, &mut read_end.partial_read);
            let (write_end, exth, pending_recvs) = {
                let (w, eh_res) = tokio::join!(send_ext_handshake, recv_ext_handshake);
                let (exth, pending_recvs) = eh_res?;
                (w??, exth, pending_recvs)
            };

            let mut s =
                BTStream::<T>::reunite(read_end, write_end).expect("reunite BTStream should OK");
            // Log extended-handshake details after negotiation (peer first)
            info!(
                "{} extended-handshake recv: {:?}, pending_msgs={}",
                s.peer_addr(),
                exth,
                pending_recvs.len()
            );
            s.extension_id = exth
                .m
                .iter()
                .filter_map(|(s, id)| extension_type(s).map(|ss| (ss, *id)))
                .filter(|(_, id)| *id != 0)
                .collect();
            s.metadata_size = exth.metadata_size.unwrap_or(0) as usize;
            s.peer_listen_port = exth.p;
            s.pending_recvs = pending_recvs;
            s.reqq_limit = exth.reqq.unwrap_or(0) as usize;
            return Ok(s);
        }
        Ok(s)
    }

    pub async fn accept<F>(mut t: T, accept: F, opt: HandshakeOption) -> io::Result<Self>
    where
        <T as Split>::R: Reunite<W = <T as Split>::W, U = T>,
        F: AsyncFnOnce(&InfoHash) -> AcceptOpt,
    {
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
        // Log outgoing handshake reply
        info!(
            "{} sending handshake reserved={:?} client_id={} info_hash={}",
            t.remote_addr(),
            h.reserved,
            to_hex(&h.client_id),
            to_hex(&peer_info_hash)
        );

        let peer_addr = t.remote_addr();
        let reserved = peer_handshake.reserved.common(&h.reserved);
        let support_dht = reserved.have_dht();
        let s = BTStream {
            inner: t,
            peer_addr,
            partial_read: init_partial_read(),
            extension_id: HashMap::new(),
            peer_id: peer_handshake.client_id,
            info_hash: peer_info_hash,
            reserved,
            metadata_size: 0,
            pending_recvs: vec![],
            reqq_limit: 0,
            peer_listen_port: None,
            is_income: true,
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
            let recv_ext_handshake =
                recv_extend_handshake(&mut read_end.inner, &mut read_end.partial_read);
            let (write_end, exth, pending_recvs) = {
                let (w, eh_res) = tokio::join!(send_ext_handshake, recv_ext_handshake);
                let (exth, pending_recvs) = eh_res?;
                (w??, exth, pending_recvs)
            };

            let mut s =
                BTStream::<T>::reunite(read_end, write_end).expect("reunite BTStream should OK");
            // Log extended-handshake details after negotiation (peer first)
            info!(
                "{} extended-handshake recv: {:?}, pending_msgs={}",
                s.peer_addr(),
                exth,
                pending_recvs.len()
            );
            s.extension_id = exth
                .m
                .iter()
                .filter_map(|(s, id)| extension_type(s).map(|ss| (ss, *id)))
                .filter(|(_, id)| *id != 0)
                .collect();
            s.metadata_size = exth.metadata_size.unwrap_or(0) as usize;
            s.peer_listen_port = exth.p;
            s.pending_recvs = pending_recvs;
            s.reqq_limit = exth.reqq.unwrap_or(0) as usize;
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

    pub async fn send_extend_metadata(&mut self, meta: ExtendedMetadata) -> io::Result<()> {
        send_extend_metadata(&mut self.inner, meta, &self.extension_id).await
    }

    pub async fn send_extend_pex(&mut self, pex: &ExtendedPex) -> io::Result<()> {
        send_extend_pex(&mut self.inner, pex, &self.extension_id).await
    }
}

/// returns added and dropped peers, and update pex_map to now_connected
fn make_added_and_dropped(
    now_connected: &HashMap<SocketAddr, Option<PexFlag>>,
    old_connected: &HashMap<SocketAddr, Option<PexFlag>>,
) -> (Vec<(SocketAddr, Option<PexFlag>)>, Vec<SocketAddr>) {
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
    pex_map: &mut HashMap<SocketAddr, Option<PexFlag>>,
    added: &[(SocketAddr, Option<PexFlag>)],
    dropped: &[SocketAddr],
) {
    for (ip, pex) in added {
        pex_map.insert(*ip, *pex);
    }
    for ip in dropped {
        pex_map.remove(ip);
    }
}

/// Compute a PEX delta for one peer.
///
/// `peer_addr` is the peer's own advertised address — it is excluded from `now_peers`
/// so we never advertise a peer to itself. `old_peers` tracks what we have already
/// told this peer; it is updated in place as a side effect.
///
/// Returns `None` when there is nothing new to send.
pub(crate) fn pex_delta(
    peer_addr: SocketAddr,
    now_peers: &HashMap<SocketAddr, Option<PexFlag>>,
    old_peers: &mut HashMap<SocketAddr, Option<PexFlag>>,
) -> Option<ExtendedPex> {
    const MAX_PEERS: usize = 200;
    let added: Vec<_> = now_peers
        .iter()
        .filter(|(a, _)| **a != peer_addr && !old_peers.contains_key(*a))
        .take(MAX_PEERS)
        .map(|(a, f)| (*a, *f))
        .collect();
    let dropped: Vec<_> = old_peers
        .iter()
        .filter(|(a, _)| !now_peers.contains_key(*a))
        .take(MAX_PEERS)
        .map(|(a, _)| *a)
        .collect();
    if added.is_empty() && dropped.is_empty() {
        return None;
    }
    update_pex_map(old_peers, &added, &dropped);
    Some(ExtendedPex {
        added: added.iter().filter(|(a, _)| a.is_ipv4()).cloned().collect(),
        added6: added.iter().filter(|(a, _)| a.is_ipv6()).cloned().collect(),
        dropped: dropped.iter().filter(|a| a.is_ipv4()).copied().collect(),
        dropped6: dropped.iter().filter(|a| a.is_ipv6()).copied().collect(),
    })
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

    pub async fn send_extend_metadata(&mut self, meta: ExtendedMetadata) -> io::Result<()> {
        send_extend_metadata(&mut self.inner, meta, &self.extension_id).await
    }

    pub async fn send_extend_pex(&mut self, pex: &ExtendedPex) -> io::Result<()> {
        send_extend_pex(&mut self.inner, pex, &self.extension_id).await
    }
}

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Read the next message header. BitField and Extended bodies are read internally.
    /// Returns [`RecvResult::PiecePending`] for Piece (both fresh and cancel-resume).
    /// Cancel-safe.
    pub async fn recv_msg_header(&mut self) -> io::Result<RecvResult> {
        recv_msg_header_pub(&mut self.inner, &mut self.partial_read).await
    }

    /// Read piece body data into `buf`, appending the outstanding portion (`len` bytes on
    /// a fresh call, fewer bytes on cancel-resume). Requires that `recv_msg_header` returned
    /// [`RecvResult::PiecePending`].
    /// Cancel-safe: if the future is dropped, re-call with the same `buf` to resume.
    pub async fn recv_piece_body<B: BufMut>(&mut self, buf: &mut B) -> io::Result<()> {
        recv_piece_body_pub(&mut self.inner, &mut self.partial_read, buf).await
    }

    /// Receive one complete message. Cancel-safe, including for Piece messages.
    /// For [`Message::Piece`], `Piece::buf` holds the received block data.
    pub async fn recv_msg(&mut self) -> io::Result<Message> {
        recv_msg(&mut self.inner, &mut self.partial_read).await
    }
}

impl<T> ReadStream<T>
where
    T: AsyncRead + Unpin,
{
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Read the next message header. BitField and Extended bodies are read internally.
    /// Returns [`RecvResult::PiecePending`] for Piece (both fresh and cancel-resume).
    /// Cancel-safe.
    pub async fn recv_msg_header(&mut self) -> io::Result<RecvResult> {
        recv_msg_header_pub(&mut self.inner, &mut self.partial_read).await
    }

    /// Read piece body data into `buf`, appending the outstanding portion (`len` bytes on
    /// a fresh call, fewer bytes on cancel-resume). Requires that `recv_msg_header` returned
    /// [`RecvResult::PiecePending`].
    /// Cancel-safe: if the future is dropped, re-call with the same `buf` to resume.
    pub async fn recv_piece_body<B: BufMut>(&mut self, buf: &mut B) -> io::Result<()> {
        recv_piece_body_pub(&mut self.inner, &mut self.partial_read, buf).await
    }

    /// Receive one complete message. Cancel-safe, including for Piece messages.
    /// For [`Message::Piece`], `Piece::buf` holds the received block data.
    pub async fn recv_msg(&mut self) -> io::Result<Message> {
        recv_msg(&mut self.inner, &mut self.partial_read).await
    }

    /// Drains messages buffered during the extension handshake phase.
    /// For [`Message::Piece`] entries, `Piece::buf` holds the owned block data.
    pub async fn maybe_recv_pending_msg(&mut self) -> Vec<Message> {
        self.pending_recvs.drain(0..).collect()
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
        self.0[7] & 0x4 > 0
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

/// Result returned by [`ReadStream::recv_msg_header`] and [`BTStream::recv_msg_header`].
///
/// - [`RecvResult::Message`]: message is complete, no further action needed.
/// - [`RecvResult::PiecePending`]: a piece header is pending body data. Call `recv_piece_body`
///   with a buffer. On first return allocate a fresh buffer; on cancel-resume reuse the
///   same partial buffer (or rely on `recv_msg` which manages this automatically).
#[derive(Debug)]
#[must_use]
pub enum RecvResult {
    /// A fully-received message (simple control, BitField body already read, Extended body already read).
    Message(Message),
    /// A Piece header is consumed; call `recv_piece_body` to read the block data.
    /// Returned both on first encounter and on cancel-resume.
    PiecePending { index: u32, begin: u32, len: u32 },
}

impl MessageHeader {
    /// Convert a simple (no-body) message header into the corresponding Message.
    /// Panics if called on a body-carrying variant (Piece, BitField, Extended, Discard).
    pub fn into_simple_message(self) -> Message {
        match self {
            MessageHeader::KeepAlive => Message::KeepAlive,
            MessageHeader::Choke => Message::Choke,
            MessageHeader::Unchoke => Message::Unchoke,
            MessageHeader::Interested => Message::Interested,
            MessageHeader::NotInterested => Message::NotInterested,
            MessageHeader::Have(h) => Message::Have(h),
            MessageHeader::Request(r) => Message::Request(r),
            MessageHeader::Cancel(r) => Message::Cancel(r),
            MessageHeader::Port(p) => Message::Port(p),
            MessageHeader::SuggestPiece(i) => Message::SuggestPiece(i),
            MessageHeader::AllowedFast(i) => Message::AllowedFast(i),
            MessageHeader::HaveAll => Message::HaveAll,
            MessageHeader::HaveNone => Message::HaveNone,
            MessageHeader::Reject(r) => Message::Reject(r),
            _ => unreachable!("body-carrying MessageHeader has no simple Message conversion"),
        }
    }
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
        if bit_index as usize >= self.bitfield.len() * 8 {
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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct Request {
    pub index: u32,
    pub begin: u32,
    pub len: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Piece {
    pub index: u32,
    pub begin: u32,
    pub len: u32,
    pub buf: Option<BytesMut>,
}

impl Piece {
    pub fn to_request(&self) -> Request {
        Request {
            index: self.index,
            begin: self.begin,
            len: self.len,
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
            let at = de.byte_offset();
            let rest = data.freeze().split_off(at);
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
    pub added: Vec<(SocketAddr, Option<PexFlag>)>,
    pub added6: Vec<(SocketAddr, Option<PexFlag>)>,
    pub dropped: Vec<SocketAddr>,
    pub dropped6: Vec<SocketAddr>,
}

impl From<ExtendedPexWire> for ExtendedPex {
    // Required method
    fn from(v: ExtendedPexWire) -> Self {
        let mut r = Self::default();
        for (i, bip) in v.added.chunks_exact(6).enumerate() {
            let ip = IpAddr::from(Ipv4Addr::from([bip[0], bip[1], bip[2], bip[3]]));
            let port = u16::from_be_bytes([bip[4], bip[5]]);
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
            r.added.push((SocketAddr::new(ip, port), f))
        }

        for (i, bip) in v.added6.chunks_exact(18).enumerate() {
            let ip = IpAddr::from(Ipv6Addr::from([
                bip[0], bip[1], bip[2], bip[3], bip[4], bip[5], bip[6], bip[7], bip[8], bip[9],
                bip[10], bip[11], bip[12], bip[13], bip[14], bip[15],
            ]));
            let port = u16::from_be_bytes([bip[16], bip[17]]);
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
            r.added6.push((SocketAddr::new(ip, port), f))
        }

        r.dropped = v
            .dropped
            .chunks_exact(6)
            .map(|bip| {
                let ip = IpAddr::from(Ipv4Addr::from([bip[0], bip[1], bip[2], bip[3]]));
                let port = u16::from_be_bytes([bip[4], bip[5]]);
                SocketAddr::new(ip, port)
            })
            .collect();

        r.dropped6 = v
            .dropped6
            .chunks_exact(18)
            .map(|bip| {
                let ip = IpAddr::from(Ipv6Addr::from([
                    bip[0], bip[1], bip[2], bip[3], bip[4], bip[5], bip[6], bip[7], bip[8], bip[9],
                    bip[10], bip[11], bip[12], bip[13], bip[14], bip[15],
                ]));
                let port = u16::from_be_bytes([bip[16], bip[17]]);
                SocketAddr::new(ip, port)
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

    handle.write_all(&buf).await?;

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
    pex: &ExtendedPex,
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
    for (a, f) in pex.added.iter().chain(pex.added6.iter()) {
        match a {
            SocketAddr::V4(v4) => {
                added_bin.extend_from_slice(&v4.ip().octets());
                added_bin.extend_from_slice(&[
                    ((v4.port() >> 8) as u8) & 0xff,
                    (v4.port() as u8) & 0xff,
                ]);
                addedf_bin.push(f.unwrap_or(PexFlag::from(0)).0);
            }
            SocketAddr::V6(v6) => {
                added6_bin.extend_from_slice(&v6.ip().octets());
                added6_bin.extend_from_slice(&[
                    ((v6.port() >> 8) as u8) & 0xff,
                    (v6.port() as u8) & 0xff,
                ]);
                added6f_bin.push(f.unwrap_or(PexFlag::from(0)).0);
            }
        }
    }
    for a in pex.dropped.iter().chain(pex.dropped6.iter()) {
        match a {
            SocketAddr::V4(v4) => {
                dropped_bin.extend_from_slice(&v4.ip().octets());
                dropped_bin.extend_from_slice(&[
                    ((v4.port() >> 8) as u8) & 0xff,
                    (v4.port() as u8) & 0xff,
                ]);
            }
            SocketAddr::V6(v6) => {
                dropped6_bin.extend_from_slice(&v6.ip().octets());
                dropped6_bin.extend_from_slice(&[
                    ((v6.port() >> 8) as u8) & 0xff,
                    (v6.port() as u8) & 0xff,
                ]);
            }
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

fn init_partial_read() -> PartialRead {
    PartialRead::Header {
        partial_header: EMPTY_PARTIAL_HEADER,
    }
}

/// Cancel-safe: if cancelled during piece body read, `PartialRead::Piece` state is preserved;
/// the next call resumes from where it left off without re-reading the header.
/// For [`Message::Piece`], `Piece::buf` is filled with the received block data.
async fn recv_msg<T>(reader: &mut T, partial_read: &mut PartialRead) -> io::Result<Message>
where
    T: AsyncRead + Unpin,
{
    match recv_msg_header_pub(reader, partial_read).await? {
        RecvResult::Message(msg) => Ok(msg),
        RecvResult::PiecePending { index, begin, len } => {
            // partial_read is now PartialRead::Piece(p).
            // p.buf is the cancel-safe buffer: lazily allocated on first entry,
            // preserved across cancellation so the next call can append remaining bytes.
            let PartialRead::Piece(ref mut p) = partial_read else {
                unreachable!("PiecePending implies PartialRead::Piece");
            };
            if p.buf.is_none() {
                p.buf = Some(BytesMut::with_capacity(p.len));
            }
            recv_piece_msg(reader, &mut p.remain, p.buf.as_mut().unwrap()).await?;
            let buf = p.buf.take().unwrap();
            *partial_read = init_partial_read();
            Ok(Message::Piece(Piece {
                index,
                begin,
                len,
                buf: Some(buf),
            }))
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

async fn recv_piece_msg<'a, T, B>(
    reader: &'a mut T,
    remain: &'a mut usize,
    piece_buf: &'a mut B,
) -> io::Result<()>
where
    T: AsyncRead + Unpin,
    B: BufMut,
{
    let mut limit_reader = reader.take(*remain as u64);

    while *remain > 0 {
        *remain -= match limit_reader.read_buf(piece_buf).await? {
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

// waiting for extend handshake, returns extend handshake and message
// before extend handshake
/// Reads the next message. Cancel-safe.
///
/// - Simple control messages: returned immediately as [`RecvResult::Message`].
/// - BitField / Extended: body is read internally; returned as [`RecvResult::Message`].
/// - Piece (fresh): header just consumed → returns [`RecvResult::PiecePending`].
///   Caller must supply a buffer and call `recv_piece_body`.
/// - Piece (resume): stream already in `PartialRead::Piece` (cancel mid-body) →
///   returns [`RecvResult::PiecePending`] **without any I/O**.
/// - Discard: unknown messages are consumed internally; loop continues to the next.
async fn recv_msg_header_pub<T>(
    reader: &mut T,
    partial_read: &mut PartialRead,
) -> io::Result<RecvResult>
where
    T: AsyncRead + Unpin,
{
    loop {
        match partial_read {
            PartialRead::Header { partial_header } => {
                let hdr = recv_msg_header(reader, partial_header).await?;
                match hdr {
                    MessageHeader::Piece { index, begin, len } => {
                        *partial_read = PartialRead::Piece(PartialPiece {
                            index,
                            begin,
                            len: len as usize,
                            remain: len as usize,
                            buf: None,
                        });
                        return Ok(RecvResult::PiecePending { index, begin, len });
                    }
                    MessageHeader::BitField { capacity } => {
                        *partial_read = PartialRead::BitField(PartialExtend {
                            id: 0,
                            remain: capacity,
                            buf: BytesMut::new(),
                        });
                        // Fall through to BitField arm to read body.
                    }
                    MessageHeader::Extended { id, len } => {
                        *partial_read = PartialRead::Extend(PartialExtend {
                            id,
                            remain: len,
                            buf: BytesMut::new(),
                        });
                        // Fall through to Extend arm to read body.
                    }
                    MessageHeader::Discard { len } => {
                        *partial_read = PartialRead::Discard(PartialExtend {
                            id: 0,
                            remain: len,
                            buf: BytesMut::new(),
                        });
                        // Fall through to Discard arm.
                    }
                    simple => {
                        *partial_read = init_partial_read();
                        return Ok(RecvResult::Message(simple.into_simple_message()));
                    }
                }
            }
            PartialRead::Piece(p) => {
                // Resume: piece header already consumed. Return PiecePending so caller
                // can supply / reuse a buffer and call recv_piece_body.
                return Ok(RecvResult::PiecePending {
                    index: p.index,
                    begin: p.begin,
                    len: p.len as u32,
                });
            }
            PartialRead::BitField(p) => {
                let res = recv_bitfield_msg(reader, p).await?;
                *partial_read = init_partial_read();
                return Ok(RecvResult::Message(Message::BitField(res)));
            }
            PartialRead::Extend(p) => {
                let res = recv_extend_msg(reader, p).await?;
                *partial_read = init_partial_read();
                return Ok(RecvResult::Message(Message::Extended(res)));
            }
            PartialRead::Discard(p) => {
                discard_remain(reader, p).await?;
                *partial_read = init_partial_read();
                // Loop back to read the next header.
            }
        }
    }
}

/// Reads piece body data into `buf`. Appends exactly `remain` bytes.
/// Cancel-safe: pass the same `buf` on resume; already-received bytes stay at the
/// front and only the remaining bytes are appended.
/// Requires the stream is in [`PartialRead::Piece`] state (i.e. `recv_msg_header_pub`
/// returned `RecvResult::PiecePending`).
async fn recv_piece_body_pub<T, B>(
    reader: &mut T,
    partial_read: &mut PartialRead,
    buf: &mut B,
) -> io::Result<()>
where
    T: AsyncRead + Unpin,
    B: BufMut,
{
    match partial_read {
        PartialRead::Piece(p) => {
            recv_piece_msg(reader, &mut p.remain, buf).await?;
            *partial_read = init_partial_read();
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "recv_piece_body called but not in piece-reading state",
        )),
    }
}

async fn recv_extend_handshake<T>(
    reader: &mut T,
    pr: &mut PartialRead,
) -> io::Result<(ExtendedHandshake, Vec<Message>)>
where
    T: AsyncRead + Unpin,
{
    let mut pending_recvs = vec![];
    loop {
        let msg = recv_msg(reader, pr).await?;
        match msg {
            Message::Extended(ExtendedMsg::Handshake(e)) => break Ok((e, pending_recvs)),
            m => pending_recvs.push(m),
        }
    }
}

async fn recv_handshake<T: AsyncRead + Unpin>(handle: &mut T) -> io::Result<(InfoHash, Handshake)> {
    let first = handle.read_u8().await?;
    if first != 19 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid handshake pstrlen",
        ));
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
                    partial_read: init_partial_read(),
                    peer_id: [0; 20],
                    info_hash: [0; 20],
                    reserved: [0; 8].into(),
                    metadata_size: 0,
                    pending_recvs: self.pending_recvs,
                    is_income: false,
                },
                WriteStream {
                    inner: write_end,
                    peer_addr: DEFAULT_ADDR,
                    extension_id: self.extension_id,
                    peer_id: [0; 20],
                    info_hash: [0; 20],
                    reserved: [0; 8].into(),
                    metadata_size: 0,
                    reqq_limit: 0,
                    is_income: false,
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
        assert_eq!(piece.buf.as_deref().unwrap(), &random_bytes[..]);

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
        assert_eq!(piece.buf.as_deref().unwrap(), &random_bytes[..]);
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
        assert_eq!(piece.buf.as_deref().unwrap(), &random_bytes[..]);

        peer1
            .send_piece(index, begin, &random_bytes)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        let piece = extract_enum!(received, Message::Piece);
        assert_eq!(piece.index, index);
        assert_eq!(piece.begin, begin);
        assert_eq!(piece.len, random_bytes.len() as u32);
        assert_eq!(piece.buf.as_deref().unwrap(), &random_bytes[..]);
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
        // Dummy address not present in any peer map — used to satisfy the exclude parameter.
        let no_self: SocketAddr = "0.0.0.0:0".parse().unwrap();

        let (mut peer1, mut peer2) = make_ends().await;
        let initial: Vec<(SocketAddr, Option<PexFlag>)> = vec![
            ("1.2.3.4:1234".parse().unwrap(), Some(PexFlag(1))),
            ("[::9]:1234".parse().unwrap(), Some(PexFlag(2))),
        ];
        let initial_map: HashMap<SocketAddr, Option<PexFlag>> = initial.iter().cloned().collect();
        let mut pex_peers: HashMap<SocketAddr, Option<PexFlag>> = HashMap::new();

        // -- first send: initial set, all entries are "added" --
        let msg = pex_delta(no_self, &initial_map, &mut pex_peers).unwrap();
        peer1.send_extend_pex(&msg).await.expect("should send ok");
        let hdr = peer2.recv_msg().await.unwrap();
        let extend_recv = extract_enum!(hdr, Message::Extended);
        let pex_msg = extract_enum!(extend_recv, ExtendedMsg::Pex);
        assert_eq!(
            pex_msg,
            ExtendedPex {
                added: vec![("1.2.3.4:1234".parse().unwrap(), Some(PexFlag(1))),],
                added6: vec![("[::9]:1234".parse().unwrap(), Some(PexFlag(2)))],
                dropped: vec![],
                dropped6: vec![],
            }
        );

        // -- second send: swap 1.2.3.4 for 4.3.2.1, keep [::9] --
        let then: Vec<(SocketAddr, Option<PexFlag>)> = vec![
            ("4.3.2.1:1234".parse().unwrap(), Some(PexFlag(1))),
            ("[::9]:1234".parse().unwrap(), Some(PexFlag(2))),
        ];
        let then_map: HashMap<SocketAddr, Option<PexFlag>> = then.iter().cloned().collect();
        let msg = pex_delta(no_self, &then_map, &mut pex_peers).unwrap();
        peer1.send_extend_pex(&msg).await.expect("should send ok");
        let hdr = peer2.recv_msg().await.unwrap();
        let extend_recv = extract_enum!(hdr, Message::Extended);
        let pex_msg = extract_enum!(extend_recv, ExtendedMsg::Pex);
        assert_eq!(
            pex_msg,
            ExtendedPex {
                added: vec![("4.3.2.1:1234".parse().unwrap(), Some(PexFlag(1))),],
                added6: vec![],
                dropped: vec!["1.2.3.4:1234".parse().unwrap()],
                dropped6: vec![],
            }
        );

        // -- WriteStream variant: first send --
        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        let mut p1w_pex_peers: HashMap<SocketAddr, Option<PexFlag>> = HashMap::new();
        let msg = pex_delta(no_self, &initial_map, &mut p1w_pex_peers).unwrap();
        p1w.send_extend_pex(&msg).await.expect("should send ok");
        let hdr = p2r.recv_msg().await.unwrap();
        let extend_recv = extract_enum!(hdr, Message::Extended);
        let pex_msg = extract_enum!(extend_recv, ExtendedMsg::Pex);
        assert_eq!(
            pex_msg,
            ExtendedPex {
                added: vec![("1.2.3.4:1234".parse().unwrap(), Some(PexFlag(1))),],
                added6: vec![("[::9]:1234".parse().unwrap(), Some(PexFlag(2)))],
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
    async fn recv_msg_header_cancel_safe() {
        // Piece header (13 bytes): u32(len=9+6) | u8(7) | u32(index=3) | u32(begin=7)
        // body (6 bytes) is written separately after recv_msg_header to avoid confusing the test.
        let piece_header: [u8; 13] = [0, 0, 0, 15, 7, 0, 0, 0, 3, 0, 0, 0, 7];

        struct MockWaker3;
        impl Wake for MockWaker3 {
            fn wake(self: Arc<Self>) {}
        }
        let waker = Arc::new(MockWaker3).into();
        let mut cx = Context::from_waker(&waker);

        // Test every split point of the 13-byte header.
        for split_at in 1..piece_header.len() {
            let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;

            // Write first half.
            p1w.inner
                .write_all(&piece_header[..split_at])
                .await
                .unwrap();
            p1w.inner.flush().await.unwrap();

            {
                // Poll once — must be Pending because the header is incomplete.
                let mut fut = Box::pin(p2r.recv_msg_header());
                let res = fut.as_mut().poll(&mut cx);
                assert!(
                    matches!(res, Poll::Pending),
                    "split_at={split_at}: expected Pending"
                );
                // Simulate cancellation by dropping the future.
            }

            // Write the second half.
            p1w.inner
                .write_all(&piece_header[split_at..])
                .await
                .unwrap();
            p1w.inner.flush().await.unwrap();

            // Create a fresh future and drive it to completion.
            let result = p2r.recv_msg_header().await.unwrap();
            assert!(
                matches!(
                    result,
                    RecvResult::PiecePending {
                        index: 3,
                        begin: 7,
                        len: 6
                    }
                ),
                "split_at={split_at}: unexpected result {result:?}",
            );

            // Consume the 6 body bytes to leave the stream clean.
            let mut sink = BytesMut::new();
            p1w.inner.write_all(&[0u8; 6]).await.unwrap();
            p2r.recv_piece_body(&mut sink).await.unwrap();
        }
    }

    #[tokio::test]
    async fn recv_msg_cancel_safe() {
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

    /// `recv_piece_body` accepts any `BufMut`, here `Vec<u8>`.
    #[tokio::test]
    async fn recv_piece_body_with_generic_buf() {
        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        // length=13 (9+4), type=7, index=1, begin=2, body=[0xAA,0xBB,0xCC,0xDD]
        let mut msg = vec![0u8, 0, 0, 13, 7, 0, 0, 0, 1, 0, 0, 0, 2];
        msg.extend_from_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
        p1w.inner.write_all(&msg).await.unwrap();
        p1w.inner.flush().await.unwrap();

        let res = p2r.recv_msg_header().await.unwrap();
        assert!(matches!(
            res,
            RecvResult::PiecePending {
                index: 1,
                begin: 2,
                len: 4
            }
        ));

        let mut buf: Vec<u8> = Vec::new();
        p2r.recv_piece_body(&mut buf).await.unwrap();
        assert_eq!(buf, vec![0xAA, 0xBB, 0xCC, 0xDD]);
    }

    /// Dropping `recv_piece_body` mid-read leaves partial state in the stream.
    /// A new call with the same buffer appends the remaining bytes correctly.
    #[tokio::test]
    async fn recv_piece_body_cancel_safe() {
        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        // length=15 (9+6), type=7, index=0, begin=0, body=[0x11..0x66]
        let header = [0u8, 0, 0, 15, 7, 0, 0, 0, 0, 0, 0, 0, 0];
        let body = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66];

        p1w.inner.write_all(&header).await.unwrap();
        p1w.inner.flush().await.unwrap();

        let res = p2r.recv_msg_header().await.unwrap();
        assert!(matches!(
            res,
            RecvResult::PiecePending {
                index: 0,
                begin: 0,
                len: 6
            }
        ));

        struct MockWaker2;
        impl Wake for MockWaker2 {
            fn wake(self: Arc<Self>) {}
        }
        let waker = Arc::new(MockWaker2).into();
        let mut cx = Context::from_waker(&waker);

        let mut buf = BytesMut::new();

        // Write only the first half; poll once — should be Pending because 3 of 6 bytes remain.
        p1w.inner.write_all(&body[..3]).await.unwrap();
        p1w.inner.flush().await.unwrap();
        {
            let mut fut_pin = Box::pin(p2r.recv_piece_body(&mut buf));
            let res = fut_pin.as_mut().poll(&mut cx);
            assert!(matches!(res, Poll::Pending));
            // Drop fut_pin here — simulates cancellation.
        }

        // After cancellation the ReadStream retains `remain = 3` in its PartialRead state.
        let res = p2r.recv_msg_header().await.unwrap();
        assert!(matches!(res, RecvResult::PiecePending { .. }));

        // Write remaining bytes and resume with the same buffer; it should complete.
        p1w.inner.write_all(&body[3..]).await.unwrap();
        p1w.inner.flush().await.unwrap();
        p2r.recv_piece_body(&mut buf).await.unwrap();
        assert_eq!(&buf[..], &body[..]);
    }

    /// BufWrite batches multiple messages and delivers them all after a single flush().
    #[tokio::test]
    async fn buf_write_batches_messages() {
        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        {
            let mut bw = p1w.buf_write();
            bw.send_choke().await.unwrap();
            bw.send_unchoke().await.unwrap();
            bw.send_interested().await.unwrap();
            bw.flush().await.unwrap();
        }
        assert!(matches!(p2r.recv_msg().await.unwrap(), Message::Choke));
        assert!(matches!(p2r.recv_msg().await.unwrap(), Message::Unchoke));
        assert!(matches!(p2r.recv_msg().await.unwrap(), Message::Interested));
    }

    /// BufWrite::send_request sends multiple Request messages, all received after flush.
    #[tokio::test]
    async fn buf_write_send_requests() {
        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;
        let reqs = [(1u32, 0u32, 16384u32), (1, 16384, 16384), (2, 0, 16384)];
        {
            let mut bw = p1w.buf_write();
            for (index, begin, len) in reqs {
                bw.send_request(index, begin, len).await.unwrap();
            }
            bw.flush().await.unwrap();
        }
        for (index, begin, len) in reqs {
            let msg = p2r.recv_msg().await.unwrap();
            assert!(
                matches!(msg, Message::Request(Request { index: i, begin: b, len: l }) if i == index && b == begin && l == len),
                "unexpected message {msg:?}"
            );
        }
    }

    /// BufWrite can be used multiple times sequentially on the same WriteStream.
    #[tokio::test]
    async fn buf_write_reusable() {
        let ((_, mut p1w), (mut p2r, _)) = make_ends_split().await;

        let mut bw = p1w.buf_write();
        bw.send_have(10).await.unwrap();
        bw.flush().await.unwrap();
        drop(bw);

        let mut bw2 = p1w.buf_write();
        bw2.send_have(20).await.unwrap();
        bw2.flush().await.unwrap();
        drop(bw2);

        let m1 = p2r.recv_msg().await.unwrap();
        let m2 = p2r.recv_msg().await.unwrap();
        assert!(matches!(m1, Message::Have(10)));
        assert!(matches!(m2, Message::Have(20)));
    }
}

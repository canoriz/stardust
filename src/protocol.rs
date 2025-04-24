use bytes::BufMut;
use core::fmt;
use sha1::digest::crypto_common::Key;
use std::fmt::Formatter;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::io::{
    self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufStream, BufWriter,
    ReadHalf, WriteHalf,
};
use tokio::net;
use tokio::net::tcp;

pub trait Split {
    type R: AsyncRead + Send + Unpin + 'static;
    type W: AsyncWrite + Send + Unpin + 'static;
    fn split(self) -> (Self::R, Self::W);
    fn peer_addr(&self) -> SocketAddr;
}

impl Split for net::TcpStream {
    type R = tcp::OwnedReadHalf;
    type W = tcp::OwnedWriteHalf;

    fn split(self) -> (Self::R, Self::W) {
        self.into_split()
    }

    fn peer_addr(&self) -> SocketAddr {
        const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
        self.peer_addr().unwrap_or(DEFAULT_ADDR)
    }
}

// non-blocking io
pub trait TryRead {
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize>;
    fn try_read_buf<B: BufMut>(&self, buf: &mut B) -> io::Result<usize>;
}

pub trait TryWrite {
    fn try_write(&self, buf: &[u8]) -> io::Result<usize>;
}

pub struct BTStream<T> {
    // inner: BufStream<T>,
    inner: T,
}

pub struct ReadStream<T> {
    inner: BufReader<T>,
    peer_addr: SocketAddr,
}

pub struct WriteStream<T> {
    inner: BufWriter<T>,
    peer_addr: SocketAddr,
}

impl BTStream<net::TcpStream> {
    pub async fn connect_tcp(peer_addr: SocketAddr) -> io::Result<BTStream<net::TcpStream>> {
        // TODO: fix type of peer_addr
        // TODO: add timeout
        let tcp_stream = net::TcpStream::connect(peer_addr).await?;
        Ok(BTStream::<net::TcpStream> {
            // inner: BufStream::new(tcp_stream),
            inner: tcp_stream,
        })
    }

    pub fn peer_addr(&self) -> SocketAddr {
        // TODO: is this possible to be error?
        // self.inner.get_ref().peer_addr().expect("expect ok")
        self.inner.peer_addr().expect("expect ok")
    }

    pub fn local_addr(&self) -> SocketAddr {
        // TODO: is this possible to be error?
        // self.inner.get_ref().local_addr().expect("expect ok")
        self.inner.local_addr().expect("expect ok")
    }
}

impl<T> From<T> for BTStream<T>
where
    T: AsyncRead + AsyncWrite,
{
    fn from(t: T) -> Self {
        Self {
            // inner: BufStream::new(t),
            inner: t,
        }
    }
}

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Split,
{
    pub fn split(self) -> (ReadStream<<T as Split>::R>, WriteStream<<T as Split>::W>) {
        let peer_addr = self.inner.peer_addr();
        let (read_end, write_end) = self.inner.split();
        (
            ReadStream {
                inner: BufReader::new(read_end),
                peer_addr,
            },
            WriteStream {
                inner: BufWriter::new(write_end),
                peer_addr,
            },
        )
    }
}

impl fmt::Debug for BTStream<net::TcpStream> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_fmt(format_args!(
            "tcp conn from {:?} to {:?}",
            // self.inner.get_ref().local_addr(),
            // self.inner.get_ref().peer_addr(),
            self.inner.local_addr(),
            self.inner.peer_addr(),
        ))
    }
}

pub(crate) struct WriteHandle<'a, T>
where
    T: AsyncWrite + Unpin,
{
    wr: &'a mut WriteStream<T>,
    flushed: bool,
}

impl<'a, T> Drop for WriteHandle<'a, T>
where
    T: AsyncWrite + Unpin,
{
    fn drop(&mut self) {
        if !self.flushed {
            panic!("write handle not flushed but dropped")
        }
    }
}

impl<'a, T> WriteHandle<'a, T>
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

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn send_handshake(&mut self, h: &Handshake) -> io::Result<()> {
        send_handshake(&mut self.inner, h).await
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
}

impl<T> WriteStream<T>
where
    T: AsyncWrite + Unpin,
{
    pub async fn send_handshake(&mut self, h: &Handshake) -> io::Result<()> {
        send_handshake(&mut self.inner, h).await
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
}

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn recv_msg_header(&mut self) -> io::Result<Message<'_, T>> {
        recv_msg_header(&mut self.inner).await
    }

    pub async fn recv_handshake(&mut self) -> io::Result<Handshake> {
        recv_handshake(&mut self.inner).await
    }
}

impl<T> ReadStream<T>
where
    T: AsyncRead + Unpin,
{
    // receive one message header
    // It's header because if it's a piece message,
    // further handling of body is required
    pub async fn recv_msg_header(&mut self) -> io::Result<Message<'_, BufReader<T>>> {
        recv_msg_header(&mut self.inner).await
    }

    pub async fn recv_handshake(&mut self) -> io::Result<Handshake> {
        recv_handshake(&mut self.inner).await
    }

    pub fn peer_addr(&self) -> SocketAddr {
        // TODO: change a different name
        self.peer_addr
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub torrent_hash: [u8; 20],
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
}

#[derive(Eq, Debug, PartialEq)]
pub enum Message<'a, T> {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    BitField(BitField),
    Request(Request),
    Piece(Piece<'a, T>),
    Cancel(Request),
}

impl<T> Message<'_, T> {
    const fn len(&self) -> u32 {
        match self {
            Message::KeepAlive => 0,
            Message::Choke => 1,
            Message::Unchoke => 1,
            Message::Interested => 1,
            Message::NotInterested => 1,
            Message::Have(_) => 5,
            Message::BitField(_) => unimplemented!(), //1 + ((3 + b.len()) >> 2),
            Message::Request(_) => 13,
            Message::Piece(_) => unimplemented!(),
            Message::Cancel(_) => 13,
        }
    }
    fn ty(&self) -> u8 {
        match self {
            Message::KeepAlive => unimplemented!(),
            Message::Choke => MsgTy::CHOKE,
            Message::Unchoke => MsgTy::UNCHOKE,
            Message::Interested => MsgTy::INTERESTED,
            Message::NotInterested => MsgTy::NOTINTERESTED,
            Message::Have(_) => MsgTy::HAVE,
            Message::BitField(_) => MsgTy::BITFIELD,
            Message::Request(_) => MsgTy::REQUEST,
            Message::Piece(_) => MsgTy::PIECE,
            Message::Cancel(_) => MsgTy::PIECE,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct BitField {
    bitfield: Vec<u8>, // use array?
}

impl BitField {
    const TYPE: u8 = 5;

    pub fn new(bitfield: Vec<u8>) -> Self {
        Self { bitfield }
    }

    pub fn bitfield_bytes(&self) -> &[u8] {
        self.bitfield.as_ref()
    }

    fn u8_len(&self) -> u32 {
        self.bitfield.len() as u32
    }

    pub fn set(&mut self, bit_index: u32) {
        let u8_index = bit_index >> 3;
        let bit_offset = 7 - (bit_index % 8);
        self.bitfield[u8_index as usize] |= 1 << bit_offset;
    }

    pub fn unset(&mut self, bit_index: u32) {
        let u8_index = bit_index >> 3;
        let bit_offset = 7 - (bit_index % 8);
        self.bitfield[u8_index as usize] &= !(1 << bit_offset);
    }

    pub fn get(&self, bit_index: u32) -> bool {
        let u8_index = bit_index >> 3;
        let bit_offset = 7 - (bit_index % 8);
        self.bitfield[u8_index as usize] & (1 << bit_offset) != 0
    }

    pub fn iter(&self) -> BitFieldIter {
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
        Self {
            bitfield: s
                .chunks(8)
                .map(|bs| {
                    let mut ret = 0u8;
                    for (i, b) in bs.iter().enumerate() {
                        ret |= (*b as u8) << (7 - i);
                    }
                    ret
                })
                .collect(),
        }
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Request {
    pub index: u32,
    pub begin: u32,
    pub len: u32,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Piece<'a, T> {
    pub index: u32,
    pub begin: u32,
    pub len: u32,

    handle: &'a mut T,
}
impl<T> Piece<'_, T>
where
    T: AsyncRead + Unpin,
{
    // read all remaining piece(block) data into buf
    // read should be called once, buf.len should
    // be large enough to store the entire block
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        assert!(buf.len() >= self.len as usize);

        let n = self
            .handle
            .read_exact(&mut buf[..(self.len as usize)])
            .await?;

        assert_eq!(n, self.len as usize);
        Ok(n)
    }
}

async fn send_handshake<T: AsyncWrite + Unpin>(handle: &mut T, h: &Handshake) -> io::Result<()> {
    handle.write_u8(19).await?;
    handle.write_all(b"BitTorrent protocol").await?;
    handle.write_all(&h.reserved).await?;
    handle.write_all(&h.torrent_hash).await?;
    handle.write_all(&h.client_id).await?;
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

async fn send_cancel<T: AsyncWrite + Unpin>(
    handle: &mut T,
    index: u32,
    begin: u32,
    len: u32,
) -> io::Result<()> {
    // TODO: len must be 16KiB unless end of file
    handle.write_u32(MsgTy::CANCEL_LEN).await?; // length
    handle.write_u8(MsgTy::CANCEL).await?;
    handle.write_u32(index).await?;
    handle.write_u32(begin).await?;
    handle.write_u32(len).await?;
    handle.flush().await
}

async fn recv_msg_header<T: AsyncRead + Unpin>(handle: &mut T) -> io::Result<Message<'_, T>> {
    // TODO: what to do if some malicious peer sends a long len data
    // and a lot of garbage data? use timeout
    let len = handle.read_u32().await?;
    if len == 0 {
        return Ok(Message::KeepAlive);
    }
    let msg_ty = handle.read_u8().await?;
    match msg_ty {
        MsgTy::CHOKE => Ok(Message::Choke),
        MsgTy::UNCHOKE => Ok(Message::Unchoke),
        MsgTy::INTERESTED => Ok(Message::Interested),
        MsgTy::NOTINTERESTED => Ok(Message::NotInterested),
        MsgTy::HAVE => {
            // TODO: check length match, absorb remain length in case
            // unimplemented extension
            let index = handle.read_u32().await?;
            Ok(Message::Have(index))
        }
        MsgTy::BITFIELD => {
            let capacity = (len - 1) as usize;
            let mut bitfield: Vec<u8> = unsafe {
                let mut piece: Vec<MaybeUninit<u8>> = Vec::with_capacity(capacity);
                piece.set_len(capacity);
                std::mem::transmute(piece)
            };
            handle.read_exact(bitfield.as_mut_slice()).await?;
            let len = bitfield.len();
            Ok(Message::BitField(BitField::new(bitfield)))
        }
        MsgTy::REQUEST => {
            // TODO: check length match
            let index = handle.read_u32().await?;
            let begin = handle.read_u32().await?;
            let len = handle.read_u32().await?;
            Ok(Message::Request(Request { index, begin, len }))
        }
        MsgTy::PIECE => {
            // TODO: check length match
            let capacity = (len - 4 - 4 - 1) as usize;
            let index = handle.read_u32().await?;
            let begin = handle.read_u32().await?;

            Ok(Message::Piece(Piece {
                index,
                begin,
                len: capacity as u32,
                handle,
            }))
        }
        MsgTy::CANCEL => {
            // TODO: check length match
            let index = handle.read_u32().await?;
            let begin = handle.read_u32().await?;
            let len = handle.read_u32().await?;
            Ok(Message::Cancel(Request { index, begin, len }))
        }
        _ => {
            panic!();
        }
    }
}

async fn recv_handshake<T: AsyncRead + Unpin>(handle: &mut T) -> io::Result<Handshake> {
    let mut header = [0u8; 19];
    let first = handle.read_u8().await?;
    if first != 19 {
        todo!();
    }
    handle.read_exact(&mut header).await?;
    if header != *b"BitTorrent protocol" {
        todo!();
    }
    let mut reserved = [0u8; 8];
    let mut torrent_hash = [0u8; 20];
    let mut client_id = [0u8; 20];
    handle.read_exact(&mut reserved).await?;
    handle.read_exact(&mut torrent_hash).await?;
    handle.read_exact(&mut client_id).await?;
    Ok(Handshake {
        reserved,
        torrent_hash,
        client_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, split, DuplexStream, ReadHalf, WriteHalf};

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
        let a = BitField::from(&test_bits);
        assert_eq!(a.bitfield, [0b01010000, 0b10000000]);
        for (b1, b2) in a.iter().zip(test_bits.iter()) {
            assert_eq!(b1, *b2);
        }
    }

    fn make_ends() -> (BTStream<DuplexStream>, BTStream<DuplexStream>) {
        let (end1, end2) = duplex(1024 * 1024);
        (
            BTStream::<DuplexStream> {
                // inner: tokio::io::BufStream::new(end1),
                inner: end1,
            },
            BTStream::<DuplexStream> {
                // inner: tokio::io::BufStream::new(end2),
                inner: end2,
            },
        )
    }

    impl BTStream<DuplexStream> {
        pub fn into_split(
            self,
        ) -> (
            ReadStream<ReadHalf<DuplexStream>>,
            WriteStream<WriteHalf<DuplexStream>>,
        ) {
            const DEFAULT_ADDR: SocketAddr =
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
            let (read_end, write_end) = split(self.inner);
            (
                ReadStream {
                    inner: BufReader::new(read_end),
                    peer_addr: DEFAULT_ADDR,
                },
                WriteStream {
                    inner: BufWriter::new(write_end),
                    peer_addr: DEFAULT_ADDR,
                },
            )
        }
    }
    fn make_ends_split() -> (
        (
            ReadStream<ReadHalf<DuplexStream>>,
            WriteStream<WriteHalf<DuplexStream>>,
        ),
        (
            ReadStream<ReadHalf<DuplexStream>>,
            WriteStream<WriteHalf<DuplexStream>>,
        ),
    ) {
        let (end1, end2) = duplex(1024 * 1024);
        (
            BTStream::<DuplexStream> {
                // inner: tokio::io::BufStream::new(end1),
                inner: end1,
            }
            .into_split(),
            BTStream::<DuplexStream> {
                // inner: tokio::io::BufStream::new(end2),
                inner: end2,
            }
            .into_split(),
        )
    }

    #[tokio::test]
    async fn handshake() {
        // TODO: change value of hash, id and reserved
        const HANDSHAKE: Handshake = Handshake {
            reserved: [0x1, 0x2, 0x3, 0x4, 0x1, 0x2, 0x3, 0x4],
            torrent_hash: [
                0x05, 0xb7, 0x49, 0x26, 0xfc, 0xb6, 0x0e, 0x28, 0x87, 0x02, 0xb4, 0x89, 0xc9, 0x99,
                0x88, 0x6d, 0x0d, 0x08, 0xcc, 0x90,
            ],
            client_id: *b"-ST0010-qwertyuiopas",
        };
        let (mut peer1, mut peer2) = make_ends();
        peer1
            .send_handshake(&HANDSHAKE)
            .await
            .expect("should send ok");
        let received = peer2.recv_handshake().await.expect("should recv ok");
        assert_eq!(received, HANDSHAKE);

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_handshake(&HANDSHAKE)
            .await
            .expect("should send ok");
        let received = p2r.recv_handshake().await.expect("should recv ok");
        assert_eq!(received, HANDSHAKE);
    }

    #[tokio::test]
    #[ignore]
    async fn test_connect_real() {
        let mut bstream = BTStream::<net::TcpStream>::connect_tcp("[::0]:35515".parse().unwrap())
            .await
            .unwrap();
        let _ = bstream
            .send_handshake(&Handshake {
                reserved: [0; 8],
                torrent_hash: [0; 20],
                client_id: [0; 20],
            })
            .await;
        let rcv = bstream.recv_handshake().await.unwrap();
        dbg!(rcv);
    }

    #[tokio::test]
    async fn keepalive() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_keepalive().await.expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::KeepAlive));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_keepalive().await.expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::KeepAlive));
    }

    #[tokio::test]
    async fn keepalive_split() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_keepalive().await.expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::KeepAlive));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_keepalive().await.expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::KeepAlive));
    }

    #[tokio::test]
    async fn choke() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_choke().await.expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::Choke));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_choke().await.expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::Choke));
    }

    #[tokio::test]
    async fn unchoke() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_unchoke().await.expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::Unchoke));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_unchoke().await.expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::Unchoke));
    }

    #[tokio::test]
    async fn intrested() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_interested().await.expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::Interested));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_interested().await.expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::Interested));
    }

    #[tokio::test]
    async fn notintrested() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_notinterested().await.expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::NotInterested));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_notinterested().await.expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::NotInterested));
    }

    #[tokio::test]
    async fn have() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_have(533).await.expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::Have(533)));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_have(533).await.expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        assert!(matches!(received, Message::Have(533)));
    }

    #[tokio::test]
    async fn bitfield() {
        let (mut peer1, mut peer2) = make_ends();
        let fields = rand::random::<[u8; 143]>();
        peer1
            .send_bitfield(&BitField::new(fields.clone().into()))
            .await
            .expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        let b = extract_enum!(received, Message::BitField);
        assert_eq!(b, BitField::new(fields.into()));

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        let fields = rand::random::<[u8; 143]>();
        p1w.send_bitfield(&BitField::new(fields.clone().into()))
            .await
            .expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        let b = extract_enum!(received, Message::BitField);
        assert_eq!(b, BitField::new(fields.into()));
    }

    #[tokio::test]
    async fn request() {
        let (mut peer1, mut peer2) = make_ends();
        let index = rand::random::<u32>();
        let begin = rand::random::<u32>();
        peer1
            .send_request(index, begin, 4)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        let r = extract_enum!(received, Message::Request);
        assert_eq!(
            r,
            Request {
                index: index,
                begin: begin,
                len: 4,
            }
        );
        // TODO: test long request

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_request(index, begin, 4)
            .await
            .expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        let r = extract_enum!(received, Message::Request);
        assert_eq!(
            r,
            Request {
                index: index,
                begin: begin,
                len: 4,
            }
        );
        // TODO: test long request
    }

    #[tokio::test]
    async fn piece() {
        let (mut peer1, mut peer2) = make_ends();
        let random_bytes = rand::random::<[u8; 143]>();
        let index = rand::random::<u32>();
        let begin = rand::random::<u32>();
        peer1
            .send_piece(index, begin, &random_bytes)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");
        let mut piece = extract_enum!(received, Message::Piece);
        assert_eq!(piece.index, index);
        assert_eq!(piece.begin, begin);
        assert_eq!(piece.len, random_bytes.len() as u32);

        let mut buf = [0u8; 143];
        let n = piece.read(&mut buf).await;
        assert!(matches!(n, Ok(143)));
        assert_eq!(random_bytes, buf);

        // TODO: test long piece are dropped

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        let random_bytes = rand::random::<[u8; 143]>();
        p1w.send_piece(index, begin, &random_bytes)
            .await
            .expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");

        let mut piece = extract_enum!(received, Message::Piece);
        assert_eq!(piece.index, index);
        assert_eq!(piece.begin, begin);
        assert_eq!(piece.len, random_bytes.len() as u32);

        let mut buf = [0u8; 143];
        let n = piece.read(&mut buf).await;
        assert!(matches!(n, Ok(143)));
        assert_eq!(random_bytes, buf);
        // TODO: test long piece are dropped
    }

    #[tokio::test]
    async fn cancel() {
        let (mut peer1, mut peer2) = make_ends();
        let index = rand::random::<u32>();
        let begin = rand::random::<u32>();
        peer1
            .send_cancel(index, begin, 4)
            .await
            .expect("should send ok");
        let received = peer2.recv_msg_header().await.expect("should recv ok");

        let msg = extract_enum!(received, Message::Cancel);
        assert_eq!(msg.begin, begin);
        assert_eq!(msg.index, index);
        assert_eq!(msg.len, 4);
        // TODO: test long request

        let ((_, mut p1w), (mut p2r, _)) = make_ends_split();
        p1w.send_cancel(index, begin, 4)
            .await
            .expect("should send ok");
        let received = p2r.recv_msg_header().await.expect("should recv ok");
        let msg = extract_enum!(received, Message::Cancel);
        assert_eq!(msg.begin, begin);
        assert_eq!(msg.index, index);
        assert_eq!(msg.len, 4);
        // TODO: test long request
    }

    #[tokio::test]
    async fn bi_direction() {
        let ((mut p1r, mut p1w), (mut p2r, mut p2w)) = make_ends_split();
        p1w.send_interested().await.expect("p1 should send ok");
        let p2_recv = p2r.recv_msg_header().await.expect("p2 should recv ok");
        p2w.send_choke().await.expect("p2 should send ok");
        let p1_recv = p1r.recv_msg_header().await.expect("p1 should recv ok");
        assert!(matches!(p2_recv, Message::Interested));
        assert!(matches!(p1_recv, Message::Choke));
    }
}

use std::mem::MaybeUninit;
use std::net::SocketAddr;
use tokio::io::{
    self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufStream, BufWriter,
};
use tokio::net;

pub struct BTStream<T> {
    inner: BufStream<T>,
}

pub struct ReadStream<T> {
    inner: BufReader<T>,
}

impl<T> ReadStream<T>
where
    T: AsyncRead + Unpin,
{
    fn new(t: T) -> Self {
        Self {
            inner: BufReader::new(t),
        }
    }
}

pub struct WriteStream<T> {
    inner: BufWriter<T>,
}
impl<T> WriteStream<T>
where
    T: AsyncWrite + Unpin,
{
    fn new(t: T) -> Self {
        Self {
            inner: BufWriter::new(t),
        }
    }
}

impl<T> BTStream<T> {
    pub async fn connect_tcp(peer_addr: SocketAddr) -> io::Result<BTStream<net::TcpStream>> {
        // TODO: fix type of peer_addr
        let tcp_stream = net::TcpStream::connect(peer_addr).await?;
        Ok(BTStream::<net::TcpStream> {
            inner: BufStream::new(tcp_stream),
        })
    }
}

impl BTStream<net::TcpStream> {
    pub fn into_split(
        self,
    ) -> (
        ReadStream<net::tcp::OwnedReadHalf>,
        WriteStream<net::tcp::OwnedWriteHalf>,
    ) {
        let (read_end, write_end) = self.inner.into_inner().into_split();
        (ReadStream::new(read_end), WriteStream::new(write_end))
    }
}

pub trait BTSend {
    async fn send_handshake(&mut self, h: &Handshake) -> io::Result<()>;

    async fn send_keepalive(&mut self) -> io::Result<()>;

    async fn send_choke(&mut self) -> io::Result<()>;

    async fn send_unchoke(&mut self) -> io::Result<()>;

    async fn send_interested(&mut self) -> io::Result<()>;

    async fn send_notinterested(&mut self) -> io::Result<()>;

    async fn send_have(&mut self, index: u32) -> io::Result<()>;

    async fn send_bitfield(&mut self, b: &BitField) -> io::Result<()>;

    async fn send_request(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()>;

    async fn send_piece(&mut self, index: u32, begin: u32, piece: &[u8]) -> io::Result<()>;

    async fn send_cancel(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()>;
}

pub trait BTRecv {
    async fn recv_msg(&mut self) -> io::Result<Message>;

    async fn recv_handshake(&mut self) -> io::Result<Handshake>;
}

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn send_handshake(&mut self, h: &Handshake) -> io::Result<()> {
        self.inner.write_u8(19).await?;
        self.inner.write_all(b"BitTorrent protocol").await?;
        self.inner.write_all(&h.reserved).await?;
        self.inner.write_all(&h.torrent_hash).await?;
        self.inner.write_all(&h.client_id).await?;
        self.inner.flush().await
    }

    pub async fn send_keepalive(&mut self) -> io::Result<()> {
        self.inner.write_u32(Message::KeepAlive.len()).await?;
        self.inner.flush().await
    }

    pub async fn send_choke(&mut self) -> io::Result<()> {
        self.inner.write_u32(Message::Choke.len()).await?;
        self.inner.write_u8(MsgTy::CHOKE).await?;
        self.inner.flush().await
    }

    pub async fn send_unchoke(&mut self) -> io::Result<()> {
        self.inner.write_u32(Message::Unchoke.len()).await?;
        self.inner.write_u8(MsgTy::UNCHOKE).await?;
        self.inner.flush().await
    }

    pub async fn send_interested(&mut self) -> io::Result<()> {
        self.inner.write_u32(Message::Interested.len()).await?;
        self.inner.write_u8(MsgTy::INTERESTED).await?;
        self.inner.flush().await
    }

    pub async fn send_notinterested(&mut self) -> io::Result<()> {
        self.inner.write_u32(Message::NotInterested.len()).await?;
        self.inner.write_u8(MsgTy::NOTINTERESTED).await?;
        self.inner.flush().await
    }

    pub async fn send_have(&mut self, index: u32) -> io::Result<()> {
        self.inner.write_u32(Message::Have(index).len()).await?;
        self.inner.write_u8(MsgTy::HAVE).await?;
        self.inner.write_u32(index).await?;
        self.inner.flush().await
    }

    pub async fn send_bitfield(&mut self, b: &BitField) -> io::Result<()> {
        self.inner.write_u32(1 + b.u8_len()).await?;
        self.inner.write_u8(BitField::TYPE).await?;
        self.inner.write_all(&b.bitfield_bytes()).await?;
        self.inner.flush().await
    }

    pub async fn send_request(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        // TODO: len must be 16KiB unless end of file
        self.inner
            .write_u32(Message::Request(Request { index, begin, len }).len())
            .await?; // length
        self.inner.write_u8(MsgTy::REQUEST).await?;
        self.inner.write_u32(index).await?;
        self.inner.write_u32(begin).await?;
        self.inner.write_u32(len).await?;
        self.inner.flush().await
    }

    pub async fn send_piece(&mut self, index: u32, begin: u32, piece: &[u8]) -> io::Result<()> {
        // TODO: len must be 16KiB unless end of file
        self.inner.write_u32(1 + 4 + 4 + piece.len() as u32).await?; // length
        self.inner.write_u8(MsgTy::PIECE).await?;
        self.inner.write_u32(index).await?;
        self.inner.write_u32(begin).await?;
        self.inner.write_all(piece).await?;
        self.inner.flush().await
    }

    pub async fn send_cancel(&mut self, index: u32, begin: u32, len: u32) -> io::Result<()> {
        // TODO: len must be 16KiB unless end of file
        self.inner
            .write_u32(Message::Cancel(Request { index, begin, len }).len())
            .await?; // length
        self.inner.write_u8(MsgTy::CANCEL).await?;
        self.inner.write_u32(index).await?;
        self.inner.write_u32(begin).await?;
        self.inner.write_u32(len).await?;
        self.inner.flush().await
    }
}

impl<T> BTStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn recv_msg(&mut self) -> io::Result<Message> {
        // TODO: what to do if some malicious peer sends a long len data
        // and a lot of garbage data? use timeout
        let len = self.inner.read_u32().await?;
        if len == 0 {
            return Ok(Message::KeepAlive);
        }
        let msg_ty = self.inner.read_u8().await?;
        match msg_ty {
            MsgTy::CHOKE => Ok(Message::Choke),
            MsgTy::UNCHOKE => Ok(Message::Unchoke),
            MsgTy::INTERESTED => Ok(Message::Interested),
            MsgTy::NOTINTERESTED => Ok(Message::NotInterested),
            MsgTy::HAVE => {
                // TODO: check length match, absorb remain length in case
                // unimplemented extension
                let index = self.inner.read_u32().await?;
                Ok(Message::Have(index))
            }
            MsgTy::BITFIELD => {
                let capacity = (len - 1) as usize;
                let mut bitfield: Vec<u8> = unsafe {
                    let mut piece: Vec<MaybeUninit<u8>> = Vec::with_capacity(capacity);
                    piece.set_len(capacity);
                    std::mem::transmute(piece)
                };
                self.inner.read_exact(bitfield.as_mut_slice()).await?;
                Ok(Message::BitField(BitField::new(bitfield)))
            }
            MsgTy::REQUEST => {
                // TODO: check length match
                let index = self.inner.read_u32().await?;
                let begin = self.inner.read_u32().await?;
                let len = self.inner.read_u32().await?;
                Ok(Message::Request(Request { index, begin, len }))
            }
            MsgTy::PIECE => {
                // TODO: check length match
                let capacity = (len - 4 - 4 - 1) as usize;
                let mut piece: Vec<u8> = unsafe {
                    let mut piece: Vec<MaybeUninit<u8>> = Vec::with_capacity(capacity);
                    piece.set_len(capacity);
                    std::mem::transmute(piece)
                };
                let index = self.inner.read_u32().await?;
                let begin = self.inner.read_u32().await?;
                self.inner.read_exact(piece.as_mut_slice()).await?;
                Ok(Message::Piece(Piece {
                    index,
                    begin,
                    piece,
                }))
            }
            MsgTy::CANCEL => {
                // TODO: check length match
                let index = self.inner.read_u32().await?;
                let begin = self.inner.read_u32().await?;
                let len = self.inner.read_u32().await?;
                Ok(Message::Cancel(Request { index, begin, len }))
            }
            _ => {
                panic!();
            }
        }
    }

    pub async fn recv_handshake(&mut self) -> io::Result<Handshake> {
        let mut header = [0u8; 19];
        let first = self.inner.read_u8().await?;
        if first != 19 {
            todo!();
        }
        self.inner.read_exact(&mut header).await?;
        if header != *b"BitTorrent protocol" {
            todo!();
        }
        let mut reserved = [0u8; 8];
        let mut torrent_hash = [0u8; 20];
        let mut client_id = [0u8; 20];
        self.inner.read_exact(&mut reserved).await?;
        self.inner.read_exact(&mut torrent_hash).await?;
        self.inner.read_exact(&mut client_id).await?;
        Ok(Handshake {
            reserved,
            torrent_hash,
            client_id,
        })
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
}

#[derive(Eq, Debug, PartialEq)]
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
}

impl Message {
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

#[derive(Debug, Eq, PartialEq)]
pub struct BitField {
    bitfield: Vec<u8>,
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
        let u8_index = (bit_index + 3) >> 2;
        let bit_offset = 7 - (bit_index % 8);
        self.bitfield[u8_index as usize] |= 1 << bit_offset;
    }

    pub fn unset(&mut self, bit_index: u32) {
        let u8_index = (bit_index + 3) >> 2;
        let bit_offset = 7 - (bit_index % 8);
        self.bitfield[u8_index as usize] &= !(1 << bit_offset);
    }

    pub fn get(&self, bit_index: u32) -> bool {
        let u8_index = (bit_index + 3) >> 2;
        let bit_offset = 7 - (bit_index % 8);
        self.bitfield[u8_index as usize] & (1 << bit_offset) != 0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Request {
    pub index: u32,
    pub begin: u32,
    pub len: u32,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Piece {
    pub index: u32,
    pub begin: u32,
    pub piece: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use tokio::io::{duplex, DuplexStream};

    fn make_ends() -> (BTStream<DuplexStream>, BTStream<DuplexStream>) {
        let (end1, end2) = duplex(1024 * 1024);
        (
            BTStream::<DuplexStream> {
                inner: tokio::io::BufStream::new(end1),
            },
            BTStream::<DuplexStream> {
                inner: tokio::io::BufStream::new(end2),
            },
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
    }

    #[tokio::test]
    async fn keepalive() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_keepalive().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(received, Message::KeepAlive);
    }

    #[tokio::test]
    async fn choke() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_choke().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(received, Message::Choke);
    }

    #[tokio::test]
    async fn unchoke() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_unchoke().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(received, Message::Unchoke);
    }

    #[tokio::test]
    async fn intrested() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_interested().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(received, Message::Interested);
    }

    #[tokio::test]
    async fn notintrested() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_notinterested().await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(received, Message::NotInterested);
    }

    #[tokio::test]
    async fn have() {
        let (mut peer1, mut peer2) = make_ends();
        peer1.send_have(533).await.expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(received, Message::Have(533));
    }

    #[tokio::test]
    async fn bitfield() {
        let (mut peer1, mut peer2) = make_ends();
        let fields = rand::random::<[u8; 143]>();
        peer1
            .send_bitfield(&BitField::new(fields.clone().into()))
            .await
            .expect("should send ok");
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(received, Message::BitField(BitField::new(fields.into())));
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
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(
            received,
            Message::Request(Request {
                index: index,
                begin: begin,
                len: 4,
            })
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
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(
            received,
            Message::Piece(Piece {
                index,
                begin,
                piece: random_bytes.into(),
            })
        );

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
        let received = peer2.recv_msg().await.expect("should recv ok");
        assert_eq!(
            received,
            Message::Cancel(Request {
                index: index,
                begin: begin,
                len: 4,
            })
        );
        // TODO: test long request
    }
}

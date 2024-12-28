use std::net::SocketAddr;
// use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net;
use tokio::time::{sleep, Duration};
mod protocol;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let mut bstream =
        protocol::BTStream::<net::TcpStream>::connect_tcp("192.168.71.36:57197".parse().unwrap())
            .await
            .unwrap();
    bstream.send_handshake(&HANDSHAKE).await;
    sleep(Duration::from_secs(1)).await;
    bstream.send_interested().await;
    sleep(Duration::from_secs(1)).await;
    bstream.send_request(0, 0, 1 << 14).await;
    sleep(Duration::from_secs(3)).await;
}

const HANDSHAKE: protocol::Handshake = protocol::Handshake {
    reserved: [0; 8],
    torrent_hash: [
        0x05, 0xb7, 0x49, 0x26, 0xfc, 0xb6, 0x0e, 0x28, 0x87, 0x02, 0xb4, 0x89, 0xc9, 0x99, 0x88,
        0x6d, 0x0d, 0x08, 0xcc, 0x90,
    ],
    client_id: *b"-ST0010-qwertyuiopas",
};

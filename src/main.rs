use std::net::SocketAddr;
// use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use anyhow::{anyhow, Result};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net;
use tokio::time::{sleep, Duration};
use tracing::{info, Level};
mod metadata;
mod picker;
mod protocol;
mod torrent_manager;

use protocol::BTStream;
use torrent_manager::TorrentManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    tracing_subscriber::fmt::init();
    let torrent_f = include_bytes!("../ubuntu-24.10-desktop-amd64.iso.torrent");
    let torrent = metadata::Metadata::load(torrent_f).unwrap();

    let mut tm = TorrentManager::new(torrent);
    tm.start().await;

    // let announce_req = metadata::TrackerGet {
    //     peer_id: "-ZS0405-qwerasdfzxcv",
    //     uploaded: 0,
    //     port: 35515,
    //     downloaded: 0,
    //     left: 0,
    //     ip: None,
    // };

    let listener = net::TcpListener::bind("::0:35515").await?;
    let server = tokio::spawn(async move {
        let mut set = tokio::task::JoinSet::new();
        loop {
            let (bt_stream, addr) = match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("input from addr {}", addr);
                    (protocol::BTStream::from(stream), addr)
                }
                Err(e) => {
                    info!("accept error {}", e);
                    continue;
                }
            };
            let handle = set.spawn(handle_income_connection(bt_stream, addr));
        }
        set.join_all().await;
    });
    server.await;

    Ok(())
}

async fn handle_income_connection<T: AsyncRead + AsyncWrite + Unpin>(
    mut bt_stream: BTStream<T>,
    addr: SocketAddr,
) -> Result<()> {
    let mut handshake = bt_stream.recv_handshake().await?;
    info!("get handshake {:?}", handshake);
    handshake.reserved = [0; 8];
    handshake.client_id = *CLIENT_ID;
    bt_stream.send_handshake(&handshake).await?;
    bt_stream.send_choke().await?;
    loop {
        let msg = bt_stream.recv_msg().await?;
        info!("received msg {:?} from {}", msg, addr);
    }
}

const CLIENT_ID: &[u8; 20] = b"-ZS0405-qwerasdfzxcv";

const HANDSHAKE: protocol::Handshake = protocol::Handshake {
    reserved: [0; 8],
    torrent_hash: [
        0x05, 0xb7, 0x49, 0x26, 0xfc, 0xb6, 0x0e, 0x28, 0x87, 0x02, 0xb4, 0x89, 0xc9, 0x99, 0x88,
        0x6d, 0x0d, 0x08, 0xcc, 0x90,
    ],
    client_id: *b"-ST0010-qwertyuiopas",
};

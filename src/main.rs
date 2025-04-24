use std::net::SocketAddr;
// use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{sleep, Duration};
use tokio::{net, time};
use tracing::{info, Level};
mod announce_manager;
mod connection_manager;
mod metadata;
mod picker;
mod protocol;
mod storage;
mod torrent_manager;
mod transmit_manager;

use protocol::{BTStream, Message};
use torrent_manager::{TorrentManagerHandle, TransmitManager};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    // let announce_req = metadata::TrackerGet {
    //     peer_id: "-ZS0405-qwerasdfzxcv",
    //     uploaded: 0,
    //     port: 35515,
    //     downloaded: 0,
    //     left: 0,
    //     ip: None,
    // };

    let torrent_f = include_bytes!("../ubuntu-24.10-desktop-amd64.iso.torrent");
    let torrent = metadata::FileMetadata::load(torrent_f).unwrap();

    let (metadata, announce_list) = torrent.to_metadata();
    let metadata_clone = metadata.clone();
    info!("{:?}", &announce_list);

    let ready = Arc::new(tokio::sync::Notify::new());
    let wait_ready = ready.clone();
    let listener = net::TcpListener::bind("::0:35515").await?;
    let server = tokio::spawn(async move {
        let mut set = tokio::task::JoinSet::new();
        ready.notify_one();
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
            let handle = set.spawn(handle_income_connection(
                bt_stream,
                addr,
                metadata_clone.clone(),
            ));
        }
        set.join_all().await;
    });
    tokio::spawn(server);

    // let mut tm = TransmitManager::new(metadata).with_announce_list(announce_list);
    let mut tm = TorrentManagerHandle::new(Arc::new(metadata));
    // tm.send_announce_msg(announce_manager::Msg::AddUrl(announce_list[0].clone()));
    // tm.send_announce_msg(announce_manager::Msg::AddUrl(announce_list[1].clone()));
    wait_ready.notified().await;

    if let Ok(mut conn) = protocol::BTStream::connect_tcp("127.0.0.1:35515".parse().unwrap()).await
    {
        conn.send_handshake(&HANDSHAKE).await?;
        conn.recv_handshake().await?;
        tm.send_msg(transmit_manager::Msg::NewPeer(conn));
    }
    // tm.start().await;
    time::sleep(Duration::from_secs(500)).await;
    // tm.send_announce_msg(announce_manager::Msg::RemoveUrl(
    //     announce_list[0][0].clone(),
    // ));
    // tm.send_announce_msg(announce_manager::Msg::RemoveUrl(
    //     announce_list[1][0].clone(),
    // ));
    // time::sleep(Duration::from_secs(1000)).await;
    println!("before wait close");
    tm.wait_close().await;
    println!("after wait close");
    Ok(())
}

async fn handle_income_connection<T>(
    mut bt_stream: BTStream<T>,
    addr: SocketAddr,
    metadata: metadata::Metadata,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin + std::fmt::Debug, // TODO: maybe remove this Debug
{
    let mut handshake = bt_stream.recv_handshake().await?;
    info!("get handshake {:?}", handshake);
    handshake.reserved = [0; 8];
    handshake.client_id = *CLIENT_ID;
    bt_stream.send_handshake(&handshake).await?;
    info!("handshake sent");
    bt_stream
        .send_bitfield(&protocol::BitField::new(vec![0xffu8; 3000]))
        .await?;
    info!("bitfield sent");
    bt_stream.send_choke().await?;
    bt_stream.send_keepalive().await?;
    bt_stream.send_keepalive().await?;
    bt_stream.send_keepalive().await?;
    info!("all keep alive sent");
    loop {
        let msg = bt_stream.recv_msg_header().await?;
        match msg {
            Message::Request(r) => {
                bt_stream
                    .send_piece(r.index, r.begin, &vec![0x00; r.len as usize])
                    .await;
            }
            _ => {
                info!("received msg {:?} from {}", msg, addr);
            }
        }
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

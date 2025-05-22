use std::net::SocketAddr;
// use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{sleep, Duration};
use tokio::{net, time};
use tracing::{info, warn, Level};

mod announce_manager;
mod backfile;
mod bandwidth;
mod connection_manager;
mod metadata;
mod picker;
mod protocol;
mod storage;
mod torrent_manager;
mod transmit_manager;

use protocol::{BTStream, Handshake, Message};
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
    // let torrent_f = include_bytes!("../c8.torrent");
    let torrent = metadata::FileMetadata::load(torrent_f).unwrap();

    let (metadata, announce_list) = torrent.to_metadata();
    let metadata_clone = metadata.clone();
    info!("{:?}", &announce_list);

    let ready = Arc::new(tokio::sync::Notify::new());
    let wait_ready = ready.clone();
    {
        let metadata_clone = metadata.clone();
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
                let total = metadata_clone.info.pieces.len() / 20;
                let handle = set.spawn(handle_income_connection(
                    bt_stream,
                    addr,
                    metadata_clone.clone(),
                    // vec![vec![true; 1], vec![false; total - 1]]
                    //     .into_iter()
                    //     .flatten()
                    //     .collect(),
                    vec![vec![false; total / 2], vec![true; total - total / 2]]
                        .into_iter()
                        .flatten()
                        .collect(),
                ));
            }
            set.join_all().await;
        });
    }
    let ready2 = Arc::new(tokio::sync::Notify::new());
    let wait_ready2 = ready2.clone();
    {
        let metadata_clone = metadata.clone();
        let listener = net::TcpListener::bind("::0:35516").await?;
        let server = tokio::spawn(async move {
            let mut set = tokio::task::JoinSet::new();
            ready2.notify_one();
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
                let total = metadata_clone.info.pieces.len() / 20;
                let handle = set.spawn(handle_income_connection(
                    bt_stream,
                    addr,
                    metadata_clone.clone(),
                    vec![vec![true; total / 2], vec![false; total - total / 2]]
                        .into_iter()
                        .flatten()
                        .collect(),
                ));
            }
            set.join_all().await;
        });
    }

    let info_hash = metadata.info_hash;
    // let mut tm = TransmitManager::new(metadata).with_announce_list(announce_list);
    let mut tm = TorrentManagerHandle::new(Arc::new(metadata));
    info!("{announce_list:?}");
    for addr in announce_list {
        tm.send_announce_msg(announce_manager::Msg::AddUrl(addr));
    }
    wait_ready.notified().await;
    wait_ready2.notified().await;

    {
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        // process input message
        hasher.update(&[0u8; 100000]);
    }

    // if let Ok(mut conn) =
    //     protocol::BTStream::connect_tcp("192.168.71.36:62227".parse().unwrap()).await
    // // protocol::BTStream::connect_tcp("127.0.0.1:35515".parse().unwrap()).await
    // {
    //     info!("{info_hash:?}");
    //     conn.send_handshake(&Handshake {
    //         reserved: [0u8; 8],
    //         client_id: HANDSHAKE.client_id,
    //         torrent_hash: info_hash,
    //     })
    //     .await?;
    //     conn.recv_handshake().await?;
    //     tm.send_msg(transmit_manager::Msg::NewPeer(conn));
    // }
    // if let Ok(mut conn) =
    //     // protocol::BTStream::connect_tcp("192.168.71.36:62227".parse().unwrap()).await
    //     protocol::BTStream::connect_tcp("127.0.0.1:35516".parse().unwrap()).await
    // {
    //     info!("{info_hash:?}");
    //     conn.send_handshake(&Handshake {
    //         reserved: [0u8; 8],
    //         client_id: HANDSHAKE.client_id,
    //         torrent_hash: info_hash,
    //     })
    //     .await?;
    //     conn.recv_handshake().await?;
    //     tm.send_msg(transmit_manager::Msg::NewPeer(conn));
    // }
    time::sleep(Duration::from_secs(100000)).await;
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
    field: Vec<bool>,
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
    let bitfield_total = (metadata.info.pieces.len() / 20).div_ceil(8);
    info!("{bitfield_total}");
    assert_eq!(field.len(), metadata.info.pieces.len() / 20);
    bt_stream
        .send_bitfield(&protocol::BitField::from(field))
        .await?;
    info!("bitfield sent");
    bt_stream.send_keepalive().await?;
    bt_stream.send_keepalive().await?;
    bt_stream.send_keepalive().await?;
    info!("all keep alive sent");
    let mut ticker5 = tokio::time::interval(time::Duration::from_secs(3));
    let mut ticker1 = tokio::time::interval(time::Duration::from_millis(1000));
    bt_stream.send_unchoke().await;
    let limit = 200;
    let mut accum = 0;
    let mut choked = false;
    loop {
        tokio::select! {
            msg = bt_stream.recv_msg_header() => {
                match msg {
                    Ok(m) => match m {
                        Message::Request(r) => {
                            if !choked && accum < limit {
                                info!("response");
                            } else {
                                let _ = ticker1.tick().await;
                                accum = 0;
                            }
                            let a = [0u8; 16384];
                            accum += 1;
                            bt_stream
                                .send_piece(r.index, r.begin, &a)
                                .await;
                        }
                        _ => {
                            info!("received msg {:?} from {}", m, addr);
                        }
                    }
                    Err(e) => {
                        warn!("main mock close conn {} {e}", addr);
                        return Err(e.into());
                    }
                }
            }
            _ = ticker5.tick() => {
                choked = rand::random();
                if !choked {
                    info!("main {addr} unchoke");
                    bt_stream.send_unchoke().await;
                } else {
                    info!("main {addr} choke");
                    bt_stream.send_choke().await;
                }
            }
            _ = ticker1.tick() => {
                info!("in this period, {accum} blocks transferred");
                accum = 0;
            }
        };
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

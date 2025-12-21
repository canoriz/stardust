use std::net::SocketAddr;
// use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::Duration;
use tokio::{net, time};
use tracing::{error, info, warn};

use crate::dht::{self, DHT};
use crate::metadata::Magnet;
use crate::protocol::{self, BTStream, HandshakeOption, Message, Reunite, Split};
use crate::torrent_manager::TorrentManagerHandle;
use crate::transmit_manager::{self, TorrentTask, TransmitDump};
use crate::{announce_manager, metadata};

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const SELF_ID: [u8; 20] = *b"-TR0300-fjbo402nczk3";
    const SELF_PORT: u16 = 41773;
    const DHT_PORT: u16 = 41773;
    let dht_client = Arc::new(DHT::new(SELF_ID, DHT_PORT, "ST01".into()));
    _ = dht_client
        .ping_rpc(
            dht::RpcAddr::NoID(
                "[240e:b8f:5c68:8400:4c07:3e69:7b5a:741]:54032"
                    .parse()
                    .unwrap(),
            ),
            time::Duration::from_secs(5),
        )
        .await;
    // dht_client.find_closest_node_to(SELF_ID, true).await;

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

    let torrent_f = include_bytes!("../0922.torrent");
    // let torrent_f = include_bytes!("../31.torrent");
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
                let total = metadata_clone.info.pieces.len() / 20;
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!("input from addr {}", addr);
                        set.spawn(handle_income_connection(
                            stream,
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
                    Err(e) => {
                        info!("accept error {}", e);
                    }
                };
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
                let total = metadata_clone.info.pieces.len() / 20;
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        info!("input from addr {}", addr);
                        set.spawn(handle_income_connection(
                            stream,
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
                    Err(e) => {
                        info!("accept error {}", e);
                    }
                }
            }
            set.join_all().await;
        });
    }

    // let mut tm = TransmitManager::new(metadata).with_announce_list(announce_list);
    let magnet: Magnet = "magnet:?xt=urn:btih:0922fbc30ee19ed501370c98cd42c952fbe6f890&tr=http%3a%2f%2ft.nyaatracker.com%2fannounce&tr=http%3a%2f%2ftracker.kamigami.org%3a2710%2fannounce&tr=http%3a%2f%2fshare.camoe.cn%3a8080%2fannounce&tr=http%3a%2f%2fopentracker.acgnx.se%2fannounce&tr=http%3a%2f%2fanidex.moe%3a6969%2fannounce&tr=http%3a%2f%2ft.acg.rip%3a6699%2fannounce&tr=https%3a%2f%2ftr.bangumi.moe%3a9696%2fannounce&tr=udp%3a%2f%2ftr.bangumi.moe%3a6969%2fannounce&tr=http%3a%2f%2fopen.acgtracker.com%3a1096%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce".parse()?;
    let trackers = magnet.tr.clone();
    let mut tm = TorrentManagerHandle::new(
        // TorrentTask::Magnet(magnet),
        TorrentTask::Torrent(metadata),
        SELF_ID,
        SELF_PORT,
        Some(dht_client),
        // None,
    );
    // info!("{announce_list:?}");
    // if let Some(addr) = trackers {
    //     tm.send_announce_msg(announce_manager::Msg::AddUrl(addr));
    // }
    wait_ready.notified().await;
    wait_ready2.notified().await;

    {
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        // process input message
        hasher.update([0u8; 100000]);
    }

    // if let Ok(mut conn) =
    //     protocol::BTStream::connect_tcp("192.168.71.36:56089".parse().unwrap()).await
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
    // time::sleep(Duration::from_secs(100000)).await;
    // tm.send_announce_msg(announce_manager::Msg::RemoveUrl(
    //     announce_list[0][0].clone(),
    // ));
    // tm.send_announce_msg(announce_manager::Msg::RemoveUrl(
    //     announce_list[1][0].clone(),
    // ));
    // time::sleep(Duration::from_secs(1000)).await;
    println!("before wait close");
    // tm.stop_wait().await;
    let r = tm.check().await;
    println!("check result {:?}", r);
    let progress: TransmitDump = serde_json::from_slice(include_bytes!("./dump.json")).unwrap();
    tm.load_progress(progress).await;
    let dump = tm.dump_progress().await;
    println!("dump result {:?}", dump);
    tm.change_state(transmit_manager::RunningCmd::Resume).await;
    // tm.stop_wait().await;
    loop {
        time::sleep(Duration::from_secs(20)).await;
        tm.dump_progress().await;
    }
    println!("after wait close");
    Ok(())
}

async fn handle_income_connection<T>(
    mut raw_conn: T,
    addr: SocketAddr,
    metadata: metadata::Metadata,
    field: Vec<bool>,
) -> Result<()>
where
    T: AsyncRead + AsyncWrite + Split + Unpin + std::fmt::Debug, // TODO: maybe remove this Debug
    <T as Split>::R: Reunite<W = <T as Split>::W, U = T>,
{
    let client_id = [
        0x54, 0x42, 0x54, 0x69, 0x21, 0x58, 0x21, 0x58, 0x68, 0x69, 0x93, 0x51, 0x54, 0x42, 0x54,
        0x69, 0x21, 0x58, 0x21, 0x58,
    ];
    let opt = HandshakeOption::builder()
        .client_id(client_id)
        .client_version("1".into())
        .pex(true)
        .metadata(true)
        .info_hash([0; 20])
        .dht_port(None)
        .build();

    let mut bt_stream = protocol::BTStream::accept(raw_conn, opt).await?;
    info!("handshake done");

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
    let ticker5 = tokio::time::interval(time::Duration::from_secs(3));
    let mut ticker1 = tokio::time::interval(time::Duration::from_millis(1000));
    bt_stream.send_unchoke().await;
    let limit = 10;
    let mut accum = 0;
    let choked = false;
    const A: [u8; 16384] = [0u8; 16384];
    loop {
        tokio::select! {
            msg = bt_stream.recv_msg() => {
                match msg {
                    Ok(m) => match m {
                        Message::Request(r) => {
                            if !choked && accum < limit {
                                info!("response");
                            } else {
                                let _ = ticker1.tick().await;
                                accum = 0;
                            }
                            accum += 1;
                            bt_stream
                                .send_piece(r.index, r.begin, &A[..(r.len as usize)])
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
            // _ = ticker5.tick() => {
            //     choked = rand::random();
            //     if !choked {
            //         info!("main {addr} unchoke");
            //         bt_stream.send_unchoke().await;
            //     } else {
            //         info!("main {addr} choke");
            //         bt_stream.send_choke().await;
            //     }
            // }
            _ = ticker1.tick() => {
                info!("in this period, {accum} blocks transferred");
                accum = 0;
            }
        };
    }
}

const CLIENT_ID: &[u8; 20] = b"-ZS0405-qwerasdfzxcv";

const HANDSHAKE: protocol::Handshake = protocol::Handshake {
    reserved: protocol::FuncBits::none().set_extension(),
    torrent_hash: [
        0x05, 0xb7, 0x49, 0x26, 0xfc, 0xb6, 0x0e, 0x28, 0x87, 0x02, 0xb4, 0x89, 0xc9, 0x99, 0x88,
        0x6d, 0x0d, 0x08, 0xcc, 0x90,
    ],
    client_id: *b"-ST0010-qwertyuiopas",
};

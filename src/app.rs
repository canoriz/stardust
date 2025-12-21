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
    dht_client.find_closest_node_to(SELF_ID, true).await;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    let torrent_f = include_bytes!("../0922.torrent");
    let torrent = metadata::FileMetadata::load(torrent_f).unwrap();

    let (metadata, announce_list) = torrent.to_metadata();

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

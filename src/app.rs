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
use crate::session::Session;
use crate::torrent_manager::TorrentManagerHandle;
use crate::transmit_manager::{self, RunningCmd, TorrentTask, TransmitDump};
use crate::{announce_manager, metadata};

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const SELF_ID: [u8; 20] = *b"-TR0300-fjbo402nczk3";
    const SELF_PORT: u16 = 41773;
    const DHT_PORT: u16 = 41774;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    let mut session = Session::new(SELF_ID, SELF_PORT, DHT_PORT);
    // let torrent_f = include_bytes!("../w.pcnp.torrent");
    // let torrent = metadata::FileMetadata::load(torrent_f).unwrap();
    // let (metadata, announce_list) = torrent.to_metadata();

    let magnet: Magnet = ("magnet:?xt=urn:btih:f58725384a5705aec262390daedde7804fcdf38e".to_string()
        + "&tr=http%3a%2f%2ft.nyaatracker.com%2fannounce&tr=http%3a%2f%2ftracker.kamigami.org"
        + "%3a2710%2fannounce&tr=http%3a%2f%2fshare.camoe.cn%3a8080%2fannounce&"
        + "tr=http%3a%2f%2fopentracker.acgnx.se%2fannounce&tr=http%3a%2f%2fanidex.moe%3a6969%2f"
        + "announce&tr=http%3a%2f%2ft.acg.rip%3a6699%2fannounce&tr=https%3a%2f%2ftr.bangumi.moe"
        + "%3a9696%2fannounce&tr=udp%3a%2f%2ftr.bangumi.moe%3a6969%2fannounce&tr="
        + "http%3a%2f%2fopen.acgtracker.com%3a1096%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org"
        + "%3a1337%2fannounce")
        .parse()
        .unwrap();
    let info_hash = magnet.info_hash;
    session
        .add_torrent(TorrentTask::Magnet(magnet), vec![])
        .await;
    session
        .do_work(&info_hash, async |tm| {
            tm.change_state(RunningCmd::Resume).await;
        })
        .await;

    time::sleep(Duration::from_secs(2000)).await;
    Ok(())
}

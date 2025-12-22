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
use crate::transmit_manager::{self, TorrentTask, TransmitDump};
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
    let torrent_f = include_bytes!("../w.pcnp.torrent");
    let torrent = metadata::FileMetadata::load(torrent_f).unwrap();
    let (metadata, announce_list) = torrent.to_metadata();

    session.add_torrent(TorrentTask::Torrent(metadata), announce_list);

    time::sleep(Duration::from_secs(2000)).await;
    Ok(())
}

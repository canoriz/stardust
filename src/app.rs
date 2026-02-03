use anyhow::Result;
use tokio::time::{self, Duration};
use tracing::info;

use crate::metadata::{self, Magnet};
use crate::session::{Session, SessionOpt};
use crate::transmit_manager::{RunningCmd, TorrentTask};

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const SELF_ID: [u8; 20] = *b"-TR0300-fjbo402nczk3";
    const SELF_PORT: u16 = 41773;
    const DHT_PORT: u16 = 41774;

    tracing_subscriber::registry();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    let mut session = Session::new(
        SessionOpt::builder()
            // .maybe_dht_port(None)
            .dht_port(DHT_PORT)
            .port(SELF_PORT)
            .self_id(SELF_ID)
            .build(),
    );
    // let torrent_f = include_bytes!("../test-large.torrent");
    // let torrent = metadata::FileMetadata::load(torrent_f).unwrap();
    // let (metadata, announce_list) = torrent.to_metadata();
    // let info_hash = metadata.info_hash;

    let magnet: Magnet = ("magnet:?xt=urn:btih:acdb329299ad80c76ad8a9f073371df8d1c3d991&tr=http%3a%2f%2ft.nyaatracker.com%2fannounce&tr=http%3a%2f%2ftracker.kamigami.org%3a2710%2fannounce&tr=http%3a%2f%2fshare.camoe.cn%3a8080%2fannounce&tr=http%3a%2f%2fopentracker.acgnx.se%2fannounce&tr=http%3a%2f%2fanidex.moe%3a6969%2fannounce&tr=http%3a%2f%2ft.acg.rip%3a6699%2fannounce&tr=https%3a%2f%2ftr.bangumi.moe%3a9696%2fannounce&tr=udp%3a%2f%2ftr.bangumi.moe%3a6969%2fannounce&tr=http%3a%2f%2fopen.acgtracker.com%3a1096%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce")
        .parse()
        .unwrap();
    let info_hash = magnet.info_hash;
    session
        .add_torrent(TorrentTask::Magnet(magnet), vec![])
        // .add_torrent(TorrentTask::Torrent(metadata), vec![vec!["1".into()]])
        // .add_torrent(TorrentTask::Torrent(metadata), vec![vec![]])
        // .add_torrent(TorrentTask::Torrent(metadata), announce_list)
        .await;
    session
        .do_work(&info_hash, async |tm| {
            // TODO: this is ugly though, only sender can clone
            // tm.check().await;
            tm.change_state(RunningCmd::Resume).await;
            tm.wait_downloaded().await;
        })
        .await;
    if let Some(mut tm) = session.remove_torrent(&info_hash).await {
        tm.wait_downloaded().await;
        info!("stopped");
    }
    info!("after");
    Ok(())
}

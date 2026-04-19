use anyhow::Result;
use tokio::time::{self, Duration};
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::metadata::{self, Magnet};
use crate::session::{Session, SessionOpt};
use crate::transmit_manager::{RunningCmd, TorrentTask};

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("{}", std::process::id());
    const SELF_ID: [u8; 20] = *b"-TR0300-fjbo402nczk3";
    const SELF_PORT: u16 = 41773;
    const DHT_PORT: u16 = 41774;

    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::registry();
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .with_span_events(FmtSpan::CLOSE)
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    #[cfg(feature = "mock_delay")]
    let (mut session, info_hash) = {
        let mut session = Session::new(
            SessionOpt::builder()
                .maybe_dht_port(None)
                .port(SELF_PORT)
                .self_id(SELF_ID)
                .build(),
        );
        let torrent_f = include_bytes!("../tutu.torrent");
        let torrent = metadata::FileMetadata::load(torrent_f).unwrap();
        let (metadata, announce_list) = torrent.to_metadata();
        let info_hash = metadata.info_hash;
        session
            // .add_torrent(TorrentTask::Magnet(magnet), vec![])
            .add_torrent(TorrentTask::Torrent(metadata), vec![vec!["1".into()]])
            .await;
        (session, info_hash)
    };
    #[cfg(not(feature = "mock_delay"))]
    let (mut session, info_hash) = {
        let mut session = Session::new(
            SessionOpt::builder()
                // .maybe_dht_port(None)
                .dht_port(DHT_PORT)
                .port(SELF_PORT)
                .self_id(SELF_ID)
                .build(),
        );

        let magnet = true;
        if magnet {
            let magnet: Magnet = ("magnet:?xt=urn:btih:e3cdec4b5d4699de1f7df866c04418d689ba0647&tr=http%3a%2f%2ft.nyaatracker.com%2fannounce&tr=http%3a%2f%2ftracker.kamigami.org%3a2710%2fannounce&tr=http%3a%2f%2fshare.camoe.cn%3a8080%2fannounce&tr=http%3a%2f%2fopentracker.acgnx.se%2fannounce&tr=http%3a%2f%2fanidex.moe%3a6969%2fannounce&tr=http%3a%2f%2ft.acg.rip%3a6699%2fannounce&tr=https%3a%2f%2ftr.bangumi.moe%3a9696%2fannounce&tr=udp%3a%2f%2ftr.bangumi.moe%3a6969%2fannounce&tr=http%3a%2f%2fopen.acgtracker.com%3a1096%2fannounce&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce")
        .parse()
        .unwrap();
            let info_hash = magnet.info_hash;
            println!("magnet: {magnet:?}");
            session
                .add_torrent(TorrentTask::Magnet(magnet), vec![])
                .await;
            (session, info_hash)
        } else {
            let torrent_f = include_bytes!("../hikari.torrent");
            let torrent = metadata::FileMetadata::load(torrent_f).unwrap();
            let (metadata, announce_list) = torrent.to_metadata();
            let info_hash = metadata.info_hash;
            session
                .add_torrent(TorrentTask::Torrent(metadata), announce_list)
                .await;
            (session, info_hash)
        }
    };

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

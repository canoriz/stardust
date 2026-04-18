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
            let magnet: Magnet = ("magnet:?xt=urn:btih:bb471037d5ef5ab9b3b426b5094e3dee9bfbb8aa&dn=MFYD-128&xl=5200222742&tr=http://sukebei.tracker.wf:8888/announce&tr=udp://tracker.archlinux.org.theoks.net:6969/announce&tr=udp://tracker.openbittorrent.com:6969&tr=http://tracker.tasvideos.org:6969/announce&tr=udp://tracker.leech.ie:1337/announce&tr=udp://tracker.opentrackr.org:1337/announce&tr=udp://tracker.coppersurfer.tk:6969/announce&tr=udp://tracker.internetwarriors.net:1337&tr=udp://tracker.internetwarriors.net:1337/announce&tr=udp://open.stealth.si:80/announce&tr=http://anidex.moe:6969/announce&tr=http://freerainbowtables.com:6969/announce&tr=http://www.freerainbowtables.com:6969/announce&tr=http://tracker2.itzmx.com:6961/announce&tr=http://tracker.etree.org:6969/announce&tr=http://www.thetradersden.org/forums/tracker:80/announce.php&tr=udp://udp-tracker.shittyurl.org:6969/announce&tr=https://tracker.shittyurl.org/announce&tr=http://tracker.shittyurl.org/announce&tr=udp://bt.firebit.org:2710/announce&tr=http://bt.firebit.org:2710/announce&tr=udp://exodus.desync.com:6969/announce&tr=udp://tracker.torrent.eu.org:451/announce")
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

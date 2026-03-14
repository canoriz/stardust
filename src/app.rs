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
    // let torrent_f = include_bytes!("../tutu.torrent");
    // let torrent = metadata::FileMetadata::load(torrent_f).unwrap();
    // let (metadata, announce_list) = torrent.to_metadata();
    // let info_hash = metadata.info_hash;

    let magnet: Magnet = ("magnet:?xt=urn:btih:81166d42bdd0db49f8f5f61805dd78b86f186688&dn=SNOS-152&xl=6562547911&tr=http://sukebei.tracker.wf:8888/announce&tr=udp://tracker.archlinux.org.theoks.net:6969/announce&tr=udp://tracker.openbittorrent.com:6969&tr=http://tracker.tasvideos.org:6969/announce&tr=udp://tracker.leech.ie:1337/announce&tr=udp://tracker.opentrackr.org:1337/announce&tr=udp://tracker.coppersurfer.tk:6969/announce&tr=udp://tracker.internetwarriors.net:1337&tr=udp://tracker.internetwarriors.net:1337/announce&tr=udp://open.stealth.si:80/announce&tr=http://anidex.moe:6969/announce&tr=http://freerainbowtables.com:6969/announce&tr=http://www.freerainbowtables.com:6969/announce&tr=http://tracker2.itzmx.com:6961/announce&tr=http://tracker.etree.org:6969/announce&tr=http://www.thetradersden.org/forums/tracker:80/announce.php&tr=udp://udp-tracker.shittyurl.org:6969/announce&tr=https://tracker.shittyurl.org/announce&tr=http://tracker.shittyurl.org/announce&tr=udp://bt.firebit.org:2710/announce&tr=http://bt.firebit.org:2710/announce&tr=udp://exodus.desync.com:6969/announce&tr=udp://tracker.torrent.eu.org:451/announce")
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

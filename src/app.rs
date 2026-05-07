/// Add the hardcoded test torrent to `session` for mock-delay testing.
///
/// Only compiled when the `mock_delay` Cargo feature is enabled.
#[cfg(feature = "mock_delay")]
pub async fn add_mock_torrent(session: &crate::session::Session) -> [u8; 20] {
    use crate::metadata;
    use crate::transmit_manager::TorrentTask;

    let torrent_f = include_bytes!("../test-large.torrent");
    let torrent = metadata::FileMetadata::load(torrent_f).unwrap();
    let (metadata, _announce_list) = torrent.to_metadata();
    let info_hash = metadata.info_hash;
    match session.transmit_handle_of(&info_hash).await {
        Some(mut t) => {
            let _wait_check_finish = t.check().unwrap();
        }
        None => {
            session
                .add_torrent(TorrentTask::Torrent(metadata), vec![vec!["1".into()]])
                .await;
        }
    }
    info_hash
}

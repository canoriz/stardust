use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use stardust::metadata;
use stardust::protocol::{self, AcceptOpt, BitField, HandshakeOption, Message, RecvResult};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Configure these paths here (expands HOME automatically)
    let home = std::env::var("HOME").unwrap_or_default();
    let file_path = format!("{}/Downloads/ubuntu-25.10-desktop-amd64.iso", home);
    let torrent_path = "./test-large.torrent";

    info!(
        "serving file: {} using torrent: {}",
        file_path, torrent_path
    );

    let torrent_bytes = std::fs::read(torrent_path)?;
    let filemeta = metadata::FileMetadata::load(&torrent_bytes)?;
    let (meta, _announce) = filemeta.to_metadata();
    let meta = Arc::new(meta);

    // Build handshake option
    let client_id = *b"-MOCKPEER-EXISTING01"; // 20 bytes
    let opt = HandshakeOption::builder()
        .client_id(client_id)
        .client_version("mock-1".into())
        .pex(false)
        .metadata(true)
        .dht_port(None)
        .build();

    // Listen
    let listener = TcpListener::bind("0.0.0.0:6881").await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        let meta = meta.clone();
        let file_path = file_path.clone();
        let opt = opt.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, addr, meta, file_path, opt).await {
                eprintln!("connection {} error: {}", addr, e);
            }
        });
    }
}

async fn handle_conn(
    raw_conn: tokio::net::TcpStream,
    addr: SocketAddr,
    meta: Arc<metadata::Metadata>,
    file_path: String,
    opt: HandshakeOption,
) -> Result<()> {
    // Accept and provide metadata to the peer. keep a separate Arc clone for the accept closure
    let meta_for_accept = meta.clone();
    let accept_fn =
        async move |_: &protocol::InfoHash| AcceptOpt::HaveMetadata(meta_for_accept.clone());

    let bt_stream: protocol::BTStream<tokio::net::TcpStream> =
        protocol::BTStream::accept(raw_conn, accept_fn, opt).await?;
    info!("handshake done from {}", addr);

    // split into read and write halves so we can pipeline responses
    let (mut read_stream, mut write_stream) = bt_stream.split_buffered();

    // Immediately unchoke so the peer can request pieces right away.
    write_stream.send_unchoke().await?;

    // advertise that we have all pieces
    let total = meta.total_pieces();
    let mut have_field = vec![false; total];
    for i in 0..total {
        have_field[i] = true;
    }
    write_stream
        .send_bitfield(&BitField::from(have_field))
        .await?;
    write_stream.send_unchoke().await?;

    // channel for queued responses: (index, begin, data)
    let (tx, mut rx) = mpsc::unbounded_channel::<(u32, u32, Vec<u8>)>();

    // writer task: consumes queued responses and sends immediately
    tokio::spawn(async move {
        while let Some((index, begin, buf)) = rx.recv().await {
            if let Err(e) = write_stream.send_piece(index, begin, &buf).await {
                info!("failed to send piece {}@{}: {}", index, begin, e);
                break;
            } else {
                info!("sent piece {} begin {}", index, begin);
            }
        }
        info!("writer task ending for {}", addr);
    });

    // main read loop: on Request, spawn a task that waits 3s then enqueues the response
    loop {
        let m = match read_stream.recv_msg_header().await {
            Ok(RecvResult::Message(m)) => m,
            Ok(RecvResult::PiecePending { len, .. }) => {
                // seeder doesn't receive pieces; just discard the body and continue
                let mut discard = bytes::BytesMut::with_capacity(len as usize);
                if read_stream.recv_piece_body(&mut discard).await.is_err() {
                    return Ok(());
                }
                continue;
            }
            Err(e) => {
                info!("peer {} closed: {}", addr, e);
                return Ok(());
            }
        };
        match m {
                Message::Request(r) => {
                    let tx = tx.clone();
                    let file_path = file_path.clone();
                    let meta = meta.clone();
                    let peer = addr;
                    info!("sent piece {} begin {} after 5 sec", r.index, r.begin);
                    tokio::spawn(async move {
                        // compute piece size and file offset
                        let piece_len = meta.regular_piece_size() as u64;
                        let offset = (r.index as u64) * piece_len + (r.begin as u64);
                        let expected_len = r.len as usize;

                        // open and read file for this request
                        match tokio::fs::File::open(&file_path).await {
                            Ok(mut f) => {
                                if let Err(e) = f.seek(SeekFrom::Start(offset)).await {
                                    info!("seek error {} for {}: {}", peer, file_path, e);
                                    return;
                                }
                                let mut buf = vec![0u8; expected_len];
                                if let Err(e) = f.read_exact(&mut buf).await {
                                    info!("read error {} for {}: {}", peer, file_path, e);
                                    return;
                                }

                                // wait 3 seconds, then enqueue the prepared piece
                                sleep(Duration::from_secs(3)).await;
                                let _ = tx.send((r.index, r.begin, buf));
                            }
                            Err(e) => {
                                info!("open file {} error: {}", file_path, e);
                            }
                        }
                    });
                }
                other => {
                    info!("received {:?} from {}", other, addr);
                }
        }
    }
}

use clap::Parser;
use stardust::{Session, SessionDump, SessionOpt};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

const SELF_ID: [u8; 20] = *b"-TR3000-fjbo402nczk3";

#[derive(Parser)]
#[command(about = "Stardust BitTorrent client")]
struct Args {
    /// TCP listen port for incoming peer connections
    #[arg(long, default_value_t = 41773)]
    port: u16,

    /// UDP port for DHT; omit to disable DHT
    #[arg(long)]
    dht_port: Option<u16>,

    /// Path to a session file (bencode); restored on start and overwritten on shutdown
    #[arg(long)]
    session: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
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

    let opt = SessionOpt::builder()
        .self_id(SELF_ID)
        .port(args.port)
        .maybe_dht_port(args.dht_port)
        .build();

    let session = match &args.session {
        Some(path) if path.exists() => {
            let data = std::fs::read_to_string(path)?;
            let dump: SessionDump = serde_json::from_str(&data)?;
            tracing::info!("restoring session from {}", path.display());
            Session::restore_from_dump(dump, opt)
        }
        _ => Session::new(opt),
    };

    #[cfg(feature = "mock_delay")]
    stardust::app::add_mock_torrent(&session).await;

    tokio::signal::ctrl_c().await?;
    info!("ctrl-c received, shutting down");

    let dump = session.shutdown().await;

    if let Some(path) = &args.session {
        let data = serde_json::to_string_pretty(&dump)?;
        std::fs::write(path, data)?;
        info!("session saved to {}", path.display());
    }

    Ok(())
}

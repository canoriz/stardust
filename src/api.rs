//! HTTP API server.
//!
//! Listens on `POST /api/rpc`. Request/response JSON uses Serde default
//! externally tagged enums (no `tag`/`content` fields).
//!
//! # Example – add a magnet link
//! ```json
//! {"add_torrent":{"source":{"magnet":"magnet:?xt=urn:btih:..."}}}
//! ```
//! Response:
//! ```json
//! "add_torrent_accepted"
//! ```
//!
//! # Commands
//! | command            | extra fields                       | success response              |
//! |-----------------|------------------------------------|-------------------------------|
//! | add_torrent      | `source`, `announce_list?`         | `"add_torrent_accepted"` |
//! | pause_torrent    | `info_hash`                        | `"pause_torrent_accepted"` |
//! | resume_torrent   | `info_hash`                        | `"resume_torrent_accepted"` |
//! | recheck_torrent  | `info_hash`                        | `"recheck_torrent_accepted"` |
//! | remove_torrent   | `info_hash`                        | `"remove_torrent_accepted"` |
//! | shutdown         | —                                  | `"shutdown_accepted"` |
//! | list_torrents    | —                                  | `{"list_torrents":{"torrents":["<hex>", ...]}}` |
//! | get_torrent_status | `info_hash`                      | `{"torrent_status":{"info_hash":"<hex>","process":0.42,"bandwidth_bps":12345.0,"selected":[0,1],"have":[0]}}` |
//!
//! On error: `{"error":{"error":"<message>"}}`
//!
//! Notes:
//! - HTTP response only confirms command was accepted and queued.
//! - Actual execution happens asynchronously in main loop that owns `Session`.

use axum::{extract::State, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::metadata::{FileMetadata, Magnet};
use crate::session::Session;
use crate::transmit_manager::{RunningCmd, TorrentTask};

// ── request types ────────────────────────────────────────────────────────────

/// The source from which a new torrent should be loaded.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentSource {
    /// A magnet URI, e.g. `magnet:?xt=urn:btih:…`
    Magnet(String),
    /// Absolute path to a `.torrent` file accessible on the server filesystem.
    FilePath(String),
}

/// Every command the API accepts, dispatched via `POST /api/rpc`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcRequest {
    /// Add a new torrent to the session and start downloading.
    AddTorrent {
        source: TorrentSource,
        /// Additional tracker URLs to use (`[[tier-0-url, …], [tier-1-url, …], …]`).
        #[serde(default)]
        announce_list: Vec<Vec<String>>,
    },
    /// Pause an active download (stops sending requests to peers).
    PauseTorrent { info_hash: String },
    /// Resume a paused download.
    ResumeTorrent { info_hash: String },
    /// Trigger a full hash-check of all downloaded data.  Returns immediately;
    /// the check runs in the background.
    RecheckTorrent { info_hash: String },
    /// Remove a torrent from the session.  In-flight data is discarded.
    RemoveTorrent { info_hash: String },
    /// List all torrent info-hashes currently in this session.
    ListTorrents,
    /// Get process and bandwidth status for one torrent.
    GetTorrentStatus { info_hash: String },
    /// Ask the server to shut down gracefully.
    Shutdown,
}

// ── response types ───────────────────────────────────────────────────────────

/// Response envelope; shape depends on the command that was issued.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
// TODO: fixme: change all *Accepted to OK
pub enum RpcResponse {
    /// HTTP accepted AddTorrent and queued it for async processing.
    AddTorrentAccepted,
    /// HTTP accepted PauseTorrent and queued it for async processing.
    PauseTorrentAccepted,
    /// HTTP accepted ResumeTorrent and queued it for async processing.
    ResumeTorrentAccepted,
    /// HTTP accepted RecheckTorrent and queued it for async processing.
    RecheckTorrentAccepted,
    /// HTTP accepted RemoveTorrent and queued it for async processing.
    RemoveTorrentAccepted,
    /// List of all current torrents.
    ListTorrents { torrents: Vec<String> },
    /// Runtime status of a torrent.
    TorrentStatus {
        info_hash: String,
        process: f64,
        bandwidth_bps: f64,
        selected: Vec<u32>,
        have: Vec<u32>,
    },
    /// HTTP accepted Shutdown and queued it for async processing.
    ShutdownAccepted,
    /// Something went wrong.
    Error { error: String },
}

impl RpcResponse {
    fn err(msg: impl std::fmt::Display) -> Self {
        RpcResponse::Error {
            error: msg.to_string(),
        }
    }
}

/// A command forwarded by HTTP layer to the session-owning loop.
pub struct ApiCommand {
    pub request: RpcRequest,
    pub reply: Option<oneshot::Sender<RpcResponse>>,
}

// ── axum state ───────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    command_tx: mpsc::UnboundedSender<ApiCommand>,
}

// ── public entry point ───────────────────────────────────────────────────────

/// Start the API HTTP server.
///
/// Binds to `0.0.0.0:{port}` and returns when `shutdown` is cancelled
/// (either from a `Shutdown` API command or externally).
pub async fn serve(
    port: u16,
    command_tx: mpsc::UnboundedSender<ApiCommand>,
    shutdown: CancellationToken,
) {
    let state = AppState { command_tx };
    let app = Router::new()
        .route("/api/rpc", post(rpc_handler))
        .with_state(state);

    let bind_addr = format!("0.0.0.0:{port}");
    let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("api server failed to bind on {bind_addr}: {e}");
            return;
        }
    };
    info!("api server listening on {bind_addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .unwrap_or_else(|e| tracing::error!("api server error: {e}"));

    info!("api server stopped");
}

// ── handler ──────────────────────────────────────────────────────────────────

async fn rpc_handler(
    State(state): State<AppState>,
    Json(req): Json<RpcRequest>,
) -> Json<RpcResponse> {
    if is_query_request(&req) {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = ApiCommand {
            request: req,
            reply: Some(reply_tx),
        };

        if let Err(e) = state.command_tx.send(cmd) {
            return Json(RpcResponse::err(format!(
                "failed to forward command to session loop: {e}"
            )));
        }

        return match reply_rx.await {
            Ok(rsp) => Json(rsp),
            Err(e) => Json(RpcResponse::err(format!(
                "session loop dropped response channel: {e}"
            ))),
        };
    }

    let accepted = accepted_response(&req);
    let cmd = ApiCommand {
        request: req,
        reply: None,
    };

    if let Err(e) = state.command_tx.send(cmd) {
        return Json(RpcResponse::err(format!(
            "failed to forward command to session loop: {e}"
        )));
    }

    Json(accepted)
}

/// Handle one forwarded RPC command using the session owned by main loop.
///
/// Returns `(response, should_shutdown)`.
pub async fn handle_rpc(session: &Session, req: RpcRequest) -> (RpcResponse, bool) {
    match req {
        // ── add torrent ──────────────────────────────────────────────────────
        RpcRequest::AddTorrent {
            source,
            announce_list,
        } => {
            let task = match source {
                TorrentSource::Magnet(uri) => match uri.parse::<Magnet>() {
                    Ok(m) => TorrentTask::Magnet(m),
                    Err(e) => {
                        return (RpcResponse::err(format!("invalid magnet URI: {e}")), false);
                    }
                },
                TorrentSource::FilePath(path) => {
                    let bytes = match std::fs::read(&path) {
                        Ok(b) => b,
                        Err(e) => {
                            return (RpcResponse::err(format!("read torrent file: {e}")), false);
                        }
                    };
                    match FileMetadata::load(&bytes) {
                        Ok(fm) => {
                            let (metadata, _announce) = fm.to_metadata();
                            TorrentTask::Torrent(metadata)
                        }
                        Err(e) => {
                            return (RpcResponse::err(format!("parse torrent file: {e}")), false);
                        }
                    }
                }
            };
            session.add_torrent(task, announce_list).await;
            (RpcResponse::AddTorrentAccepted, false)
        }

        // ── pause ────────────────────────────────────────────────────────────
        RpcRequest::PauseTorrent { info_hash } => {
            let rsp = match do_change_state(session, &info_hash, RunningCmd::Pause).await {
                Ok(()) => RpcResponse::PauseTorrentAccepted,
                Err(e) => RpcResponse::err(e),
            };
            (rsp, false)
        }

        // ── resume ───────────────────────────────────────────────────────────
        RpcRequest::ResumeTorrent { info_hash } => {
            let rsp = match do_change_state(session, &info_hash, RunningCmd::Resume).await {
                Ok(()) => RpcResponse::ResumeTorrentAccepted,
                Err(e) => RpcResponse::err(e),
            };
            (rsp, false)
        }

        // ── recheck (fire-and-forget) ────────────────────────────────────────
        RpcRequest::RecheckTorrent { info_hash } => {
            let rsp = match parse_info_hash(&info_hash) {
                Err(e) => RpcResponse::err(e),
                Ok(ih) => match session.transmit_handle_of(&ih).await {
                    None => RpcResponse::err("torrent not found"),
                    Some(mut sender) => match sender.check() {
                        Ok(_check_handle) => RpcResponse::RecheckTorrentAccepted,
                        Err(e) => RpcResponse::err(e),
                    },
                },
            };
            (rsp, false)
        }

        // ── remove ───────────────────────────────────────────────────────────
        RpcRequest::RemoveTorrent { info_hash } => {
            let rsp = match parse_info_hash(&info_hash) {
                Err(e) => RpcResponse::err(e),
                Ok(ih) => match session.remove_torrent(&ih).await {
                    Some(_) => RpcResponse::RemoveTorrentAccepted,
                    None => RpcResponse::err("torrent not found"),
                },
            };
            (rsp, false)
        }
        RpcRequest::ListTorrents => {
            let torrents = session
                .list_torrents()
                .into_iter()
                .map(hex::encode)
                .collect();
            (RpcResponse::ListTorrents { torrents }, false)
        }
        RpcRequest::GetTorrentStatus { info_hash } => {
            let rsp = match parse_info_hash(&info_hash) {
                Err(e) => RpcResponse::err(e),
                Ok(ih) => match session.get_torrent_status(&ih).await {
                    Ok(st) => RpcResponse::TorrentStatus {
                        info_hash: hex::encode(st.info_hash),
                        process: st.process,
                        bandwidth_bps: st.bandwidth_bps,
                        selected: st.selected,
                        have: st.have,
                    },
                    Err(e) => RpcResponse::err(e),
                },
            };
            (rsp, false)
        }

        // ── shutdown ─────────────────────────────────────────────────────────
        RpcRequest::Shutdown => {
            info!("shutdown requested via API");
            (RpcResponse::ShutdownAccepted, true)
        }
    }
}

fn is_query_request(req: &RpcRequest) -> bool {
    matches!(
        req,
        RpcRequest::ListTorrents | RpcRequest::GetTorrentStatus { .. }
    )
}

fn accepted_response(req: &RpcRequest) -> RpcResponse {
    match req {
        RpcRequest::AddTorrent { .. } => RpcResponse::AddTorrentAccepted,
        RpcRequest::PauseTorrent { .. } => RpcResponse::PauseTorrentAccepted,
        RpcRequest::ResumeTorrent { .. } => RpcResponse::ResumeTorrentAccepted,
        RpcRequest::RecheckTorrent { .. } => RpcResponse::RecheckTorrentAccepted,
        RpcRequest::RemoveTorrent { .. } => RpcResponse::RemoveTorrentAccepted,
        RpcRequest::ListTorrents => RpcResponse::err("list_torrents is a query command"),
        RpcRequest::GetTorrentStatus { .. } => {
            RpcResponse::err("get_torrent_status is a query command")
        }
        RpcRequest::Shutdown => RpcResponse::ShutdownAccepted,
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn parse_info_hash(s: &str) -> Result<[u8; 20], String> {
    let bytes = hex::decode(s).map_err(|e| format!("invalid info_hash hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| "info_hash must be exactly 20 bytes".into())
}

async fn do_change_state(
    session: &Session,
    info_hash: &str,
    cmd: RunningCmd,
) -> Result<(), String> {
    let ih = parse_info_hash(info_hash)?;
    let mut sender = session
        .transmit_handle_of(&ih)
        .await
        .ok_or_else(|| "torrent not found".to_string())?;
    sender.change_state(cmd).await.map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::{RpcRequest, RpcResponse, TorrentSource};

    #[test]
    fn request_deserialize_add_torrent_doc_example() {
        let input = r#"{"add_torrent":{"source":{"magnet":"magnet:?xt=urn:btih:abc"}}}"#;
        let req: RpcRequest = serde_json::from_str(input).expect("deserialize add torrent");
        match req {
            RpcRequest::AddTorrent {
                source: TorrentSource::Magnet(v),
                announce_list,
            } => {
                assert_eq!(v, "magnet:?xt=urn:btih:abc");
                assert!(announce_list.is_empty());
            }
            _ => panic!("unexpected request variant"),
        }
    }

    #[test]
    fn request_serialize_pause_torrent_matches_docs_shape() {
        let req = RpcRequest::PauseTorrent {
            info_hash: "00112233445566778899aabbccddeeff00112233".to_string(),
        };
        let out = serde_json::to_string(&req).expect("serialize pause request");
        assert_eq!(
            out,
            r#"{"pause_torrent":{"info_hash":"00112233445566778899aabbccddeeff00112233"}}"#
        );
    }

    #[test]
    fn request_deserialize_list_and_get_status() {
        let list_input = r#""list_torrents""#;
        let list_req: RpcRequest = serde_json::from_str(list_input).expect("deserialize list");
        assert!(matches!(list_req, RpcRequest::ListTorrents));

        let st_input =
            r#"{"get_torrent_status":{"info_hash":"00112233445566778899aabbccddeeff00112233"}}"#;
        let st_req: RpcRequest = serde_json::from_str(st_input).expect("deserialize status");
        match st_req {
            RpcRequest::GetTorrentStatus { info_hash } => {
                assert_eq!(
                    info_hash,
                    "00112233445566778899aabbccddeeff00112233".to_string()
                );
            }
            _ => panic!("unexpected request variant"),
        }
    }

    #[test]
    fn response_serialize_matches_docs_variants() {
        let add = serde_json::to_string(&RpcResponse::AddTorrentAccepted)
            .expect("serialize AddTorrentAccepted");
        assert_eq!(add, r#""add_torrent_accepted""#);

        let pause = serde_json::to_string(&RpcResponse::PauseTorrentAccepted)
            .expect("serialize PauseTorrentAccepted");
        assert_eq!(pause, r#""pause_torrent_accepted""#);

        let shutdown = serde_json::to_string(&RpcResponse::ShutdownAccepted)
            .expect("serialize ShutdownAccepted");
        assert_eq!(shutdown, r#""shutdown_accepted""#);

        let list = serde_json::to_string(&RpcResponse::ListTorrents {
            torrents: vec!["001122".into(), "aabbcc".into()],
        })
        .expect("serialize list torrents");
        assert_eq!(
            list,
            r#"{"list_torrents":{"torrents":["001122","aabbcc"]}}"#
        );

        let status = serde_json::to_string(&RpcResponse::TorrentStatus {
            info_hash: "001122".into(),
            process: 0.42,
            bandwidth_bps: 12345.0,
            selected: vec![0, 2],
            have: vec![0],
        })
        .expect("serialize torrent status");
        assert_eq!(
            status,
            r#"{"torrent_status":{"info_hash":"001122","process":0.42,"bandwidth_bps":12345.0,"selected":[0,2],"have":[0]}}"#
        );
    }

    #[test]
    fn response_deserialize_error_shape() {
        let input = r#"{"error":{"error":"boom"}}"#;
        let rsp: RpcResponse = serde_json::from_str(input).expect("deserialize error response");
        match rsp {
            RpcResponse::Error { error } => assert_eq!(error, "boom"),
            _ => panic!("unexpected response variant"),
        }
    }
}

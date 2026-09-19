//! Stardust GUI client.
//!
//! The GUI is always a frontend. It talks to a backend through a [`Transport`]:
//!   - `Target::Remote` — a stardust server over the JSON-RPC HTTP API
//!     (`POST /api/rpc`); the address is chosen inside the app (toolbar
//!     "Server" field + Connect).
//!   - `Target::Local` — the in-process backend started with `--backend`,
//!     reached directly over a command channel (no HTTP round-trip).
//!
//! With `--backend`, the program additionally starts a local backend
//! (owning a [`Session`] + HTTP API server for remote clients) and
//! auto-connects the frontend to it in-process via the "Local" target.
//!
//! TCP peer port : 41773   DHT UDP port : 41774   Session file : session-gui.json
//!
//! Threads:
//!   - main thread: eframe/egui UI
//!   - one background Tokio runtime hosting the frontend poll loop and, when
//!     `--backend` is set, the backend server.
//!
//! Shared state:
//!   - unbounded MPSC channel        (GUI → frontend commands)
//!   - `Arc<Mutex<Vec<TorrentRow>>>` (backend → GUI state)
//!   - `Arc<Mutex<Target>>`          (GUI → frontend loop: connection target)
//!   - `CancellationToken`           (GUI window close → shutdown)

use clap::Parser;
use eframe::egui;
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc as async_mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::format::FmtSpan;

use stardust::api::{
    handle_rpc, ApiCommand, BufferPoolStats, CacheStats, RpcRequest, RpcResponse, RunningStateDump,
    StableState, TorrentSource,
};
use stardust::{Session, SessionDump, SessionOpt};

// ── CLI ────────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(about = "Stardust GUI client")]
struct Args {
    /// Also start a local backend server in-process and auto-connect to it.
    /// Omit to run as a pure frontend and pick a remote server inside the GUI.
    #[arg(long)]
    backend: bool,

    /// TCP port the local backend server binds its JSON-RPC API to (only used
    /// with `--backend`); the frontend also auto-connects to this port.
    #[arg(long, default_value_t = 9026)]
    api_port: u16,
}

/// Connection state to the server, shown in the toolbar.
#[derive(Clone)]
enum ConnStatus {
    Disconnected,
    Connecting,
    Connected,
    Error(String),
}

/// What the frontend loop should talk to. Chosen inside the GUI.
#[derive(Clone)]
enum Target {
    /// Not connected to anything.
    None,
    /// The in-process backend running in this program (only with `--backend`).
    Local,
    /// A remote stardust server over HTTP; holds the full `.../api/rpc` URL.
    Remote(String),
}

/// Transport the frontend uses to issue one RPC. The in-process variant talks
/// to the local backend's command channel directly (no HTTP), while the HTTP
/// variant POSTs to a remote server. Cheap to construct per request.
enum Transport {
    InProcess(async_mpsc::UnboundedSender<ApiCommand>),
    Http {
        client: reqwest::Client,
        endpoint: String,
    },
}

impl Transport {
    async fn call(&self, req: RpcRequest) -> Result<RpcResponse, String> {
        match self {
            Transport::InProcess(tx) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                tx.send(ApiCommand {
                    request: req,
                    reply: reply_tx,
                })
                .map_err(|e| format!("backend channel closed: {e}"))?;
                reply_rx
                    .await
                    .map_err(|e| format!("backend dropped reply: {e}"))
            }
            Transport::Http { client, endpoint } => rpc_call(client, endpoint, &req).await,
        }
    }
}

// ── constants ─────────────────────────────────────────────────────────────────

const SELF_ID: [u8; 20] = *b"-TR3000-fjbo402nczk3";
const TCP_PORT: u16 = 41773;
const DHT_PORT: u16 = 41774;
const SESSION_FILE: &str = "session-gui.json";

// ── inter-thread types ────────────────────────────────────────────────────────

/// Display snapshot for one torrent; refreshed every ~500 ms.
#[derive(Clone)]
struct TorrentRow {
    info_hash: String,
    name: Option<String>,
    progress: f64, // 0.0 … 1.0
    speed_bps: f64,
    state: RunningStateDump,
}

impl Default for TorrentRow {
    fn default() -> Self {
        Self {
            info_hash: String::new(),
            name: None,
            progress: 0.0,
            speed_bps: 0.0,
            state: RunningStateDump::StableState(StableState::Stopped),
        }
    }
}

// ── GUI app ───────────────────────────────────────────────────────────────────

struct GuiApp {
    cmd_tx: async_mpsc::UnboundedSender<RpcRequest>,
    shared: Arc<Mutex<Vec<TorrentRow>>>,
    /// Latest cache statistics snapshot, refreshed by the backend.
    cache_shared: Arc<Mutex<Option<CacheStats>>>,
    /// Latest block buffer pool snapshot, refreshed by the backend.
    pool_shared: Arc<Mutex<Option<BufferPoolStats>>>,
    /// Live connection status to the current server.
    status_shared: Arc<Mutex<ConnStatus>>,
    /// Target the frontend loop should talk to; `Target::None` = disconnected.
    target_shared: Arc<Mutex<Target>>,
    /// Whether an in-process backend exists (i.e. started with `--backend`).
    has_local: bool,
    /// Receives a `()` from the ctrl-c listener task; triggers a graceful close.
    ctrl_c_rx: std_mpsc::Receiver<()>,

    /// Server address text field in the toolbar.
    server_input: String,

    // Add-torrent dialog state
    show_add: bool,
    add_input: String,
    /// Receives a picked file path from the native file dialog (one-shot).
    file_rx: Option<oneshot::Receiver<Option<String>>>,
}

impl GuiApp {
    fn new(
        cmd_tx: async_mpsc::UnboundedSender<RpcRequest>,
        shared: Arc<Mutex<Vec<TorrentRow>>>,
        cache_shared: Arc<Mutex<Option<CacheStats>>>,
        pool_shared: Arc<Mutex<Option<BufferPoolStats>>>,
        status_shared: Arc<Mutex<ConnStatus>>,
        target_shared: Arc<Mutex<Target>>,
        has_local: bool,
        server_input: String,
        ctrl_c_rx: std_mpsc::Receiver<()>,
    ) -> Self {
        Self {
            cmd_tx,
            shared,
            cache_shared,
            pool_shared,
            status_shared,
            target_shared,
            has_local,
            ctrl_c_rx,
            server_input,
            show_add: false,
            add_input: String::new(),
            file_rx: None,
        }
    }

    /// Apply the current `server_input` as a remote HTTP connection target.
    fn connect(&mut self) {
        let addr = self.server_input.trim();
        if addr.is_empty() {
            return;
        }
        *self.target_shared.lock().unwrap() = Target::Remote(build_endpoint(addr));
        *self.status_shared.lock().unwrap() = ConnStatus::Connecting;
    }

    /// Connect to the in-process backend (only meaningful with `--backend`).
    fn connect_local(&mut self) {
        *self.target_shared.lock().unwrap() = Target::Local;
        *self.status_shared.lock().unwrap() = ConnStatus::Connecting;
    }
}

impl eframe::App for GuiApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        // Trigger a repaint every 500 ms so speed / progress stay fresh.
        ctx.request_repaint_after(std::time::Duration::from_millis(500));

        // If the ctrl-c listener task fired, close the viewport gracefully.
        if self.ctrl_c_rx.try_recv().is_ok() {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }

        // ── top toolbar ──────────────────────────────────────────────────────
        egui::Panel::top("toolbar").show_inside(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Stardust");
                ui.separator();
                if ui.button("Add Torrent").clicked() {
                    self.show_add = true;
                    self.add_input.clear();
                }
                ui.separator();

                // Server connection controls.
                ui.label("Server:");
                let resp = ui.add(
                    egui::TextEdit::singleline(&mut self.server_input)
                        .desired_width(200.0)
                        .hint_text("host:port"),
                );
                let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                if ui.button("Connect").clicked() || enter {
                    self.connect();
                }
                if self.has_local && ui.button("Local").clicked() {
                    self.connect_local();
                }
                match &*self.status_shared.lock().unwrap() {
                    ConnStatus::Disconnected => {
                        ui.colored_label(egui::Color32::DARK_GRAY, "● disconnected");
                    }
                    ConnStatus::Connecting => {
                        ui.colored_label(egui::Color32::from_rgb(220, 140, 0), "● connecting");
                    }
                    ConnStatus::Connected => {
                        ui.colored_label(egui::Color32::GREEN, "● connected");
                    }
                    ConnStatus::Error(e) => {
                        let short: String = e.chars().take(60).collect();
                        ui.colored_label(egui::Color32::RED, format!("● {short}"));
                    }
                }

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Quit").clicked() {
                        ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
            });
        });

        // Poll file-picker result (set by Browse button callback).
        if let Some(rx) = &mut self.file_rx {
            if let Ok(maybe_path) = rx.try_recv() {
                if let Some(path) = maybe_path {
                    self.add_input = path;
                }
                self.file_rx = None;
            }
        }

        // ── add-torrent dialog ────────────────────────────────────────────────
        let mut close_add = false;
        if self.show_add {
            egui::Window::new("Add Torrent")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(&ctx, |ui| {
                    ui.label("Enter a magnet URI or .torrent path, or browse:");
                    ui.horizontal(|ui| {
                        ui.add(
                            egui::TextEdit::singleline(&mut self.add_input)
                                .desired_width(440.0)
                                .hint_text("magnet:?xt=urn:btih:…   or   /path/to/file.torrent"),
                        );
                        let picking = self.file_rx.is_some();
                        if ui
                            .add_enabled(!picking, egui::Button::new("Browse…"))
                            .clicked()
                        {
                            let (tx, rx) = oneshot::channel();
                            self.file_rx = Some(rx);
                            let ctx2 = ctx.clone();
                            thread::spawn(move || {
                                let picked = rfd::FileDialog::new()
                                    .add_filter("Torrent files", &["torrent"])
                                    .add_filter("All files", &["*"])
                                    .pick_file();
                                let path = picked.map(|f| f.to_string_lossy().into_owned());
                                let _ = tx.send(path);
                                ctx2.request_repaint();
                            });
                        }
                    });
                    ui.separator();
                    ui.horizontal(|ui| {
                        let has_input = !self.add_input.trim().is_empty();
                        if ui
                            .add_enabled(has_input, egui::Button::new("Add"))
                            .clicked()
                        {
                            let s = self.add_input.trim().to_string();
                            let source = if s.starts_with("magnet:") {
                                TorrentSource::Magnet(s)
                            } else {
                                TorrentSource::FilePath(s)
                            };
                            let _ = self.cmd_tx.send(RpcRequest::AddTorrent {
                                source,
                                announce_list: vec![vec!["1".into()]],
                            });
                            close_add = true;
                        }
                        if ui.button("Cancel").clicked() {
                            close_add = true;
                        }
                    });
                });
        }
        if close_add {
            self.show_add = false;
        }

        // ── cache statistics panel ────────────────────────────────────────────
        egui::Panel::bottom("cache_stats").show_inside(ui, |ui| {
            let stats = *self.cache_shared.lock().unwrap();
            ui.add_space(2.0);
            match stats {
                None => {
                    ui.weak("Cache: no data yet");
                }
                Some(s) => {
                    egui::CollapsingHeader::new("Cache statistics")
                        .default_open(true)
                        .show(ui, |ui| {
                            ui.horizontal_wrapped(|ui| {
                                ui.label(egui::RichText::new(format!(
                                    "slots {}/{} (vacant {})",
                                    s.occupied, s.capacity, s.vacant
                                ))
                                .monospace());
                                ui.separator();
                                ui.label(egui::RichText::new(format!(
                                    "clean {}  dirty {}  lent {}  reading {}  waiting {}",
                                    s.clean_pieces, s.dirty_pieces, s.lent_pieces, s.reading_pieces, s.waiting_requests
                                ))
                                .monospace());
                            });
                            ui.horizontal_wrapped(|ui| {
                                ratio_bar(ui, "clear", s.clear_ratio, egui::Color32::from_rgb(100, 200, 120));
                                ratio_bar(ui, "dirty", s.dirty_ratio, egui::Color32::from_rgb(220, 140, 0));
                            });
                            ui.horizontal_wrapped(|ui| {
                                ui.label(egui::RichText::new(format!(
                                    "req {:.1}/s  lend {:.1}/s  return {:.1}/s  flush {:.1}/s  cache-flush {:.1}/s  evict {:.1}/s",
                                    s.request_rate,
                                    s.lend_rate,
                                    s.return_rate,
                                    s.flush_rate,
                                    s.cache_flush_rate,
                                    s.evict_rate,
                                ))
                                .monospace());
                            });
                            if let Some(p) = *self.pool_shared.lock().unwrap() {
                                ui.horizontal_wrapped(|ui| {
                                    ui.label(egui::RichText::new(format!(
                                        "block pool {}/{} in use (available {})",
                                        p.in_use, p.capacity, p.available
                                    ))
                                    .monospace());
                                });
                            }
                        });
                }
            }
        });

        // ── torrent list ──────────────────────────────────────────────────────
        egui::CentralPanel::default().show_inside(ui, |ui| {
            let rows: Vec<TorrentRow> = self.shared.lock().unwrap().clone();

            if rows.is_empty() {
                let disconnected = matches!(
                    &*self.status_shared.lock().unwrap(),
                    ConnStatus::Disconnected
                );
                ui.centered_and_justified(|ui| {
                    if disconnected {
                        ui.label("Not connected — enter a server address and click Connect.");
                    } else {
                        ui.label("No active torrents — click \"Add Torrent\" to begin.");
                    }
                });
                return;
            }

            // Single grid for both header and data rows — ensures columns align.
            egui::ScrollArea::vertical().show(ui, |ui| {
                egui::Grid::new("torrent_list")
                    .striped(true)
                    .min_col_width(0.0)
                    .show(ui, |ui| {
                        // ── header row ──
                        ui.strong("Name");
                        ui.strong("Progress");
                        ui.strong("Speed");
                        ui.strong("Status");
                        ui.strong("Actions");
                        ui.end_row();

                        // ── separator via a full-width label trick ──
                        for _ in 0..5 {
                            ui.separator();
                        }
                        ui.end_row();

                        // ── data rows ──
                        for row in &rows {
                            match &row.name {
                                Some(n) => {
                                    let display: String = n.chars().take(45).collect();
                                    let display = if n.chars().count() > 45 {
                                        format!("{}\u{2026}", display)
                                    } else {
                                        display
                                    };
                                    ui.label(display);
                                }
                                None => {
                                    ui.monospace(
                                        egui::RichText::new(&row.info_hash)
                                            .color(egui::Color32::GRAY),
                                    );
                                }
                            }

                            // Progress bar
                            let pct = row.progress as f32;
                            let pct_label = if row.progress >= 1.0 {
                                "100%".to_string()
                            } else {
                                format!("{:.1}%", pct * 100.0)
                            };
                            ui.add(
                                egui::ProgressBar::new(pct)
                                    .desired_width(190.0)
                                    .text(pct_label),
                            );

                            // Speed
                            ui.label(egui::RichText::new(fmt_speed(row.speed_bps)).monospace());

                            // Status label
                            match &row.state {
                                RunningStateDump::StableState(StableState::Paused) => {
                                    ui.colored_label(
                                        egui::Color32::from_rgb(220, 140, 0),
                                        "Paused",
                                    );
                                }
                                RunningStateDump::StableState(StableState::Stopped) => {
                                    ui.colored_label(egui::Color32::DARK_GRAY, "Stopped");
                                }
                                RunningStateDump::StableState(StableState::Seeding) => {
                                    ui.colored_label(egui::Color32::GREEN, "Seeding");
                                }
                                RunningStateDump::StableState(StableState::Downloading) => {
                                    ui.colored_label(
                                        egui::Color32::from_rgb(100, 180, 255),
                                        "Downloading",
                                    );
                                }
                                RunningStateDump::StableState(StableState::Fatal(reason)) => {
                                    ui.colored_label(
                                        egui::Color32::RED,
                                        format!("Fatal: {reason}"),
                                    );
                                }
                                RunningStateDump::Checking { checked, .. } => {
                                    let total = checked.total_checking_pieces();
                                    let done = checked.checked_count();
                                    let pct = if total > 0 {
                                        done as f32 / total as f32
                                    } else {
                                        0.0
                                    };
                                    ui.add(
                                        egui::ProgressBar::new(pct)
                                            .desired_width(90.0)
                                            .fill(egui::Color32::from_rgb(255, 200, 80))
                                            .text(format!("Check {:.0}%", pct * 100.0)),
                                    );
                                }
                            }

                            // Action buttons
                            ui.horizontal(|ui| {
                                let is_paused = matches!(
                                    &row.state,
                                    RunningStateDump::StableState(
                                        StableState::Paused
                                            | StableState::Stopped
                                            | StableState::Fatal(_)
                                    )
                                );
                                if is_paused {
                                    if ui.small_button("▶ Resume").clicked() {
                                        let _ = self.cmd_tx.send(RpcRequest::ResumeTorrent {
                                            info_hash: row.info_hash.clone(),
                                        });
                                    }
                                } else if ui.small_button("⏸ Pause").clicked() {
                                    let _ = self.cmd_tx.send(RpcRequest::PauseTorrent {
                                        info_hash: row.info_hash.clone(),
                                    });
                                }
                                if ui.small_button("🔄 Recheck").clicked() {
                                    let _ = self.cmd_tx.send(RpcRequest::RecheckTorrent {
                                        info_hash: row.info_hash.clone(),
                                    });
                                }
                                if ui.small_button("✕ Remove").clicked() {
                                    let _ = self.cmd_tx.send(RpcRequest::RemoveTorrent {
                                        info_hash: row.info_hash.clone(),
                                    });
                                }
                            });

                            ui.end_row();
                        }
                    });
            });
        });
    }
}

fn ratio_bar(ui: &mut egui::Ui, label: &str, ratio: f64, color: egui::Color32) {
    ui.add(
        egui::ProgressBar::new(ratio as f32)
            .desired_width(120.0)
            .fill(color)
            .text(format!("{label} {:.0}%", ratio * 100.0)),
    );
}

fn fmt_speed(bps: f64) -> String {
    if bps >= 1_048_576.0 {
        format!("{:.2} MB/s", bps / 1_048_576.0)
    } else if bps >= 1024.0 {
        format!("{:.1} KB/s", bps / 1024.0)
    } else if bps > 0.0 {
        format!("{:.0} B/s", bps)
    } else {
        "—".to_string()
    }
}

// ── backend server (local, --backend) ─────────────────────────────────────────

/// Run an in-process backend server: owns a [`Session`], serves the JSON-RPC
/// HTTP API on `api_port` (for remote clients), and also consumes commands that
/// the local frontend sends in-process via `cmd_tx`. Persists the session on
/// shutdown. Mirrors the standalone `main` binary.
async fn backend_server_main(
    api_port: u16,
    cmd_tx: async_mpsc::UnboundedSender<ApiCommand>,
    mut cmd_rx: async_mpsc::UnboundedReceiver<ApiCommand>,
    shutdown: CancellationToken,
) {
    let opt = SessionOpt::builder()
        .self_id(SELF_ID)
        .port(TCP_PORT)
        .maybe_dht_port(Some(DHT_PORT));

    let session = {
        let path = std::path::Path::new(SESSION_FILE);
        if path.exists() {
            match std::fs::read_to_string(path) {
                Ok(data) => match serde_json::from_str::<SessionDump>(&data) {
                    Ok(dump) => {
                        tracing::info!("restoring session from {SESSION_FILE}");
                        let opt = opt.previous(dump);
                        Session::new(opt.build())
                    }
                    Err(e) => {
                        tracing::warn!("failed to parse {SESSION_FILE}: {e} — starting fresh");
                        Session::new(opt.build())
                    }
                },
                Err(e) => {
                    tracing::warn!("failed to read {SESSION_FILE}: {e} — starting fresh");
                    Session::new(opt.build())
                }
            }
        } else {
            Session::new(opt.build())
        }
    };

    // Serve the HTTP API for remote clients, feeding the same command channel.
    tokio::spawn(stardust::api::serve(api_port, cmd_tx, shutdown.clone()));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("backend server: shutdown signal — saving session");
                break;
            }
            maybe_cmd = cmd_rx.recv() => {
                let Some(cmd) = maybe_cmd else { break };
                let (rsp, should_shutdown) = handle_rpc(&session, cmd.request).await;
                let _ = cmd.reply.send(rsp);
                if should_shutdown {
                    tracing::info!("shutdown requested via API");
                    shutdown.cancel();
                    break;
                }
            }
        }
    }

    // Persist session state to disk.
    let dump = session.shutdown().await;
    match serde_json::to_string(&dump) {
        Ok(data) => {
            if let Err(e) = std::fs::write(SESSION_FILE, &data) {
                tracing::error!("failed to write {SESSION_FILE}: {e}");
            } else {
                tracing::info!("session saved to {SESSION_FILE}");
            }
        }
        Err(e) => tracing::error!("failed to serialize session dump: {e}"),
    }
}

// ── frontend loop ─────────────────────────────────────────────────────────────

/// Normalize a user-entered server address into a full
/// `http://host:port/api/rpc` URL.
fn build_endpoint(server: &str) -> String {
    let base = if server.starts_with("http://") || server.starts_with("https://") {
        server.to_string()
    } else {
        format!("http://{server}")
    };
    let base = base.trim_end_matches('/');
    if base.ends_with("/api/rpc") {
        base.to_string()
    } else {
        format!("{base}/api/rpc")
    }
}

async fn rpc_call(
    client: &reqwest::Client,
    endpoint: &str,
    req: &RpcRequest,
) -> Result<RpcResponse, String> {
    let resp = client
        .post(endpoint)
        .json(req)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json::<RpcResponse>().await.map_err(|e| e.to_string())
}

/// Frontend loop: reads the current [`Target`] from `target_shared` each tick,
/// resolves it to a [`Transport`] (in-process channel to the local backend, or
/// HTTP to a remote server), pulls all stats, and forwards UI commands. Owns no
/// `Session`. Reconnects automatically when the target changes.
async fn frontend_main(
    mut cmd_rx: async_mpsc::UnboundedReceiver<RpcRequest>,
    shared: Arc<Mutex<Vec<TorrentRow>>>,
    cache_shared: Arc<Mutex<Option<CacheStats>>>,
    pool_shared: Arc<Mutex<Option<BufferPoolStats>>>,
    status_shared: Arc<Mutex<ConnStatus>>,
    target_shared: Arc<Mutex<Target>>,
    local_tx: Option<async_mpsc::UnboundedSender<ApiCommand>>,
    shutdown: CancellationToken,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build http client");

    // Resolve the current target into a transport, or None when disconnected.
    let resolve = |target: Target| -> Option<Transport> {
        match target {
            Target::None => None,
            Target::Local => local_tx.clone().map(Transport::InProcess),
            Target::Remote(endpoint) => Some(Transport::Http {
                client: client.clone(),
                endpoint,
            }),
        }
    };

    let mut interval = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("frontend loop: shutdown signal");
                break;
            }
            _ = interval.tick() => {
                let Some(transport) = resolve(target_shared.lock().unwrap().clone()) else {
                    *status_shared.lock().unwrap() = ConnStatus::Disconnected;
                    continue;
                };
                match refresh_rows(&transport, &shared).await {
                    Ok(()) => *status_shared.lock().unwrap() = ConnStatus::Connected,
                    Err(e) => {
                        *status_shared.lock().unwrap() = ConnStatus::Error(e);
                        continue;
                    }
                }
                if let Ok(RpcResponse::CacheStats(stats)) =
                    transport.call(RpcRequest::GetCacheStats).await
                {
                    *cache_shared.lock().unwrap() = Some(stats);
                }
                if let Ok(RpcResponse::BufferPoolStats(stats)) =
                    transport.call(RpcRequest::GetBufferPoolStats).await
                {
                    *pool_shared.lock().unwrap() = Some(stats);
                }
            }
            Some(cmd) = cmd_rx.recv() => {
                let Some(transport) = resolve(target_shared.lock().unwrap().clone()) else {
                    tracing::warn!("command dropped: not connected to a server");
                    continue;
                };
                match transport.call(cmd).await {
                    Ok(RpcResponse::Error { error }) => tracing::warn!("rpc error: {error}"),
                    Err(e) => tracing::warn!("rpc call failed: {e}"),
                    _ => {}
                }
            }
        }
    }
}

async fn refresh_rows(
    transport: &Transport,
    shared: &Arc<Mutex<Vec<TorrentRow>>>,
) -> Result<(), String> {
    let hashes = match transport.call(RpcRequest::ListTorrents).await? {
        RpcResponse::ListTorrents { torrents } => torrents,
        RpcResponse::Error { error } => return Err(error),
        _ => return Err("unexpected list_torrents response".into()),
    };

    let mut rows = Vec::with_capacity(hashes.len());
    for hash in &hashes {
        let rsp = transport
            .call(RpcRequest::GetTorrentStatus {
                info_hash: hash.clone(),
            })
            .await?;
        if let RpcResponse::TorrentStatus {
            info_hash,
            name,
            process,
            bandwidth_bps,
            state,
            ..
        } = rsp
        {
            rows.push(TorrentRow {
                state,
                info_hash,
                name,
                progress: process,
                speed_bps: bandwidth_bps,
            });
        }
    }

    *shared.lock().unwrap() = rows;
    Ok(())
}

// ── entry point ───────────────────────────────────────────────────────────────

fn main() {
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

    let shared: Arc<Mutex<Vec<TorrentRow>>> = Arc::new(Mutex::new(Vec::new()));
    let cache_shared: Arc<Mutex<Option<CacheStats>>> = Arc::new(Mutex::new(None));
    let pool_shared: Arc<Mutex<Option<BufferPoolStats>>> = Arc::new(Mutex::new(None));
    let (cmd_tx, cmd_rx) = async_mpsc::unbounded_channel::<RpcRequest>();
    let shutdown = CancellationToken::new();
    // Channel for the ctrl-c task to signal the GUI to close its viewport.
    let (gui_ctrl_c_tx, gui_ctrl_c_rx) = std_mpsc::channel::<()>();

    // With --backend, auto-connect the frontend to the in-process backend;
    // otherwise start disconnected and let the user pick a remote server.
    let local_addr = format!("127.0.0.1:{}", args.api_port);
    let (initial_target, initial_status) = if args.backend {
        (Target::Local, ConnStatus::Connecting)
    } else {
        (Target::None, ConnStatus::Disconnected)
    };
    let server_input = local_addr;
    let status_shared: Arc<Mutex<ConnStatus>> = Arc::new(Mutex::new(initial_status));
    let target_shared: Arc<Mutex<Target>> = Arc::new(Mutex::new(initial_target));

    // Launch the Tokio runtime on a dedicated OS thread: it hosts the frontend
    // poll loop and, with --backend, the in-process backend server.
    let backend_handle = {
        let shared = shared.clone();
        let cache_shared = cache_shared.clone();
        let pool_shared = pool_shared.clone();
        let status_shared = status_shared.clone();
        let target_shared = target_shared.clone();
        let shutdown = shutdown.clone();
        let start_backend = args.backend;
        let api_port = args.api_port;
        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            rt.block_on(async move {
                // Ctrl-c: notify the GUI to close and cancel the shutdown token.
                {
                    let shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        if tokio::signal::ctrl_c().await.is_ok() {
                            tracing::info!("ctrl-c received — notifying GUI and backend");
                            let _ = gui_ctrl_c_tx.send(());
                            shutdown.cancel();
                        }
                    });
                }

                // In --backend mode, create the command channel shared by the
                // in-process frontend transport and the HTTP API server.
                let (local_tx, server_task) = if start_backend {
                    let (api_tx, api_rx) = async_mpsc::unbounded_channel::<ApiCommand>();
                    let task = tokio::spawn(backend_server_main(
                        api_port,
                        api_tx.clone(),
                        api_rx,
                        shutdown.clone(),
                    ));
                    (Some(api_tx), Some(task))
                } else {
                    (None, None)
                };

                frontend_main(
                    cmd_rx,
                    shared,
                    cache_shared,
                    pool_shared,
                    status_shared,
                    target_shared,
                    local_tx,
                    shutdown,
                )
                .await;

                // Wait for the backend server to finish saving the session.
                if let Some(task) = server_task {
                    let _ = task.await;
                }
            });
        })
    };

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("Stardust — BitTorrent Client")
            .with_inner_size([1000.0, 620.0])
            .with_min_inner_size([640.0, 400.0]),
        ..Default::default()
    };

    if let Err(e) = eframe::run_native(
        "Stardust",
        options,
        Box::new(move |cc| {
            // Load a CJK fallback font so torrent names in Chinese/Japanese/Korean
            // render correctly instead of showing replacement boxes (□).
            const CJK_FONT_PATH: &str = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc";
            if let Ok(font_data) = std::fs::read(CJK_FONT_PATH) {
                let mut fonts = egui::FontDefinitions::default();
                fonts.font_data.insert(
                    "noto_cjk".to_owned(),
                    egui::FontData::from_owned(font_data).into(),
                );
                // Append as last fallback for both proportional and monospace families.
                fonts
                    .families
                    .entry(egui::FontFamily::Proportional)
                    .or_default()
                    .push("noto_cjk".to_owned());
                fonts
                    .families
                    .entry(egui::FontFamily::Monospace)
                    .or_default()
                    .push("noto_cjk".to_owned());
                cc.egui_ctx.set_fonts(fonts);
            }
            Ok(Box::new(GuiApp::new(
                cmd_tx,
                shared,
                cache_shared,
                pool_shared,
                status_shared,
                target_shared,
                args.backend,
                server_input,
                gui_ctrl_c_rx,
            )))
        }),
    ) {
        eprintln!("eframe error: {e}");
    }

    // Window has closed. Signal the backend/frontend to stop (and save the
    // session in --backend mode) and wait before the process exits.
    shutdown.cancel();
    if let Err(e) = backend_handle.join() {
        eprintln!("backend thread panicked: {e:?}");
    }
    println!("Goodbye!");
}

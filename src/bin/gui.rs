//! Stardust GUI client.
//!
//! TCP peer port : 41773
//! DHT UDP port  : 41774
//! Session file  : session-gui.json
//!
//! The GUI runs on the main thread (eframe/egui); a separate background thread
//! hosts a Tokio multi-thread runtime that owns the [`Session`].  The two
//! sides communicate through:
//!   - an unbounded MPSC channel  (GUI → backend commands)
//!   - a shared `Arc<Mutex<Vec<TorrentRow>>>` (backend → GUI state)
//!   - a `CancellationToken`       (GUI window close → backend shutdown)

use eframe::egui;
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::thread;
use tokio::sync::mpsc as async_mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::format::FmtSpan;

use stardust::api::{
    handle_rpc, RpcRequest, RpcResponse, RunningStateDump, StableState, TorrentSource,
};
use stardust::{Session, SessionDump, SessionOpt};

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
    progress: f64, // 0.0 … 1.0
    speed_bps: f64,
    state: RunningStateDump,
}

impl Default for TorrentRow {
    fn default() -> Self {
        Self {
            info_hash: String::new(),
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
    /// Receives a `()` from the ctrl-c listener task; triggers a graceful close.
    ctrl_c_rx: std_mpsc::Receiver<()>,

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
        ctrl_c_rx: std_mpsc::Receiver<()>,
    ) -> Self {
        Self {
            cmd_tx,
            shared,
            ctrl_c_rx,
            show_add: false,
            add_input: String::new(),
            file_rx: None,
        }
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
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Shutdown").clicked() {
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

        // ── torrent list ──────────────────────────────────────────────────────
        egui::CentralPanel::default().show_inside(ui, |ui| {
            let rows: Vec<TorrentRow> = self.shared.lock().unwrap().clone();

            if rows.is_empty() {
                ui.centered_and_justified(|ui| {
                    ui.label("No active torrents — click \"Add Torrent\" to begin.");
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
                        ui.strong("Info Hash");
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
                            ui.monospace(
                                egui::RichText::new(&row.info_hash).color(egui::Color32::GRAY),
                            );

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
                                        StableState::Paused | StableState::Stopped
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

// ── backend ───────────────────────────────────────────────────────────────────

async fn backend_main(
    mut cmd_rx: async_mpsc::UnboundedReceiver<RpcRequest>,
    shared: Arc<Mutex<Vec<TorrentRow>>>,
    shutdown: CancellationToken,
    gui_ctrl_c_tx: std_mpsc::Sender<()>,
) {
    // Dedicated task: waits for ctrl-c, then notifies both the GUI and the
    // backend select loop so both sides shut down gracefully.
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

    let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("backend: shutdown signal — saving session");
                break;
            }
            _ = interval.tick() => {
                refresh_rows(&session, &shared).await;
            }
            Some(cmd) = cmd_rx.recv() => {
                let (rsp, _) = handle_rpc(&session, cmd).await;
                if let RpcResponse::Error { error } = rsp {
                    tracing::warn!("rpc error: {error}");
                }
            }
        }
    }

    // Persist session state to disk.
    let dump = session.shutdown().await;
    match serde_json::to_string_pretty(&dump) {
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

async fn refresh_rows(session: &Session, shared: &Arc<Mutex<Vec<TorrentRow>>>) {
    let (list_rsp, _) = handle_rpc(session, RpcRequest::ListTorrents).await;
    let hashes = match list_rsp {
        RpcResponse::ListTorrents { torrents } => torrents,
        _ => return,
    };

    let mut rows = Vec::with_capacity(hashes.len());
    for hash in &hashes {
        let (status_rsp, _) = handle_rpc(
            session,
            RpcRequest::GetTorrentStatus {
                info_hash: hash.clone(),
            },
        )
        .await;
        if let RpcResponse::TorrentStatus {
            info_hash,
            process,
            bandwidth_bps,
            state,
            ..
        } = status_rsp
        {
            rows.push(TorrentRow {
                state,
                info_hash,
                progress: process,
                speed_bps: bandwidth_bps,
            });
        }
    }

    *shared.lock().unwrap() = rows;
}

// ── entry point ───────────────────────────────────────────────────────────────

fn main() {
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
    let (cmd_tx, cmd_rx) = async_mpsc::unbounded_channel::<RpcRequest>();
    let shutdown = CancellationToken::new();
    // Channel for the ctrl-c task to signal the GUI to close its viewport.
    let (gui_ctrl_c_tx, gui_ctrl_c_rx) = std_mpsc::channel::<()>();

    // Launch the tokio backend on a dedicated OS thread.
    let backend_handle = {
        let shared = shared.clone();
        let shutdown = shutdown.clone();
        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            rt.block_on(backend_main(cmd_rx, shared, shutdown, gui_ctrl_c_tx));
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
        Box::new(move |_cc| Ok(Box::new(GuiApp::new(cmd_tx, shared, gui_ctrl_c_rx)))),
    ) {
        eprintln!("eframe error: {e}");
    }

    // Window has closed.  Signal the backend to save the session and wait for
    // it to finish before the process exits.
    shutdown.cancel();
    if let Err(e) = backend_handle.join() {
        eprintln!("backend thread panicked: {e:?}");
    }
    println!("Goodbye!");
}

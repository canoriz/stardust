use crate::tracker::{self, AnnounceResult, AnnounceType, TrackerGet};
use crate::transmit_manager;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};
use tokio_util::time::{delay_queue, DelayQueue};
use tracing::info;

#[derive(Debug)]
pub enum Msg {
    AddUrl(Vec<String>),
    RemoveUrl(String),
    Pause,
    Resume,
}

pub struct AnnounceManagerHandle {
    cmd_tx: mpsc::UnboundedSender<Msg>,
    cancel: DropGuard,
    done: oneshot::Receiver<()>,
}

impl AnnounceManagerHandle {
    pub fn new(
        peer_id: [u8; 20],
        port: u16,
        info_hash: [u8; 20],
        tx: mpsc::UnboundedSender<transmit_manager::Msg>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let cancel = CancellationToken::new();
        let (done_tx, done_rx) = oneshot::channel();

        let manager = AnnounceManager {
            peer_id,
            port,
            announce_list: vec![],
            receiver: cmd_rx,

            transmit_mgr: tx,
            announce_timer: DelayQueue::new(),
            url_list: HashMap::new(),
            paused: false,
        };

        #[cfg(feature = "mock_delay")]
        tokio::spawn(run_announce_manager::<FakeAnnouncer>(
            manager,
            info_hash,
            cancel.clone(),
            done_tx,
        ));
        #[cfg(not(feature = "mock_delay"))]
        tokio::spawn(run_announce_manager::<tracker::Announcer>(
            manager,
            info_hash,
            cancel.clone(),
            done_tx,
        ));

        Self {
            cmd_tx,
            cancel: cancel.drop_guard(),
            done: done_rx,
        }
    }

    pub fn send(&self, msg: Msg) {
        self.cmd_tx.send(msg);
    }

    pub async fn stop_wait(self) {
        self.cancel.disarm().cancel();
        self.done.await;
    }
}

struct AnnounceManager {
    peer_id: [u8; 20],
    port: u16,
    announce_list: Vec<Vec<String>>,
    receiver: mpsc::UnboundedReceiver<Msg>,
    transmit_mgr: mpsc::UnboundedSender<transmit_manager::Msg>,
    announce_timer: DelayQueue<(AnnounceType, Arc<String>)>,
    url_list: HashMap<(AnnounceType, Arc<String>), (Option<delay_queue::Key>, u64)>,
    paused: bool,
}

impl AnnounceManager {
    fn handle_msg(&mut self, m: Msg) {
        match m {
            // TODO: use ref?
            Msg::AddUrl(urls) => {
                // self.announce_list.push(vec![url]);
                for url in urls {
                    let u = Arc::new(url);
                    let u2 = u.clone();
                    let u3 = u.clone();
                    let u4 = u.clone();
                    if self.paused {
                        self.url_list.insert((AnnounceType::V4, u3), (None, 0));
                        self.url_list.insert((AnnounceType::V6, u4), (None, 0));
                    } else {
                        let k1 = self
                            .announce_timer
                            .insert((AnnounceType::V4, u), Duration::from_secs(0));
                        let k2 = self
                            .announce_timer
                            .insert((AnnounceType::V6, u2), Duration::from_secs(0));
                        self.url_list.insert((AnnounceType::V4, u3), (Some(k1), 0));
                        self.url_list.insert((AnnounceType::V6, u4), (Some(k2), 0));
                    }
                }
            }
            Msg::RemoveUrl(url) => {
                info!("removing announce url {url}");
                let ptr = Arc::new(url);
                if let Some((Some(k), _)) = self.url_list.remove(&(AnnounceType::V4, ptr.clone()))
                {
                    info!("abort {}", &ptr.as_ref());
                    self.announce_timer.try_remove(&k);
                }
                if let Some((Some(k), _)) = self.url_list.remove(&(AnnounceType::V6, ptr)) {
                    info!("abort 2");
                    self.announce_timer.try_remove(&k);
                }
                info!("announce manager url list {}", self.url_list.len());
            }
            Msg::Pause => {
                info!("announce manager paused");
                self.paused = true;
                self.announce_timer.clear();
                for (key, _) in self.url_list.values_mut() {
                    *key = None;
                }
            }
            Msg::Resume => {
                info!("announce manager resumed");
                self.paused = false;
                for (k, (key, sleeped)) in self.url_list.iter_mut() {
                    *sleeped = 0;
                    *key = Some(
                        self.announce_timer
                            .insert(k.clone(), Duration::from_secs(0)),
                    );
                }
            }
        }
    }
    // pub fn new(m: metadata::Metadata) -> Self {
    //     mpsc::unbounded_channel();
    //     Self {
    //         announce_list: Vec::new(),
    //         metadata: m,
    //         receiver: cmd_receiver,
    //         self_handle: TransmitManagerHandle(cmd_sender),
    //         // announce_handle: None,
    //         // announce_tx: None,
    //         connected_peers: HashMap::new(),
    //     }
    // }
}

async fn run_announce_manager<A>(
    mut manager: AnnounceManager,
    info_hash: [u8; 20],
    cancel: CancellationToken,
    done: oneshot::Sender<()>,
) where
    A: tracker::Announce + 'static,
{
    // async fn start_worker<'a, A>(&mut self, tg: &TrackerGet<'a>, m: &Metadata)
    // where
    // A: metadata::Announce,
    // {
    // for url in manager.announce_list.iter().flat_map(|e| e) {
    //     let res = A::announce_tier(AnnounceType::V4, tg, m, url.clone()).await;
    //     if let Err(e) = self.main_tx.send(Msg::AnnounceFinish(res)) {
    //         info!("announce send to main failed {e}");
    //     }
    //     let res = A::announce_tier(AnnounceType::V6, tg, m, url.clone()).await;
    //     if let Err(e) = self.main_tx.send(Msg::AnnounceFinish(res)) {
    //         info!("announce send to main failed {e}");
    //     }
    // }
    // }
    let (announce_task_tx, announce_task_rx) = mpsc::unbounded_channel();
    let (output_tx, mut output_rx) = mpsc::unbounded_channel();
    tokio::spawn(announce_task::<A>(
        announce_task_rx,
        output_tx,
        cancel.child_token(),
        info_hash,
        manager.peer_id,
        manager.port,
    ));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("announce manager cancelled");
                break;
            }
            r = manager.receiver.recv() => {
                if let Some(msg) = r {
                    info!("announce manager received msg {msg:?}");
                    manager.handle_msg(msg);
                } else {
                    info!("announce manager received None");
                    break;
                }
            }
            Some(expired) = manager.announce_timer.next() => {
                let (announce_type, url) = expired.into_inner();
                info!("announce {:?} url {}", announce_type, url);
                if let Some((key, sleeped)) = manager.url_list.get_mut(&(announce_type, url.clone())) {
                    *key = None;
                    announce_task_tx.send(TimeUp {
                        announce_type,
                        url,
                        sleeped: *sleeped,
                    });
                }
                // else: removed while its timer was in flight; drop it.
            }
            Some((resp, req)) = output_rx.recv() => {
                info!("announce output rx received");
                let next_interval = match &resp {
                    Ok(r) => {
                        info!("announce {} OK, next announce {}", req.url.as_ref(), r.interval);
                        // output.send(resp);
                        r.interval as u64
                    }
                    Err(e) => {
                        // TODO: send event
                        info!("announce failed reason {e}");
                        if req.sleeped == 0 {
                            1
                        } else if req.sleeped > 120 {
                            req.sleeped
                        } else {
                            req.sleeped * 2
                        }
                    }
                };
                info!("next announce interval {next_interval}");

                if let Some((key, sleeped)) = manager.url_list.get_mut(&(req.announce_type, req.url.clone())) {
                    *sleeped = next_interval;
                    if !manager.paused {
                        *key = Some(manager.announce_timer.insert(
                            (req.announce_type, req.url.clone()),
                            Duration::from_secs(next_interval),
                        ));
                    }
                }
                // else: removed while the announce request was in flight; drop it.

                let send_res = manager.transmit_mgr.send(transmit_manager::Msg::AnnounceFinish(resp));
                if send_res.is_err() {
                    info!("send announce res to main error {send_res:?}");
                }
            }
        };
    }
    let _ = done.send(());
}

struct FakeAnnouncer {}
impl tracker::Announce for FakeAnnouncer {
    async fn announce_tier(
        _net_type: tracker::AnnounceType,
        _req: &TrackerGet,
        _torrent: &[u8; 20],
        _url: String,
    ) -> tracker::AnnounceResult {
        // return Err(metadata::AnnounceError::ClientErr(
        //     metadata::ClientErr::Ipv4Err,
        // ));
        Ok(tracker::AnnounceResp {
            interval: 1800,
            peers: vec![
                tracker::Peer {
                    peer_id: Some("1384".into()),
                    addr: std::net::SocketAddr::new(
                        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                        51774,
                    ),
                },
                // metadata::Peer {
                //     peer_id: "1384".into(),
                //     ip: "127.0.0.1".into(),
                //     port: 35516,
                // },
            ],
            peers6: vec![],
        })
    }
}

#[derive(Debug)]
struct TimeUp {
    announce_type: AnnounceType,
    url: Arc<String>,
    sleeped: u64,
}

// all announce request go to this task
// sends to output channel
async fn announce_task<A>(
    mut rx: mpsc::UnboundedReceiver<TimeUp>,
    output: mpsc::UnboundedSender<(AnnounceResult, TimeUp)>,
    cancel: CancellationToken,
    info_hash: [u8; 20],
    id: [u8; 20],
    port: u16,
) where
    A: tracker::Announce,
{
    let tg = TrackerGet {
        peer_id: id,
        port: port,
        uploaded: 0,   // TODO: get real uploaded/downloaded/left
        downloaded: 0, // TODO: get real uploaded/downloaded/left
        ip: None,
        left: 0, // TODO: get real uploaded/downloaded/left
    };
    loop {
        tokio::select! {
            r = rx.recv() => {
                if let Some(req) = r {
                    let resp = A::announce_tier(
                        req.announce_type,
                        &tg,
                        &info_hash,
                        req.url.as_ref().clone(),
                    ).await;
                    info!("announce {:?} {} response result {resp:?}", req.announce_type, &req.url);
                    output.send((resp, req));
                } else {
                    info!("announce rx recv None");
                    break;
                }
            }
            _ = cancel.cancelled() => {
                info!("announce task cancelled");
                break;
            }
        }
    }
    info!("announce task ends");
}

// async fn announce_url<'a, A>(
//     main_tx: mpsc::UnboundedSender<Msg>,
//     announce_req: Arc<metadata::TrackerGet<'a>>,
//     m: Arc<metadata::Metadata>,
//     urls: Vec<String>,
//     mut input_cmd: mpsc::Receiver<u32>,
// ) where
//     A: metadata::Announce,
// {
//     // TODO: simplify into a struct methods

//     #[derive(Eq, PartialEq, Hash, Debug)]
//     struct TimeUp(AnnounceType, Arc<String>, u64);

//     let mut timers = task::JoinSet::<TimeUp>::new();
//     let mut map: HashMap<(AnnounceType, Arc<String>), tokio::task::AbortHandle> = HashMap::new();

//     for url in urls {
//         let u = Arc::new(url);
//         let u2 = u.clone();
//         let u3 = u.clone();
//         let u4 = u.clone();
//         let h1 = timers.spawn(async { TimeUp(AnnounceType::V4, u, 0) });
//         let h2 = timers.spawn(async { TimeUp(AnnounceType::V6, u2, 0) });
//         map.insert((AnnounceType::V4, u3), h1);
//         map.insert((AnnounceType::V6, u4), h2);
//     }

//     loop {
//         // TODO: simplify macro inners
//         tokio::select! {
//             c = input_cmd.recv() => {
//                 info!("announce task received cmd {c:?}");
//                 match c {
//                     Some(_) => {},
//                     None => break // closed
//                 }
//             }
//             join_res = timers.join_next() => {
//                 let TimeUp(announce_type, url, sleeped) = match join_res {
//                     Some(Ok(timeup)) => {
//                         info!("announcing {timeup:?}");
//                         timeup
//                     },
//                     Some(Err(e)) => {
//                         info!("join error {e}");
//                         continue
//                     },
//                     None => {break},
//                 };
//                 map.remove(&(announce_type, url.clone()));

//                 let res = A::announce_tier(
//                     announce_type,
//                     &announce_req,
//                     m.as_ref(),
//                     url.as_ref().clone(),
//                 )
//                 .await;
//                 let interval = match &res {
//                     Ok(r) => {
//                         info!("announce task OK, next announce {}", r.interval);
//                         r.interval as u64
//                     }
//                     Err(e) => {
//                         // TODO: send event
//                         info!("announce failed reason {e}");
//                         if sleeped == 0 {
//                             1
//                         } else {
//                             sleeped * 2
//                         }
//                     }
//                 };

//                 timers.spawn(
//                     async move {
//                         time::sleep(Duration::from_secs(interval)).await;
//                         TimeUp(announce_type, url, interval)
//                     }
//                 );

//                 let send_res = main_tx.send(Msg::AnnounceFinish(res));
//                 if send_res.is_err() {
//                     info!("send announce res to main error {send_res:?}");
//                 }
//             }
//         }
//     }
//     timers.shutdown().await;
// }

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::info;

use crate::dht::DHT;
use crate::protocol::{self, AcceptOpt, BTStream, HandshakeOption};
use crate::torrent_manager::TorrentManagerHandle;
use crate::transmit_manager::{TorrentTask, TransmitManagerHandle};
use crate::{Reunite, Split};

type InfoHash = [u8; 20];
pub struct Session {
    self_id: [u8; 20], // TODO: use randomized self_id

    tasks: Arc<Mutex<HashMap<InfoHash, TorrentManagerHandle>>>,
    port: u16,

    cancel: DropGuard,
}

impl Session {
    pub fn new(port: u16, self_id: [u8; 20]) -> Self {
        let tasks = Arc::new(Mutex::new(HashMap::new()));
        let cancel = CancellationToken::new();

        let l = Listener {
            self_id,
            tasks: tasks.clone(),
            cancel: cancel.clone(),
        };
        tokio::spawn(run_listener(l, port));

        Self {
            self_id,
            tasks,
            port,
            cancel: cancel.drop_guard(),
        }
    }

    /// add new torrent
    pub fn add_torrent(&mut self, job: TorrentTask) {
        // TODO: randomized self_id
        const DHT_PORT: u16 = 41773;
        const SELF_PORT: u16 = 41773;
        const SELF_ID: [u8; 20] = *b"-TR0300-fjbo402nczk3";
        let info_hash = job.info_hash();
        let dht_client = Arc::new(DHT::new(SELF_ID, DHT_PORT, "ST01".into()));
        let tm = TorrentManagerHandle::new(job, SELF_ID, SELF_PORT, Some(dht_client));
        self.tasks.lock().unwrap().insert(info_hash, tm);
    }

    /// remove torrent by info_hash
    pub async fn remove_torrent(&mut self, info_hash: &InfoHash) {
        match self.tasks.lock().unwrap().remove(info_hash) {
            Some(tm) => tm.stop_wait().await,
            None => {}
        }
    }
}

struct Listener {
    tasks: Arc<Mutex<HashMap<InfoHash, TorrentManagerHandle>>>,
    cancel: CancellationToken,
    self_id: [u8; 20],
}

async fn run_listener(l: Listener, port: u16) -> std::io::Result<()> {
    // TODO: set v6_only to false
    let listener = TcpListener::bind(format!("[::]:{port}")).await?;
    loop {
        tokio::select! {
            _ = l.cancel.cancelled() => {
                info!("session listener cancelled");
                break;
            }
            Ok((conn, addr)) = listener.accept() => {
                let ic = IncomeConn {
                    conn,
                    addr,
                    self_id: l.self_id,
                    tasks: l.tasks.clone(),
                };
                handle_income_connection(ic).await;
            }
        }
    }
    todo!()
}

struct IncomeConn<T> {
    conn: T,
    addr: SocketAddr,
    self_id: [u8; 20],
    tasks: Arc<Mutex<HashMap<InfoHash, TorrentManagerHandle>>>,
}

async fn handle_income_connection<T>(conn: IncomeConn<T>) -> std::io::Result<()>
where
    T: AsyncRead + AsyncWrite + Split + Unpin + Send + 'static,
    <T as Split>::R: Reunite<W = <T as Split>::W, U = T>,
{
    let opt = HandshakeOption::builder()
        .client_id([0; 20])
        .client_version("1".into())
        .info_hash([0; 20])
        .dht_port(Some(1))
        .build();

    let map = conn.tasks.clone();
    let accept = async move |h: &protocol::Handshake| -> AcceptOpt {
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = map.lock().unwrap();
            if let Some(tm) = guard.get_mut(&h.torrent_hash) {
                match tm.send_msg(crate::transmit_manager::Msg::RequestMetadata(tx)) {
                    Ok(_) => {}
                    Err(e) => {
                        info!("request metadata from transmit manager error: {}", e);
                        return AcceptOpt::Reject;
                    }
                }
            } else {
                return AcceptOpt::Reject;
            }
        };

        match rx.await {
            Ok(Some(m)) => AcceptOpt::HaveMetadata(m),
            Ok(None) => AcceptOpt::NoMetadata,
            Err(e) => {
                info!("request metadata error: {}", e);
                AcceptOpt::Reject
            }
        }
    };

    let bt_conn = match BTStream::accept(conn.conn, accept, opt).await {
        Ok(c) => c,
        Err(e) => {
            info!("accept handshake from {} error: {}", conn.addr, e);
            return Err(e);
        }
    };
    bt_conn.info();

    todo!("insert connection to proper torrent manager");
    // let mut guard = map.lock().unwrap();
    // if let Some(tm) = guard.get_mut(&h.torrent_hash) {
    //     match tm.send_msg(crate::transmit_manager::Msg::RequestMetadata(tx)) {
    //         Ok(_) => {}
    //         Err(e) => {
    //             info!("request metadata from transmit manager error: {}", e);
    //             return AcceptOpt::Reject;
    //         }
    //     }
    // } else {
    //     return AcceptOpt::Reject;
    // }
}

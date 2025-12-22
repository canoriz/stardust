use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::{io, time};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::info;

use crate::dht::{self, DHT};
use crate::protocol::{AcceptOpt, BTStream, HandshakeOption, InfoHash};
use crate::torrent_manager::TorrentManagerHandle;
use crate::transmit_manager::{TorrentTask, TransmitManagerHandle};
use crate::{announce_manager, Reunite, Split};

pub struct Session {
    self_id: [u8; 20], // TODO: use randomized self_id

    tasks: Arc<Mutex<HashMap<InfoHash, TorrentManagerHandle>>>,
    port: u16,
    dht_client: Arc<DHT>,

    cancel: DropGuard,
}

impl Session {
    pub fn new(self_id: [u8; 20], port: u16, dht_port: u16) -> Self {
        let tasks = Arc::new(Mutex::new(HashMap::new()));
        let cancel = CancellationToken::new();

        let l = Listener {
            self_id,
            tasks: tasks.clone(),
            cancel: cancel.clone(),
            dht_port, // TODO: what if dht client init failed?
        };
        tokio::spawn(run_listener(l, port));

        // TODO: clients connect to us who prefers uTP will be rejected by our DHT handler
        // and not trying to connect with TCP
        // support dual protocol on DHT port, or choose a different dht/tcp port
        let dht_client = Arc::new(DHT::new(self_id, dht_port, "ST01".into()));
        let c = dht_client.clone();

        // TODO: optimize: maybe wait dht bootstrap done then return session
        // TODO: share dht network between sessions?
        tokio::spawn(async move {
            _ = c
                .ping_rpc(
                    dht::RpcAddr::NoID(
                        "[240e:b8f:5c68:8400:4c07:3e69:7b5a:741]:54032"
                            .parse()
                            .unwrap(),
                    ),
                    time::Duration::from_secs(5),
                )
                .await;
            c.find_closest_node_to(self_id, true).await;
        });

        Self {
            self_id,
            tasks,
            port,
            dht_client,
            cancel: cancel.drop_guard(),
        }
    }

    /// add new torrent
    pub fn add_torrent(&mut self, job: TorrentTask, announce_list: Vec<Vec<String>>) {
        let info_hash = job.info_hash();
        let mut tm =
            TorrentManagerHandle::new(job, self.self_id, self.port, Some(self.dht_client.clone()));

        for addr in announce_list {
            tm.send_announce_msg(announce_manager::Msg::AddUrl(addr));
        }
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
    dht_port: u16,
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
                    dht_port: l.dht_port,
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
    dht_port: u16,
    addr: SocketAddr,
    self_id: [u8; 20],
    tasks: Arc<Mutex<HashMap<InfoHash, TorrentManagerHandle>>>,
}

async fn handle_income_connection<T>(conn: IncomeConn<T>) -> io::Result<()>
where
    T: AsyncRead + AsyncWrite + Split + Unpin + Send + 'static,
    <T as Split>::R: Reunite<W = <T as Split>::W, U = T>,
{
    let opt = HandshakeOption::builder()
        .client_id(conn.self_id)
        .client_version("ST01".into())
        .dht_port(Some(conn.dht_port))
        .build();

    let map = conn.tasks.clone();
    let accept = async move |hash: &InfoHash| -> AcceptOpt {
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = map.lock().unwrap();
            if let Some(tm) = guard.get_mut(hash) {
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

    let mut guard = conn.tasks.lock().unwrap();
    if let Some(tm) = guard.get_mut(bt_conn.info().info_hash) {
        tm.send_msg(crate::transmit_manager::Msg::NewPeer(Ok((
            bt_conn.to_dyn(),
            true,
        ))))
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                "task dropped during incoming connection processing",
            )
        })
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "task deleted during incoming connection processing",
        ))
    }
}

mod bandwidth_mode;
mod conn;
mod inflight;

pub use bandwidth_mode::BandwidthMode;
use conn::INIT_RTT_STARTUP;
pub use conn::{PeerConn, Stat, Version};

use std::net::SocketAddr;

/// A connection's address together with the id of that connection. A later
/// connection to the same address gets a new id, so a stale message can be told
/// apart from the live one.
pub type PeerAddr = (SocketAddr, Version);
pub use inflight::Inflight;

#[cfg(feature = "blocktrace")]
pub use bandwidth_mode::bt_bw_mode;

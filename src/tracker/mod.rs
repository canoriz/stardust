use core::fmt;
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::LazyLock,
};

use bt_bencode::ByteString;
use reqwest::Client;
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct TrackerGet {
    pub peer_id: [u8; 20],
    pub ip: Option<Ipv4Addr>,
    pub port: u16,
    pub uploaded: usize,
    pub downloaded: usize,
    pub left: usize,
    // event: Option<Enum<...>>
}

impl TrackerGet {
    pub fn url(&self, info_hash: &[u8; 20], url: String) -> String {
        fn percent_encoding_str<T: AsRef<[u8]>, P: AsRef<[u8]>>(k: &T, v: &P) -> String {
            percent_encoding::percent_encode(k.as_ref(), percent_encoding::NON_ALPHANUMERIC)
                .collect::<String>()
                + "="
                + &percent_encoding::percent_encode(v.as_ref(), percent_encoding::NON_ALPHANUMERIC)
                    .collect::<String>()
        }

        let mut query = [
            percent_encoding_str(&"info_hash", &info_hash),
            percent_encoding_str(&"peer_id", &self.peer_id),
            percent_encoding_str(&"port", &self.port.to_string()),
            percent_encoding_str(&"uploaded", &self.uploaded.to_string()),
            percent_encoding_str(&"downloaded", &self.downloaded.to_string()),
            percent_encoding_str(&"left", &self.left.to_string()),
            percent_encoding_str(&"compact", &"1"),
        ]
        .join("&");
        if let Some(ref ip) = self.ip {
            query += "&";
            query += &percent_encoding_str(&"ip", &ip.to_string());
        }

        url + "?" + &query
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
enum TrackerResp {
    #[serde(untagged)]
    Failure(Failure),
    #[serde(untagged)]
    Success(AnnounceResp),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct AnnounceResp {
    pub interval: u32,
    #[serde(deserialize_with = "deserialize_peers")]
    pub peers: Vec<Peer>,
    /// Compact IPv6 peers (BEP7): 18 bytes each — 16-byte IPv6 address + 2-byte port, big-endian.
    /// Absent in most responses; defaults to an empty vec.
    #[serde(default, deserialize_with = "deserialize_peers6")]
    pub peers6: Vec<Peer>,
}

/// Deserialize `peers` from either:
/// - compact format: a bencode byte string, 6 bytes per peer (4 IP + 2 port, big-endian)
/// - dict format: a bencode list of dicts with `peer id`, `ip`, `port` keys
fn deserialize_peers<'de, D>(deserializer: D) -> Result<Vec<Peer>, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct PeersVisitor;

    impl<'de> Visitor<'de> for PeersVisitor {
        type Value = Vec<Peer>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("compact peer bytes or list of peer dicts")
        }

        // Compact format: tracker sends peers as a raw byte string.
        // bt_bencode delivers bencode byte strings via visit_bytes / visit_byte_buf.
        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
            parse_compact_peers(v).map_err(de::Error::custom)
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
            parse_compact_peers(&v).map_err(de::Error::custom)
        }

        // Dict-list format: tracker sends peers as a list of dicts.
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut peers = Vec::new();
            while let Some(p) = seq.next_element::<Peer>()? {
                peers.push(p);
            }
            Ok(peers)
        }

        // Some trackers send an empty dict `de` instead of an empty list `le` for no peers.
        fn visit_map<A: de::MapAccess<'de>>(self, _map: A) -> Result<Self::Value, A::Error> {
            Ok(vec![])
        }
    }

    deserializer.deserialize_any(PeersVisitor)
}

fn parse_compact_peers(data: &[u8]) -> Result<Vec<Peer>, &'static str> {
    if data.len() % 6 != 0 {
        return Err("compact peers length must be a multiple of 6");
    }
    Ok(data
        .chunks_exact(6)
        .map(|chunk| {
            let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            Peer {
                peer_id: None,
                addr: SocketAddr::new(IpAddr::V4(ip), port),
            }
        })
        .collect())
}

/// Deserialize `peers6` from a compact byte string: 18 bytes per peer
/// (16-byte IPv6 address + 2-byte port, big-endian).
fn deserialize_peers6<'de, D>(deserializer: D) -> Result<Vec<Peer>, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct Peers6Visitor;

    impl<'de> Visitor<'de> for Peers6Visitor {
        type Value = Vec<Peer>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("compact IPv6 peer bytes (18 bytes per peer)")
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
            parse_compact_peers6(v).map_err(de::Error::custom)
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
            parse_compact_peers6(&v).map_err(de::Error::custom)
        }

        // Dict-list format: tracker sends peers as a list of dicts.
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut peers = Vec::new();
            while let Some(p) = seq.next_element::<Peer>()? {
                peers.push(p);
            }
            Ok(peers)
        }

        // Some trackers send an empty dict `de` instead of an empty list `le` for no peers.
        fn visit_map<A: de::MapAccess<'de>>(self, _map: A) -> Result<Self::Value, A::Error> {
            Ok(vec![])
        }
    }

    deserializer.deserialize_any(Peers6Visitor)
}

fn parse_compact_peers6(data: &[u8]) -> Result<Vec<Peer>, &'static str> {
    if data.len() % 18 != 0 {
        return Err("compact peers6 length must be a multiple of 18");
    }
    Ok(data
        .chunks_exact(18)
        .map(|chunk| {
            let addr: [u8; 16] = chunk[..16].try_into().unwrap();
            let ip = Ipv6Addr::from(addr);
            let port = u16::from_be_bytes([chunk[16], chunk[17]]);
            Peer {
                peer_id: None,
                addr: SocketAddr::new(IpAddr::V6(ip), port),
            }
        })
        .collect())
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq)]
pub struct Peer {
    pub peer_id: Option<ByteString>,
    pub addr: SocketAddr,
}

impl<'de> de::Deserialize<'de> for Peer {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PeerVisitor;
        impl<'de> Visitor<'de> for PeerVisitor {
            type Value = Peer;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("peer dict with ip and port fields")
            }
            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut peer_id: Option<ByteString> = None;
                let mut ip: Option<IpAddr> = None;
                let mut port: Option<u16> = None;
                while let Some(key) = map.next_key::<ByteString>()? {
                    match key.as_ref() {
                        b"peer id" => peer_id = Some(map.next_value()?),
                        b"ip" => {
                            let s = map.next_value::<ByteString>()?;
                            ip = Some(
                                std::str::from_utf8(s.as_ref())
                                    .map_err(de::Error::custom)?
                                    .parse()
                                    .map_err(de::Error::custom)?,
                            );
                        }
                        b"port" => port = Some(map.next_value()?),
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                let ip = ip.ok_or_else(|| de::Error::missing_field("ip"))?;
                let port = port.ok_or_else(|| de::Error::missing_field("port"))?;
                Ok(Peer {
                    peer_id,
                    addr: SocketAddr::new(ip, port),
                })
            }
        }
        deserializer.deserialize_map(PeerVisitor)
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Failure {
    #[serde(rename = "failure reason")]
    pub reason: String,
}

fn ipv6_client() -> Result<Client, reqwest::Error> {
    const V6_ADDR: Ipv6Addr = Ipv6Addr::from_bits(0);
    Client::builder().local_address(IpAddr::V6(V6_ADDR)).build()
}

fn ipv4_client() -> Result<Client, reqwest::Error> {
    const V4_ADDR: Ipv4Addr = Ipv4Addr::from_bits(0);
    let builder = Client::builder();
    builder.local_address(IpAddr::V4(V4_ADDR)).build()
}

static IPV4_CLIENT: LazyLock<Option<Client>> = LazyLock::new(|| ipv4_client().ok());
static IPV6_CLIENT: LazyLock<Option<Client>> = LazyLock::new(|| ipv6_client().ok());

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AnnounceType {
    V4,
    V6,
}

#[derive(Error, Debug)]
pub enum ClientErr {
    #[error("Ipv4 client unavailable")]
    Ipv4Err,
    #[error("Ipv6 client unavailable")]
    Ipv6Err,
}

#[derive(Error, Debug)]
pub enum AnnounceError {
    #[error("client error")]
    ClientErr(#[from] ClientErr),
    #[error("tracker failure")]
    TrackerFailure(Failure),
    #[error("request error")]
    RequestErr(#[from] reqwest::Error),
    #[error("bencode error")]
    BencodeErr(#[from] bt_bencode::Error),
}

pub type AnnounceResult = Result<AnnounceResp, AnnounceError>;

// let res: Result<metadata::AnnounceResp, metadata::AnnounceError> =
//     Ok(metadata::AnnounceResp {
//         interval: 1800,
//         peers: vec![metadata::Peer {
//             peer_id: "1384".into(),
//             ip: "127.0.0.1".into(),
//             port: 35515,
//         }],
//     });

pub trait Announce {
    // TODO: maybe don't need announcer, just a function is enough
    fn announce_tier(
        net_type: AnnounceType,
        req: &TrackerGet,
        torrent: &[u8; 20],
        url: String,
    ) -> impl Future<Output = AnnounceResult> + Send;
}

#[derive(Clone)]
pub struct Announcer {}

impl Announce for Announcer {
    async fn announce_tier(
        net_type: AnnounceType,
        req: &TrackerGet,
        info_hash: &[u8; 20],
        url: String,
    ) -> AnnounceResult {
        announce_one(net_type, req, info_hash, url).await
    }
}

async fn announce_one(
    net_type: AnnounceType,
    req: &TrackerGet,
    info_hash: &[u8; 20],
    url: String,
) -> AnnounceResult {
    let url2 = url.clone();
    let request = match net_type {
        AnnounceType::V4 => match *IPV4_CLIENT {
            Some(ref client) => client.get(req.url(info_hash, url)),
            None => return Err(ClientErr::Ipv4Err.into()),
        },
        AnnounceType::V6 => match *IPV6_CLIENT {
            Some(ref client) => client.get(req.url(info_hash, url)),
            None => return Err(ClientErr::Ipv6Err.into()),
        },
    }; // TODO: request more tiers url

    let resp_bytes = request.send().await?.bytes().await?;
    let decoded = bt_bencode::from_slice::<TrackerResp>(&resp_bytes);
    match decoded {
        Ok(TrackerResp::Success(s)) => Ok(s),
        Ok(TrackerResp::Failure(f)) => Err(AnnounceError::TrackerFailure(f)),
        Err(e) => {
            warn!(
                "announce bencode decode error, url: {}, raw: {:?}",
                url2, &resp_bytes,
            );
            Err(AnnounceError::from(e))
        }
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_compact_peers_two_peers() {
        // BEP23 compact format: 6 bytes per peer (4 IP + 2 port, big-endian)
        // peers: 1.2.3.4:256 and 192.168.0.1:6881
        let compact: &[u8] = &[1, 2, 3, 4, 1, 0, 192, 168, 0, 1, 26, 225];
        // bencode: d8:intervali1800e5:peers12:<compact bytes>e
        let mut encoded = b"d8:intervali1800e5:peers12:".to_vec();
        encoded.extend_from_slice(compact);
        encoded.push(b'e');

        let decoded = bt_bencode::from_slice::<AnnounceResp>(&encoded).unwrap();
        assert_eq!(decoded.interval, 1800);
        assert_eq!(decoded.peers.len(), 2);
        assert_eq!(
            decoded.peers[0].addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 256)
        );
        assert_eq!(
            decoded.peers[1].addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)), 6881)
        );
        assert_eq!(decoded.peers6.len(), 0);
    }

    #[test]
    fn test_compact_peers6() {
        // BEP7 compact IPv6 format: 18 bytes per peer (16 IP + 2 port, big-endian)
        // peer: ::1 (loopback) on port 6881
        let mut ipv6_bytes = [0u8; 18];
        ipv6_bytes[15] = 1; // ::1
        ipv6_bytes[16] = 0x1A;
        ipv6_bytes[17] = 0xE1; // 6881

        // bencode: d8:intervali1800e5:peers0:6:peers618:<bytes>e
        let mut encoded = b"d8:intervali1800e5:peers0:6:peers618:".to_vec();
        encoded.extend_from_slice(&ipv6_bytes);
        encoded.push(b'e');

        let decoded = bt_bencode::from_slice::<AnnounceResp>(&encoded).unwrap();
        assert_eq!(decoded.interval, 1800);
        assert_eq!(decoded.peers.len(), 0);
        assert_eq!(decoded.peers6.len(), 1);
        assert_eq!(
            decoded.peers6[0].addr,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 6881)
        );
    }

    #[test]
    fn test_list_peers6() {
        // BEP7 non-compact format for peers6: a list of dicts with "ip" (IPv6 string) and "port".
        use bt_bencode::Value;
        use std::collections::BTreeMap;

        let mut peer = BTreeMap::new();
        peer.insert(
            ByteString::from("ip"),
            Value::ByteStr(ByteString::from("2001:db8::1")),
        );
        peer.insert(ByteString::from("port"), Value::from(6881u32));

        let mut resp = BTreeMap::new();
        resp.insert(ByteString::from("interval"), Value::from(1800u32));
        resp.insert(
            ByteString::from("peers"),
            Value::ByteStr(ByteString::from("")),
        );
        resp.insert(
            ByteString::from("peers6"),
            Value::List(vec![Value::Dict(peer)]),
        );
        let encoded = bt_bencode::to_vec(&Value::Dict(resp)).unwrap();

        let decoded = bt_bencode::from_slice::<AnnounceResp>(&encoded).unwrap();
        assert_eq!(decoded.interval, 1800);
        assert_eq!(decoded.peers.len(), 0);
        assert_eq!(decoded.peers6.len(), 1);
        assert_eq!(
            decoded.peers6[0].addr,
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
                6881
            )
        );
    }

    #[test]
    fn test_deserialize_empty_peers6_dict_quirk() {
        // Some trackers send an empty dict `de` instead of an empty list `le` for peers6.
        let resp = b"d8:intervali1800e5:peers0:6:peers6dee";
        let decoded = bt_bencode::from_slice::<AnnounceResp>(resp).unwrap();
        assert_eq!(decoded.interval, 1800);
        assert_eq!(decoded.peers.len(), 0);
        assert_eq!(decoded.peers6.len(), 0);
    }

    #[test]
    fn test_list_peers() {
        // BEP3 non-compact format: peers as a list of dicts with "peer id", "ip", "port"
        use bt_bencode::Value;
        use std::collections::BTreeMap;

        let mut peer = BTreeMap::new();
        peer.insert(
            ByteString::from("peer id"),
            Value::ByteStr(ByteString::from("xxxx")),
        );
        peer.insert(
            ByteString::from("ip"),
            Value::ByteStr(ByteString::from("127.0.0.1")),
        );
        peer.insert(ByteString::from("port"), Value::from(8080u32));

        let mut resp = BTreeMap::new();
        resp.insert(ByteString::from("interval"), Value::from(1800u32));
        resp.insert(
            ByteString::from("peers"),
            Value::List(vec![Value::Dict(peer)]),
        );
        let encoded = bt_bencode::to_vec(&Value::Dict(resp)).unwrap();

        let decoded = bt_bencode::from_slice::<AnnounceResp>(&encoded).unwrap();
        assert_eq!(decoded.interval, 1800);
        assert_eq!(decoded.peers.len(), 1);
        assert_eq!(
            decoded.peers[0].addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
        );
    }

    #[test]
    // some trackers return empty peers dict, not empty peer list, test if we can decode it correctly
    fn test_deserialize_empty_peers_dict_quirk() {
        let resp = *b"d8:intervali1800e5:peersdee";
        let r0 = bt_bencode::to_vec(&TrackerResp::Success(AnnounceResp {
            interval: 1800,
            peers: vec![],
            peers6: vec![],
        }))
        .unwrap();
        println!("r0 = {:?}", String::from_utf8(r0));
        let decoded = bt_bencode::from_slice::<TrackerResp>(&resp).unwrap();
        assert_eq!(
            decoded,
            TrackerResp::Success(AnnounceResp {
                interval: 1800,
                peers: vec![],
                peers6: vec![],
            })
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_real_torrent() {
        use crate::metadata::FileMetadata;
        let torrent_f = include_bytes!("../../ubuntu-24.10-desktop-amd64.iso.torrent");
        let torrent = FileMetadata::load(torrent_f).unwrap();
        let (metadata, announce_list) = torrent.to_metadata();

        let announce_req = TrackerGet {
            peer_id: *b"-ZS0405-qwerasdfzxcv",
            uploaded: 0,
            port: 35515,
            downloaded: 0,
            left: 0,
            ip: None,
        };

        let z = Announcer::announce_tier(
            AnnounceType::V4,
            &announce_req,
            &metadata.info_hash,
            announce_list[0][0].clone(),
        )
        .await
        .unwrap();
        dbg!(z);
    }
}

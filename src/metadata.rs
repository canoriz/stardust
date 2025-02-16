// use anyhow::{anyhow, Result};
pub use bt_bencode::ByteString;
use serde::{Deserialize, Serialize};
use tracing::{info, Level};
mod workaround;
use reqwest::Client;
use sha1::{Digest, Sha1};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::LazyLock;
use thiserror::Error;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metadata {
    announce: String,
    #[serde(rename = "announce-list")]
    announce_list: Option<Vec<Vec<String>>>,

    info: Info,

    #[serde(skip)]
    info_hash: [u8; 20],

    comment: Option<String>,
    #[serde(rename = "created by")]
    created_by: Option<String>,
    #[serde(rename = "creation date")]
    creation_date: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Info {
    name: String,
    #[serde(rename = "piece length")]
    piece_length: u32,
    pieces: ByteString,
    #[serde(flatten)]
    len_or_files: LenFiles,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum LenFiles {
    #[serde(rename = "length")]
    Length(usize),
    #[serde(rename = "files")]
    Files(Vec<File>),
}

impl Metadata {
    pub fn load<T: AsRef<[u8]>>(input: T) -> anyhow::Result<Self> {
        let mut torrent: Metadata = bt_bencode::from_slice(input.as_ref())?;
        let mut hasher = Sha1::new();
        bt_bencode::to_writer(&mut hasher, &torrent.info)?;
        torrent.info_hash = hasher.finalize().into();
        Ok(torrent)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct File {
    length: usize,
    path: String,
}

#[derive(Debug)]
pub struct TrackerGet<'a> {
    pub peer_id: &'a str,
    pub ip: Option<Ipv4Addr>,
    pub port: u32,
    pub uploaded: usize,
    pub downloaded: usize,
    pub left: usize,
    // event: Option<Enum<...>>
}

impl<'a> TrackerGet<'a> {
    pub fn url(&self, meta: &Metadata) -> Vec<String> {
        fn percent_encoding_str<T: AsRef<[u8]>, P: AsRef<[u8]>>(k: &T, v: &P) -> String {
            percent_encoding::percent_encode(k.as_ref(), percent_encoding::NON_ALPHANUMERIC)
                .collect::<String>()
                + "="
                + &percent_encoding::percent_encode(v.as_ref(), percent_encoding::NON_ALPHANUMERIC)
                    .collect::<String>()
        }
        let url: Vec<String> = if let Some(ref announce_list) = meta.announce_list {
            announce_list.iter().flat_map(|url| url.clone()).collect() // TODO: tier
        } else {
            [meta.announce.clone()].into()
        };

        let mut query = [
            percent_encoding_str(&"info_hash", &meta.info_hash),
            percent_encoding_str(&"peer_id", &self.peer_id),
            percent_encoding_str(&"port", &self.port.to_string()),
            percent_encoding_str(&"uploaded", &self.uploaded.to_string()),
            percent_encoding_str(&"downloaded", &self.downloaded.to_string()),
            percent_encoding_str(&"left", &self.left.to_string()),
        ]
        .join("&");
        if let Some(ref ip) = self.ip {
            query += &percent_encoding_str(&"ip", &ip.to_string());
        }

        url.into_iter().map(|u| u + "?" + &query).collect()
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum TrackerResp {
    #[serde(untagged)]
    Failure(Failure),
    #[serde(untagged)]
    Success(AnnounceResp),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AnnounceResp {
    pub interval: u32,
    pub peers: Vec<Peer>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Peer {
    #[serde(rename = "peer id")]
    pub peer_id: ByteString,
    pub ip: String,
    pub port: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Failure {
    #[serde(rename = "failure reason")]
    pub reason: String,
}

fn ipv6_client() -> Result<Client, reqwest::Error> {
    const V6_ADDR: Ipv6Addr = Ipv6Addr::from_bits(0);
    Client::builder()
        .local_address(IpAddr::V6(V6_ADDR))
        .build()
        .map_err(|e| e.into())
}

fn ipv4_client() -> Result<Client, reqwest::Error> {
    const V4_ADDR: Ipv4Addr = Ipv4Addr::from_bits(0);
    let builder = Client::builder();
    builder
        .local_address(IpAddr::V4(V4_ADDR))
        .build()
        .map_err(|e| e.into())
}

static IPV4_CLIENT: LazyLock<Option<Client>> = LazyLock::new(|| ipv4_client().ok());
static IPV6_CLIENT: LazyLock<Option<Client>> = LazyLock::new(|| ipv4_client().ok());

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

pub async fn announce<'a>(
    net_type: AnnounceType,
    req: &TrackerGet<'a>,
    torrent: &Metadata,
) -> Result<AnnounceResp, AnnounceError> {
    let request = match net_type {
        AnnounceType::V4 => match *IPV4_CLIENT {
            Some(ref client) => client.get(&req.url(torrent)[0]),
            None => return Err(ClientErr::Ipv4Err.into()),
        },
        AnnounceType::V6 => match *IPV6_CLIENT {
            Some(ref client) => client.get(&req.url(torrent)[0]),
            None => return Err(ClientErr::Ipv6Err.into()),
        },
    }; // TODO: request more tiers url

    let r = request.send().await?;
    let decoded = bt_bencode::from_slice::<TrackerResp>(&r.bytes().await?);
    match decoded {
        Ok(TrackerResp::Success(s)) => Ok(s),
        Ok(TrackerResp::Failure(f)) => Err(AnnounceError::TrackerFailure(f)),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_real_torrent() {
        let torrent_f = include_bytes!("../ubuntu-24.10-desktop-amd64.iso.torrent");
        let torrent = Metadata::load(torrent_f).unwrap();

        let announce_req = TrackerGet {
            peer_id: "-ZS0405-qwerasdfzxcv".into(),
            uploaded: 0,
            port: 35515,
            downloaded: 0,
            left: 0,
            ip: None,
        };

        let z = announce(AnnounceType::V4, &announce_req, &torrent)
            .await
            .unwrap();
        dbg!(z);
    }

    // #[test]
    // fn test_enum() {
    //     let serialized = serde_json::to_string(&Metadata {
    //         announce: "afasg".into(),
    //         info: Info {
    //             name: "namename".to_string(),
    //             piece_length: 1245,
    //             pieces: vec!["123".into(), "456".into()],
    //             len_or_files: LenFiles::Length(5),
    //         },
    //     })
    //     .unwrap();
    //     println!("serialized = {}", serialized);

    //     let deserialized: Metadata = serde_json::from_str(&serialized).unwrap();
    //     println!("deserialized = {:?}", deserialized);

    //     let serialized = serde_json::to_string(&Metadata {
    //         announce: "afasg".into(),
    //         info: Info {
    //             name: "namename".to_string(),
    //             piece_length: 1245,
    //             pieces: vec!["123".into(), "456".into()],
    //             len_or_files: LenFiles::Files(vec![File {
    //                 length: 124,
    //                 path: "fakg".to_string(),
    //             }]),
    //         },
    //     })
    //     .unwrap();
    //     println!("serialized = {}", serialized);

    //     let deserialized: Metadata = serde_json::from_str(&serialized).unwrap();
    //     println!("deserialized = {:?}", deserialized);

    //     let serialized = bt_bencode::to_vec(&Metadata {
    //         announce: "afasg".into(),
    //         info: Info {
    //             name: "namename".to_string(),
    //             piece_length: 1245,
    //             pieces: vec!["123".into(), "456".into()],
    //             len_or_files: LenFiles::Files(vec![File {
    //                 length: 124,
    //                 path: "fakg".to_string(),
    //             }]),
    //         },
    //     })
    //     .unwrap();
    //     // println!("serialized = {:x?}", serialized);
    //     let deserialized: Metadata = bt_bencode::from_slice(&serialized).unwrap();
    //     println!("deserialized = {:?}", deserialized);
    // }
}

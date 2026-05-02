pub use bt_bencode::ByteString;
use bt_bencode::RawValue;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::LazyLock;
use thiserror::Error;
use tracing::warn;

mod magnet;
pub use magnet::Magnet;

// Metadata is a universal structure
#[derive(Debug, Clone)]
pub struct Metadata {
    pub info: Info,
    pub raw_info: RawValue, // raw, byte-format info, for sending metadata to peers
    pub info_hash: [u8; 20],

    pub len: usize,

    pub files: Vec<File>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub creation_date: Option<u64>,
}

impl Metadata {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn files(&self) -> &Vec<File> {
        &self.files
    }

    pub fn regular_piece_size(&self) -> usize {
        self.info.piece_length as usize
    }

    pub fn total_pieces(&self) -> usize {
        (self.len() + self.regular_piece_size() - 1) / self.regular_piece_size()
    }

    pub fn piece_size_of(&self, index: u32) -> usize {
        assert!((index as usize) < self.total_pieces());
        let n_full_piece = self.len() / self.regular_piece_size();
        let full_piece_total_size = n_full_piece * self.regular_piece_size();
        if (index as usize) < n_full_piece {
            self.regular_piece_size()
        } else {
            assert_eq!((index as usize), self.total_pieces() - 1);
            assert_eq!(n_full_piece + 1, self.total_pieces());
            self.len() - full_piece_total_size
        }
    }

    pub fn verify_info_hash(&self) -> io::Result<bool> {
        let mut hasher = Sha1::new();
        hasher.update(self.raw_info.get());
        let info_hash: [u8; 20] = hasher.finalize().into();
        Ok(info_hash == self.info_hash)
    }
}

// FileMetadata is raw data from .torrent file
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileMetadata {
    announce: Option<String>,
    #[serde(rename = "announce-list")]
    announce_list: Option<Vec<Vec<String>>>,

    #[serde(serialize_with = "serialize_raw_only")]
    info: InfoWithRaw,

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
    // TODO: FIXME: need RawValue to support unknown field, for
    // example:
    // pub private: u8,
    // and also support original torrent have a non-alphabetical order
    pub name: String,
    #[serde(rename = "piece length")]
    pub piece_length: u32,
    pub pieces: ByteString,
    #[serde(flatten)]
    len_or_files: LenFiles,

    // raw, byte-format info, for sending metadata to peers
    #[serde(skip)]
    pub raw: Vec<u8>,
}

fn serialize_raw_only<S>(i: &InfoWithRaw, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    i.raw.serialize(serializer)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "RawValue")]
pub struct InfoWithRaw {
    info: Info,
    raw: RawValue,
}

impl TryFrom<RawValue> for InfoWithRaw {
    type Error = &'static str; // TODO: better printable error type
    fn try_from(raw: RawValue) -> Result<Self, Self::Error> {
        let info: Info = bt_bencode::from_slice(raw.get()).map_err(|_| "invalid info RawValue")?;
        Ok(Self { info, raw })
    }
}

impl InfoWithRaw {
    pub fn to_metadata(self, info_hash: [u8; 20]) -> Metadata {
        let info = &self.info;
        let (len, files) = match &info.len_or_files {
            LenFiles::Length(l) => (
                *l,
                vec![File {
                    length: *l,
                    path: vec![info.name.clone()],
                }],
            ),
            LenFiles::Files(fs) => (
                fs.iter().map(|f| f.length).sum(),
                fs.iter()
                    .map(|sub| {
                        let mut path = vec![info.name.clone()];
                        path.extend_from_slice(&sub.path);
                        File {
                            length: sub.length,
                            path,
                        }
                    })
                    .collect(),
            ),
        };
        Metadata {
            info: self.info,
            raw_info: self.raw,
            info_hash: info_hash,
            comment: None,
            created_by: None,
            creation_date: None,
            len,
            files,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum LenFiles {
    #[serde(rename = "length")]
    Length(usize),

    #[serde(rename = "files")]
    Files(Vec<File>),
}

impl FileMetadata {
    pub fn load<T: AsRef<[u8]>>(input: T) -> io::Result<Self> {
        let mut torrent: FileMetadata = bt_bencode::from_slice(input.as_ref())?;
        let mut hasher = Sha1::new();
        hasher.update(torrent.info.raw.get());
        torrent.info_hash = hasher.finalize().into();
        Ok(torrent)
    }

    /// convert FileMetadata to Metadata and announce list
    pub fn to_metadata(self) -> (Metadata, Vec<Vec<String>>) {
        let m = self.info.to_metadata(self.info_hash);
        (
            Metadata {
                comment: self.comment,
                created_by: self.created_by,
                creation_date: self.creation_date,
                ..m
            },
            if let Some(li) = self.announce_list {
                li
            } else if let Some(a) = self.announce {
                vec![vec![a]]
            } else {
                vec![vec![]]
            },
        )
    }
}

pub trait ToMetadata {
    fn to_metadata(self) -> FileMetadata;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct File {
    pub length: usize,

    // TODO: many sub path are same, e.g. a sub directory containing many files
    // use a more effeicient structure
    pub path: Vec<String>,
}

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
        ]
        .join("&");
        if let Some(ref ip) = self.ip {
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
    pub peers: Vec<Peer>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Peer {
    #[serde(rename = "peer id")]
    pub peer_id: ByteString,
    pub ip: String,
    pub port: u16,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_real_torrent() {
        let torrent_f = include_bytes!("../ubuntu-24.10-desktop-amd64.iso.torrent");
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

    #[test]
    // some trackers return empty peers dict, not empty peer list, test if we can decode it correctly
    fn test_deserialize_empty_announce_list() {
        let resp = *b"d8:intervali1800e5:peersdee";
        let r0 = bt_bencode::to_vec(&TrackerResp::Success(AnnounceResp {
            interval: 1800,
            peers: vec![],
        }))
        .unwrap();
        println!("r0 = {:?}", String::from_utf8(r0));
        let decoded = bt_bencode::from_slice::<TrackerResp>(&resp).unwrap();
        assert_eq!(
            decoded,
            TrackerResp::Success(AnnounceResp {
                interval: 1800,
                peers: vec![],
            })
        );
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

use anyhow::Result;
pub use bt_bencode::ByteString;
use serde::{Deserialize, Serialize};
mod workaround;
use sha1::{Digest, Sha1};
use std::net::Ipv4Addr;
use workaround::ByteParam;

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct Info {
    name: String,
    #[serde(rename = "piece length")]
    piece_length: u32,
    pieces: ByteString,
    #[serde(flatten)]
    len_or_files: LenFiles,
}

#[derive(Serialize, Deserialize, Debug)]
enum LenFiles {
    // #[serde(untagged)]
    #[serde(rename = "length")]
    Length(usize),
    // #[serde(untagged)]
    #[serde(rename = "files")]
    Files(Vec<File>),
}

impl Metadata {
    fn load<T: AsRef<[u8]>>(input: T) -> anyhow::Result<Self> {
        let mut torrent: Metadata = bt_bencode::from_slice(input.as_ref())?;
        let mut hasher = Sha1::new();
        bt_bencode::to_writer(&mut hasher, &torrent.info)?;
        torrent.info_hash = hasher.finalize().into();
        Ok(torrent)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct File {
    length: usize,
    path: String,
}

#[derive(Debug)]
struct TrackerGet<'a> {
    peer_id: &'a str,
    ip: Option<Ipv4Addr>,
    port: u32,
    uploaded: usize,
    downloaded: usize,
    left: usize,
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
        let mut url: Vec<String> = if let Some(ref announce_list) = meta.announce_list {
            announce_list.iter().flat_map(|url| url.clone()).collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_real_torrent() {
        let torrent_f = include_bytes!("../ubuntu-24.10-desktop-amd64.iso.torrent");
        let torrent = Metadata::load(torrent_f).unwrap();

        let announce_req = TrackerGet {
            peer_id: "-qB0405-qwerasdfzxcv".into(),
            uploaded: 0,
            port: 35515,
            downloaded: 0,
            left: 0,
            ip: None,
        };

        use reqwest;
        use std::net::{IpAddr, Ipv6Addr};
        let builder = reqwest::Client::builder();
        const V6_ADDR: Ipv6Addr = Ipv6Addr::from_bits(0);
        let client = builder.local_address(IpAddr::V6(V6_ADDR)).build().unwrap();
        let request = client.get(&announce_req.url(&torrent)[0]).build().unwrap();
        dbg!(request.url());
        let r = client.execute(request).await.unwrap();
        let z: bt_bencode::Value = bt_bencode::from_slice(&r.bytes().await.unwrap()).unwrap();
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

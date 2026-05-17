pub use bt_bencode::ByteString;
use bt_bencode::RawValue;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::io;

mod magnet;
pub use magnet::Magnet;

// Metadata is a universal structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub info: Info,
    pub raw_info: Vec<u8>, // raw, byte-format info, for sending metadata to peers
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
        hasher.update(&self.raw_info);
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
    // TODO: OPTIMIZE: add a ref version of RawValue
    RawValue::from_slice(&i.raw).serialize(serializer)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "RawValue")]
pub struct InfoWithRaw {
    info: Info,
    raw: Vec<u8>,
}

impl TryFrom<RawValue> for InfoWithRaw {
    type Error = &'static str; // TODO: better printable error type
    fn try_from(raw: RawValue) -> Result<Self, Self::Error> {
        let info: Info = bt_bencode::from_slice(raw.get()).map_err(|_| "invalid info RawValue")?;
        Ok(Self {
            info,
            raw: raw.into_inner(),
        })
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
            info_hash,
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
        hasher.update(&torrent.info.raw);
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

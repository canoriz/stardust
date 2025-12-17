use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
/// implements magnet link parsing, see BEP 9
use std::net::SocketAddr;
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Magnet {
    pub info_hash: [u8; 20],

    // display name
    pub dn: Option<String>,

    // peers
    pub pe: Option<Vec<SocketAddr>>,

    // tracker url
    pub tr: Option<Vec<String>>,
}

impl FromStr for Magnet {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let body = if let Some(b) = s.strip_prefix("magnet:?") {
            b
        } else {
            return hash_only(s);
        };

        let mut ret = Magnet {
            info_hash: [0; 20],
            dn: None,
            pe: None,
            tr: None,
        };
        let mut btih_seen = false;
        for part in body.split("&") {
            match part.split_once("=") {
                Some((k, v)) => match k {
                    "xt" => {
                        if let Some(ih) = v.strip_prefix("urn:btih:") {
                            ret.info_hash = parse_info_hash(ih)?;
                            btih_seen = true;
                        }
                    }
                    "dn" => {
                        if let Ok(dec) = percent_decode_str(v).decode_utf8() {
                            ret.dn = Some(dec.into());
                        }
                    }
                    "tr" => {
                        if let Ok(dec) = percent_decode_str(v).decode_utf8() {
                            match &mut ret.tr {
                                None => {
                                    ret.tr = Some(vec![dec.into()]);
                                }
                                Some(trs) => {
                                    trs.push(dec.into());
                                }
                            }
                        }
                    }
                    "x.pe" => {
                        if let Ok(s) = percent_decode_str(v).decode_utf8() {
                            if let Ok(a) = s.parse::<SocketAddr>() {
                                match &mut ret.pe {
                                    None => {
                                        ret.pe = Some(vec![a]);
                                    }
                                    Some(peers) => {
                                        peers.push(a);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                },
                None => return Err("invalid magnet"),
            }
        }
        if btih_seen {
            Ok(ret)
        } else {
            Err("xt=urn:btih not in maget")
        }
    }
}

fn parse_info_hash(s: &str) -> Result<[u8; 20], &'static str> {
    match hex::decode(s) {
        Ok(ih) if ih.len() == 20 => {
            let mut info_hash = [0; 20];
            info_hash.copy_from_slice(&ih);
            Ok(info_hash)
        }
        Ok(_) => Err("hash length must be 40"),
        Err(_) => Err("invalid hash"),
    }
}

fn hash_only(s: &str) -> Result<Magnet, &'static str> {
    Ok(Magnet {
        info_hash: parse_info_hash(s)?,
        dn: None,
        pe: None,
        tr: None,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_magnet_links_ok() {
        let case = [
            (
                "magnet:?xt=urn:btih:623bb951f89a9300c0179f1fbc2af32ad13c61fa".to_string()
                    + "&tr=http%3a%2f%2ft.nyaatracker.com%2fannounce"
                    + "&tr=udp%3a%2f%2ftracker.opentrackr.org%3a1337%2fannounce",
                Magnet {
                    info_hash: parse_info_hash("623bb951f89a9300c0179f1fbc2af32ad13c61fa").unwrap(),
                    dn: None,
                    pe: None,
                    tr: Some(vec![
                        "http://t.nyaatracker.com/announce".into(),
                        "udp://tracker.opentrackr.org:1337/announce".into(),
                    ]),
                },
            ),
            (
                "623bb951f89a9300c0179f1fbc2af32ad13c61fa".to_string(),
                Magnet {
                    info_hash: parse_info_hash("623bb951f89a9300c0179f1fbc2af32ad13c61fa").unwrap(),
                    dn: None,
                    pe: None,
                    tr: None,
                },
            ),
        ];
        for (link, exp) in case {
            let m = Magnet::from_str(&link);
            assert!(m.is_ok(), "{link}");
            assert_eq!(m.unwrap(), exp, "{link}");
        }
    }
}

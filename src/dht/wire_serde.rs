use core::fmt;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use super::{ByteSocketAddr, VecNode4, VecNode6};
use serde::{self, de, de::Visitor, Deserializer, Serializer};

impl serde::Serialize for VecNode4 {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buf = Vec::with_capacity(20 + 4 + 2);
        for (nid, s) in &self.0 {
            _ = buf.write_all(nid);
            _ = buf.write_all(&s.ip().octets());
            _ = buf.write_all(&s.port().to_be_bytes());
        }
        s.serialize_bytes(&buf)
    }
}

impl<'de> serde::Deserialize<'de> for VecNode4 {
    fn deserialize<D>(d: D) -> Result<VecNode4, D::Error>
    where
        D: Deserializer<'de>,
    {
        d.deserialize_byte_buf(VecNode4Visitor)
    }
}

struct VecNode4Visitor;

impl<'de> Visitor<'de> for VecNode4Visitor {
    type Value = VecNode4;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("byte string of length multiple of 26")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut ret = Vec::with_capacity(v.len() / 38);
        for node in v.chunks(26) {
            match node.len() {
                26 => {
                    let mut nid = [0u8; 20];
                    nid.copy_from_slice(&node[..20]);
                    let v4 = Ipv4Addr::from([node[20], node[21], node[22], node[23]]);
                    ret.push((
                        nid,
                        SocketAddrV4::new(v4, u16::from_be_bytes([node[24], node[25]])),
                    ));
                }
                _ => {
                    let unexp = de::Unexpected::Str(&format!(
                        "get byte string {v:02x?} of length {}",
                        v.len()
                    ));
                    return Err(de::Error::invalid_value(unexp, &self));
                }
            }
        }
        Ok(VecNode4(ret))
    }
}
impl serde::Serialize for VecNode6 {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buf = Vec::with_capacity(20 + 16 + 2);
        for (nid, s) in &self.0 {
            _ = buf.write_all(nid);
            _ = buf.write_all(&s.ip().octets());
            _ = buf.write_all(&s.port().to_be_bytes());
        }
        s.serialize_bytes(&buf)
    }
}

impl<'de> serde::Deserialize<'de> for VecNode6 {
    fn deserialize<D>(d: D) -> Result<VecNode6, D::Error>
    where
        D: Deserializer<'de>,
    {
        d.deserialize_byte_buf(VecNode6Visitor)
    }
}

struct VecNode6Visitor;

impl<'de> Visitor<'de> for VecNode6Visitor {
    type Value = VecNode6;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("byte string of length multiple of 38")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut ret = Vec::with_capacity(v.len() / 38);
        for node in v.chunks(38) {
            match node.len() {
                38 => {
                    let mut nid = [0u8; 20];
                    nid.copy_from_slice(&node[..20]);
                    let v6 = Ipv6Addr::from([
                        node[20], node[21], node[22], node[23], node[24], node[25], node[26],
                        node[27], node[28], node[29], node[30], node[31], node[32], node[33],
                        node[34], node[35],
                    ]);
                    let port: u16 = (node[36] as u16) << 8 | node[37] as u16;
                    ret.push((nid, SocketAddrV6::new(v6, port, 0, 0)));
                }
                _ => {
                    let unexp = de::Unexpected::Str(&format!(
                        "get byte string {v:02x?} of length {}",
                        v.len()
                    ));
                    return Err(de::Error::invalid_value(unexp, &self));
                }
            }
        }
        Ok(VecNode6(ret))
    }
}

impl serde::Serialize for ByteSocketAddr {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut w = Vec::with_capacity(2 + 16);
        match &self.0 {
            SocketAddr::V4(v4) => {
                _ = w.write_all(&v4.ip().octets());
                _ = w.write_all(&v4.port().to_be_bytes());
            }
            SocketAddr::V6(v6) => {
                _ = w.write_all(&v6.ip().octets());
                _ = w.write_all(&v6.port().to_be_bytes());
            }
        }
        s.serialize_bytes(&w)
    }
}

impl<'de> serde::Deserialize<'de> for ByteSocketAddr {
    fn deserialize<D>(d: D) -> Result<ByteSocketAddr, D::Error>
    where
        D: Deserializer<'de>,
    {
        d.deserialize_byte_buf(ByteSocketAddrVisitor)
    }
}

struct ByteSocketAddrVisitor;

impl<'de> Visitor<'de> for ByteSocketAddrVisitor {
    type Value = ByteSocketAddr;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("byte string of length multiple of 5")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match v.len() {
            6 => Ok(ByteSocketAddr(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from([v[0], v[1], v[2], v[3]])),
                u16::from_be_bytes([v[4], v[5]]),
            ))),
            18 => Ok(ByteSocketAddr(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from([
                    v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11],
                    v[12], v[13], v[14], v[15],
                ])),
                u16::from_be_bytes([v[16], v[17]]),
            ))),
            other => Err(de::Error::invalid_value(
                de::Unexpected::Str(&format!("get byte string {v:02x?} of length {other}")),
                &self,
            )),
        }
    }
}

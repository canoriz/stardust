use core::{
    borrow::{Borrow, BorrowMut},
    fmt, net,
    ops::{Deref, DerefMut},
};

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer,
};

fn serialize<S, T>(data: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: serde::Serialize,
{
    for d in data.iter() {
        d.serialize(serializer)?;
    }
    Ok(())
}

struct ConcatVecVisitor;

impl<'de> Visitor<'de> for IpAddrVisitor {
    type Value = ByteIpAddr;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("byte string of length 4(ipv4) or 16(ipv6)")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match v.len() {
            4 => Ok(ByteIpAddr(net::IpAddr::V4(net::Ipv4Addr::from([
                v[0], v[1], v[2], v[3],
            ])))),
            16 => Ok(ByteIpAddr(net::IpAddr::V6(net::Ipv6Addr::from([
                v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12],
                v[13], v[14], v[15],
            ])))),
            other => Err(de::Error::invalid_value(
                de::Unexpected::Str(&format!("get byte string {v:02x?} of length {other}")),
                &self,
            )),
        }
    }
}

impl<'de> Deserialize<'de> for ByteIpAddr {
    fn deserialize<D>(deserializer: D) -> Result<ByteIpAddr, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(IpAddrVisitor)
    }
}

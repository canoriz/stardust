// use serde::de::{SeqAccess, Visitor};
// use serde::{Deserialize, Deserializer};
use std::borrow::{Borrow, BorrowMut};
use std::ops::{Deref, DerefMut};
use std::{cmp, fmt};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteParam(Vec<u8>);

impl AsRef<[u8]> for ByteParam {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsMut<[u8]> for ByteParam {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl Borrow<[u8]> for ByteParam {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl BorrowMut<[u8]> for ByteParam {
    fn borrow_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl fmt::Debug for ByteParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl Deref for ByteParam {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ByteParam {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> From<&'a [u8]> for ByteParam {
    fn from(value: &'a [u8]) -> Self {
        Self(Vec::from(value))
    }
}

impl<'a> From<&'a str> for ByteParam {
    fn from(value: &'a str) -> Self {
        Self(Vec::from(value))
    }
}

impl From<String> for ByteParam {
    fn from(value: String) -> Self {
        Self(Vec::from(value))
    }
}

impl From<Vec<u8>> for ByteParam {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl serde::Serialize for ByteParam {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let a = percent_encoding::percent_encode(&self.0, &percent_encoding::NON_ALPHANUMERIC)
            .into_iter()
            .collect::<String>();
        serializer.serialize_str(&dbg!(a))
    }
}

// struct BStringVisitor;

// impl<'de> Visitor<'de> for BStringVisitor {
//     type Value = ByteParam;

//     fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
//         formatter.write_str("byte string")
//     }

//     fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
//     where
//         V: SeqAccess<'de>,
//     {
//         let capacity = cmp::min(visitor.size_hint().unwrap_or_default(), 4096);
//         let mut bytes = Vec::with_capacity(capacity);

//         while let Some(b) = visitor.next_element()? {
//             bytes.push(b);
//         }

//         Ok(ByteParam::from(bytes))
//     }

//     fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> {
//         Ok(ByteParam::from(v))
//     }

//     fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
//     where
//         E: serde::de::Error,
//     {
//         Ok(ByteParam::from(v))
//     }

//     fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
//     where
//         E: serde::de::Error,
//     {
//         Ok(ByteParam::from(v))
//     }

//     fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
//     where
//         E: serde::de::Error,
//     {
//         Ok(ByteParam::from(v))
//     }
// }

// impl<'de> Deserialize<'de> for ByteParam {
//     fn deserialize<D>(deserializer: D) -> Result<ByteParam, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         deserializer.deserialize_byte_buf(BStringVisitor)
//     }
// }

impl ByteParam {
    /// Returns the inner vector.
    #[inline]
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

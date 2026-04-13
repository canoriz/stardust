use core::fmt::{self, Write};

pub fn format_hex<T, F>(data: &T, mut f: F) -> fmt::Result
where
    T: AsRef<[u8]>,
    F: Write,
{
    write!(f, "[")?;
    for (_, byte) in data.as_ref().iter().enumerate() {
        write!(f, "{:02x}", byte)?;
    }
    write!(f, "]")
}

pub fn to_hex<T: AsRef<[u8]>>(data: &T) -> String {
    let mut s = String::new();
    format_hex(data, &mut s).unwrap();
    s
}

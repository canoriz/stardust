use core::fmt;

pub fn format_hex<T: AsRef<[u8]>>(data: &T, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[")?;
    for (_, byte) in data.as_ref().iter().enumerate() {
        write!(f, "{:02x}", byte)?;
    }
    write!(f, "]")
}

use std::io::{self, Write};

#[derive(Copy, Clone, Debug)]
pub struct HashState<T> {
    // state of hasher
    hasher: T,

    // the next offset to hash with
    next_offset: usize,
}

impl<T> HashState<T> {
    pub fn new(hasher: T) -> Self {
        Self {
            hasher,
            next_offset: 0,
        }
    }
}

impl<T> HashState<T>
where
    T: Write,
{
    pub fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.hasher.write_all(data)?;
        self.next_offset += data.len();
        Ok(())
    }

    pub fn next_offset(&self) -> usize {
        self.next_offset
    }

    pub fn finalize(self) -> T {
        self.hasher
    }
}

use std::cmp::{max, min};
use std::fs::File;
use std::io::Result;
use std::marker::PhantomData;
use std::{path::Path, sync::Arc};

use crate::metadata::Metadata;

#[cfg(unix)]
use std::os::unix::prelude::*;
use tracing::{debug, info, warn};

pub struct FileMetadata {
    pub len: usize, // length of file
}

pub struct NormalFile {
    file: File,
}

pub trait Access: Sized + Sync {
    // Now have difficulties set attributes for opener
    // every single change needs a totally new type
    //
    // TODO: maybe use OpenDAL's pattern, first generate a "Opener"
    // then let opener open Handle, Handle impls FileAt
    // then discard the types.
    fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>;
    fn write_all_at(&mut self, buf: &[u8], offset: usize) -> Result<()>;
    fn read_exact_at(&mut self, buf: &mut [u8], offset: usize) -> Result<()>;
    fn metadata(&self) -> Result<FileMetadata>;
}

trait AccessDyn
where
    Self: Access + Send + 'static,
{
    fn open_dyn(path: &Path) -> Result<Box<dyn FileAt + Send>> {
        let fh = Self::open(path)?;
        Ok(Box::new(fh))
    }
}

impl<A: Access + Send + 'static> AccessDyn for A {}

trait FileAt {
    fn file_write_all_at(&mut self, buf: &[u8], offset: usize) -> Result<()>;
    fn file_read_exact_at(&mut self, buf: &mut [u8], offset: usize) -> Result<()>;
    fn file_metadata(&self) -> Result<FileMetadata>;

    #[cfg(test)]
    fn get_inner(&mut self, offset: usize, len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0; len];
        self.file_read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }
}

impl<A: Access> FileAt for A {
    fn file_write_all_at(&mut self, buf: &[u8], offset: usize) -> Result<()> {
        self.write_all_at(buf, offset)
    }

    fn file_read_exact_at(&mut self, buf: &mut [u8], offset: usize) -> Result<()> {
        self.read_exact_at(buf, offset)
    }

    fn file_metadata(&self) -> Result<FileMetadata> {
        self.metadata()
    }
}

fn open_fn<A: AccessDyn>() -> fn(&Path) -> Result<Box<dyn FileAt + Send>> {
    A::open_dyn
}

impl Access for NormalFile {
    fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        // TODO: this is wild
        let prefix = path.as_ref().parent().unwrap();
        std::fs::create_dir_all(prefix).unwrap();

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self { file })
    }

    fn write_all_at(&mut self, buf: &[u8], offset: usize) -> Result<()> {
        warn!("write_all at offset {offset} len {}", buf.len());
        self.file.write_all_at(buf, offset as u64)
        // Ok(())
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: usize) -> Result<()> {
        info!("read_exact at offset {offset} len {}", buf.len());
        self.file.read_exact_at(buf, offset as u64)
        // Ok(())
    }

    fn metadata(&self) -> Result<FileMetadata> {
        let meta = self.file.metadata()?;
        Ok(FileMetadata {
            len: meta.len() as usize,
        })
    }
}

struct FileRange {
    handle: Option<Box<dyn FileAt + Send>>,
    path: String,
    begin: usize,
    len: usize,
}

pub struct Builder<T> {
    m: Option<Arc<Metadata>>,
    _t: PhantomData<T>,
}

impl<T> Builder<T>
where
    T: Access + Send + 'static,
{
    pub fn metadata(mut self, m: Arc<Metadata>) -> Self {
        self.m = Some(m);
        self
    }

    pub fn build(self) -> BackFile {
        match self.m {
            Some(m) => {
                let files = m.files();
                let mut file_range = Vec::with_capacity(files.capacity());
                let mut last = 0usize;
                for f in files {
                    file_range.push(FileRange {
                        handle: None,
                        path: f.path.join("/"), // TODO: platform independent? filename too long?
                        begin: last,
                        len: f.length,
                    });
                    last += f.length;
                }
                BackFile {
                    opener: open_fn::<T>(),
                    file_range,
                }
            }
            None => {
                let file_range = vec![FileRange {
                    handle: None,
                    path: "None path".into(),
                    begin: 0,
                    len: usize::MAX,
                }];
                BackFile {
                    opener: open_fn::<T>(),
                    file_range,
                }
            }
        }
    }
}

pub struct BackFile {
    opener: fn(path: &Path) -> Result<Box<dyn FileAt + Send>>,
    file_range: Vec<FileRange>,
}

// get intersection of (x1, len1) and (x2, len2)
fn intersection(range1: (usize, usize), range2: (usize, usize)) -> Option<(usize, usize)> {
    let ir0 = max(range1.0, range2.0);
    let ir1 = min(range1.0 + range1.1, range2.0 + range2.1);
    if ir0 < ir1 {
        Some((ir0, ir1 - ir0))
    } else {
        None
    }
}

impl BackFile {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<A: Access>() -> Builder<A> {
        Builder {
            m: None,
            _t: PhantomData,
        }
    }

    // TODO: optimize this to use binary search or whatever, not iterating
    fn find_files(&mut self, offset: usize, buf: &[u8]) -> impl Iterator<Item = FileOp<'_>> {
        let len = buf.len();
        self.file_range.iter_mut().filter_map(move |f| {
            intersection((f.begin, f.len), (offset, len)).map(move |(w_offset, len)| {
                let f_begin = f.begin;
                FileOp {
                    file: f,
                    offset: w_offset - f_begin,
                    buf_begin: w_offset - offset,
                    buf_len: len,
                }
            })
        })
    }
}

impl BackFile {
    pub fn write_all_at(&mut self, offset: usize, buf: &[u8]) -> Result<()> {
        let opener = self.opener;
        let wops = self.find_files(offset, buf);
        for w in wops {
            debug!(
                "write path {} offset {} len {}",
                w.file.path, w.offset, w.buf_len
            );

            if w.file.handle.is_none() {
                w.file.handle = match opener(w.file.path.as_ref()) {
                    Err(e) => {
                        warn!("error open file {} {e:?}", w.file.path);
                        None
                    }
                    Ok(fh) => Some(fh),
                };
            }

            if let Some(ref mut fh) = w.file.handle {
                // TODO: FIXME: one error write should not trigger fn call error
                fh.file_write_all_at(&buf[w.buf_begin..w.buf_begin + w.buf_len], w.offset)?;
            }
        }
        Ok(())
    }

    pub fn read_exact_at(&mut self, offset: usize, buf: &mut [u8]) -> Result<()> {
        let opener = self.opener;
        let rops = self.find_files(offset, buf);
        for r in rops {
            info!(
                "read path {} offset {} len {}",
                r.file.path, r.offset, r.buf_len
            );

            if r.file.handle.is_none() {
                r.file.handle = match opener(r.file.path.as_ref()) {
                    Err(e) => {
                        warn!("error open file {} {e:?}", r.file.path);
                        None
                    }
                    Ok(fh) => Some(fh),
                };
            }

            if let Some(ref mut fh) = r.file.handle {
                // TODO: FIXME: one error should not trigger fn call error
                let meta = fh.file_metadata()?;
                if r.offset < meta.len {
                    let len_limit = (meta.len - r.offset).min(r.buf_len);

                    if r.offset < meta.len {
                        fh.file_read_exact_at(
                            &mut buf[r.buf_begin..r.buf_begin + len_limit],
                            r.offset,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn get_inner(&mut self, offset: usize, len: usize) -> Option<Result<Vec<u8>>> {
        assert_eq!(self.file_range.len(), 1);
        self.file_range[0]
            .handle
            .as_mut()
            .map(|h| h.get_inner(offset, len))
    }
}

struct FileOp<'a> {
    file: &'a mut FileRange, // the corresponding "real" file

    // Operation's begin offset relative to this "real" file, not relative to
    // the whole "logical" file
    offset: usize,

    // the range of buffer
    buf_begin: usize,
    buf_len: usize,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_intersection() {
        assert_eq!(intersection((1, 4), (2, 4)), Some((2, 3)));
        assert_eq!(intersection((1, 4), (0, 3)), Some((1, 2)));
        assert_eq!(intersection((1, 9), (2, 3)), Some((2, 3)));
        assert_eq!(intersection((1, 3), (4, 3)), None);
    }
}

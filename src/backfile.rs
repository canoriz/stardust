use std::cmp::{max, min};
use std::fs::File;
use std::io::Result;
use std::sync::{mpsc, Mutex};
use std::{path::Path, sync::Arc};

use crate::cache::Ref;
use crate::metadata::Metadata;

#[cfg(unix)]
use std::os::unix::prelude::*;
use tracing::{info, warn};
use FileExt;

// trait FileImpl
// where
//     Self: Sized,
// {
//     fn open(path: Path) -> Result<Self>;
//     fn write(&mut self, offset: usize, buf: &[u8]) -> Result<()>;
// }

struct NormalFile {
    file: File,
}

// impl FileImpl for NormalFile {
impl NormalFile {
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

    fn write_all(&mut self, offset: usize, buf: &[u8]) -> Result<()> {
        warn!("write_all at {offset} {}", buf.len());
        self.file.write_all_at(buf, offset as u64)
    }

    fn read_all(&mut self, offset: usize, buf: &mut [u8]) -> Result<()> {
        self.file.read_exact_at(buf, offset as u64)
    }
}

struct FileRange {
    handle: Option<NormalFile>,
    path: String,
    begin: usize,
    len: usize,
}

pub struct BackFile {
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
    pub fn new(m: Arc<Metadata>) -> Self {
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
        Self { file_range }
    }

    // TODO: optimize this to use binary search or whatever, not iterating
    // TODO: make this iterator instead of vec
    fn find_file<'b>(&'b mut self, offset: usize, buf: &'b [u8]) -> Vec<FileWriteOp<'b>> {
        let write_ops: Vec<_> = self
            .file_range
            .iter_mut()
            .filter_map(|f| {
                intersection((f.begin, f.len), (offset, buf.len())).map(|(w_offset, len)| {
                    let f_begin = f.begin;
                    FileWriteOp {
                        file: f,
                        offset: w_offset - f_begin,
                        buf: &buf[w_offset - offset..w_offset - offset + len],
                    }
                })
            })
            .collect();
        write_ops
    }

    // TODO: does buf need static?
    pub fn write(&mut self, offset: usize, buf: &[u8]) -> Result<()> {
        let mut wops = self.find_file(offset, buf);
        for w in wops.iter_mut() {
            info!(
                "path {} offset {} len {}",
                w.file.path,
                w.offset,
                w.buf.len()
            );
            // TODO: these jobs should be sent to IO worker threads
            // shall not block net requests.

            if w.file.handle.is_none() {
                w.file.handle = match NormalFile::open(&w.file.path) {
                    Err(e) => {
                        warn!("error open file {} {e:?}", w.file.path);
                        None
                    }
                    Ok(fh) => Some(fh),
                };
            }

            if let Some(ref mut fh) = w.file.handle {
                // TODO: FIXME: one error write should not trigger fn call error
                fh.write_all(w.offset, w.buf)?;
            }
        }
        Ok(())
    }
}

struct FileWriteOp<'a> {
    file: &'a mut FileRange,
    offset: usize,
    buf: &'a [u8],
}

// impl Iterator for FileWriteIter {
//     type Item = FileWriteOp;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//         for f in &self.file_range {
//             let intersect = intersection((f.begin, f.begin + f.len), (offset, offset + len));
//             if let Some((begin, len)) = intersect {}
//         }
//     }
// }

pub struct WriteJob<T>
where
    T: AsRef<[u8]>,
{
    pub f: Arc<Mutex<BackFile>>,
    pub offset: usize,
    pub buf: Ref<T>,
    pub write_tx: tokio::sync::oneshot::Sender<std::io::Result<()>>,
}

pub fn write_worker<T>(r: mpsc::Receiver<WriteJob<T>>)
where
    T: AsRef<[u8]>,
{
    loop {
        match r.recv() {
            Ok(mut job) => {
                // TODO: dead lock?
                let mut bf = job.f.lock().unwrap();
                let r = bf.write(job.offset, job.buf.to_slice());
                warn!(
                    "write offset {} len {} result {r:?}",
                    job.offset,
                    job.buf.len()
                );
                let r = Ok(());
                job.write_tx.send(r);
            }
            Err(e) => {
                info!("write worker error {e:?}");
                return;
            }
        }
    }
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

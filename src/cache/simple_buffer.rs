use std::{
    collections::{HashMap, VecDeque},
    fmt, io,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex,
    },
};

use super::{BackFile, MutexBackFile};
use bytes::BytesMut;
use tokio::{sync::mpsc, time};
use tracing::warn;

const FLUSHING: u32 = 0b1;
const DIRTY: u32 = 0b10;

/// A pool of objects
struct Pool<T> {
    limit: usize,
    pool: VecDeque<T>,
}

impl<T> Pool<T> {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            pool: VecDeque::with_capacity(limit),
        }
    }

    fn put(&mut self, t: T) {
        if self.pool.len() < self.limit {
            self.pool.push_back(t)
        }
    }

    fn get(&mut self) -> Option<T> {
        self.pool.pop_front()
    }
}

#[derive(Debug)]
pub struct FlushErr {
    pub offset: usize,
    pub len: usize,
    pub err: io::Error,
}

pub trait ErrorCallback: FnOnce(FlushErr) + Send + 'static {}
impl<T> ErrorCallback for T where T: FnOnce(FlushErr) + Send + 'static {}

pub struct PieceBuf {
    /// always Some, except in drop
    buf: Option<BytesMut>,
    offset: usize,
    index: usize,
    touch: time::Instant,
    state: Arc<AtomicU32>,
    dropping: Arc<AtomicBool>,
    file: MutexBackFile,
    pool: Arc<Mutex<Pool<BytesMut>>>,

    /// always Some, except drop takes this
    on_error: Option<Box<dyn ErrorCallback>>,
}

impl fmt::Debug for PieceBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::Relaxed);
        let dirty = if state & DIRTY > 0 { "DIRTY" } else { "CLEAR" };
        f.debug_struct("PieceBuf")
            .field("buf", &self.buf)
            .field("offset", &self.offset)
            .field("index", &self.index)
            .field("touch", &self.touch)
            .field("state", &dirty)
            .finish()
    }
}

impl AsMut<[u8]> for PieceBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.state.fetch_or(DIRTY, Ordering::Acquire);
        self.touch = time::Instant::now();
        self.buf.as_mut().unwrap()
    }
}

impl AsRef<[u8]> for PieceBuf {
    fn as_ref(&self) -> &[u8] {
        // TODO: update touch time
        self.buf.as_ref().unwrap()
    }
}

impl Drop for PieceBuf {
    fn drop(&mut self) {
        let on_err = self.on_error.take();

        let f = self.file.clone();
        let s = self.state.clone();
        let offset = self.offset;
        let pool = self.pool.clone();
        let buf = self.buf.take().unwrap();
        let dropping = self.dropping.clone();

        // if drop is running, all background flush should stop
        // set in_drop, if other worker see in_drop is true, they stop
        dropping.swap(true, Ordering::Acquire);
        let old_state = s.fetch_or(FLUSHING, Ordering::Acquire);
        if old_state & FLUSHING > 0 || old_state & DIRTY > 0 {
            // buffer may be dirty while DIRTY bit is 0 if other is FLUSHING
            let index = self.index;
            tokio::task::spawn_blocking(move || {
                let r = Self::force_flush(buf, f, pool, offset, s, dropping, true);
                if let Err(e) = r {
                    warn!("PieceBuf::Drop flush error index {index} {e:?}, data lost");
                    on_err.unwrap()(e)
                }
            });
        }
    }
}

impl PieceBuf {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn flush<F>(&mut self, result_callback: F)
    where
        F: FnOnce(Result<(), FlushErr>) + Send + 'static,
    {
        // set flushing bit and clear dirty bit
        // dirty flushing
        // 00 -> 00 and return
        // 01 -> 01 flushing in progress and no further change, return
        // 10 -> 01 dirty, flushing not in progress, start one
        // 11 -> 11 dirty, flushing in progress, not more flush, return

        // we have the &mut, only we can caange flushing bit from 0 to 1.
        // If parallel flushing working, flushing bit may change from 1 to 0,
        // dirty bit may change from 0 to 1

        let state = self.state.load(Ordering::Acquire);
        match state {
            0b00 => {}
            0b01 => {}
            0b10 => {
                // no flushing in progress, no one else can change state
                let old_state = self.state.swap(0b01, Ordering::Acquire);
                assert_eq!(old_state, 0b10);

                let f = self.file.clone();
                let s = self.state.clone();
                let offset = self.offset;
                let pool = self.pool.clone();
                let dropping = self.dropping.clone();

                let buf = if let Some(mut p) = self.pool.lock().unwrap().get() {
                    let buf = self.buf.as_ref().unwrap();
                    p.resize(buf.len(), 0u8);
                    p.as_mut().copy_from_slice(buf.as_ref());
                    p
                } else {
                    self.buf.clone().unwrap()
                };
                tokio::task::spawn_blocking(move || {
                    let r = Self::force_flush(buf, f, pool, offset, s, dropping, false);
                    result_callback(r);
                });
            }
            0b11 => {}
            _ => unreachable!(),
        }
    }

    fn force_flush(
        buf: BytesMut,
        file: MutexBackFile,
        pool: Arc<Mutex<Pool<BytesMut>>>,
        offset: usize,
        state: Arc<AtomicU32>,
        dropping: Arc<AtomicBool>,
        from_drop: bool,
    ) -> Result<(), FlushErr> {
        flush_buf_to_file(buf, offset, state, file, dropping, from_drop, move |b| {
            pool.lock().unwrap().put(b);
        })
    }
}

/// flush buffer to file
/// recycle_buf should recycle BytesMut
/// dirty may change from 0 to 1
/// flushing must change from 1 to 0
fn flush_buf_to_file<F>(
    buf: BytesMut,
    offset: usize,
    state: Arc<AtomicU32>,
    file: MutexBackFile,
    dropping: Arc<AtomicBool>,
    from_drop: bool,
    recycle_buf: F,
) -> Result<(), FlushErr>
where
    F: FnOnce(BytesMut) + Send + 'static,
{
    let r = {
        let mut f = file.lock().unwrap();
        let dropping = dropping.load(Ordering::Relaxed);
        if dropping && from_drop || !dropping {
            f.write_all_at(offset, buf.as_ref())
        } else {
            Ok(())
        }
    };

    let len = buf.len();
    recycle_buf(buf);
    if let Err(e) = r {
        // dirty flush bits
        // 01 -> 10 no new write after flushing begins
        // 11 -> 10 new write after flushing begins, the dirty bit is set by others
        // in all cases, should set dirty=1, flush=0
        let _old_state = state.swap(DIRTY, Ordering::Release);
        // assert!(old_state & FLUSHING > 0);
        Err(FlushErr {
            offset,
            len,
            err: e,
        })
    } else {
        // dirty flush bits
        // 01 -> 00 no new write after flushing begins
        // 11 -> 10 new write after flushing begins, the dirty bit is set by others
        let _old_state = state.fetch_and(!FLUSHING, Ordering::Release);
        // assert!(old_state & FLUSHING > 0);
        Ok(())
    }
}

fn read_from_file(mut p: PieceBuf, file: MutexBackFile) -> io::Result<PieceBuf> {
    let mut f = file.lock().unwrap();
    f.read_exact_at(p.offset, p.buf.as_mut().unwrap())?;
    Ok(p)
}

pub struct BufStorage {
    pieces: HashMap<usize, PieceBuf>,

    piece_size: usize,
    last_piece_size: usize,
    piece_total: usize,

    back_file: MutexBackFile,

    /// currently loading pieces
    /// if piece is not present, add to loading list
    loading: Arc<Mutex<HashMap<usize, PieceState>>>,

    /// BytesMut pool
    // TODO: another layer of global pool shared between many BufStorages
    pool: Arc<Mutex<Pool<BytesMut>>>,
}

#[derive(Debug)]
pub enum GetPieceErr {
    InvalidPiece,
    Loading,
    Returned,
}

enum PieceState {
    Loading,
    Returned,
}

impl BufStorage {
    pub fn new(total_length: usize, piece_size: usize, back_file: BackFile) -> Self {
        let (piece_total, last_piece_size) = piece_total_and_last_size(total_length, piece_size);
        Self {
            pieces: HashMap::new(),
            back_file: Arc::new(Mutex::new(back_file)),

            piece_size,
            last_piece_size,
            piece_total,

            loading: Arc::new(Mutex::new(HashMap::new())),
            pool: Arc::new(Mutex::new(Pool::new(16))),
        }
    }

    pub async fn shutdown(mut self) {
        let n = self.pieces.len();
        let (tx, mut rx) = mpsc::channel(n);
        for (_, piece) in self.pieces.iter_mut() {
            let ti = tx.clone();
            piece.flush(move |r| {
                ti.try_send(r)
                    .expect("allocated exact n slots, should not send fail")
            });
        }

        let mut c = 0;
        while let Some(r) = rx.recv().await {
            c += 1;
            if let Err(e) = r {
                warn!("shutdown flush error {e:?}, data lost");
            }
        }
        assert_eq!(c, n);
    }

    /// get piecebuf from local buffer, if piecebuf in buffer, return it.
    /// If not in buffer, returns None
    pub fn get_buffered_piece(&mut self, piece_idx: usize) -> Option<&mut PieceBuf> {
        self.pieces.get_mut(&piece_idx)
    }

    /// returns all buffered pieces
    pub fn iter_buffered(&mut self) -> impl Iterator<Item = (&usize, &mut PieceBuf)> {
        self.pieces.iter_mut()
    }

    /// If piece is in storage, the piece is returned.
    /// If piece is not in storage, Err will return and
    /// `on_ready` callback will be called then piece is ready
    /// in storage.
    /// NOTE: if multiple get_piece to same piece_idx are all
    /// LOADING, only one of the on_ready will be called!
    pub fn get_piece<F>(
        &mut self,
        piece_idx: usize,
        on_ready: F,
        on_flush_err: Box<dyn ErrorCallback>,
    ) -> Result<&mut PieceBuf, GetPieceErr>
    where
        F: FnOnce(io::Result<PieceBuf>) + Send + 'static,
    {
        let piece_idx = piece_idx as usize;
        if piece_idx >= self.piece_total {
            return Err(GetPieceErr::InvalidPiece);
        }
        if let Some(p) = self.pieces.get_mut(&piece_idx) {
            p.touch = time::Instant::now();
            Ok(p)
        } else {
            let mut guard = self.loading.lock().unwrap();
            match guard.get(&piece_idx) {
                Some(PieceState::Loading) => return Err(GetPieceErr::Loading),
                Some(PieceState::Returned) => return Err(GetPieceErr::Returned),
                None => {
                    guard.insert(piece_idx, PieceState::Loading);
                }
            }
            // This is the first request of piece_idx
            let offset = piece_idx * self.piece_size;
            let len = if piece_idx + 1 == self.piece_total {
                self.last_piece_size as usize
            } else {
                self.piece_size as usize
            };

            let buf = self
                .pool
                .lock()
                .unwrap()
                .get()
                .map(|mut b| {
                    b.resize(len, 0);
                    b
                })
                .unwrap_or(BytesMut::zeroed(len));

            let p = PieceBuf {
                buf: Some(buf),
                touch: time::Instant::now(),
                index: piece_idx,
                offset,
                state: Arc::new(AtomicU32::new(0)),
                dropping: Arc::new(AtomicBool::new(false)),
                file: self.back_file.clone(),
                pool: self.pool.clone(),
                on_error: Some(on_flush_err),
            };
            let f = self.back_file.clone();
            let loading_map = self.loading.clone();
            tokio::task::spawn_blocking(move || match read_from_file(p, f) {
                Ok(p) => {
                    let mut guard = loading_map.lock().unwrap();
                    let v = guard
                        .get_mut(&piece_idx)
                        .expect("file read done, corresponding piece_idx should exist in map");
                    *v = PieceState::Returned;
                    on_ready(Ok(p));
                }
                Err(e) => {
                    let mut guard = loading_map.lock().unwrap();
                    guard.remove(&piece_idx);
                    on_ready(Err(e));
                }
            });
            Err(GetPieceErr::Loading)
        }
    }

    pub fn add_piece(&mut self, p: PieceBuf) {
        {
            let mut guard = self.loading.lock().unwrap();

            // assert check
            // inserted piece should be from get_piece's on_ready
            // and by that way, loading[piece_idx] should be PieceState::Returned
            matches!(guard.remove(&p.index), Some(PieceState::Returned));
            self.pieces.insert(p.index, p);
        }
        self.purge_by_size(16);
    }

    pub fn purge_by_time(&mut self, timeout: time::Duration) {
        self.pieces.retain(|_, v| v.touch.elapsed() > timeout)
    }

    pub fn purge_by_size(&mut self, keep: usize) {
        // TODO: OPTIMIZE
        while self.pieces.len() > keep {
            if let Some(&idx) = self
                .pieces
                .iter()
                .min_by_key(|(_, p)| p.touch)
                .map(|(k, _)| k)
            {
                self.pieces.remove(&idx);
            }
            // let mut ps: Vec<_> = self.pieces.drain().collect();
            // ps.sort_by_key(|(_, p)| p.touch);
            // ps.reverse();
            // while ps.len() > keep {
            //     ps.pop();
            // }
            // self.pieces = HashMap::from_iter(ps.into_iter());
        }
    }
}

fn piece_total_and_last_size(total_length: usize, piece_size: usize) -> (usize, usize) {
    let n_full_piece = total_length / piece_size;
    let full_piece_total_size = n_full_piece * piece_size;
    if full_piece_total_size == total_length {
        (n_full_piece, piece_size)
    } else {
        (n_full_piece + 1, (total_length - full_piece_total_size))
    }
}

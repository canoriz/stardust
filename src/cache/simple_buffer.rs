use std::{
    cmp,
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt, io,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
};

use super::{BackFile, MutexBackFile};
use bytes::BytesMut;
use tokio::{sync::mpsc, time};
use tracing::{info, warn};

pub const POOL_SIZE: usize = 60;

const FLUSHING: u32 = 0b1;
const DIRTY: u32 = 0b10;
const DROPPING: u32 = 0b100;

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

/// A copy-on-write buffer that can be either owned or shared.
/// Note: this is not same as [std::borrow::Cow], std Cow starts with
/// a reference, but we start with an owned buffer.
#[derive(Debug)]
enum CowBuf<T> {
    // Option is always Some
    Owned(Option<T>),
    Shared(Arc<T>),
}

impl<T> CowBuf<T> {
    fn new(t: T) -> Self {
        CowBuf::Owned(Some(t))
    }

    fn clone(&mut self) -> Self {
        match self {
            CowBuf::Owned(b) => {
                let s = Arc::new(b.take().unwrap());
                *self = CowBuf::Shared(s.clone());
                Self::Shared(s)
            }
            CowBuf::Shared(a) => CowBuf::Shared(a.clone()),
        }
    }
}

impl<T> Deref for CowBuf<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match self {
            CowBuf::Owned(b) => b.as_ref().unwrap(),
            CowBuf::Shared(a) => a.as_ref(),
        }
    }
}

impl<T> DerefMut for CowBuf<T>
where
    T: Clone,
{
    fn deref_mut(&mut self) -> &mut T {
        match self {
            CowBuf::Owned(b) => b.as_mut().unwrap(),
            CowBuf::Shared(a) => {
                *self = CowBuf::Owned(Some((**a).clone()));
                self.deref_mut()
            }
        }
    }
}

impl<T, U> AsRef<U> for CowBuf<T>
where
    T: AsRef<U>,
    U: ?Sized,
{
    fn as_ref(&self) -> &U {
        self.deref().as_ref()
    }
}

/// A [`BytesMut`] that automatically recycles itself back into the shared pool
/// on drop, removing the need for a manual `recycle_buf` callback in the
/// background flush path.
struct PooledBuf {
    buf: Option<BytesMut>,
    pool: Arc<Mutex<Pool<BytesMut>>>,
}

impl fmt::Debug for PooledBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledBuf")
            .field("len", &self.buf.as_ref().map(|b| b.len()))
            .finish()
    }
}

impl PooledBuf {
    fn new(pool: Arc<Mutex<Pool<BytesMut>>>, len: usize) -> Self {
        let buf = pool
            .lock()
            .unwrap()
            .get()
            .map(|mut b| {
                b.resize(len, 0);
                b
            })
            .unwrap_or(BytesMut::zeroed(len));
        Self {
            buf: Some(buf),
            pool,
        }
    }
}

impl Deref for PooledBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.buf.as_ref().unwrap()
    }
}

impl DerefMut for PooledBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.buf.as_mut().unwrap()
    }
}

impl AsRef<[u8]> for PooledBuf {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for PooledBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

/// Clone prefers a recycled allocation from the pool; falls back to a fresh
/// heap copy. The pool lock is held only for the `get()` call; the memcpy
/// happens after the lock is released.
impl Clone for PooledBuf {
    fn clone(&self) -> Self {
        let pooled = self.pool.lock().unwrap().get();
        let inner = if let Some(mut p) = pooled {
            p.clear();
            p.extend_from_slice(self);
            p
        } else {
            self.buf.as_ref().unwrap().clone()
        };
        PooledBuf {
            buf: Some(inner),
            pool: self.pool.clone(),
        }
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.pool.lock().unwrap().put(buf);
        }
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
    buf: CowBuf<PooledBuf>,
    offset: usize,
    index: usize,
    touch: time::Instant,
    state: Arc<AtomicU32>,
    file: MutexBackFile,

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

impl Deref for PieceBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl DerefMut for PieceBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl AsMut<[u8]> for PieceBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self.state.fetch_or(DIRTY, Ordering::Acquire);
        self.touch = time::Instant::now();
        self.buf.as_mut()
    }
}

impl AsRef<[u8]> for PieceBuf {
    fn as_ref(&self) -> &[u8] {
        // TODO: update touch time, but it requires interior mutability
        self.buf.as_ref()
    }
}

impl Drop for PieceBuf {
    fn drop(&mut self) {
        let on_err = self.on_error.take();

        let f = self.file.clone();
        let s = self.state.clone();
        let offset = self.offset;
        let buf = self.buf.clone();

        // if drop is running, all background flush should stop
        // set in_drop, if other worker see in_drop is true, they stop
        let old_state = s.fetch_or(FLUSHING | DROPPING, Ordering::Acquire);
        if old_state & (DIRTY | FLUSHING) > 0 {
            // buffer may be dirty while DIRTY bit is 0 if other is FLUSHING
            let index = self.index;
            tokio::task::spawn_blocking(move || {
                let r = flush_buf_force(buf, offset, s, f, true);
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

    pub fn is_dirty(&self) -> bool {
        let s = self.state.load(Ordering::Relaxed);
        (s & (DIRTY | FLUSHING)) > 0
    }

    pub fn is_flushing(&self) -> bool {
        let s = self.state.load(Ordering::Relaxed);
        (s & FLUSHING) > 0
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

        let state = self.state.load(Ordering::Acquire) & (DIRTY | FLUSHING);
        match state {
            0b00 => {}
            0b01 => {}
            0b10 => {
                // no flushing in progress, no one else can change state
                let old_state = self.state.swap(FLUSHING, Ordering::Acquire);
                assert_eq!(old_state, 0b10);

                let f = self.file.clone();
                let s = self.state.clone();
                let offset = self.offset;

                // create a cheap copy of buf, and implicitly make ourself read-only
                // next time we write to ourself, we will clone the buf
                let buf = self.buf.clone();
                tokio::task::spawn_blocking(move || {
                    let r = flush_buf_force(buf, offset, s, f, false);
                    result_callback(r);
                });
            }
            0b11 => {}
            _ => unreachable!(),
        }
    }
}

/// flush buffer to file
/// dirty may change from 0 to 1
/// flushing must change from 1 to 0
fn flush_buf_force<T>(
    buf: T,
    offset: usize,
    state: Arc<AtomicU32>,
    file: MutexBackFile,
    from_drop: bool,
) -> Result<(), FlushErr>
where
    T: AsRef<[u8]>,
{
    let r = {
        let mut f = file.lock().unwrap();
        let old_state = state.load(Ordering::Relaxed);
        let dropping = old_state & DROPPING > 0;
        if dropping && from_drop || !dropping {
            f.write_all_at(offset, buf.as_ref())
        } else {
            Ok(())
        }
    };

    let len = buf.as_ref().len();
    if let Err(e) = r {
        // dirty flush bits
        // 01 -> 10 no new write after flushing begins
        // 11 -> 10 new write after flushing begins, the dirty bit is set by others
        // in all cases, should set dirty=1, flush=0
        let _old_state = state.fetch_or(DIRTY, Ordering::Relaxed);
        state.fetch_and(!FLUSHING, Ordering::Release);
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
    // change p, but don't set p to be dirty
    let mut_but_clear = match &mut p.buf {
        CowBuf::Owned(b) => b.as_mut().unwrap(),
        CowBuf::Shared(_) => panic!("should not be write to shared Cow"),
    };
    let mut f = file.lock().unwrap();
    f.read_exact_at(p.offset, mut_but_clear)?;
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
            pool: Arc::new(Mutex::new(Pool::new(POOL_SIZE))),
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

        // Drop the original sender so rx.recv() terminates once all
        // spawn_blocking flush tasks drop their clones.
        drop(tx);
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

            let p = PieceBuf {
                buf: CowBuf::new(PooledBuf::new(self.pool.clone(), len)),
                touch: time::Instant::now(),
                index: piece_idx,
                offset,
                state: Arc::new(AtomicU32::new(0)),
                file: self.back_file.clone(),
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
            assert!(matches!(guard.remove(&p.index), Some(PieceState::Returned)));
            info!("insert piece buffer {}", p.index());
            self.pieces.insert(p.index, p);
        }
        self.purge_by_size(POOL_SIZE);
    }

    pub fn purge_by_time(&mut self, timeout: time::Duration) {
        self.pieces.retain(|_, v| v.touch.elapsed() > timeout)
    }

    pub fn purge_by_size(&mut self, keep: usize) {
        if self.pieces.len() <= keep {
            return;
        }

        // TODO: OPTIMIZE
        let mut n_purge = self.pieces.len() - keep;

        let mut remove_pieces = BinaryHeap::new();

        for (k, v) in self.pieces.iter().filter(|(_, v)| !v.is_dirty()) {
            remove_pieces.push(cmp::Reverse((v.touch, *k)));
            if remove_pieces.len() > n_purge {
                remove_pieces.pop();
            }
        }
        while n_purge > 0 {
            if let Some(cmp::Reverse((_, i))) = remove_pieces.pop() {
                self.pieces.remove(&i);
                info!("purge clear piece {i}");
                n_purge -= 1;
            }
        }

        if n_purge > 0 {
            // now we remove dirty pieces
            for (k, v) in self.pieces.iter() {
                remove_pieces.push(cmp::Reverse((v.touch, *k)));
                if remove_pieces.len() > n_purge {
                    remove_pieces.pop();
                }
            }
            while n_purge > 0 {
                if let Some(cmp::Reverse((_, i))) = remove_pieces.pop() {
                    info!("purge dirty piece {i}");
                    self.pieces.remove(&i);
                    n_purge -= 1;
                }
            }
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

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering as AtomicOrd;
    use std::sync::atomic::{AtomicBool, AtomicU32};
    use std::sync::{Arc, Mutex};

    use crate::backfile::VoidFile;
    use bytes::BytesMut;
    use tokio::time;

    use super::{
        BackFile, CowBuf, FlushErr, MutexBackFile, PieceBuf, Pool, PooledBuf, DIRTY, FLUSHING,
    };

    fn void_file() -> MutexBackFile {
        Arc::new(Mutex::new(BackFile::new::<VoidFile>().build()))
    }

    fn make_piece_buf_state(
        pool: Arc<Mutex<Pool<BytesMut>>>,
        len: usize,
        initial_state: u32,
    ) -> PieceBuf {
        PieceBuf {
            buf: CowBuf::new(PooledBuf::new(pool, len)),
            offset: 0,
            index: 0,
            touch: time::Instant::now(),
            state: Arc::new(AtomicU32::new(initial_state)),
            file: void_file(),
            on_error: Some(Box::new(|_: FlushErr| {})),
        }
    }

    // ── CowBuf ────────────────────────────────────────────────────────────────

    #[test]
    fn cowbuf_clone_transitions_owned_to_shared() {
        let mut cow: CowBuf<Vec<u8>> = CowBuf::new(vec![1u8, 2, 3]);
        assert!(matches!(cow, CowBuf::Owned(_)));

        let shared = cow.clone(); // inherent clone(&mut self)

        assert!(matches!(cow, CowBuf::Shared(_)));
        assert!(matches!(shared, CowBuf::Shared(_)));
        assert_eq!(&**cow, &[1u8, 2, 3]);
        assert_eq!(&**shared, &[1u8, 2, 3]);

        // Both variants point to the same Arc allocation.
        if let (CowBuf::Shared(a), CowBuf::Shared(b)) = (&cow, &shared) {
            assert!(Arc::ptr_eq(a, b));
        } else {
            panic!("expected Shared after clone");
        }
    }

    #[test]
    fn cowbuf_shared_clone_shares_same_arc() {
        let mut cow: CowBuf<Vec<u8>> = CowBuf::new(vec![10u8]);
        let _s1 = cow.clone(); // Owned → Shared
        let s2 = cow.clone(); // Shared → Shared (same Arc)

        if let (CowBuf::Shared(a), CowBuf::Shared(b)) = (&cow, &s2) {
            assert!(Arc::ptr_eq(a, b));
        } else {
            panic!("expected Shared");
        }
    }

    #[test]
    fn cowbuf_deref_mut_on_shared_causes_cow() {
        let mut cow: CowBuf<Vec<u8>> = CowBuf::new(vec![1u8, 2, 3]);
        let shared = cow.clone(); // both Shared, same Arc

        // Mutate through cow — triggers COW.
        use std::ops::DerefMut;
        cow.deref_mut().push(4);

        // cow is now a new Owned copy; shared still points to original.
        assert!(matches!(cow, CowBuf::Owned(_)));
        assert!(matches!(shared, CowBuf::Shared(_)));
        assert_eq!(&**cow, &[1u8, 2, 3, 4]);
        assert_eq!(&**shared, &[1u8, 2, 3]);
    }

    #[test]
    fn cowbuf_deref_mut_on_owned_stays_owned() {
        let mut cow: CowBuf<Vec<u8>> = CowBuf::new(vec![1u8, 2]);
        use std::ops::DerefMut;
        cow.deref_mut().push(3);
        assert!(matches!(cow, CowBuf::Owned(_)));
        assert_eq!(&**cow, &[1u8, 2, 3]);
    }

    // ── PooledBuf ─────────────────────────────────────────────────────────────

    #[test]
    fn pooledbuf_drop_recycles_to_pool() {
        let pool = Arc::new(Mutex::new(Pool::<BytesMut>::new(4)));
        assert_eq!(pool.lock().unwrap().pool.len(), 0);

        let buf = PooledBuf::new(pool.clone(), 16);
        assert_eq!(pool.lock().unwrap().pool.len(), 0);

        drop(buf);
        assert_eq!(pool.lock().unwrap().pool.len(), 1); // recycled
    }

    #[test]
    fn pooledbuf_new_reuses_from_pool() {
        let pool = Arc::new(Mutex::new(Pool::<BytesMut>::new(4)));
        drop(PooledBuf::new(pool.clone(), 16)); // seed pool
        assert_eq!(pool.lock().unwrap().pool.len(), 1);

        let _buf = PooledBuf::new(pool.clone(), 16);
        assert_eq!(pool.lock().unwrap().pool.len(), 0); // taken from pool
    }

    #[test]
    fn pooledbuf_clone_prefers_pool_allocation() {
        let pool = Arc::new(Mutex::new(Pool::<BytesMut>::new(4)));

        // Seed pool with one allocation.
        {
            let mut p = pool.lock().unwrap();
            p.put(BytesMut::zeroed(8));
        }
        assert_eq!(pool.lock().unwrap().pool.len(), 1);

        let original = PooledBuf::new(pool.clone(), 8);
        // Pool was empty for `new` (the seeded slot stayed) — actually new() takes it.
        // Re-seed so clone() has something to take.
        {
            let mut p = pool.lock().unwrap();
            p.put(BytesMut::zeroed(8));
        }
        assert_eq!(pool.lock().unwrap().pool.len(), 1);

        let cloned = original.clone();
        // Clone consumed the pool slot.
        assert_eq!(pool.lock().unwrap().pool.len(), 0);
        // Content is identical.
        assert_eq!(&*original, &*cloned);
    }

    // ── PieceBuf flush / COW ──────────────────────────────────────────────────

    #[tokio::test]
    async fn piecebuf_flush_transitions_buf_to_shared() {
        let pool = Arc::new(Mutex::new(Pool::new(4)));
        let mut pb = make_piece_buf_state(pool, 8, DIRTY);

        assert!(matches!(pb.buf, CowBuf::Owned(_)), "should start Owned");

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), FlushErr>>();
        pb.flush(move |r| {
            let _ = tx.send(r);
        });

        // After flush() the buffer is shared — no memcpy yet.
        assert!(
            matches!(pb.buf, CowBuf::Shared(_)),
            "buf should be Shared after flush"
        );

        rx.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn piecebuf_write_after_flush_triggers_cow() {
        let pool = Arc::new(Mutex::new(Pool::new(4)));
        let mut pb = make_piece_buf_state(pool, 8, DIRTY);
        pb.as_mut()[..4].copy_from_slice(&[0xAA; 4]);

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), FlushErr>>();
        pb.flush(move |r| {
            let _ = tx.send(r);
        });
        assert!(matches!(pb.buf, CowBuf::Shared(_)));

        // Write new data → COW clone happens here.
        pb.as_mut()[..4].copy_from_slice(&[0xBB; 4]);

        assert!(
            matches!(pb.buf, CowBuf::Owned(_)),
            "buf should be Owned (new allocation) after COW write"
        );
        assert_eq!(pb.as_ref()[..4], [0xBB; 4]);

        rx.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn piecebuf_flush_snapshot_survives_subsequent_write() {
        // Verify: the Arc holding the original data stays alive until the flush
        // task is done, even after a COW write gives PieceBuf a new buffer.
        let pool = Arc::new(Mutex::new(Pool::new(4)));
        let mut pb = make_piece_buf_state(pool, 8, DIRTY);
        pb.as_mut()[..4].copy_from_slice(&[0xAA; 4]);

        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), FlushErr>>();
        pb.flush(move |r| {
            let _ = tx.send(r);
        });

        // Grab a reference to the shared Arc before COW.
        let snapshot = if let CowBuf::Shared(a) = &pb.buf {
            a.clone()
        } else {
            panic!("expected Shared");
        };
        // 3 owners: pb.buf, flush task, snapshot.
        assert_eq!(Arc::strong_count(&snapshot), 3);

        // COW write — pb.buf detaches into a new Owned allocation.
        pb.as_mut()[..4].copy_from_slice(&[0xBB; 4]);
        assert!(matches!(pb.buf, CowBuf::Owned(_)));

        // pb.buf no longer holds the old Arc; 2 owners remain (flush task + snapshot).
        // (The flush task may have already finished and dropped its ref, leaving 1.)
        let count = Arc::strong_count(&snapshot);
        assert!(
            count == 1 || count == 2,
            "expected 1–2 strong refs, got {count}"
        );

        // Wait for flush, then only snapshot holds the Arc.
        rx.await.unwrap().unwrap();
        assert_eq!(Arc::strong_count(&snapshot), 1);

        // The original data is intact in the snapshot.
        assert_eq!(snapshot.as_ref()[..4], [0xAA; 4]);
    }

    #[tokio::test]
    async fn piecebuf_flush_clean_does_not_call_callback() {
        let pool = Arc::new(Mutex::new(Pool::new(4)));
        let mut pb = make_piece_buf_state(pool, 8, 0); // NOT dirty

        let called = Arc::new(AtomicBool::new(false));
        let called2 = called.clone();
        pb.flush(move |_| {
            called2.store(true, AtomicOrd::SeqCst);
        });

        // Callback must NOT be called for a clean buffer.
        assert!(!called.load(AtomicOrd::SeqCst));
        // buf stays Owned (no sharing happened).
        assert!(matches!(pb.buf, CowBuf::Owned(_)));
    }

    #[tokio::test]
    async fn piecebuf_flush_already_flushing_does_not_call_callback() {
        let pool = Arc::new(Mutex::new(Pool::new(4)));
        let mut pb = make_piece_buf_state(pool, 8, FLUSHING); // FLUSHING, not dirty

        let called = Arc::new(AtomicBool::new(false));
        let called2 = called.clone();
        pb.flush(move |_| {
            called2.store(true, AtomicOrd::SeqCst);
        });

        assert!(!called.load(AtomicOrd::SeqCst));
    }

    #[test]
    #[should_panic(expected = "should not be write to shared Cow")]
    fn read_from_file_panics_on_shared_buf() {
        let pool = Arc::new(Mutex::new(Pool::new(4)));
        let file = void_file();
        let mut pb = PieceBuf {
            buf: CowBuf::new(PooledBuf::new(pool, 8)),
            offset: 0,
            index: 0,
            touch: time::Instant::now(),
            state: Arc::new(AtomicU32::new(0)),
            file: file.clone(),
            on_error: Some(Box::new(|_: FlushErr| {})),
        };
        let _shared = pb.buf.clone(); // make Shared
                                      // read_from_file must panic when buf is Shared.
        super::read_from_file(pb, file).unwrap();
    }
}

use std::{
    collections::VecDeque,
    fmt, io,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
};

use super::MutexBackFile;
use crate::protocol::Request;
use bytes::BytesMut;
use tokio::time;
use tracing::warn;

pub type PieceIndex = u32;
pub type SubPieceIndex = u32;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct JointIndex(u64);
impl std::fmt::Debug for JointIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let piece_idx = self.index();
        let sub_idx = self.sub_index();
        f.debug_tuple("JointIndex")
            .field(&format_args!("piece {piece_idx}, sub {sub_idx}"))
            .finish()
    }
}

impl JointIndex {
    #[inline]
    pub fn new(piece_idx: PieceIndex, in_piece_offset: SubPieceIndex) -> Self {
        let sub_idx = in_piece_offset / SUB_PIECE_SIZE;
        JointIndex((piece_idx as u64) << 32 | sub_idx as u64)
    }

    #[inline]
    pub fn index(&self) -> PieceIndex {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub fn sub_index(&self) -> SubPieceIndex {
        self.0 as u32
    }

    #[inline]
    /// in_piece_offset returns the starting byte offset of this sub-piece
    /// within its piece
    pub fn in_piece_offset(&self) -> usize {
        (self.sub_index() * SUB_PIECE_SIZE) as usize
    }
}

impl From<Request> for JointIndex {
    #[inline]
    fn from(req: Request) -> Self {
        JointIndex::new(req.index, req.begin)
    }
}

/// Must be a multiple of the BT block size (16 384 bytes) so that no block
/// ever straddles a sub-piece boundary inside `copy_to_piecebuf`.
pub const SUB_PIECE_SIZE: u32 = 512 * 1024; // 512 KiB
pub const POOL_SIZE: usize = 60;

const FLUSHING: u32 = 0b1;
const DIRTY: u32 = 0b10;
const DROPPING: u32 = 0b100;

/// A pool of objects
pub(crate) struct Pool<T> {
    limit: usize,
    pool: VecDeque<T>,
}

impl<T> Pool<T> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            pool: VecDeque::with_capacity(limit),
        }
    }

    pub(crate) fn put(&mut self, t: T) {
        if self.pool.len() < self.limit {
            self.pool.push_back(t)
        }
    }

    pub(crate) fn get(&mut self) -> Option<T> {
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
    // Option is always Some
    Shared(Option<Arc<T>>),
}

impl<T> CowBuf<T> {
    fn new(t: T) -> Self {
        CowBuf::Owned(Some(t))
    }

    fn clone(&mut self) -> Self {
        match self {
            CowBuf::Owned(b) => {
                let s = Arc::new(b.take().unwrap());
                *self = CowBuf::Shared(Some(s.clone()));
                Self::Shared(Some(s))
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
            CowBuf::Shared(a) => a.as_ref().unwrap(),
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
                let arc = a.take().unwrap();
                let inner = match Arc::try_unwrap(arc) {
                    Ok(owned) => owned,
                    Err(arc) => (*arc).clone(),
                };
                *self = CowBuf::Owned(Some(inner));
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
    index: JointIndex,
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
            .field("touch", &self.touch.elapsed())
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
                    warn!("PieceBuf::Drop flush error index {index:?} {e:?}, data lost");
                    on_err.unwrap()(e)
                }
            });
        }
    }
}

impl PieceBuf {
    pub fn index(&self) -> JointIndex {
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

    pub fn access_time(&self) -> time::Instant {
        self.touch
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
        // 11 -> 11 dirty, flushing in progress, no more flush, return

        // we have the &mut, only we can change flushing bit from 0 to 1.
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

    /// Allocate a new PieceBuf with the given parameters.
    /// Used by `CacheManager` to create pieces for file loading.
    pub(crate) fn alloc(
        pool: &Arc<Mutex<Pool<BytesMut>>>,
        index: JointIndex,
        offset: usize,
        len: usize,
        file: MutexBackFile,
        on_error: Box<dyn ErrorCallback>,
    ) -> Self {
        PieceBuf {
            buf: CowBuf::new(PooledBuf::new(pool.clone(), len)),
            touch: time::Instant::now(),
            index,
            offset,
            state: Arc::new(AtomicU32::new(0)),
            file,
            on_error: Some(on_error),
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

pub(crate) fn read_from_file(mut p: PieceBuf, file: MutexBackFile) -> io::Result<PieceBuf> {
    // change p, but don't set p to be dirty
    let mut_but_clear = match &mut p.buf {
        CowBuf::Owned(b) => b.as_mut().unwrap(),
        CowBuf::Shared(_) => panic!("should not be write to shared Cow"),
    };
    let mut f = file.lock().unwrap();
    f.read_exact_at(p.offset, mut_but_clear)?;
    Ok(p)
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
        read_from_file, CowBuf, FlushErr, JointIndex, MutexBackFile, PieceBuf, Pool, PooledBuf,
        DIRTY, FLUSHING,
    };
    use crate::backfile::BackFile;

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
            index: JointIndex::new(0, 0),
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
            assert!(Arc::ptr_eq(a.as_ref().unwrap(), b.as_ref().unwrap()));
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
            assert!(Arc::ptr_eq(a.as_ref().unwrap(), b.as_ref().unwrap()));
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

    /// When only one CowBuf holds the Arc (sole owner), `deref_mut` should
    /// reclaim the allocation via `try_unwrap` instead of cloning.
    #[test]
    fn cowbuf_deref_mut_on_sole_shared_owner_reuses_allocation() {
        // Pre-allocate with extra capacity so push(4) below does not reallocate,
        // keeping the buffer pointer stable across the deref_mut call.
        let mut v = Vec::with_capacity(4);
        v.extend_from_slice(&[1u8, 2, 3]);
        let mut cow: CowBuf<Vec<u8>> = CowBuf::new(v);
        // Transition to Shared, then drop the second handle so cow is the only owner.
        let shared = cow.clone();
        drop(shared);
        // cow is Shared but with strong_count == 1.
        assert!(matches!(cow, CowBuf::Shared(_)));

        // Capture the Vec's internal buffer pointer from inside the Arc.
        let buf_ptr = if let CowBuf::Shared(ref a) = cow {
            a.as_ref().unwrap().as_ptr()
        } else {
            unreachable!()
        };

        use std::ops::DerefMut;
        cow.deref_mut().push(4);

        // Must become Owned.
        assert!(matches!(cow, CowBuf::Owned(_)));
        assert_eq!(&**cow, &[1u8, 2, 3, 4]);

        // The inner Vec should use the *same* buffer (try_unwrap moved it, no clone).
        if let CowBuf::Owned(ref b) = cow {
            assert_eq!(
                buf_ptr,
                b.as_ref().unwrap().as_ptr(),
                "expected try_unwrap to reuse the Vec buffer, but a new one was allocated"
            );
        }
    }

    /// When multiple CowBufs share the Arc (strong_count > 1), `deref_mut`
    /// must clone the data so the other handles are unaffected.
    #[test]
    fn cowbuf_deref_mut_on_shared_with_multiple_owners_clones_data() {
        let mut cow: CowBuf<Vec<u8>> = CowBuf::new(vec![10u8, 20, 30]);
        let other = cow.clone(); // strong_count == 2

        assert!(matches!(cow, CowBuf::Shared(_)));

        use std::ops::DerefMut;
        cow.deref_mut().push(40);

        // cow is now a distinct Owned copy.
        assert!(matches!(cow, CowBuf::Owned(_)));
        assert_eq!(&**cow, &[10u8, 20, 30, 40]);

        // `other` still sees the original unmodified data.
        assert!(matches!(other, CowBuf::Shared(_)));
        assert_eq!(&**other, &[10u8, 20, 30]);
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
        assert_eq!(Arc::strong_count(snapshot.as_ref().unwrap()), 3);

        // COW write — pb.buf detaches into a new Owned allocation.
        pb.as_mut()[..4].copy_from_slice(&[0xBB; 4]);
        assert!(matches!(pb.buf, CowBuf::Owned(_)));

        // pb.buf no longer holds the old Arc; 2 owners remain (flush task + snapshot).
        // (The flush task may have already finished and dropped its ref, leaving 1.)
        let count = Arc::strong_count(snapshot.as_ref().unwrap());
        assert!(
            count == 1 || count == 2,
            "expected 1–2 strong refs, got {count}"
        );

        // Wait for flush, then only snapshot holds the Arc.
        rx.await.unwrap().unwrap();
        assert_eq!(Arc::strong_count(snapshot.as_ref().unwrap()), 1);

        // The original data is intact in the snapshot.
        assert_eq!(snapshot.as_ref().unwrap()[..4], [0xAA; 4]);
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
            index: JointIndex::new(0, 0),
            touch: time::Instant::now(),
            state: Arc::new(AtomicU32::new(0)),
            file: file.clone(),
            on_error: Some(Box::new(|_: FlushErr| {})),
        };
        let _shared = pb.buf.clone(); // make Shared
                                      // read_from_file must panic when buf is Shared.
        read_from_file(pb, file).unwrap();
    }
}

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::time;
use tracing::warn;

use crate::backfile::BackFile;

use super::{AllocErr, ArcCache, GetRefErr, MutexBackFile, PieceBuf, PieceBufPool, PieceKey, Ref};

struct BufDetail {
    piece_buffer: HashMap<u32, ArcCache<PieceBuf>>,
}

pub struct BufStorage {
    // TODO: FIXME: maintain when reload a piece while this piece is flushing
    // and not written to disk yet
    // since in inner BufTree, the block is not freed yet, we will get Allocated err?
    piece_buffer: Mutex<BufDetail>,
    buffer_pool: PieceBufPool,

    piece_size: usize,
    last_piece_size: usize,
    piece_total: usize,

    back_file: MutexBackFile,
}

impl BufStorage {
    pub fn new(total_length: usize, piece_size: usize, back_file: BackFile) -> Self {
        let (piece_total, last_piece_size) = piece_total_and_last_size(total_length, piece_size);
        Self {
            piece_buffer: Mutex::new(BufDetail {
                piece_buffer: HashMap::new(),
            }),
            back_file: Arc::new(Mutex::new(back_file)),

            piece_size,
            last_piece_size,
            piece_total,

            // TODO: FIXME: use a shared buffer_pool
            buffer_pool: PieceBufPool::new(80 * 16 * 16384),
        }
    }

    pub async fn get_part_ref(
        &self,
        piece_idx: u32,
        offset: u32,
        len: u32,
        key: PieceKey,
    ) -> Result<(ArcCache<PieceBuf>, Ref<PieceBuf>), GetRefErr> {
        let piece_len = self.piece_len(piece_idx as usize);
        'outer: loop {
            let maybe_pb: Option<ArcCache<_>> = {
                // must drop pb_map before await point
                // pb_map is not Send
                let pb_map = self.piece_buffer.lock().unwrap();
                pb_map.piece_buffer.get(&piece_idx).cloned()
            };

            let (block_ref, piece_buf) = {
                let pb: ArcCache<PieceBuf> = match maybe_pb {
                    Some(pb) if pb.is_valid() => {
                        // if exist in cache, someone else must allocated cache before,
                        // load_from_file should be set to true? Technically should
                        // be after flushed.
                        // TODO: is this correct?
                        pb
                    }
                    _invalid_buf_or_no_buf => {
                        // if exist in cache, someone else must allocated cache before, once
                        // cache is valid, load_from_file should be set to true? Technically should
                        // be after flushed.
                        // TODO: is this correct?
                        match self
                            .buffer_pool
                            .async_alloc_abort::<ArcCache<_>>(
                                key.clone(),
                                Some(self.back_file.clone()),
                                piece_len,
                            )
                            .await
                        {
                            Ok(pb) => {
                                // if this piece buffer already exists(allocated by other peer handler),
                                // use exist one
                                let mut pb_map = self.piece_buffer.lock().unwrap();
                                match pb_map.piece_buffer.get(&piece_idx) {
                                    Some(exist_pb) => {
                                        // some one else loaded this, this PieceBuf gets dropped
                                        // and piece must be clean, so no flush to disk which corrupts data
                                        assert!(pb.piece_detail(|p| !p.is_dirty()).unwrap_or(true));
                                        exist_pb.clone()
                                    }
                                    None => {
                                        pb_map.piece_buffer.insert(piece_idx, pb.clone());
                                        pb
                                    }
                                }
                            }
                            Err(AllocErr::Allocated) => {
                                // TODO: another connection allocated this piece
                                // but we did not find that piece in buffer hashmap before
                                // so the connection allocated this is inserting to buffer
                                // concurrently.
                                // we should drop lock and wait some time?
                                // FIXME: we should wake as soon as the piece is added to
                                // hashmap, not some fixed 10ms
                                time::sleep(time::Duration::from_millis(10)).await;
                                continue 'outer;
                            }
                            Err(e) => {
                                unreachable!("we using async alloc get error {e:?}");
                            }
                        }
                    }
                };

                let block_ref = pb.async_get_part_ref(offset as usize, len as usize).await;

                (block_ref, pb)
            };

            // TODO: need a biglock. What if some peer else is doing operation now?
            // i.e. operation between two locks?
            match block_ref {
                Ok(bbuf) => {
                    let pb_map = self.piece_buffer.lock().unwrap();
                    return Ok((piece_buf, bbuf));
                }
                Err(GetRefErr::Invalidated) => {
                    // TODO: FIXME: will this cause dead loop?
                    // get buffer then invalidated by other, then re-get
                    // re-invalidate and loops forever?
                    warn!("piece invalidated");
                    continue 'outer;
                }
                Err(GetRefErr::Paused) => {
                    // if using async mode, won't return paused
                    unreachable!()
                }
                Err(e) => {
                    warn!(
                        "buf storage get ref error: {e:?} PIECE {} {} {}",
                        piece_idx, offset, len,
                    );
                    // TODO: what should we do now?
                    // we don't have that space
                    // maybe reads to supplementary buffer?
                    // just return now
                    return Err(e);
                }
            }
        }
    }

    pub fn set_can_flush(&self, piece_idx: u32) {
        let mut buf = self
            .piece_buffer
            .lock()
            .expect("mark complete lock should OK");
        buf.piece_buffer.remove(&piece_idx);
        // TODO: FIXME: add counters for flushing pieces
    }

    fn piece_len(&self, piece_idx: usize) -> usize {
        if piece_idx + 1 == self.piece_total {
            self.last_piece_size // last piece
        } else {
            self.piece_size
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

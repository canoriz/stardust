mod buffer_storage;
mod global;
mod piece;
pub(crate) use buffer_storage::BufStorage;
pub(crate) use global::{wake_next_waiting_alloc, AllocReq};
pub(crate) use global::{AllocErr, PieceBuf, PieceBufPool, PieceKey};
pub(crate) use global::{DynFileImpl, FileImpl};
pub(crate) use piece::AsyncAbortRead;
pub(crate) use piece::*;

const DROPPED: u32 = 0b01;
const DONE: u32 = 0b010; // TODO: really need this?
const WAITING: u32 = 0;
const WAKING: u32 = 0b100;

#[cfg(test)]
mod test {
    use std::future::pending;

    use super::*;
    use tokio_test::task;
}

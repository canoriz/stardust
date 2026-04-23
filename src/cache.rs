pub mod cache_manager;
pub mod simple_buffer;
use std::sync::{Arc, Mutex};

use crate::backfile::BackFile;

pub(crate) type MutexBackFile = Arc<Mutex<BackFile>>;

#[cfg(test)]
mod test {}

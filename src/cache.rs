pub mod simple_buffer;
use std::sync::{Arc, Mutex};

use crate::backfile::BackFile;

type MutexBackFile = Arc<Mutex<BackFile>>;

#[cfg(test)]
mod test {}

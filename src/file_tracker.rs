use crate::metadata::Metadata;

/// Tracks which files have been fully downloaded by counting verified pieces
/// against each file's piece span.
pub struct FileTracker {
    files: Vec<FileRange>,
}

struct FileRange {
    begin: u64,
    len: u64,
    n_pieces: u64,
}

impl FileTracker {
    pub fn new(meta: &Metadata) -> Self {
        let mut files = vec![];
        let mut last = 0u64;
        let piece_size = meta.piece_size_of(0) as u64;
        for f in meta.files() {
            let begin = last;
            last += f.length;
            let p_start = begin.saturating_sub(1) / piece_size;
            let p_end = last.div_ceil(piece_size);
            files.push(FileRange {
                begin: begin,
                len: f.length,
                n_pieces: p_end - p_start,
            });
        }
        Self { files }
    }

    /// `index`` piece is verified
    /// returns indexies of completed file
    pub fn piece_verified(&mut self, index: u32) -> Vec<usize> {
        todo!()
    }
}

use crate::metadata::Metadata;

/// Tracks which files have been fully downloaded by counting verified pieces
/// against each file's piece span.
#[derive(Debug, Clone)]
pub struct FileTracker {
    files: Vec<FileRange>,
}

#[derive(Debug, Clone)]
struct FileRange {
    /// first piece index overlapping this file (inclusive)
    p_start: u64,
    /// last piece index overlapping this file (exclusive)
    p_end: u64,
    /// pieces in [p_start, p_end) not yet verified; file complete when 0
    remaining: u64,
}

impl FileTracker {
    pub fn new(meta: &Metadata) -> Self {
        let piece_size = meta.regular_piece_size() as u64;
        Self::from_lengths(piece_size, meta.files().iter().map(|f| f.length))
    }

    pub(crate) fn from_lengths(piece_size: u64, lengths: impl IntoIterator<Item = u64>) -> Self {
        let mut files = vec![];
        let mut begin = 0u64;
        for len in lengths {
            let end = begin + len;
            let p_start = begin / piece_size;
            let p_end = end.div_ceil(piece_size);
            files.push(FileRange {
                p_start,
                p_end,
                remaining: p_end - p_start,
            });
            begin = end;
        }
        Self { files }
    }

    /// Apply a piece's verified-state change to every file overlapping `index`.
    /// `verified = true` marks the piece done (decrements each file's outstanding
    /// count, returning files that just reached 0); `verified = false` undoes it
    /// (increments the count). The change is UNCONDITIONAL: the caller
    /// (`BlockPicker`) consults the piece's actual verified state and only calls
    /// on a real transition, so the tracker never double-counts.
    /// Returns indices of files that just became complete (empty when reverting).
    pub fn piece_verified(&mut self, index: u32, verified: bool) -> Vec<usize> {
        let index = index as u64;
        let mut completed = vec![];
        // files are sorted by p_start (and p_end) ascending, so the files
        // overlapping `index` form a contiguous run. Skip files that ended
        // before `index`, then scan until a file starts after `index`.
        let first = self.files.partition_point(|f| f.p_end <= index);
        for (i, f) in self.files.iter_mut().enumerate().skip(first) {
            if f.p_start > index {
                break;
            }
            if verified {
                f.remaining -= 1;
                if f.remaining == 0 {
                    completed.push(i);
                }
            } else {
                f.remaining += 1;
            }
        }
        completed
    }

    /// Reset every file to fully-incomplete (`remaining = full span`). Used
    /// before replaying verified pieces when restoring picker progress.
    pub fn reset(&mut self) {
        for f in &mut self.files {
            f.remaining = f.p_end - f.p_start;
        }
    }

    /// Indices of files not yet fully downloaded (`remaining > 0`).
    pub fn incomplete_files(&self) -> Vec<usize> {
        self.files
            .iter()
            .enumerate()
            .filter_map(|(i, f)| (f.remaining > 0).then_some(i))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::FileTracker;

    fn verify_all(t: &mut FileTracker, pieces: &[u32]) -> Vec<usize> {
        let mut done = vec![];
        for &p in pieces {
            done.extend(t.piece_verified(p, true));
        }
        done
    }

    #[test]
    fn single_file_one_piece() {
        // piece_size=10, one file of 10 bytes -> piece [0,1)
        let mut t = FileTracker::from_lengths(10, [10]);
        assert!(t.piece_verified(0, true).is_empty() == false);
    }

    #[test]
    fn file_boundary_shared_piece() {
        // piece_size=10. file0=[0,15) pieces [0,2); file1=[15,20) pieces [1,2)
        // piece 1 is shared by both files.
        let mut t = FileTracker::from_lengths(10, [15, 5]);
        assert_eq!(t.piece_verified(0, true), Vec::<usize>::new()); // file0 still needs p1
        assert_eq!(t.piece_verified(1, true), vec![0, 1]); // shared piece completes both
    }

    #[test]
    fn file_starts_on_boundary() {
        // piece_size=10. file0=[0,10) pieces [0,1); file1=[10,20) pieces [1,2)
        let mut t = FileTracker::from_lengths(10, [10, 10]);
        assert_eq!(t.piece_verified(0, true), vec![0]);
        assert_eq!(t.piece_verified(1, true), vec![1]);
    }

    #[test]
    fn multi_piece_file_completes_last() {
        // piece_size=10, one file [0,25) -> pieces [0,3)
        let mut t = FileTracker::from_lengths(10, [25]);
        assert_eq!(verify_all(&mut t, &[0, 1]), Vec::<usize>::new());
        assert_eq!(t.piece_verified(2, true), vec![0]);
    }

    #[test]
    fn many_small_files_in_one_piece() {
        // piece_size=100, files of 10 bytes each all inside piece 0
        let mut t = FileTracker::from_lengths(100, [10, 10, 10]);
        assert_eq!(t.piece_verified(0, true), vec![0, 1, 2]);
    }

    #[test]
    fn verify_then_revert_round_trips() {
        // piece_size=10, one file [0,25) -> pieces [0,3)
        let mut t = FileTracker::from_lengths(10, [25]);
        assert_eq!(verify_all(&mut t, &[0, 1, 2]), vec![0]);
        assert_eq!(t.incomplete_files(), Vec::<usize>::new());
        // reverting the last piece re-opens the file
        assert_eq!(t.piece_verified(2, false), Vec::<usize>::new());
        assert_eq!(t.incomplete_files(), vec![0]);
        // re-verifying completes it again
        assert_eq!(t.piece_verified(2, true), vec![0]);
    }
}

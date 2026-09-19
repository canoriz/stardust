/// returns number of total pieces and size of last piece
pub fn piece_total_and_last_size(total_length: u64, piece_size: usize) -> (usize, usize) {
    let ps = piece_size as u64;
    let n_full_piece = total_length / ps;
    let full_piece_total_size = n_full_piece * ps;
    if full_piece_total_size == total_length {
        (n_full_piece as usize, piece_size)
    } else {
        (
            n_full_piece as usize + 1,
            (total_length - full_piece_total_size) as usize,
        )
    }
}

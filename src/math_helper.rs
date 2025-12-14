/// returns number of total pieces and size of last piece
pub fn piece_total_and_last_size(total_length: usize, piece_size: usize) -> (usize, usize) {
    let n_full_piece = total_length / piece_size;
    let full_piece_total_size = n_full_piece * piece_size;
    if full_piece_total_size == total_length {
        (n_full_piece, piece_size)
    } else {
        (n_full_piece + 1, (total_length - full_piece_total_size))
    }
}

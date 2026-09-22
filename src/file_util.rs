// find first i that does not satisfy cond, if all i satisfy, return len()
// invariant l <= not satisfy < r
pub fn partition<A>(fr: &[A], cond: impl Fn(&A) -> bool) -> usize {
    let mut r = fr.len();
    let mut l = 0;
    while l < r {
        let mid = (l + r) / 2;
        let f = &fr[mid];
        if cond(f) {
            l = mid + 1;
        } else {
            r = mid
        }
    }
    l
}

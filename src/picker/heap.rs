pub use crate::protocol::BitField;
use std::cmp;
use std::mem;
use std::net::SocketAddr;
use std::ops::Add;
use std::ops::Deref;
use std::ptr;

pub struct PeerInfo {
    pub addr: SocketAddr,
    pub have: BitField,
}

pub struct Peers {
    peers: Vec<PeerInfo>,
}

pub struct Picker {
    heap: Heap<u16>,
}

// impl PiecePicker {
//     fn new(piece_count: usize) -> Self {
//         Self {
//             heap: Heap::new(piece_count),
//         }
//     }
// }

struct Heap<T> {
    data: Vec<(usize, T)>,
    k2i: Vec<Option<usize>>, // key to index in heap

    // delayed rebuild. marks 0 (inclusive) to rebuild_until (exclusive) needs to rebuild
    need_rebuild: bool,
    max_capacity: usize,
    // cache: Vec<Vec<usize>>,
}

impl<T> Heap<T>
where
    T: Ord,
{
    // caller ensures at any time, no more than max_capacity elements in Heap
    pub fn new(max_capacity: usize) -> Self {
        Heap {
            data: Vec::with_capacity(max_capacity),
            k2i: vec![None; max_capacity],

            need_rebuild: false,
            max_capacity,
        }
    }

    // insert element in O(log(n))
    pub fn insert(&mut self, key: usize, val: T) {
        if key >= self.max_capacity {
            return;
        }
        if unsafe { self.get_index(key) }.is_some() {
            self.update(key, val);
            return;
        }

        let idx = self.data.len();
        self.data.push((key, val));
        unsafe {
            self.update_index(key, Some(idx));
        };
        if !self.need_rebuild {
            unsafe { self.push_up(idx) };
        }
    }

    // rebuild heap in O(n), used when faster than insert m insert()
    pub fn insert_bulk<V: IntoIterator<Item = (usize, T)>>(&mut self, values: V) {
        self.need_rebuild = true;
        for (k, v) in values {
            self.insert(k, v);
        }
    }

    fn maybe_rebuild(&mut self) {
        if self.need_rebuild {
            self.need_rebuild = false;
            let mut i = self.data.len() >> 1;
            while i > 0 {
                i -= 1;

                // #[cfg(debug_assertions)]
                // println!("pushing down {i}");

                unsafe { self.push_down(i) };
            }
            debug_assert!(self.test_struct(0, None), "invalid heap after rebuild");
        }
    }

    pub fn delete(&mut self, key: usize) -> Option<(usize, T)> {
        if key >= self.max_capacity {
            return None;
        }

        // #[cfg(debug_assertions)]
        // println!("delete key {key}, rebuild {}", self.need_rebuild);

        if let Some(index) = unsafe { self.get_index(key) } {
            return self.data.pop().map(|mut item| {
                if index < self.data.len() {
                    let to_delete = unsafe { self.data.get_unchecked_mut(index) };

                    debug_assert_eq!(to_delete.0, key);

                    mem::swap(&mut item, to_delete);

                    // #[cfg(debug_assertions)] println!("asdf {item:?}, {to_delete:?}");

                    // now item contains deleted element
                    // and to_delete contains moved element

                    let delete_key = to_delete.0;
                    let greater = to_delete.1 > item.1;
                    unsafe {
                        self.update_index(delete_key, Some(index));
                    };
                    if !self.need_rebuild {
                        if greater {
                            unsafe { self.push_down(index) };
                        } else {
                            unsafe { self.push_up(index) };
                        }
                        debug_assert!(self.test_struct(index, None));
                    }
                }
                debug_assert_eq!(item.0, key);
                unsafe {
                    self.update_index(item.0, None);
                }
                debug_assert!(unsafe { self.get_index(item.0) }.is_none());
                item
            });
        }
        None
    }

    // try to update an index, if key not exist in heap, do nothing and return None
    // if key exists in heap, update value and return previous value
    fn update(&mut self, key: usize, val: T) -> Option<T> {
        // change upward or downword based on delta
        if key >= self.max_capacity {
            return None;
        }

        // #[cfg(debug_assertions)]
        // println!("update key {key} to {val:?}, rebuild {}", self.need_rebuild);

        // TODO: optimize, maybe rebuild() then update()
        if self.need_rebuild {
            return self.update_rebuild(key, val);
        }

        if let Some(idx) = unsafe { self.get_index(key) } {
            // SAFETY: take prev out, doing a compare
            // swap prev with val,

            let (key_idx, current) = unsafe { self.data.get_unchecked_mut(idx) };

            debug_assert_eq!(*key_idx, key);

            let prev = unsafe { ptr::read(current) };
            if val > prev {
                unsafe {
                    ptr::write(current, val);
                    self.push_down(idx);
                }
            } else {
                unsafe {
                    ptr::write(current, val);
                    self.push_up(idx);
                }
            }
            return Some(prev);
        }
        None
    }

    fn update_rebuild(&mut self, key: usize, val: T) -> Option<T> {
        // change upward or downword based on delta
        if key >= self.max_capacity {
            return None;
        }
        self.need_rebuild = true;
        if let Some(idx) = unsafe { self.get_index(key) } {
            let (key_idx, current) = unsafe { self.data.get_unchecked_mut(idx) };
            debug_assert_eq!(*key_idx, key);
            return Some(mem::replace(current, val));
        }
        None
    }

    // returns set number
    fn add_set_cache(&mut self) -> usize {
        todo!()
    }

    fn remove_set_cache(&mut self) -> usize {
        todo!()
    }

    // caller ensures key is valid
    unsafe fn get_index(&self, key: usize) -> Option<usize> {
        *self.k2i.get_unchecked(key)
    }

    // caller ensures key must be valid
    unsafe fn update_index(&mut self, key: usize, new_index: Option<usize>) {
        ptr::write(self.k2i.as_mut_ptr().add(key), new_index);
    }

    pub fn get_val(&self, key: usize) -> Option<&T> {
        if key >= self.max_capacity {
            return None;
        }
        unsafe {
            self.get_index(key)
                .map(|index| &self.data.get_unchecked(index).1)
        }
    }

    // get min of a specific set
    // it takes a function indicates whether some node is in set
    pub fn min_of_set<F>(
        &mut self,
        in_set: F,
        use_cache: Option<usize>,
        known_in_set: Option<usize>, // contains a key that must in set (previous answer)
    ) -> Option<&(usize, T)>
    where
        F: Fn(usize) -> bool,
    {
        self.maybe_rebuild();
        let at_most: Option<&(usize, T)> = known_in_set
            .filter(|key| *key < self.max_capacity)
            .and_then(|key| unsafe { self.get_index(key) })
            .filter(|index| in_set(*index))
            .map(|index| unsafe { self.data.get_unchecked(index) });

        // #[cfg(debug_assertions)]
        // println!("known in set {known_in_set:?} at_most {at_most:?}");

        self.min_of_set_from(0, &in_set, use_cache, at_most.map(|(_, v)| v))
            .or(at_most)
    }

    // at_most: exclusive
    fn min_of_set_from<F>(
        &self,
        from: usize,
        in_set: &F,
        use_cache: Option<usize>,
        at_most: Option<&T>,
    ) -> Option<&(usize, T)>
    where
        F: Fn(usize) -> bool,
    {
        if from >= self.data.len() {
            return None;
        }

        let val = unsafe { self.data.get_unchecked(from) };
        if in_set(val.0) && (at_most.is_some_and(|a| &(val.1) < a) || at_most.is_none()) {
            return Some(val);
        }

        if at_most.is_some_and(|most| &(val.1) >= most) {
            return None;
        }

        let left = self.min_of_set_from(from * 2 + 1, in_set, use_cache, at_most);
        let right_bound = match (left, at_most) {
            (Some(lv), Some(a)) => {
                if a < &(lv.1) {
                    at_most
                } else {
                    Some(&lv.1)
                }
            }
            (Some(lv), None) => Some(&lv.1),
            (None, _) => at_most,
        };
        let right = self.min_of_set_from(from * 2 + 2, in_set, use_cache, right_bound);

        match (left, right) {
            (l @ Some(lv), r @ Some(rv)) => {
                if lv.1 < rv.1 {
                    l
                } else {
                    r
                }
            }
            (None, Some(r)) => Some(r),
            (Some(l), None) => Some(l),
            (None, None) => None,
        }
    }

    fn min(&mut self) -> Option<&(usize, T)> {
        self.maybe_rebuild();
        if self.data.is_empty() {
            None
        } else {
            Some(&self.data[0])
        }
    }

    // up_to is inclusive
    unsafe fn push_up(&mut self, index: usize) {
        debug_assert!(!self.need_rebuild, "need_rebuild should be false");

        // TODO: std heap use a Hole struct to handle panic
        // in some thread local static panic
        let mut index = index;
        let ptr = self.data.as_mut_ptr();
        let copy = mem::ManuallyDrop::new(ptr::read(ptr.add(index)));
        while index > 0 {
            let parent_idx = (index - 1) / 2;
            let parent = self.data.get_unchecked(parent_idx);
            if parent.1 <= copy.1 {
                break;
            }
            ptr::copy_nonoverlapping(ptr.add(parent_idx), ptr.add(index), 1);
            self.update_index(parent.0, Some(index));
            index = parent_idx;
        }
        // may overlap if position unchanged
        ptr::copy(copy.deref(), ptr.add(index), 1);
        self.update_index(copy.0, Some(index));

        debug_assert!(self.test_struct(0, None), "push_up wrong");
    }

    unsafe fn push_down(&mut self, index: usize) {
        debug_assert!(!self.need_rebuild, "need_rebuild should be false");

        #[cfg(debug_assertions)]
        let index0 = index;
        // #[cfg(debug_assertions)]
        // println!("before push down {:?}", self.data);

        let mut index = index;
        let ptr = self.data.as_mut_ptr();
        let copy = mem::ManuallyDrop::new(ptr::read(ptr.add(index)));
        let end = self.data.len();
        let mut child = 2 * index + 1;

        // #[cfg(debug_assertions)]
        // println!("copy {copy:?}");

        while child < end {
            // compare with the greater of the two children
            let left_val = self.data.get_unchecked(child);
            // #[cfg(debug_assertions)]
            // println!("left val {left_val:?}");
            let val = if child + 1 < end {
                let right_val = self.data.get_unchecked(child + 1);
                // #[cfg(debug_assertions)]
                // println!("right val {right_val:?}");
                if right_val.1 < left_val.1 {
                    child += 1;
                    right_val
                } else {
                    left_val
                }
            } else {
                left_val
            };

            // #[cfg(debug_assertions)]
            // println!("val {val:?}");

            if copy.1 <= val.1 {
                break;
            }

            ptr::copy_nonoverlapping(ptr.add(child), ptr.add(index), 1);
            self.update_index(val.0, Some(index));
            index = child;
            child = 2 * index + 1;
        }

        // may overlap if position unchanged
        ptr::copy(copy.deref(), ptr.add(index), 1);
        self.update_index(copy.0, Some(index));

        // #[cfg(debug_assertions)]
        // println!("after push down {:?}", self.data);
        #[cfg(debug_assertions)]
        debug_assert!(self.test_struct(index0, None), "push_down wrong");
    }

    #[cfg(debug_assertions)]
    fn test_struct(&self, from: usize, parent: Option<&T>) -> bool {
        if from >= self.data.len() {
            return true;
        }

        let val = unsafe { self.data.get_unchecked(from) };
        assert!(unsafe { self.get_index(val.0) }.is_some_and(|i| i == from));

        if parent.is_some_and(|p| &val.1 < p) {
            return false;
        }

        let left = 2 * from + 1;
        let right = 2 * from + 2;
        self.test_struct(left, Some(&val.1)) && self.test_struct(right, Some(&val.1))
    }

    #[cfg(debug_assertions)]
    fn test_all(&self) {
        debug_assert!(self.test_struct(0, None), "invalid heap after rebuild");
        self.test_all_index();
    }

    #[cfg(debug_assertions)]
    fn test_all_index(&self) {
        for (key, idx) in self.k2i.iter().enumerate() {
            if let Some(i) = idx {
                assert_eq!(self.data[*i].0, key);
            }
        }
    }
}

impl Heap<i32> {
    fn increment(&mut self, index: usize) -> &mut Self {
        // change upward or downword based on delta
        todo!()
    }

    fn decrement(&mut self, index: usize) -> &mut Self {
        // change upward or downword based on delta
        todo!()
    }
}

mod test {
    use super::*;
    use std::collections::HashMap;

    const PIECE_RANGE: usize = 1432; // a random, not power of 2 length
    const VALUE_RANGE: u32 = 250; // a random, not power of 2 length

    fn make_n_values<const N: usize>() -> [(usize, u32); N] {
        [(0usize, 0u32); N].map(|_| {
            (
                rand::random_range(0..PIECE_RANGE),
                rand::random_range(0..VALUE_RANGE),
            )
        })
    }

    fn std_ans_fn(
        map: &mut HashMap<usize, u32>,
        in_set: impl Fn(usize) -> bool,
    ) -> Option<(usize, u32)> {
        map.iter()
            .filter(|(index, _)| in_set(**index))
            .min_by(|a, b| a.1.cmp(b.1))
            .map(|(a, b)| (*a, *b))
    }

    fn build_initial() -> (Heap<u32>, HashMap<usize, u32>) {
        const TOTAL: usize = 5134;
        let mut h = Heap::<u32>::new(PIECE_RANGE);
        let mut m = HashMap::<usize, u32>::new();

        let initial = make_n_values::<TOTAL>();

        initial.iter().for_each(|(k, v)| {
            m.insert(*k, *v);
        });
        h.insert_bulk(initial);
        (h, m)
    }

    #[test]
    fn test_build_heap_global_min() {
        let (mut h, mut m) = build_initial();
        let exp_min = std_ans_fn(&mut m, |_| true).expect("have").1;
        let (_, val) = h.min().expect("heap should have min");
        assert_eq!(exp_min, *val);

        h.test_all();
    }

    fn test_get_sets_min<const N_PIECES: usize, const N_PEERS: usize>(
        peers: &[[bool; N_PIECES]; N_PEERS],
        pre_ans: &[Option<usize>; N_PEERS],
        h: &mut Heap<u32>,
        m: &mut HashMap<usize, u32>,
    ) -> [Option<usize>; N_PEERS] {
        let mut ret = [None; N_PEERS];
        for i in 0..N_PEERS {
            let bitmap = peers[i];
            let in_set = |index| bitmap[index];
            let std_ans = std_ans_fn(m, in_set);
            let heap_ans = h.min_of_set(in_set, None, pre_ans[i]);
            assert!(
                heap_ans.is_none() && std_ans.is_none()
                    || heap_ans.is_some_and(|v| std_ans.is_some_and(|u| v.1 == u.1))
            );
            ret[i] = heap_ans.map(|(a, _)| *a);
        }
        ret
    }

    #[test]
    fn test_insert_or_update() {
        const N_OPS: usize = 351;
        const N_PEERS: usize = 104;

        let (mut h, mut m) = build_initial();

        // insert or update some values
        for (p, v) in make_n_values::<N_OPS>() {
            h.insert(p, v);
            m.insert(p, v);
        }

        // test ans
        let peers = rand::random::<[[bool; PIECE_RANGE]; N_PEERS]>();
        let ans = test_get_sets_min(&peers, &[None; N_PEERS], &mut h, &mut m);

        // insert bulk
        let vals = make_n_values::<N_OPS>();
        for (p, v) in vals {
            m.insert(p, v);
        }
        h.insert_bulk(vals);

        let ans = test_get_sets_min(&peers, &ans, &mut h, &mut m);
        // insert or update some values
        for (p, v) in make_n_values::<N_OPS>() {
            h.insert(p, v);
            m.insert(p, v);
        }

        test_get_sets_min(&peers, &ans, &mut h, &mut m);

        h.test_all();
    }

    #[test]
    fn test_delete() {
        const N_OPS: usize = 351;
        const N_PEERS: usize = 104;

        let (mut h, mut m) = build_initial();

        // delete some
        for (index, _) in make_n_values::<N_OPS>() {
            h.delete(index);
            m.remove(&index);
        }

        let peers = rand::random::<[[bool; PIECE_RANGE]; N_PEERS]>();
        let ans = test_get_sets_min(&peers, &[None; N_PEERS], &mut h, &mut m);

        // insert or update some values
        for (p, v) in make_n_values::<N_OPS>() {
            h.insert(p, v);
            m.insert(p, v);
        }
        let ans = test_get_sets_min(&peers, &ans, &mut h, &mut m);

        // insert bulk
        let vals = make_n_values::<N_OPS>();
        for (p, v) in vals {
            m.insert(p, v);
        }
        h.insert_bulk(vals);
        let ans = test_get_sets_min(&peers, &ans, &mut h, &mut m);

        // delete some
        for (index, _) in make_n_values::<N_OPS>() {
            h.delete(index);
            m.remove(&index);
        }

        // insert bulk
        let vals = make_n_values::<N_OPS>();
        for (p, v) in vals {
            m.insert(p, v);
        }
        h.insert_bulk(vals);
        // then delete some
        for (index, _) in make_n_values::<N_OPS>() {
            h.delete(index);
            m.remove(&index);
        }
        // then insert some again
        let vals = make_n_values::<N_OPS>();
        for (p, v) in vals {
            m.insert(p, v);
        }
        h.insert_bulk(vals);
        test_get_sets_min(&peers, &ans, &mut h, &mut m);

        h.test_all();
    }

    #[test]
    fn test_delete_rebuild() {
        const N_OPS: usize = 351;
        const N_PEERS: usize = 104;

        let (mut h, mut m) = build_initial();
        h.maybe_rebuild();

        // delete some
        for (index, _) in make_n_values::<N_OPS>() {
            h.delete(index);
            m.remove(&index);
        }

        let peers = rand::random::<[[bool; PIECE_RANGE]; N_PEERS]>();
        let ans = test_get_sets_min(&peers, &[None; N_PEERS], &mut h, &mut m);

        // insert or update some values
        for (p, v) in make_n_values::<N_OPS>() {
            h.insert(p, v);
            m.insert(p, v);
        }
        let ans = test_get_sets_min(&peers, &ans, &mut h, &mut m);

        // insert bulk
        let vals = make_n_values::<N_OPS>();
        for (p, v) in vals {
            m.insert(p, v);
        }
        h.insert_bulk(vals);
        let ans = test_get_sets_min(&peers, &ans, &mut h, &mut m);

        // delete some
        for (index, _) in make_n_values::<N_OPS>() {
            h.delete(index);
            m.remove(&index);
        }
        test_get_sets_min(&peers, &ans, &mut h, &mut m);

        h.test_all();
    }
}

// pub struct PiecePeerIterator {}
// impl Iterator for PiecePeerIterator {
//     type Item = PeerInfo;
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

// impl Peers {
//     pub fn new() -> Self {
//         Peers { peers: vec![] }
//     }

//     pub fn get_peers_all(&self) -> &Vec<PeerInfo> {
//         // TODO: maybe return iterator
//         &self.peers
//     }

//     pub fn get_peers_of_piece(&self, piece_index: u32) -> impl Iterator + use<'_> {
//         // TODO: maybe return iterator
//         self.peers
//             .iter()
//             .filter(move |pinfo| pinfo.have.get(piece_index))
//         // todo!()
//     }

//     pub fn add_peer(&mut self, peer: &SocketAddr, bitf: &BitField) {
//         todo!()
//     }

//     pub fn remove_peer(&mut self, peer: &SocketAddr) {
//         todo!()
//     }

//     pub fn get_rarest_piece(&self) -> u32 {
//         todo!()
//     }

//     pub fn peer_set_have(&mut self, peer: &SocketAddr) {
//         todo!()
//     }

//     pub fn iter(&self) -> impl Iterator + use<'_> {
//         self.peers.iter()
//     }

//     // TODO: may need some heuristic about choosing peers in iterators
//     // heuristics, say connected first, optimistic unchoke, etc
// }

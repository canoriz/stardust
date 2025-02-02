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

// pub struct PiecePicker {
//     heap: Heap,
// }

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
        if unsafe { self.k2i.get_unchecked(key) }.is_some() {
            self.update(key, val);
            return;
        }
        let idx = self.data.len();
        self.data.push((key, val));
        unsafe {
            // TODO: is this really needed? will push_up handles this?
            ptr::write(self.k2i.as_mut_ptr().add(key), Some(idx));
        };
        if !self.need_rebuild {
            unsafe { self.push_up(idx) };
        }
    }

    // rebuild heap in O(n), used when faster than insert m insert()
    pub fn insert_bulk<V: IntoIterator<Item = (usize, T)>>(&mut self, values: V) {
        self.need_rebuild = true;
        println!("agsd");
        for (k, v) in values {
            self.insert(k, v);
        }
        println!("ags1");
    }

    fn maybe_rebuild(&mut self) {
        if self.need_rebuild {
            self.need_rebuild = false;
            let mut i = self.data.len() >> 1;
            while i > 0 {
                i -= 1;
                unsafe { self.push_down(i) };
            }
        }
    }

    pub fn delete(&mut self, key: usize) -> Option<(usize, T)> {
        if key > self.max_capacity {
            return None;
        }
        if let Some(index) = unsafe { *self.k2i.get_unchecked_mut(key) } {
            return self.data.pop().map(|mut item| {
                if !self.data.is_empty() {
                    let to_delete = unsafe { self.data.get_unchecked_mut(index) };
                    unsafe {
                        ptr::write(self.k2i.as_mut_ptr().add(to_delete.0), None);
                        // TODO: is this really needed? will push_down handles this?
                        ptr::write(self.k2i.as_mut_ptr().add(item.0), Some(to_delete.0));
                    };
                    mem::swap(&mut item, to_delete);
                }
                if !self.need_rebuild {
                    unsafe { self.push_down(index) };
                }
                item
            });
        }
        None
    }

    // try to update an index, if key not exist in heap, do nothing and return None
    // if key exists in heap, update value and return previous value
    pub fn update(&mut self, key: usize, val: T) -> Option<T> {
        // change upward or downword based on delta
        if key > self.max_capacity {
            return None;
        }

        // TODO: optimize, maybe rebuild() then update()
        if self.need_rebuild {
            return self.update_rebuild(key, val);
        }

        if let Some(idx) = unsafe { *self.k2i.get_unchecked(key) } {
            // SAFETY: take prev out, doing a compare
            // swap prev with val,
            let (_, current) = unsafe { self.data.get_unchecked_mut(idx) };
            let prev = unsafe { ptr::read(current) };
            if val >= prev {
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
        if key > self.max_capacity {
            return None;
        }
        self.need_rebuild = true;
        if let Some(idx) = unsafe { *self.k2i.get_unchecked(key) } {
            let (_, current) = unsafe { self.data.get_unchecked_mut(idx) };
            return Some(mem::replace(current, val));
        }
        None
    }

    pub fn update_bulk<V: IntoIterator<Item = (usize, T)>>(&mut self, values: V) {
        for (k, v) in values.into_iter() {
            self.update_rebuild(k, v);
        }
    }

    // returns set number
    fn add_set_cache(&mut self) -> usize {
        todo!()
    }

    fn remove_set_cache(&mut self) -> usize {
        todo!()
    }

    // get min of a specific set
    // it takes a function indicates whether some node is in set
    pub fn min_of_set<F>(
        &mut self,
        in_set: F,
        use_cache: Option<usize>,
        at_most: Option<T>,
    ) -> Option<&(usize, T)>
    where
        F: Fn(usize) -> bool,
    {
        self.maybe_rebuild();
        self.min_of_set_from(0, &in_set, use_cache, &at_most)
    }

    fn min_of_set_from<F>(
        &self,
        from: usize,
        in_set: &F,
        use_cache: Option<usize>,
        at_most: &Option<T>,
    ) -> Option<&(usize, T)>
    where
        F: Fn(usize) -> bool,
    {
        println!("{}", from);
        if from >= self.data.len() {
            return None;
        }

        let val = unsafe { self.data.get_unchecked(from) };
        if in_set(val.0) {
            return Some(val);
        }

        if at_most.as_ref().is_some_and(|most| &(val.1) >= most) {
            None
        } else {
            let left = self.min_of_set_from(from * 2 + 1, in_set, use_cache, at_most);
            let right = self.min_of_set_from(from * 2 + 2, in_set, use_cache, at_most);
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
            ptr::write(self.k2i.as_mut_ptr().add(parent.0), Some(index));
            index = parent_idx;
        }
        // may overlap if position unchanged
        ptr::copy(copy.deref(), ptr.add(index), 1);
        ptr::write(self.k2i.as_mut_ptr().add(copy.0), Some(index));
    }

    unsafe fn push_down(&mut self, index: usize) {
        debug_assert!(!self.need_rebuild, "need_rebuild should be false");
        let mut index = index;
        let ptr = self.data.as_mut_ptr();
        let copy = mem::ManuallyDrop::new(ptr::read(ptr.add(index)));
        let end = self.data.len();
        let mut child = 2 * index + 1;

        while child < end {
            // compare with the greater of the two children
            let left_val = self.data.get_unchecked(child);
            let val = if child + 1 < end {
                let right_val = self.data.get_unchecked(child + 1);
                if right_val < left_val {
                    child += 1;
                    right_val
                } else {
                    left_val
                }
            } else {
                left_val
            };

            if copy.1 < val.1 {
                break;
            }

            ptr::copy(ptr.add(child), ptr.add(index), 1);
            ptr::write(self.k2i.as_mut_ptr().add(val.0), Some(index));
            index = child;
            child = 2 * index + 1;
        }

        // may overlap if position unchanged
        ptr::copy(copy.deref(), ptr.add(index), 1);
        ptr::write(self.k2i.as_mut_ptr().add(copy.0), Some(index));
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

    #[test]
    fn build_heap() {
        let mut a = Heap::new(13);
        a.insert(1, 512);
        a.insert(3, 513);
        // assert_eq!(Some(&(3, 513)), a.min_of_set(|b| b == 3, None, None));
        // assert_eq!(Some(&(1, 512)), a.min_of_set(|b| b == 1, None, None));
        a.insert_bulk([(6, 315), (1usize, 500), (2, 143), (8, 412)]);
        // assert_eq!(
        //     Some(&(6, 315)),
        //     a.min_of_set(|b| matches!(b, 1 | 6 | 8), None, None)
        // );
        a.delete(6);
        assert_eq!(
            Some(&(8, 412)),
            a.min_of_set(|b| matches!(b, 1 | 6 | 8), None, None)
        );
        a.update(1, 3);
        assert_eq!(
            Some(&(1, 3)),
            a.min_of_set(|b| matches!(b, 1 | 6 | 8), None, None)
        );
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

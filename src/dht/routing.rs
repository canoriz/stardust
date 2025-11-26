use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::time::Instant;

use super::{NodeAddr, NodeID};
const RANGE_MAX: usize = 160;
const K: usize = 8;

pub struct RoutingTable {
    /// ID of our own
    id: NodeID,

    /// bucket[i] stores nodes that have i common bits with self.id
    /// [160] is not used, since it's the Node ID of self.
    bucket: [Bucket; RANGE_MAX],
}

#[derive(Default)]
struct Bucket {
    inuse: HashMap<NodeAddr, Instant>,
    backup: VecDeque<(NodeAddr, Instant)>,
}

impl RoutingTable {
    pub fn new(id: NodeID) -> Self {
        Self {
            id,
            bucket: core::array::from_fn(|_| Bucket::default()),
        }
    }

    pub fn add(&mut self, addr: NodeAddr) {
        let prefix = common_bits(&self.id, &addr.id) as usize;
        if prefix < RANGE_MAX {
            let bucket = &mut self.bucket[prefix];
            let n = bucket.inuse.len();
            let t = Instant::now();
            if n < K {
                bucket.inuse.insert(addr, t);
            } else {
                while bucket.backup.len() > K {
                    // nodes at front will always be the early ones
                    bucket.backup.pop_front();
                }
                bucket.backup.push_back((addr, t));
            }
        }
    }

    pub fn remove(&mut self, addr: NodeAddr) {
        // TODO: give node some limit of times to fail?
        let prefix = common_bits(&self.id, &addr.id) as usize;
        if prefix < RANGE_MAX {
            let bucket = &mut self.bucket[prefix];
            bucket.inuse.remove(&addr);
            if bucket.inuse.len() < K {
                if let Some((node, t)) = bucket.backup.pop_back() {
                    bucket.inuse.insert(node, t);
                }
            }
        }
    }

    pub fn get_closest_nodes(&self, addr: NodeAddr, nodes: &mut Vec<NodeAddr>) {
        let prefix = common_bits(&self.id, &addr.id) as usize;
        nodes.clear();
        for bucket in &self.bucket[prefix..] {
            // example
            // 00001001001001010 ourself
            // 00100010100100110 target
            // 110............. diff
            // so all nodes starts with 00.. will be closest to target
            // and nodes starts with 00 falls in bucket[2]
            // nodes start with 01 falls in bucket[1], dist to target will be
            for (n, _) in &bucket.inuse {
                nodes.push(*n);
                if nodes.len() >= K {
                    break;
                }
            }
        }
        for bucket in self.bucket[..prefix].iter().rev() {
            // example
            // 00001001001001010 ourself
            // 00100010100100110 target
            // 110............. diff
            // so all nodes starts with 00.. will be closest to target
            // and nodes starts with 00 falls in bucket[2]
            // nodes start with 01 falls in bucket[1], dist to target will be
            for (n, _) in &bucket.inuse {
                nodes.push(*n);
                if nodes.len() >= K {
                    break;
                }
            }
        }
    }
}

fn dist(a: &NodeID, b: &NodeID) -> [u8; 20] {
    let mut ret = [0u8; 20];
    for (i, r) in ret.iter_mut().enumerate() {
        *r = a[i] ^ b[i];
    }
    ret
}

fn common_bits(a: &NodeID, b: &NodeID) -> u32 {
    let d = dist(a, b);
    let mut r = 0;
    for i in d {
        let l = i.leading_zeros();
        r += l;
        if l < u8::BITS {
            break;
        }
    }
    r
}

fn u32_to_id(a: [u32; 5]) -> NodeID {
    let mut ret = [0u8; 20];
    for i in 0..5 {
        let b = a[i].to_be_bytes();
        ret[i * 4] = b[0];
        ret[i * 4 + 1] = b[1];
        ret[i * 4 + 2] = b[2];
        ret[i * 4 + 3] = b[3];
    }
    ret
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_common_bits() {
        let a = [
            0x12, 0x34, 0x56, 0x78, 0x87, 0b00100100, 0x00, // all same from here
            0x21, 0x12, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43, 0x21, 0x12, 0x34, 0x56, 0x78,
        ];
        let b = [
            0x12, 0x34, 0x56, 0x78, 0x87, 0b01100100, 0x00, // all same from here
            0x21, 0x12, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43, 0x21, 0x12, 0x34, 0x56, 0x78,
        ];
        assert_eq!(common_bits(&a, &b), 41);

        let a = [0; 20];
        let b = [0xff; 20];
        assert_eq!(common_bits(&a, &b), 0);
    }

    #[test]
    fn u32_id() {
        let r = u32_to_id([0x12345678, 0x87654321, 0x12345678, 0x87654321, 0x12345678]);
        let exp = [
            0x12, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43, 0x21, 0x12, 0x34, 0x56, 0x78, 0x87, 0x65,
            0x43, 0x21, 0x12, 0x34, 0x56, 0x78,
        ];
        assert_eq!(r, exp);
    }
}

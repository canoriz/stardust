use std::collections::{BTreeSet, HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Instant;

use tracing::{debug, info};

use super::{NodeAddr, NodeID};
const BUCKET_MAX: usize = 160;
const K: usize = 8;

pub struct Dist {
    pub dist: NodeID,
    pub addr: NodeAddr,
}
impl core::cmp::Ord for Dist {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dist.cmp(&other.dist)
    }
}
impl core::cmp::PartialOrd for Dist {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.dist.partial_cmp(&other.dist)
    }
}
impl core::cmp::PartialEq for Dist {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}
impl core::cmp::Eq for Dist {}

pub struct RoutingTable {
    /// ID of our own
    id: NodeID,

    /// bucket[i] stores nodes that have i common bits with self.id
    /// [160] is not used, since it's the Node ID of self.
    bucket: [Bucket; BUCKET_MAX + 1],
}

type ContactInfo = (SocketAddr, Instant);

#[derive(Default)]
struct Bucket {
    inuse: HashMap<NodeID, ContactInfo>,
    backup: VecDeque<(NodeID, ContactInfo)>,
}

impl RoutingTable {
    pub fn new(id: NodeID) -> Self {
        Self {
            id,
            bucket: core::array::from_fn(|_| Bucket::default()),
        }
    }

    pub fn add_route(&mut self, addr: NodeAddr) {
        let prefix = common_bits(&self.id, &addr.id) as usize;
        let bucket = &mut self.bucket[prefix];
        let contact_info = (addr.addr, Instant::now());
        if bucket.inuse.len() < K {
            bucket.inuse.insert(addr.id, contact_info);
            debug!("dht routing: add route to bucket {:?}", addr);
        } else {
            while bucket.backup.len() > K {
                // nodes at front will always be the early ones
                bucket.backup.pop_front();
            }
            bucket.backup.push_back((addr.id, contact_info));
            debug!("dht routing: add route to backup {:?}", addr);
        }
    }

    pub fn remove_route(&mut self, id: &NodeID) {
        // TODO: give node some limit of times to fail?
        debug!("dht routing: remove route to {:?}", id);
        let prefix = common_bits(&self.id, id) as usize;
        let bucket = &mut self.bucket[prefix];
        bucket.inuse.remove(id);
        if bucket.inuse.len() < K {
            if let Some((node, t)) = bucket.backup.pop_back() {
                bucket.inuse.insert(node, t);
                debug!("dht routing: add route from backup {:?}", node);
            }
        }
    }

    /// get closest nodes to NodeID, append K closest nodes to nodes vector
    pub fn get_k_closest_nodes(&self, id: &NodeID, k: usize, nodes: &mut Vec<NodeAddr>) {
        // For every node N in bucket i, N and our id, they have i common leading bits.
        // target ^ N.id = (target ^ self.id) ^ (self.id ^ N.id)
        // Because (self.id ^ N.id) is in bucket i, (self.id ^ N.id) has i leading 0 bits
        // the possible range of (target ^ N.id) can be determined
        // t = (target ^ N.id)
        // t ^ (self.id ^ N.id) will range in
        // bits
        // 0   1   .. i   i+1        i+2  i+3  i+4 ..  159
        // -----------------------------------------------
        // t0  t1  .. ti  t_(i+1)^1  0    0    0   ..  0
        // -----------------------------------------------
        // to
        // t0  t1  .. ti  t_(i+1)^1  1    1    1   ..  1
        let t = dist(id, &self.id);
        let mut closest: BTreeSet<Dist> = BTreeSet::new();

        let find_closer = |j: i32, bucket: &Bucket, closest: &mut BTreeSet<Dist>| {
            // mask_upper = (1 << j) - 1
            // mask_lower = ~mask_upper
            let mask_upper = j_ending_ones(j); // 00011111 j ones;
            let mask_lower = bitwise_not(mask_upper); // 11100000 j zeros
            let xor_mask = j_one_only(j);
            // upper = (t ^ xor_mask) | mask_upper
            // lower = (t ^ xor_mask) & mask_lower;
            use core::ops::{BitAnd, BitXor};
            let tmp = bitwise_op(t, xor_mask, u8::bitxor);
            // let upper = bitwise_op(tmp, mask_upper, u8::bitor);
            let lower = bitwise_op(tmp, mask_lower, u8::bitand);

            let add_nodes = if let Some(Dist { dist, .. }) = closest.last() {
                // only add nodes closest have less than k node inside
                // of if this bucket's possible minimum distance is lesser
                // than closest have.
                lower < *dist || closest.len() < k
            } else {
                true
            };

            if add_nodes {
                // only add nodes if this bucket may contains closer nodes
                for (nid, (addr, _)) in bucket.inuse.iter() {
                    closest.insert(Dist {
                        dist: dist(id, nid),
                        addr: NodeAddr {
                            id: *nid,
                            addr: *addr,
                        },
                    });
                }
                while closest.len() > k {
                    closest.pop_last();
                }
            }
        };

        let prefix = common_bits(&self.id, &id) as usize;

        // iterating from bucket [prefix] to END
        for (mut i, bucket) in self.bucket[prefix..BUCKET_MAX].iter().enumerate() {
            i = i + prefix; // bucket i
            let j = BUCKET_MAX - i;
            find_closer(j as i32, bucket, &mut closest);
        }
        // then iterating from prefix down to 0
        for (i, bucket) in self.bucket[..prefix].iter().enumerate() {
            let j = BUCKET_MAX - i;
            find_closer(j as i32, bucket, &mut closest);
        }
        *nodes = closest.into_iter().map(|Dist { addr, .. }| addr).collect()
    }
}

pub(crate) fn dist(a: &NodeID, b: &NodeID) -> [u8; 20] {
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

/// simulating (1 << j) - 1; // 00011111 j ones;
fn j_ending_ones(mut j: i32) -> [u8; 20] {
    let mut ret = [0u8; 20];
    for n in ret.iter_mut().rev() {
        match j {
            ..=0 => break,
            1..=7 => {
                *n = ((1u32 << j) - 1) as u8;
                j -= 8;
            }
            8.. => {
                *n = !0u8;
                j -= 8;
            }
        }
    }
    ret
}

/// simulating 1 << j;
fn j_one_only(mut j: i32) -> [u8; 20] {
    let mut ret = [0u8; 20];
    for n in ret.iter_mut().rev() {
        match j {
            ..=0 => break,
            1..=8 => {
                *n = 1 << (j - 1);
                j -= 8;
            }
            _ => j -= 8,
        }
    }
    ret
}

fn bitwise_op<F>(mut a: [u8; 20], b: [u8; 20], op: F) -> [u8; 20]
where
    F: Fn(u8, u8) -> u8,
{
    for i in 0..20 {
        a[i] = op(a[i], b[i]);
    }
    a
}

fn bitwise_not(mut a: [u8; 20]) -> [u8; 20] {
    for i in 0..20 {
        a[i] = !a[i];
    }
    a
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
    fn test_j_ending_ones() {
        let a = j_ending_ones(0);
        let exp = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(a, exp);

        let a = j_ending_ones(80);
        let exp = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        assert_eq!(a, exp);

        let a = j_ending_ones(160);
        let exp = [
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        assert_eq!(a, exp);

        let a = j_ending_ones(19);
        let exp = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x7, 0xff, 0xff,
        ];
        assert_eq!(a, exp);
    }

    #[test]
    fn test_j_one_only() {
        let a = j_one_only(0);
        let exp = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(a, exp);

        let a = j_one_only(27);
        let exp = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        ];
        assert_eq!(a, exp);

        let a = j_one_only(160);
        let exp = [
            0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(a, exp);
    }

    #[test]
    fn test_bin_op() {
        let a = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let b = [
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ];
        let c = bitwise_op(a, b, |x, y| x & y);
        assert_eq!(a, c);
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

    #[test]
    fn u32_id() {
        let r = u32_to_id([0x12345678, 0x87654321, 0x12345678, 0x87654321, 0x12345678]);
        let exp = [
            0x12, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43, 0x21, 0x12, 0x34, 0x56, 0x78, 0x87, 0x65,
            0x43, 0x21, 0x12, 0x34, 0x56, 0x78,
        ];
        assert_eq!(r, exp);
    }

    fn node_id(id: u32) -> NodeID {
        u32_to_id([0, 0, 0, 0, id])
    }

    fn node_addr(id: NodeID) -> NodeAddr {
        let fixed_sock = "0.0.0.0:0".parse().unwrap();
        NodeAddr {
            id,
            addr: fixed_sock,
        }
    }

    /// test route table in a 8bit(256) range
    #[test]
    fn test_get_k_route() {
        let id = node_id(73);
        let mut rt = RoutingTable::new(id);
        for i in (0..=255).filter(|x| *x != 73) {
            rt.add_route(node_addr(node_id(i)));
        }

        fn k_closest(rt: &mut RoutingTable, target: &NodeID, k: usize) -> Vec<u32> {
            let mut ret = vec![];
            rt.get_k_closest_nodes(&target, k, &mut ret);
            ret.into_iter()
                .map(|x| u32::from_be_bytes([x.id[16], x.id[17], x.id[18], x.id[19]]))
                .collect()
        }

        let mut test_f = |target: u32, k: usize, exp: Vec<u32>| {
            let v = k_closest(&mut rt, &node_id(target), k);
            assert_eq!(v, exp, "self 73, target {target} k {k}");
        };
        let tcase = [
            (9, 3, vec![1, 0, 3]),
            (9, 9, vec![1, 0, 3, 2, 5, 4, 7, 6, 72]),
            (5, 9, vec![5, 4, 7, 6, 1, 0, 3, 2, 69]),
        ];
        for (t, k, exp) in tcase {
            test_f(t, k, exp);
        }
    }

    #[test]
    fn test_remove_route() {
        let id = node_id(113);
        let mut rt = RoutingTable::new(id);
        let node_ids = [
            38, 39, 40, 41, 42, 43, 44, 45, // <- in bucket
            46, 47, // <- in backup
            95, 77, 194, 166,
        ]
        .map(|i| node_id(i));
        for id in &node_ids {
            rt.add_route(node_addr(*id));
        }

        {
            let mut r: Vec<_> = rt.bucket[160 - 8 + 1].inuse.keys().map(|x| *x).collect();
            r.sort();
            assert_eq!(
                r,
                vec![38, 39, 40, 41, 42, 43, 44, 45]
                    .into_iter()
                    .map(|x| node_id(x))
                    .collect::<Vec<_>>()
            )
        }

        rt.remove_route(&node_id(41));
        {
            let mut r: Vec<_> = rt.bucket[160 - 8 + 1].inuse.keys().map(|x| *x).collect();
            r.sort();
            assert_eq!(
                r,
                vec![38, 39, 40, 42, 43, 44, 45, 47]
                    .into_iter()
                    .map(|x| node_id(x))
                    .collect::<Vec<_>>()
            )
        }

        rt.remove_route(&node_id(39));
        {
            let mut r: Vec<_> = rt.bucket[160 - 8 + 1].inuse.keys().map(|x| *x).collect();
            r.sort();
            assert_eq!(
                r,
                vec![38, 40, 42, 43, 44, 45, 46, 47]
                    .into_iter()
                    .map(|x| node_id(x))
                    .collect::<Vec<_>>()
            )
        }

        rt.remove_route(&node_id(42));
        {
            let mut r: Vec<_> = rt.bucket[160 - 8 + 1].inuse.keys().map(|x| *x).collect();
            r.sort();
            assert_eq!(
                r,
                vec![38, 40, 43, 44, 45, 46, 47]
                    .into_iter()
                    .map(|x| node_id(x))
                    .collect::<Vec<_>>()
            )
        }
    }
}

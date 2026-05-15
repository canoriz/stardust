use std::collections::{BTreeSet, HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Instant;

use tracing::debug;

use super::{NodeAddr, NodeID};
const BUCKET_MAX: usize = 160;
const K: usize = 8;
const BACKUP_MAX: usize = K;

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

struct NodeEntry {
    addr: SocketAddr,
    last_seen: Instant,
    /// True once this node has replied to one of our outgoing requests.
    reachable: bool,
}

/// Per-bucket state.
///
/// Backup lists are separate for reachable and unreachable nodes so that
/// when a slot opens up we always promote a reachable node first.
#[derive(Default)]
struct Bucket {
    inuse: HashMap<NodeID, NodeEntry>,
    // reachable_backup is for nodes that have replied to us,
    reachable_backup: VecDeque<(NodeID, SocketAddr, Instant)>,
    // unreachable_backup is for nodes that have never replied to us,
    // maybe they are unreachable, or maybe we never requested them
    unreachable_backup: VecDeque<(NodeID, SocketAddr, Instant)>,
}

impl RoutingTable {
    pub fn new(id: NodeID) -> Self {
        Self {
            id,
            bucket: core::array::from_fn(|_| Bucket::default()),
        }
    }

    /// Collect every node currently tracked (inuse + both backup queues).
    /// Returns `(node_id, socket_addr)` pairs; all will be added as unreachable
    /// on restore so the DHT re-probes them.
    pub fn all_nodes(&self) -> Vec<(NodeID, SocketAddr)> {
        let mut out = Vec::new();
        for bucket in &self.bucket {
            for (id, entry) in &bucket.inuse {
                out.push((*id, entry.addr));
            }
            for (id, addr, _) in &bucket.reachable_backup {
                out.push((*id, *addr));
            }
            for (id, addr, _) in &bucket.unreachable_backup {
                out.push((*id, *addr));
            }
        }
        out
    }

    /// `false`.
    pub fn add_route(&mut self, addr: NodeAddr, reachable: bool) {
        let prefix = common_bits(&self.id, &addr.id) as usize;
        let bucket = &mut self.bucket[prefix];

        // Already in inuse: refresh, never downgrade reachability.
        if let Some(e) = bucket.inuse.get_mut(&addr.id) {
            e.addr = addr.addr;
            e.last_seen = Instant::now();
            e.reachable |= reachable;
            return;
        }

        // Slot available: insert directly.
        if bucket.inuse.len() < K {
            bucket.inuse.insert(
                addr.id,
                NodeEntry {
                    addr: addr.addr,
                    last_seen: Instant::now(),
                    reachable,
                },
            );
            debug!("dht routing: add route to bucket {:?}", addr);
            return;
        }

        if reachable {
            let replace = bucket.inuse.iter().filter(|(_, v)| !v.reachable).next();
            if let Some((ur, _)) = replace {
                let ur = ur.clone();
                bucket.inuse.remove(&ur);
                bucket.inuse.insert(
                    addr.id,
                    NodeEntry {
                        addr: addr.addr,
                        last_seen: Instant::now(),
                        reachable,
                    },
                );
                debug!("dht routing: add route to bucket {:?}", addr);
            }
        }

        // Bucket full: upsert in backup, never downgrading reachability.
        // Removing the old entry first prevents duplicates and merges the upgrade path.
        let was_reachable = Self::backup_remove(bucket, &addr.id);
        let effective = reachable || was_reachable.unwrap_or(false);
        let queue = if effective {
            &mut bucket.reachable_backup
        } else {
            &mut bucket.unreachable_backup
        };
        if queue.len() >= BACKUP_MAX {
            queue.pop_front();
        }
        queue.push_back((addr.id, addr.addr, Instant::now()));
        debug!(
            "dht routing: add route to backup {:?} reachable={}",
            addr, effective
        );
    }

    /// Remove a node from the backup queues.
    /// Returns `Some(true)` if it was in `reachable_backup`,
    /// `Some(false)` if in `unreachable_backup`, `None` if absent.
    fn backup_remove(bucket: &mut Bucket, id: &NodeID) -> Option<bool> {
        if let Some(pos) = bucket
            .unreachable_backup
            .iter()
            .position(|(nid, _, _)| nid == id)
        {
            bucket.unreachable_backup.remove(pos);
            return Some(false);
        }
        if let Some(pos) = bucket
            .reachable_backup
            .iter()
            .position(|(nid, _, _)| nid == id)
        {
            bucket.reachable_backup.remove(pos);
            return Some(true);
        }
        None
    }

    pub fn remove_route(&mut self, id: &NodeID) {
        // TODO: give node some limit of times to fail?
        debug!("dht routing: remove route to {:?}", id);
        let prefix = common_bits(&self.id, id) as usize;
        let bucket = &mut self.bucket[prefix];
        bucket.inuse.remove(id);
        if bucket.inuse.len() < K {
            // Prefer a previously-reachable node over an untested one.
            if let Some((node, addr, ts)) = bucket.reachable_backup.pop_back() {
                bucket.inuse.insert(
                    node,
                    NodeEntry {
                        addr,
                        last_seen: ts,
                        reachable: true,
                    },
                );
                debug!("dht routing: promoted reachable backup {:?}", node);
            } else if let Some((node, addr, ts)) = bucket.unreachable_backup.pop_back() {
                bucket.inuse.insert(
                    node,
                    NodeEntry {
                        addr,
                        last_seen: ts,
                        reachable: false,
                    },
                );
                debug!("dht routing: promoted unreachable backup {:?}", node);
            }
        }
    }

    /// get closest nodes to NodeID, append K closest nodes to nodes vector
    pub fn get_k_closest_nodes(&self, id: &NodeID, k: usize, nodes: &mut Vec<NodeAddr>) {
        // For every node N in bucket i, N and our id, they have i common leading bits.
        // target ^ N.id = (target ^ self.id) ^ (self.id ^ N.id)
        // Because (self.id ^ N.id) is in bucket i, (self.id ^ N.id) has i leading 0 bits
        // the possible range of (target ^ N.id) can be determined
        // t = (target ^ self.id)
        // target ^ N.id = t ^ (self.id ^ N.id) will range in
        // (all bit indices are 0-based from MSB)
        // bit:  0     1    ..  i-1      i        i+1  i+2  ..  159
        // -------------------------------------------------------
        // min:  t[0]  t[1] .. t[i-1]   t[i]^1    0    0   ..  0
        // -------------------------------------------------------
        // max:  t[0]  t[1] .. t[i-1]   t[i]^1    1    1   ..  1
        //
        // t[0..i-1] are fixed (same as in t) because d[0..i-1]=0.
        // t[i]^1 is fixed because we are at bucket[i], all node in bucket[i]
        // must haved[i]=1 (the first differing bit), otherwise they would not be in this bucket.
        // bits i+1..159 are free (d[i+1..159] can be anything).
        let t = dist(id, &self.id);
        let mut closest: BTreeSet<Dist> = BTreeSet::new();

        let find_closer =
            |common_prefix_with_self: usize, bucket: &Bucket, closest: &mut BTreeSet<Dist>| {
                let j = (BUCKET_MAX - common_prefix_with_self) as i32;
                // For bucket[i], nodes have d = dist(node, self.id) where d[0..i-1]=0 and d[i]=1.
                // So target XOR node = t XOR d, where:
                //   xor_mask  = bit i from MSB (= shift_left_by(j), since j-1 from LSB = bit i from MSB)
                //   upper_mask = bits i+1..159 from MSB (the unconstrained bits, j-1 ones from LSB)
                //   lower_mask = bits 0..i from MSB (the i+1 constrained bits)
                //
                //   lower = (t ^ xor_mask) & lower_mask = [t[0..i-1], t[i]^1, 0..0]
                //   upper = (t ^ xor_mask) | upper_mask = [t[0..i-1], t[i]^1, 1..1]
                let xor_mask = one_at_j_bit_only(j - 1); // bit i from MSB
                let upper_mask = j_ending_ones(j - 1); // bits i+1..159 from MSB
                let lower_mask = bitwise_not(upper_mask); // bits 0..i from MSB
                use core::ops::{BitAnd, BitXor};
                let tmp = bitwise_op(t, xor_mask, u8::bitxor);
                let lower = bitwise_op(tmp, lower_mask, u8::bitand);
                // let upper = bitwise_op(tmp, upper_mask, u8::bitor);

                let add_nodes = if let Some(Dist { dist, .. }) = closest.last() {
                    // only add nodes when closest have less than k node inside
                    // or
                    // if this bucket's possible minimum distance is lesser
                    // than closest have.
                    lower < *dist || closest.len() < k
                } else {
                    true
                };

                if add_nodes {
                    // only add nodes if this bucket may contains closer nodes
                    for (nid, entry) in bucket.inuse.iter() {
                        closest.insert(Dist {
                            dist: dist(id, nid),
                            addr: NodeAddr {
                                id: *nid,
                                addr: entry.addr,
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
        for (i, bucket) in self.bucket[prefix..BUCKET_MAX].iter().enumerate() {
            find_closer(i + prefix, bucket, &mut closest);
        }
        // then iterating from prefix down to 0
        for (i, bucket) in self.bucket[..prefix].iter().enumerate().rev() {
            find_closer(i, bucket, &mut closest);
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

/// place a 1 at bit j (0-based from LSB)
fn one_at_j_bit_only(mut j: i32) -> [u8; 20] {
    let mut ret = [0u8; 20];
    for n in ret.iter_mut().rev() {
        match j {
            ..0 => break,
            0..8 => {
                *n = 1 << j;
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
        let a = one_at_j_bit_only(0);
        let exp = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        ];
        assert_eq!(a, exp);

        let a = one_at_j_bit_only(27);
        let exp = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
        ];
        assert_eq!(a, exp);

        let a = one_at_j_bit_only(159);
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
            rt.add_route(node_addr(node_id(i)), false);
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
            rt.add_route(node_addr(*id), false);
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

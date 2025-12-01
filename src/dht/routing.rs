use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Instant;

use tracing::info;

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
        if prefix < RANGE_MAX {
            let bucket = &mut self.bucket[prefix];
            let n = bucket.inuse.len();
            let contact_info = (addr.addr, Instant::now());
            if n < K {
                bucket.inuse.insert(addr.id, contact_info);
                info!("dht routing: add route to bucket {:?}", addr);
            } else {
                while bucket.backup.len() > K {
                    // nodes at front will always be the early ones
                    bucket.backup.pop_front();
                }
                bucket.backup.push_back((addr.id, contact_info));
                info!("dht routing: add route to backup {:?}", addr);
            }
        }
    }

    pub fn remove_route(&mut self, id: &NodeID) {
        // TODO: give node some limit of times to fail?
        info!("dht routing: remove route to {:?}", id);
        let prefix = common_bits(&self.id, id) as usize;
        if prefix < RANGE_MAX {
            let bucket = &mut self.bucket[prefix];
            bucket.inuse.remove(id);
            if bucket.inuse.len() < K {
                if let Some((node, t)) = bucket.backup.pop_back() {
                    bucket.inuse.insert(node, t);
                    info!("dht routing: add route from backup {:?}", node);
                }
            }
        }
    }

    /// get closest nodes to NodeID, append K closest nodes to nodes vector
    pub fn get_k_closest_nodes(&self, id: &NodeID, k: usize, nodes: &mut Vec<NodeAddr>) {
        let prefix = common_bits(&self.id, &id) as usize;
        for bucket in &self.bucket[prefix..] {
            // example
            // 00001001001001010 ourself
            // 00100010100100110 target
            // 110............. diff
            // so all nodes starts with 00.. will be closest to target
            // and nodes starts with 00 falls in bucket[2]
            // nodes start with 01 falls in bucket[1], dist to target will be
            let mut sorted: Vec<_> = bucket
                .inuse
                .iter()
                .filter_map(|(x, (a, _))| (x != id).then_some((x, a)))
                .collect();
            sorted.sort_by_key(|(x, _)| dist(id, x));
            for (nid, addr) in sorted {
                nodes.push(NodeAddr {
                    id: *nid,
                    addr: *addr,
                });
                if nodes.len() >= k {
                    return;
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
            let mut sorted: Vec<_> = bucket
                .inuse
                .iter()
                .filter_map(|(x, (a, _))| (x != id).then_some((x, a)))
                .collect();
            sorted.sort_by_key(|(x, _)| dist(id, x));
            for (nid, addr) in sorted {
                nodes.push(NodeAddr {
                    id: *nid,
                    addr: *addr,
                });
                if nodes.len() >= k {
                    return;
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

    fn node_id(id: u8) -> NodeID {
        u32_to_id([0, 0, 0, 0, id as u32])
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
        let id = node_id(113);
        let mut rt = RoutingTable::new(id);
        let node_ids = [38, 95, 77, 194, 166].map(|i| node_id(i));
        for id in &node_ids {
            rt.add_route(node_addr(*id));
        }

        fn k_closest(rt: &mut RoutingTable, id: &NodeID, k: usize) -> Vec<NodeAddr> {
            let mut ret = vec![];
            rt.get_k_closest_nodes(&id, k, &mut ret);
            ret
        }
        let v = k_closest(&mut rt, &id, 4);
        println!("{:?}", v);
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

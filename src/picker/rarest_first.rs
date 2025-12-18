use super::{PeerAddr, PeerPieceDetail, PieceMap, PiecePicker, PieceState};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::{math_helper::piece_total_and_last_size, picker::BitField};

type PieceIndex = u32;
type Rarity = i32;

#[derive(Debug)]
pub struct Picker {
    /// total piece number
    n: usize,

    /// what pieces peers have
    peers: HashMap<PeerAddr, PeerPieceDetail>,

    // rarity maps index to rarity
    rarity: BTreeMap<PieceIndex, Rarity>,

    // rarity of pieces we want
    want_rarity: BTreeSet<(Rarity, PieceIndex)>,

    /// which piece we selected we want
    selected: BitField,

    /// which piece we think we have
    have: BitField,

    /// how many pieces not any peer have?
    selected_not_have: BitField,
}

impl Picker {
    pub fn new(total_size: usize, piece_size: usize) -> Self {
        let (n, _) = piece_total_and_last_size(total_size, piece_size);
        let mut rarity = BTreeMap::new();
        for i in 0..n {
            rarity.insert(i as u32, 0);
        }

        Self {
            n,
            peers: HashMap::new(),
            rarity,
            want_rarity: BTreeSet::new(),
            selected: BitField::with_bit_len(n),
            have: BitField::with_bit_len(n),
            selected_not_have: BitField::with_bit_len(n),
        }
    }

    /// update one piece's rarity, add/minus 1 rarity of piece index
    fn update_one_piece_rarity(&mut self, index: u32, add: bool) {
        let diff = if add { 1 } else { -1 };

        let v = self
            .rarity
            .get_mut(&index)
            .expect("index should exist in rarity");
        self.want_rarity.remove(&(*v, index));
        *v += diff;
        assert!(*v >= 0);

        let selected = self.selected.get(index);
        let have = self.have.get(index);
        self.selected_not_have.set(index, selected && !have);

        match (selected, have) {
            (true, false) => {
                match *v {
                    0 => {
                        // no one have this piece
                        self.selected_not_have.set(index, true);
                    }
                    _ => {
                        self.want_rarity.insert((*v, index));
                    }
                }
            }
            _ => {}
        }
    }

    fn update_rarity(&mut self, state: &PeerPieceDetail, join: bool) {
        match &state.have {
            PieceState::HaveAll => {
                for i in 0..self.n {
                    self.update_one_piece_rarity(i as u32, join);
                }
            }
            PieceState::HaveNone => {}
            PieceState::Bitfield(b) => {
                for (i, have) in b.iter().enumerate().take(self.n) {
                    if have {
                        self.update_one_piece_rarity(i as u32, join);
                    }
                }
            }
        }
    }
}

impl From<PieceState> for PeerPieceDetail {
    fn from(value: PieceState) -> Self {
        Self {
            have: value,
            choke: false,
        }
    }
}

impl PiecePicker for Picker {
    type T = PeerPieceDetail;
    fn peer_add(&mut self, addr: PeerAddr, state: PieceState) {
        self.peer_leave(&addr);
        if !self.peers.contains_key(&addr) {
            let d = state.into();
            self.update_rarity(&d, true);
            self.peers.insert(addr, d);
        }
    }

    fn peer_leave(&mut self, addr: &PeerAddr) {
        if let Some(state) = self.peers.remove(addr) {
            self.update_rarity(&state, false);
        }
    }

    fn peer_choke(&mut self, addr: &PeerAddr) {
        if let Some(state) = self.peers.get_mut(addr) {
            state.choke = true;
        }
    }

    fn peer_unchoke(&mut self, addr: &PeerAddr) {
        if let Some(state) = self.peers.get_mut(addr) {
            state.choke = false;
        }
    }

    fn peer_new_have(&mut self, addr: &PeerAddr, index: u32) {
        if index as usize >= self.n {
            return;
        }

        if let Some(d) = self.peers.get_mut(addr) {
            d.have.set_have(self.n, index, true);
        } else {
            let mut have = PieceState::HaveNone;
            have.set_have(self.n, index, true);
            self.peers
                .insert(*addr, PeerPieceDetail { have, choke: false });
        }
    }

    fn peer_detail(&mut self, addr: &PeerAddr) -> Option<&PeerPieceDetail> {
        self.peers.get(addr)
    }

    fn select(&mut self, index: u32, selected: bool) {
        self.selected.set(index, selected);

        // the rarity of index
        let r = self
            .rarity
            .get(&index)
            .expect("index should be valid and exist in rarity");

        let have = self.have.get(index);
        self.selected_not_have.set(index, selected && !have);

        match (selected, have) {
            (true, true) => {}
            (true, false) => {
                if *r > 0 {
                    self.want_rarity.insert((*r, index));
                }
            }
            (false, _) => {
                self.want_rarity.remove(&(*r, index));
            }
        }
    }

    fn set_have(&mut self, index: u32, have: bool) {
        self.have.set(index, have);
        let selected = self.selected.get(index);
        self.selected_not_have.set(index, selected && !have);

        match (selected, have) {
            (true, true) => {
                if let Some(r) = self.rarity.get(&index) {
                    self.want_rarity.remove(&(*r, index));
                }
            }
            (true, false) => {
                if let Some(r) = self.rarity.get(&index) {
                    self.want_rarity.insert((*r, index));
                }
            }
            (false, true) => {}
            (false, false) => {}
        }
    }

    fn pick_next(&mut self, peer: &PeerAddr) -> Option<u32> {
        let peer_status = if let Some(h) = self.peers.get(peer) {
            h
        } else {
            return None;
        };

        if peer_status.choke {
            return None;
        }

        let mut found = None;

        // find the first piece this peer have
        for (r, index) in self.want_rarity.iter() {
            assert_eq!(self.have.get(*index), false);
            if peer_status.have(*index) {
                let selected = self.selected.get(*index);
                assert!(selected);
                found = Some((*r, *index));
                break;
            }
        }

        if let Some((_rarity, index)) = found {
            self.set_have(index, true);
            Some(index)
        } else {
            None
        }
    }

    #[inline]
    fn is_finished(&mut self) -> bool {
        self.selected_not_have.count_ones() == 0
    }

    fn selected_pieces(&self) -> &BitField {
        &self.selected
    }

    fn have(&self, index: u32) -> bool {
        self.have.get(index)
    }

    fn dump(&mut self) -> PieceMap {
        PieceMap {
            selected: self.selected.clone(),
            have: self.have.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    const PEER1: PeerAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1);
    const PEER2: PeerAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2);
    const PEER3: PeerAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3);

    fn init() -> Picker {
        // 6 pieces, last piece length 1
        let mut p = Picker::new(11, 2);

        p.peer_add(PEER1, PieceState::HaveAll);
        assert_eq!(Vec::from_iter(p.rarity.clone().into_values()), vec![1; 6]);
        p.peer_add(
            PEER2,
            PieceState::Bitfield(BitField::from(vec![true, true, false, true, true, false])),
        );
        assert_eq!(
            Vec::from_iter(p.rarity.clone().into_values()),
            vec![2, 2, 1, 2, 2, 1]
        );
        p
    }

    #[test]
    fn test_set_have() {
        let mut p = init();
        let selected = BitField::from(vec![true, false, true, true, false, false]);
        for (i, w) in selected.iter().enumerate().take(p.n) {
            p.select(i as u32, w);
        }

        assert_eq!(p.selected, selected);

        assert_eq!(p.pick_next(&PEER1), Some(2));

        // after pick, 2 should not in want_rarity
        assert!(p
            .want_rarity
            .iter()
            .fold(true, |acc, (_, idx)| acc && *idx != 2));

        p.set_have(2, false);
        assert_eq!(p.have.bitfield_bytes(), &[0]);
    }

    #[test]
    fn test_peer_leave() {
        let mut p = init();
        p.peer_add(
            PEER3,
            PieceState::Bitfield(BitField::from(vec![
                true, false, false, true, true, true, true,
            ])),
        );

        let selected = BitField::from(vec![true, true, true, true, false, false]);
        for (i, w) in selected.iter().enumerate().take(p.n) {
            p.select(i as u32, w);
        }

        assert_eq!(p.pick_next(&PEER1), Some(2));
        assert_eq!(p.pick_next(&PEER1), Some(1));
        p.set_have(2, false);
        p.set_have(1, false);
        p.peer_leave(&PEER2);
        assert_eq!(p.pick_next(&PEER1), Some(1));
    }

    #[test]
    fn test_add_selected() {
        let mut p = init();
        let selected = BitField::from(vec![true, true, true, true, false, false]);
        for (i, w) in selected.iter().enumerate().take(p.n) {
            p.select(i as u32, w);
        }

        assert_eq!(p.pick_next(&PEER1), Some(2));
        p.select(5, true);
        p.select(4, true);
        assert_eq!(p.pick_next(&PEER1), Some(5));
    }

    #[test]
    fn test_add_peer() {
        let mut p = init();
        let selected = BitField::from(vec![true, true, true, true, false, false]);
        for (i, w) in selected.iter().enumerate().take(p.n) {
            p.select(i as u32, w);
        }

        assert_eq!(p.pick_next(&PEER1), Some(2));
        p.peer_add(
            PEER3,
            PieceState::Bitfield(BitField::from(vec![true, true, true, true, false, true])),
        );
        p.select(4, true);
        p.select(5, true);
        assert_eq!(p.pick_next(&PEER1), Some(4));
    }

    #[test]
    fn test_peer_have() {
        let mut p = init();
        let selected = BitField::from(vec![true, true, true, true, true, false]);
        for (i, w) in selected.iter().enumerate().take(p.n) {
            p.select(i as u32, w);
        }

        assert_eq!(p.pick_next(&PEER1), Some(2));
        p.peer_add(
            PEER3,
            PieceState::Bitfield(BitField::from(vec![true, true, true, true, false, false])),
        );
        p.peer_new_have(&PEER3, 5);
    }

    #[test]
    fn test_selected_not_have() {
        let mut p = Picker::new(11, 2);
        p.peer_add(PEER1, PieceState::HaveNone);
        p.peer_add(
            PEER2,
            PieceState::Bitfield(BitField::from(vec![true, true, false, true, true, false])),
        );
        p.select(1, true);
        assert_eq!(p.selected_not_have.count_ones(), 1);
        p.set_have(1, true);

        p.select(2, true);
        assert_eq!(p.selected_not_have.count_ones(), 1);
        p.set_have(1, false);
        assert_eq!(p.selected_not_have.count_ones(), 2);
    }
}

use std::collections::HashSet;

use crate::conn::PeerAddr;

/// Classify peers by their share of inverse slot time (`1 / ST`).
///
/// `samples` contains `(peer, aged_st_per_useful_block, received_bytes)` for each
/// live connection in one torrent. `received_bytes` includes duplicate and late
/// responses. Each call classifies independently of earlier results.
///
/// Rank eligible peers by descending `1 / ST`, breaking ties by peer address.
/// The smallest prefix contributing at least 85% of the total score is fast;
/// the remaining ranked peers are slow.
///
/// Peers with no received bytes or zero ST are fast and excluded from ranking.
/// Missing, negative or NaN ST defaults to fast. Positive infinite ST has zero
/// score. If no positive finite ST is eligible, all peers default to fast.
///
/// Returns the new slow-peer set, containing only peers present in `samples`.
/// All other sampled peers are fast.
pub(super) fn classify_peers(samples: &[(PeerAddr, Option<f64>, u64)]) -> HashSet<PeerAddr> {
    let mut slow = HashSet::new();
    let mut ranked = Vec::new();
    let mut min_positive_st = f64::INFINITY;

    for (peer, st) in samples
        .iter()
        .filter(|(_, _, received_bytes)| *received_bytes > 0)
        .filter_map(|&(peer, st, _)| st.map(|st| (peer, st)))
        .filter(|(peer, st)| {
            let valid = !st.is_nan() && *st >= 0.0;
            if !valid {
                tracing::warn!(?peer, st, "invalid ST excluded from peer classification");
            }
            valid && *st > 0.0
        })
    {
        // Infinite ST participates with zero weight when finite samples exist.
        min_positive_st = min_positive_st.min(st);
        ranked.push((peer, st));
    }

    if !min_positive_st.is_finite() {
        return slow;
    }

    // Scaling by the minimum avoids overflowing the reciprocal of tiny ST.
    for (_, st) in &mut ranked {
        *st = min_positive_st / *st;
    }
    ranked.sort_by(|(peer_a, score_a), (peer_b, score_b)| {
        score_b.total_cmp(score_a).then_with(|| peer_a.cmp(peer_b))
    });
    let threshold = ranked.iter().map(|(_, score)| score).sum::<f64>() * 0.85;
    let mut cumulative = 0.0;
    for (peer, score) in ranked {
        if cumulative < threshold {
            cumulative += score;
        } else {
            slow.insert(peer);
        }
    }
    slow
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};

    fn peer(id: u16) -> PeerAddr {
        PeerAddr(SocketAddr::from((Ipv4Addr::LOCALHOST, id)), id as u64)
    }

    #[test]
    fn classification_uses_eighty_five_percent_of_score_and_received_bytes() {
        let cases: &[(&str, Vec<(PeerAddr, Option<f64>, u64)>, Vec<PeerAddr>)] = &[
            (
                "70/20/10 score",
                vec![
                    (peer(1), Some(1.0 / 70.0), 1),
                    (peer(2), Some(1.0 / 20.0), 1),
                    (peer(3), Some(1.0 / 10.0), 1),
                ],
                vec![peer(3)],
            ),
            (
                "exact 85 percent",
                vec![(peer(1), Some(3.0), 1), (peer(2), Some(17.0), 1)],
                vec![peer(2)],
            ),
            (
                "duplicate received versus no response",
                vec![
                    (peer(1), Some(1.0), 1),
                    (peer(2), Some(f64::INFINITY), 16384),
                    (peer(3), Some(f64::INFINITY), 0),
                ],
                vec![peer(2)],
            ),
            (
                "independent empty-score fallback",
                vec![
                    (peer(1), None, 1),
                    (peer(2), Some(f64::INFINITY), 1),
                    (peer(3), Some(0.0), 1),
                ],
                vec![],
            ),
        ];
        for (name, samples, expected) in cases {
            assert_eq!(
                classify_peers(samples),
                expected.iter().copied().collect(),
                "{name}"
            );
        }
    }
}

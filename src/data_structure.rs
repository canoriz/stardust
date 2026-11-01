use std::collections::{BTreeSet, HashMap};

/// An IndexedTree to find element with given value at O(log n)
/// and modify/delete element at O(log n)
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct IndexTree<K: std::cmp::Eq + std::hash::Hash, V> {
    pub(crate) tree: BTreeSet<(V, K)>,
    pub(crate) index: HashMap<K, V>,
}

impl<K, V> std::ops::Index<&K> for IndexTree<K, V>
where
    K: Eq + std::cmp::Ord + std::hash::Hash + Clone,
    V: std::cmp::Ord + Clone,
{
    type Output = V;

    fn index(&self, index: &K) -> &Self::Output {
        self.get(index).unwrap()
    }
}

impl<K, V> std::default::Default for IndexTree<K, V>
where
    K: Eq + std::cmp::Ord + std::hash::Hash + Clone,
    V: std::cmp::Ord + Clone,
{
    fn default() -> Self {
        Self {
            tree: Default::default(),
            index: Default::default(),
        }
    }
}

impl<K, V> IndexTree<K, V>
where
    K: std::cmp::Ord + std::hash::Hash + Clone,
    V: std::cmp::Ord + Clone,
{
    pub fn new() -> Self {
        Self {
            tree: BTreeSet::new(),
            index: HashMap::new(),
        }
    }

    pub fn get(&self, k: &K) -> Option<&V> {
        self.index.get(k)
    }

    pub fn get_mut<'a, 'b>(&'a mut self, k: &'b K) -> Option<Val<'a, K, V>> {
        self.remove(k).map(|v| Val {
            t: self,
            k: k.clone(),
            v,
        })
    }

    pub fn contains(&self, k: &K) -> bool {
        self.index.contains_key(k)
    }

    pub fn first(&self) -> Option<(&K, &V)> {
        self.tree.first().map(|(v, k)| (k, v))
    }

    pub fn pop_first(&mut self) -> Option<(K, V)> {
        if let Some((v, k)) = self.tree.pop_first() {
            self.index.remove(&k);
            Some((k, v))
        } else {
            None
        }
    }

    pub fn last(&self) -> Option<(&K, &V)> {
        self.tree.last().map(|(v, k)| (k, v))
    }

    /// Iterate by (value, key), descending when `reverse` is true.
    pub fn iter(&self, reverse: bool) -> impl Iterator<Item = (&K, &V)> {
        let mut iter = self.tree.iter();
        std::iter::from_fn(move || {
            let next = if reverse {
                iter.next_back()
            } else {
                iter.next()
            };
            next.map(|(v, k)| (k, v))
        })
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        let old = self.index.insert(k.clone(), v.clone());
        if let Some(old_v) = &old {
            self.tree.remove(&(old_v.clone(), k.clone()));
        }
        self.tree.insert((v, k));
        old
    }

    pub fn remove(&mut self, k: &K) -> Option<V> {
        let v = self.index.remove(k);
        if let Some(v) = v {
            self.tree.remove(&(v.clone(), k.clone()));
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Val<'a, K, V>
where
    K: std::cmp::Ord + std::hash::Hash + Clone,
    V: std::cmp::Ord + Clone,
{
    t: &'a mut IndexTree<K, V>,
    k: K,
    v: V,
}

impl<'a, K, V> std::ops::Deref for Val<'_, K, V>
where
    K: std::cmp::Ord + std::hash::Hash + Clone,
    V: std::cmp::Ord + Clone,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.v
    }
}

impl<K, V> std::ops::DerefMut for Val<'_, K, V>
where
    K: std::cmp::Ord + std::hash::Hash + Clone,
    V: std::cmp::Ord + Clone,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.v
    }
}

impl<K, V> Drop for Val<'_, K, V>
where
    K: std::cmp::Ord + std::hash::Hash + Clone,
    V: std::cmp::Ord + Clone,
{
    fn drop(&mut self) {
        self.t.insert(self.k.clone(), self.v.clone());
    }
}

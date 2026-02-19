/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use rand::Rng;

/// A primary key value that can represent single-column or composite keys.
///
/// The `Single` variant stores a single `i64` inline (8 bytes, no heap
/// allocation), which is optimal for the common case of a single integer
/// primary key. The `Composite` variant uses a `Box<[i64]>` to support
/// multi-column keys with minimal inline size (16 bytes for the fat pointer).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PrimaryKeyValue {
    /// A single-column primary key (8 bytes, no heap allocation).
    Single(i64),
    /// A composite (multi-column) primary key (heap-allocated).
    Composite(Box<[i64]>),
}

impl PrimaryKeyValue {
    /// Creates a new single-column primary key value.
    pub fn single(value: i64) -> Self {
        Self::Single(value)
    }

    /// Creates a new composite primary key value from a slice.
    pub fn composite(values: &[i64]) -> Self {
        Self::Composite(values.into())
    }
}

/// An indexed set that supports O(1) amortized insertion, O(1) deletion, and
/// O(1) uniform random selection by index.
///
/// Internally uses a dense [`Vec`] for random access paired with a [`HashMap`]
/// for key-to-index lookup. Deletion uses swap-remove on the `Vec` to maintain
/// density, and updates the moved element's index in the map.
///
/// # Memory
///
/// Each entry is stored both in the `Vec` (for random selection) and the
/// `HashMap` (for lookup by value). For `i64` keys this is roughly 48 bytes
/// per entry; for large sets (hundreds of millions) this can consume several
/// gigabytes of RAM.
///
/// # Composite key support
///
/// The set is generic over any key type that implements `Eq + Hash + Clone`.
/// Use [`PrimaryKeyValue`] for runtime-polymorphic single/composite keys, or
/// specialize to `i64` for minimal overhead when the primary key is a single
/// integer column.
pub struct IndexedKeySet<K: Eq + Hash + Clone> {
    keys: Vec<K>,
    index: HashMap<K, usize>,
}

impl<K: Eq + Hash + Clone> Default for IndexedKeySet<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone> IndexedKeySet<K> {
    /// Creates a new, empty `IndexedKeySet`.
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            index: HashMap::new(),
        }
    }

    /// Creates a new `IndexedKeySet` with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            keys: Vec::with_capacity(cap),
            index: HashMap::with_capacity(cap),
        }
    }

    /// Inserts a key into the set. Returns `true` if the key was newly inserted.
    pub fn insert(&mut self, key: K) -> bool {
        if self.index.contains_key(&key) {
            return false;
        }
        let idx = self.keys.len();
        self.keys.push(key.clone());
        self.index.insert(key, idx);
        true
    }

    /// Removes a key from the set. Returns `true` if the key was present.
    ///
    /// Uses swap-remove on the internal `Vec` so that all operations remain
    /// O(1) amortized. The last element in the `Vec` is moved into the vacated
    /// slot and its index in the `HashMap` is updated.
    pub fn remove(&mut self, key: &K) -> bool {
        let Some(idx) = self.index.remove(key) else {
            return false;
        };
        self.keys.swap_remove(idx);
        // If an element was swapped from the end into `idx`, update its index.
        if idx < self.keys.len() {
            let moved = &self.keys[idx];
            self.index.insert(moved.clone(), idx);
        }
        true
    }

    /// Returns a uniformly random key from the set, or `None` if empty.
    pub fn random_key(&self, rng: &mut impl Rng) -> Option<&K> {
        if self.keys.is_empty() {
            return None;
        }
        let idx = rng.random_range(0..self.keys.len());
        Some(&self.keys[idx])
    }

    /// Samples up to `n` distinct keys uniformly at random.
    ///
    /// If `n >= len()`, returns all keys in arbitrary order. Uses rejection
    /// sampling, which is efficient when `n` is small relative to `len()`.
    pub fn sample_keys(&self, n: usize, rng: &mut impl Rng) -> Vec<K> {
        let len = self.keys.len();
        if n >= len {
            return self.keys.clone();
        }
        if n == 0 {
            return Vec::new();
        }
        let mut chosen = HashSet::with_capacity(n);
        while chosen.len() < n {
            chosen.insert(rng.random_range(0..len));
        }
        chosen.into_iter().map(|i| self.keys[i].clone()).collect()
    }

    /// Returns the number of keys in the set.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns `true` if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_len() {
        let mut set = IndexedKeySet::new();
        assert!(set.insert(1i64));
        assert!(set.insert(2));
        assert!(!set.insert(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn remove_and_consistency() {
        let mut set = IndexedKeySet::new();
        for i in 0..5i64 {
            set.insert(i);
        }
        assert!(set.remove(&2));
        assert!(!set.remove(&2)); // already removed
        assert_eq!(set.len(), 4);

        // All remaining keys should be findable.
        for key in &set.keys {
            assert!(set.index.contains_key(key));
            assert_eq!(set.keys[set.index[key]], *key);
        }
    }

    #[test]
    fn random_selection() {
        let mut set = IndexedKeySet::new();
        for i in 0..100i64 {
            set.insert(i);
        }
        let mut rng = rand::rng();
        let key = set.random_key(&mut rng).unwrap();
        assert!((0..100).contains(key));
    }

    #[test]
    fn sample_keys_distinct() {
        let mut set = IndexedKeySet::new();
        for i in 0..100i64 {
            set.insert(i);
        }
        let mut rng = rand::rng();
        let sampled = set.sample_keys(10, &mut rng);
        assert_eq!(sampled.len(), 10);

        // All sampled keys should be distinct.
        let unique: HashSet<_> = sampled.iter().collect();
        assert_eq!(unique.len(), 10);
    }

    #[test]
    fn composite_key() {
        let mut set = IndexedKeySet::<PrimaryKeyValue>::new();
        set.insert(PrimaryKeyValue::composite(&[1, 2]));
        set.insert(PrimaryKeyValue::composite(&[3, 4]));
        assert_eq!(set.len(), 2);
        assert!(set.remove(&PrimaryKeyValue::composite(&[1, 2])));
        assert_eq!(set.len(), 1);
    }
}

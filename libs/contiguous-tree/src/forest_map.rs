//! Sorted unique keys associated with roots in an ordered forest.

use std::fmt;

use crate::{Forest, NodeAnnotationsBuilder, TreeHandle, TreeStorage};

/// A failure while associating sorted unique keys with forest roots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForestMapError {
    KeyCountMismatch {
        keys: usize,
        roots: usize,
    },
    KeysNotStrictlyOrdered {
        index: usize,
    },
    DuplicateKey {
        first_index: usize,
        duplicate_index: usize,
    },
}

impl fmt::Display for ForestMapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyCountMismatch { keys, roots } => {
                write!(formatter, "forest map has {keys} keys for {roots} roots")
            }
            Self::KeysNotStrictlyOrdered { index } => write!(
                formatter,
                "forest map keys at positions {} and {index} are not strictly ordered",
                index - 1
            ),
            Self::DuplicateKey {
                first_index,
                duplicate_index,
            } => write!(
                formatter,
                "forest map key at position {duplicate_index} duplicates position {first_index}"
            ),
        }
    }
}

impl std::error::Error for ForestMapError {}

/// Sorted unique keys associated one-to-one with the ordered roots of a forest.
pub struct ForestMap<Key, Storage: TreeStorage> {
    keys: Box<[Key]>,
    forest: Forest<Storage>,
}

impl<Key: Clone, Storage: TreeStorage> Clone for ForestMap<Key, Storage> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            forest: self.forest.clone(),
        }
    }
}

impl<Key: Ord, Storage: TreeStorage> ForestMap<Key, Storage> {
    pub fn new(
        keys: impl IntoIterator<Item = Key>,
        forest: Forest<Storage>,
    ) -> Result<Self, ForestMapError> {
        let keys = keys.into_iter().collect::<Box<[_]>>();
        if keys.len() != forest.len() {
            return Err(ForestMapError::KeyCountMismatch {
                keys: keys.len(),
                roots: forest.len(),
            });
        }
        if let Some(index) = keys.windows(2).position(|pair| pair[0] >= pair[1]) {
            return Err(ForestMapError::KeysNotStrictlyOrdered { index: index + 1 });
        }
        Ok(Self { keys, forest })
    }

    /// Sort keys and roots together without revalidating the forest's nodes.
    pub fn from_unsorted(
        keys: impl IntoIterator<Item = Key>,
        forest: Forest<Storage>,
    ) -> Result<Self, ForestMapError> {
        let keys = keys.into_iter().collect::<Vec<_>>();
        if keys.len() != forest.len() {
            return Err(ForestMapError::KeyCountMismatch {
                keys: keys.len(),
                roots: forest.len(),
            });
        }

        let Forest { storage, roots } = forest;
        let mut entries = keys
            .into_iter()
            .zip(roots)
            .enumerate()
            .map(|(index, (key, root))| (key, root, index))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.2.cmp(&right.2)));
        let mut earliest_duplicate = None;
        let mut start = 0;
        while start < entries.len() {
            let mut end = start + 1;
            while end < entries.len() && entries[start].0 == entries[end].0 {
                end += 1;
            }
            if end - start > 1 {
                let candidate = (entries[start].2, entries[start + 1].2);
                if earliest_duplicate
                    .is_none_or(|(_, duplicate_index)| candidate.1 < duplicate_index)
                {
                    earliest_duplicate = Some(candidate);
                }
            }
            start = end;
        }
        if let Some((first_index, duplicate_index)) = earliest_duplicate {
            return Err(ForestMapError::DuplicateKey {
                first_index,
                duplicate_index,
            });
        }

        let (keys, roots): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .map(|(key, root, _)| (key, root))
            .unzip();
        Ok(Self {
            keys: keys.into_boxed_slice(),
            forest: Forest {
                storage,
                roots: roots.into_boxed_slice(),
            },
        })
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn keys(&self) -> impl DoubleEndedIterator<Item = &Key> + ExactSizeIterator {
        self.keys.iter()
    }

    pub fn values(
        &self,
    ) -> impl DoubleEndedIterator<Item = Storage::Cursor<'_>> + ExactSizeIterator + '_ {
        self.forest.cursors()
    }

    /// Every node in contiguous allocation order.
    pub fn nodes(
        &self,
    ) -> impl DoubleEndedIterator<Item = Storage::Cursor<'_>> + ExactSizeIterator + '_ {
        self.forest.nodes()
    }

    pub fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&Key, Storage::Cursor<'_>)> + ExactSizeIterator + '_ {
        self.keys.iter().zip(self.forest.cursors())
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        self.keys.binary_search(key).is_ok()
    }

    pub fn get(&self, key: &Key) -> Option<Storage::Cursor<'_>> {
        self.keys
            .binary_search(key)
            .ok()
            .map(|index| self.forest.cursor(index))
    }

    pub fn handle(&self, key: &Key) -> Option<TreeHandle<Storage>> {
        self.keys
            .binary_search(key)
            .ok()
            .map(|index| self.forest.handle(index))
    }

    pub fn forest(&self) -> &Forest<Storage> {
        &self.forest
    }

    pub fn annotations_builder<T>(&self) -> NodeAnnotationsBuilder<Storage, T> {
        self.forest.annotations_builder()
    }

    pub fn into_forest(self) -> Forest<Storage> {
        self.forest
    }

    pub fn into_entries(
        self,
    ) -> impl DoubleEndedIterator<Item = (Key, TreeHandle<Storage>)> + ExactSizeIterator {
        self.keys
            .into_vec()
            .into_iter()
            .zip(self.forest.into_handles())
    }
}

//! Shared owning roots and ordered forests.

use std::{iter::FusedIterator, ops::Range, rc::Rc};

use crate::{ArenaId, ForestError, NodeAnnotationsBuilder, TreeCursor, TreeStorage};

/// An owning root into shared tree storage.
pub struct TreeHandle<Storage: TreeStorage> {
    storage: Rc<Storage>,
    root: Storage::Id,
}

impl<Storage: TreeStorage> Clone for TreeHandle<Storage> {
    fn clone(&self) -> Self {
        Self {
            storage: Rc::clone(&self.storage),
            root: self.root,
        }
    }
}

impl<Storage: TreeStorage> TreeHandle<Storage> {
    pub(crate) fn from_shared(storage: Rc<Storage>, root: Storage::Id) -> Self {
        Self { storage, root }
    }

    pub fn cursor(&self) -> Storage::Cursor<'_> {
        self.storage.cursor(self.root)
    }

    pub fn subtree(&self, cursor: Storage::Cursor<'_>) -> Self {
        assert!(
            self.storage.owns(cursor),
            "subtree root belongs to different tree storage"
        );
        assert!(
            self.cursor().subtree_ids().contains(cursor.id()),
            "subtree root is not a descendant of the owning root"
        );
        Self {
            storage: Rc::clone(&self.storage),
            root: cursor.id(),
        }
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn root_id(&self) -> Storage::Id {
        self.root
    }

    pub fn same_root(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.storage, &other.storage) && self.root.index() == other.root.index()
    }

    pub fn shares_storage_with(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.storage, &other.storage)
    }

    pub fn strong_count(&self) -> usize {
        Rc::strong_count(&self.storage)
    }

    pub fn annotations_builder<T>(&self) -> NodeAnnotationsBuilder<Storage, T> {
        let mut ids = self.cursor().subtree_ids();
        let len = ids.len();
        let start = ids
            .next()
            .expect("an owning tree always contains its root")
            .index();
        NodeAnnotationsBuilder::new(Rc::clone(&self.storage), start, len)
    }

    pub fn into_storage_and_root(self) -> (Storage, Storage::Id)
    where
        Storage: Clone,
    {
        let storage = Rc::try_unwrap(self.storage).unwrap_or_else(|shared| (*shared).clone());
        (storage, self.root)
    }
}

/// An ordered, unkeyed forest owning one shared storage allocation.
pub struct Forest<Storage: TreeStorage> {
    pub(crate) storage: Rc<Storage>,
    pub(crate) roots: Box<[Storage::Id]>,
}

impl<Storage: TreeStorage> Clone for Forest<Storage> {
    fn clone(&self) -> Self {
        Self {
            storage: Rc::clone(&self.storage),
            roots: self.roots.clone(),
        }
    }
}

impl<Storage: TreeStorage> Forest<Storage> {
    pub(crate) fn from_validated_parts(storage: Storage, roots: Box<[Storage::Id]>) -> Self {
        Self {
            storage: Rc::new(storage),
            roots,
        }
    }

    pub fn new(
        storage: Storage,
        roots: impl IntoIterator<Item = Storage::Id>,
    ) -> Result<Self, ForestError> {
        let roots = roots.into_iter().collect::<Box<[_]>>();
        storage.validate_forest(&roots)?;
        Ok(Self::from_validated_parts(storage, roots))
    }

    pub fn len(&self) -> usize {
        self.roots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn root_ids(&self) -> &[Storage::Id] {
        &self.roots
    }

    pub fn cursor(&self, index: usize) -> Storage::Cursor<'_> {
        self.storage.cursor(self.roots[index])
    }

    pub fn cursors(
        &self,
    ) -> impl DoubleEndedIterator<Item = Storage::Cursor<'_>> + ExactSizeIterator + '_ {
        self.roots.iter().map(|&root| self.storage.cursor(root))
    }

    /// Every node in contiguous allocation order.
    pub fn nodes(
        &self,
    ) -> impl DoubleEndedIterator<Item = Storage::Cursor<'_>> + ExactSizeIterator + '_ {
        (0..self.storage.node_count())
            .map(|index| self.storage.cursor(Storage::Id::from_index(index)))
    }

    pub fn handle(&self, index: usize) -> TreeHandle<Storage> {
        TreeHandle::from_shared(Rc::clone(&self.storage), self.roots[index])
    }

    pub fn handles(
        &self,
    ) -> impl DoubleEndedIterator<Item = TreeHandle<Storage>> + ExactSizeIterator + '_ {
        self.roots
            .iter()
            .map(|&root| TreeHandle::from_shared(Rc::clone(&self.storage), root))
    }

    pub fn annotations_builder<T>(&self) -> NodeAnnotationsBuilder<Storage, T> {
        NodeAnnotationsBuilder::new(Rc::clone(&self.storage), 0, self.storage.node_count())
    }

    pub fn into_handles(self) -> ForestHandles<Storage> {
        let len = self.roots.len();
        ForestHandles {
            storage: self.storage,
            roots: self.roots,
            range: 0..len,
        }
    }

    pub fn try_into_storage_and_roots(self) -> Result<(Storage, Box<[Storage::Id]>), Self> {
        let Self { storage, roots } = self;
        match Rc::try_unwrap(storage) {
            Ok(storage) => Ok((storage, roots)),
            Err(storage) => Err(Self { storage, roots }),
        }
    }

    pub fn into_storage_and_roots(self) -> (Storage, Box<[Storage::Id]>)
    where
        Storage: Clone,
    {
        let Self { storage, roots } = self;
        let storage = Rc::try_unwrap(storage).unwrap_or_else(|shared| (*shared).clone());
        (storage, roots)
    }
}

/// A non-allocating consuming iterator over the handles of a [`Forest`].
pub struct ForestHandles<Storage: TreeStorage> {
    storage: Rc<Storage>,
    roots: Box<[Storage::Id]>,
    range: Range<usize>,
}

impl<Storage: TreeStorage> Iterator for ForestHandles<Storage> {
    type Item = TreeHandle<Storage>;

    fn next(&mut self) -> Option<Self::Item> {
        self.range
            .next()
            .map(|index| TreeHandle::from_shared(Rc::clone(&self.storage), self.roots[index]))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl<Storage: TreeStorage> DoubleEndedIterator for ForestHandles<Storage> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.range
            .next_back()
            .map(|index| TreeHandle::from_shared(Rc::clone(&self.storage), self.roots[index]))
    }
}

impl<Storage: TreeStorage> ExactSizeIterator for ForestHandles<Storage> {}
impl<Storage: TreeStorage> FusedIterator for ForestHandles<Storage> {}

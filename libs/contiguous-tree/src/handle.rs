//! Shared owning roots and dense side annotations.

use std::{marker::PhantomData, rc::Rc};

use crate::{ArenaId, TreeCursor};

/// Storage that can construct borrowed cursors for its node IDs.
pub trait TreeStorage: Sized {
    type Id: ArenaId;
    type Cursor<'arena>: TreeCursor<Id = Self::Id>
    where
        Self: 'arena;

    fn cursor(&self, id: Self::Id) -> Self::Cursor<'_>;
    fn owns(&self, cursor: Self::Cursor<'_>) -> bool;
}

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
    pub fn new(storage: Storage, root: Storage::Id) -> Self {
        Self {
            storage: Rc::new(storage),
            root,
        }
    }

    /// Create several roots backed by one shared storage allocation.
    pub fn forest(
        storage: Storage,
        roots: impl IntoIterator<Item = Storage::Id>,
    ) -> std::vec::IntoIter<Self> {
        let storage = Rc::new(storage);
        roots
            .into_iter()
            .map(|root| Self {
                storage: Rc::clone(&storage),
                root,
            })
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub fn cursor(&self) -> Storage::Cursor<'_> {
        self.storage.cursor(self.root)
    }

    pub fn subtree(&self, cursor: Storage::Cursor<'_>) -> Self {
        assert!(
            self.storage.owns(cursor),
            "subtree root belongs to different tree storage"
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

    pub fn into_storage_and_root(self) -> (Storage, Storage::Id)
    where
        Storage: Clone,
    {
        let storage = Rc::try_unwrap(self.storage).unwrap_or_else(|shared| (*shared).clone());
        (storage, self.root)
    }
}

/// Dense annotations indexed by the same IDs as an arena.
#[derive(Clone, Debug)]
pub struct NodeAnnotations<Id, T> {
    values: Box<[T]>,
    id: PhantomData<fn() -> Id>,
}

impl<Id: ArenaId, T> NodeAnnotations<Id, T> {
    pub fn new(values: impl Into<Box<[T]>>) -> Self {
        Self {
            values: values.into(),
            id: PhantomData,
        }
    }

    pub fn get(&self, id: Id) -> &T {
        &self.values[id.index()]
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

//! Cursor identity and structural navigation contracts.

use crate::{ArenaId, IdRange};

/// Opaque identity of the storage allocation behind a tree cursor.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageIdentity(*const ());

impl StorageIdentity {
    /// Create an identity token for a stable storage reference.
    pub fn for_ref<Storage>(storage: &Storage) -> Self {
        Self(std::ptr::from_ref(storage).cast())
    }
}

/// A copyable handle that resolves structural relationships within one tree.
pub trait TreeCursor: Copy {
    type Id: ArenaId;
    type ChildIds: DoubleEndedIterator<Item = Self::Id> + ExactSizeIterator;

    fn id(self) -> Self::Id;
    fn storage_identity(self) -> StorageIdentity;
    fn same_node(self, other: Self) -> bool;

    fn child_ids(self) -> Self::ChildIds;
    fn child(self, id: Self::Id) -> Self;
    fn subtree_ids(self) -> IdRange<Self::Id>;
}

/// A cursor paired with copyable context that is retained while traversing children.
#[derive(Clone, Copy)]
pub struct ContextCursor<Cursor, Context> {
    cursor: Cursor,
    context: Context,
}

impl<Cursor, Context> ContextCursor<Cursor, Context> {
    pub fn new(cursor: Cursor, context: Context) -> Self {
        Self { cursor, context }
    }

    pub fn cursor(self) -> Cursor
    where
        Cursor: Copy,
    {
        self.cursor
    }

    pub fn context(self) -> Context
    where
        Context: Copy,
    {
        self.context
    }
}

impl<Cursor, Context> TreeCursor for ContextCursor<Cursor, Context>
where
    Cursor: TreeCursor,
    Context: Copy,
{
    type Id = Cursor::Id;
    type ChildIds = Cursor::ChildIds;

    fn id(self) -> Self::Id {
        self.cursor.id()
    }

    fn storage_identity(self) -> StorageIdentity {
        self.cursor.storage_identity()
    }

    fn same_node(self, other: Self) -> bool {
        self.cursor.same_node(other.cursor)
    }

    fn child_ids(self) -> Self::ChildIds {
        self.cursor.child_ids()
    }

    fn child(self, id: Self::Id) -> Self {
        Self::new(self.cursor.child(id), self.context)
    }

    fn subtree_ids(self) -> IdRange<Self::Id> {
        self.cursor.subtree_ids()
    }
}

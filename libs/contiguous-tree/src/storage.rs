//! Storage contracts for custom contiguous-tree representations.

use crate::{ArenaId, ForestError, TreeCursor};

/// Storage that can construct borrowed cursors for its node IDs.
pub trait TreeStorage: Sized {
    type Id: ArenaId;
    type Cursor<'arena>: TreeCursor<Id = Self::Id>
    where
        Self: 'arena;

    fn cursor(&self, id: Self::Id) -> Self::Cursor<'_>;
    fn owns(&self, cursor: Self::Cursor<'_>) -> bool;

    /// Validate a complete caller-ordered set of occurrence-tree roots.
    fn validate_forest(&self, roots: &[Self::Id]) -> Result<(), ForestError>;
    fn node_count(&self) -> usize;
}

/// Storage that can be populated by a [`ForestBuilder`].
///
/// Implementations must append exactly one node for each call to [`Self::push_node`]
/// and use dense IDs matching allocation order. Application code should construct
/// forests through [`ForestBuilder`] rather than call these low-level methods.
pub trait PostorderStorage<Node>: TreeStorage {
    /// Append exactly `node` and return its newly allocated ID.
    fn push_node(&mut self, node: Node) -> Self::Id;
    fn reserve_nodes(&mut self, additional: usize);
    fn truncate_nodes(&mut self, len: usize);
}

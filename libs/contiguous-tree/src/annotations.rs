//! Node annotations bound to one storage range: complete, or sparse.

use std::fmt;

use crate::{ArenaId, Shared, TreeCursor, TreeStorage};

/// A failure while constructing storage-bound node annotations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnnotationError {
    CursorOutsideScope { index: usize },
    Incomplete { index: usize },
}

impl fmt::Display for AnnotationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CursorOutsideScope { index } => {
                write!(formatter, "node {index} is outside the annotation scope")
            }
            Self::Incomplete { index } => {
                write!(formatter, "node {index} does not have an annotation")
            }
        }
    }
}

impl std::error::Error for AnnotationError {}

/// Mutable construction of complete storage-bound node annotations.
pub struct NodeAnnotationsBuilder<Storage: TreeStorage, T> {
    storage: Shared<Storage>,
    start: usize,
    values: Vec<Option<T>>,
}

impl<Storage: TreeStorage, T> NodeAnnotationsBuilder<Storage, T> {
    pub(crate) fn new(storage: Shared<Storage>, start: usize, len: usize) -> Self {
        Self {
            storage,
            start,
            values: std::iter::repeat_with(|| None).take(len).collect(),
        }
    }

    fn offset(&self, cursor: Storage::Cursor<'_>) -> Result<usize, AnnotationError> {
        let index = cursor.id().index();
        if !self.storage.owns(cursor) {
            return Err(AnnotationError::CursorOutsideScope { index });
        }
        let Some(offset) = index.checked_sub(self.start) else {
            return Err(AnnotationError::CursorOutsideScope { index });
        };
        if offset >= self.values.len() {
            return Err(AnnotationError::CursorOutsideScope { index });
        }
        Ok(offset)
    }

    pub fn insert(
        &mut self,
        cursor: Storage::Cursor<'_>,
        value: T,
    ) -> Result<Option<T>, AnnotationError> {
        let offset = self.offset(cursor)?;
        Ok(self.values[offset].replace(value))
    }

    pub fn get(&self, cursor: Storage::Cursor<'_>) -> Option<&T> {
        let offset = self.offset(cursor).ok()?;
        self.values[offset].as_ref()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn finish(self) -> Result<NodeAnnotations<Storage, T>, AnnotationError> {
        let start = self.start;
        let values = self
            .values
            .into_iter()
            .enumerate()
            .map(|(offset, value)| {
                value.ok_or(AnnotationError::Incomplete {
                    index: start + offset,
                })
            })
            .collect::<Result<Box<[_]>, _>>()?;
        Ok(NodeAnnotations {
            storage: self.storage,
            start: self.start,
            values,
        })
    }
}

/// Immutable node annotations bound to one shared storage range.
#[derive(Clone)]
pub struct NodeAnnotations<Storage: TreeStorage, T> {
    storage: Shared<Storage>,
    start: usize,
    values: Box<[T]>,
}

impl<Storage: TreeStorage, T: fmt::Debug> fmt::Debug for NodeAnnotations<Storage, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeAnnotations")
            .field("start", &self.start)
            .field("values", &self.values)
            .finish_non_exhaustive()
    }
}

impl<Storage: TreeStorage, T> NodeAnnotations<Storage, T> {
    pub fn get(&self, cursor: Storage::Cursor<'_>) -> Option<&T> {
        if !self.storage.owns(cursor) {
            return None;
        }
        let offset = cursor.id().index().checked_sub(self.start)?;
        self.values.get(offset)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}
/// Mutable construction of sparse storage-bound node annotations.
pub struct SparseNodeAnnotationsBuilder<Storage: TreeStorage, T> {
    storage: Shared<Storage>,
    start: usize,
    len: usize,
    values: Vec<(usize, T)>,
}

impl<Storage: TreeStorage, T> SparseNodeAnnotationsBuilder<Storage, T> {
    pub(crate) fn new(storage: Shared<Storage>, start: usize, len: usize) -> Self {
        Self {
            storage,
            start,
            len,
            values: Vec::new(),
        }
    }

    fn index(&self, cursor: Storage::Cursor<'_>) -> Result<usize, AnnotationError> {
        let index = cursor.id().index();
        if !self.storage.owns(cursor)
            || index < self.start
            || index >= self.start.saturating_add(self.len)
        {
            return Err(AnnotationError::CursorOutsideScope { index });
        }
        Ok(index)
    }

    pub fn insert(
        &mut self,
        cursor: Storage::Cursor<'_>,
        value: T,
    ) -> Result<Option<T>, AnnotationError> {
        let index = self.index(cursor)?;
        match self
            .values
            .binary_search_by_key(&index, |(stored, _)| *stored)
        {
            Ok(position) => Ok(Some(std::mem::replace(&mut self.values[position].1, value))),
            Err(position) => {
                self.values.insert(position, (index, value));
                Ok(None)
            }
        }
    }

    pub fn finish(self) -> SparseNodeAnnotations<Storage, T> {
        SparseNodeAnnotations {
            storage: self.storage,
            start: self.start,
            len: self.len,
            values: self.values.into_boxed_slice(),
        }
    }
}

/// Immutable annotations of some nodes of one shared storage range.
///
/// Only annotated nodes occupy space. Lookup distinguishes a node of this
/// storage range that has no annotation from a node that is not in the range
/// at all, such as an equally numbered node of other storage.
#[derive(Clone)]
pub struct SparseNodeAnnotations<Storage: TreeStorage, T> {
    storage: Shared<Storage>,
    start: usize,
    len: usize,
    values: Box<[(usize, T)]>,
}

impl<Storage: TreeStorage, T: fmt::Debug> fmt::Debug for SparseNodeAnnotations<Storage, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SparseNodeAnnotations")
            .field("start", &self.start)
            .field("len", &self.len)
            .field("values", &self.values)
            .finish_non_exhaustive()
    }
}

impl<Storage: TreeStorage, T> SparseNodeAnnotations<Storage, T> {
    /// The annotation of `cursor`, `Ok(None)` for an unannotated node of this
    /// range, or an error for a node outside it.
    pub fn try_get(&self, cursor: Storage::Cursor<'_>) -> Result<Option<&T>, AnnotationError> {
        let index = cursor.id().index();
        if !self.storage.owns(cursor)
            || index < self.start
            || index >= self.start.saturating_add(self.len)
        {
            return Err(AnnotationError::CursorOutsideScope { index });
        }
        Ok(self
            .values
            .binary_search_by_key(&index, |(entry, _)| *entry)
            .ok()
            .map(|position| &self.values[position].1))
    }

    pub fn get(&self, cursor: Storage::Cursor<'_>) -> Option<&T> {
        self.try_get(cursor).ok().flatten()
    }

    /// Whether this table annotates nodes of the same storage allocation as `other`.
    pub fn shares_storage_with<U>(&self, other: &SparseNodeAnnotations<Storage, U>) -> bool {
        Shared::ptr_eq(&self.storage, &other.storage)
    }

    /// Whether `cursor` belongs to this table's storage range.
    pub fn belongs_to(&self, cursor: Storage::Cursor<'_>) -> bool {
        let index = cursor.id().index();
        self.storage.owns(cursor)
            && index >= self.start
            && index < self.start.saturating_add(self.len)
    }

    /// The annotation of `cursor`.
    ///
    /// # Panics
    ///
    /// Panics distinctly when the cursor belongs to different storage or when
    /// the node has no annotation.
    pub fn site(&self, cursor: Storage::Cursor<'_>) -> &T {
        match self.try_get(cursor) {
            Ok(Some(value)) => value,
            Ok(None) => panic!("node is not a prepared runtime-expression occurrence"),
            Err(_) => panic!("runtime-expression site belongs to different expression storage"),
        }
    }

    /// The number of annotated nodes.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Annotated nodes in allocation order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (Storage::Cursor<'_>, &T)> + '_ {
        self.values.iter().map(|(index, value)| {
            (
                self.storage
                    .cursor(<Storage::Id as crate::ArenaId>::from_index(*index)),
                value,
            )
        })
    }
}

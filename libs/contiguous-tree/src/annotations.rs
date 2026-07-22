//! Complete node annotations bound to one storage range.

use std::{fmt, rc::Rc};

use crate::{ArenaId, TreeCursor, TreeStorage};

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
    storage: Rc<Storage>,
    start: usize,
    values: Vec<Option<T>>,
}

impl<Storage: TreeStorage, T> NodeAnnotationsBuilder<Storage, T> {
    pub(crate) fn new(storage: Rc<Storage>, start: usize, len: usize) -> Self {
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
    storage: Rc<Storage>,
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

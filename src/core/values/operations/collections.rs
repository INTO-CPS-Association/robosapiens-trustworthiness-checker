use ecow::{EcoString, EcoVec};

use super::Value;
use super::dispatch::{ValueOpError, invalid_binary_named, invalid_unary_named};

pub fn tuple_get(tuple: Value, index: usize) -> Result<Value, ValueOpError> {
    match tuple {
        Value::Tuple(values) | Value::List(values) => {
            values
                .get(index)
                .cloned()
                .ok_or(ValueOpError::TupleIndexOutOfBounds {
                    index,
                    len: values.len(),
                })
        }
        operand => invalid_unary_named("tuple indexing", operand),
    }
}

pub fn list_index(list: Value, index: Value) -> Result<Value, ValueOpError> {
    match (list, index) {
        (Value::List(_), Value::Int(index)) if index < 0 => {
            Err(ValueOpError::NegativeListIndex(index))
        }
        (Value::List(values), Value::Int(index)) => {
            let index = index as usize;
            values
                .get(index)
                .cloned()
                .ok_or(ValueOpError::ListIndexOutOfBounds {
                    index,
                    len: values.len(),
                })
        }
        (list, index) => invalid_binary_named("list indexing", list, index),
    }
}

pub fn list_append(list: Value, value: Value) -> Result<Value, ValueOpError> {
    match list {
        Value::List(mut values) => {
            if !values.is_unique() {
                values = detached_copy(&values, 1);
            }
            values.push(value);
            Ok(Value::List(values))
        }
        list => invalid_binary_named("list append", list, value),
    }
}

pub fn list_concat(left: Value, right: Value) -> Result<Value, ValueOpError> {
    match (left, right) {
        // Either side may be returned as it is: lists are immutable values.
        (Value::List(left), Value::List(right)) if left.is_empty() => Ok(Value::List(right)),
        (Value::List(left), Value::List(right)) if right.is_empty() => Ok(Value::List(left)),
        (Value::List(mut left), Value::List(right)) => {
            if !left.is_unique() {
                left = detached_copy(&left, right.len());
            }
            left.extend(right);
            Ok(Value::List(left))
        }
        (left, right) => invalid_binary_named("list concatenation", left, right),
    }
}

pub fn list_head(list: Value) -> Result<Value, ValueOpError> {
    match list {
        Value::List(values) => values.first().cloned().ok_or(ValueOpError::EmptyList),
        operand => invalid_unary_named("list head", operand),
    }
}

pub fn list_tail(list: Value) -> Result<Value, ValueOpError> {
    match list {
        Value::List(values) if values.is_empty() => Err(ValueOpError::EmptyList),
        Value::List(mut values) => {
            if values.is_unique() {
                values.remove(0);
                Ok(Value::List(values))
            } else {
                Ok(Value::List(values[1..].into()))
            }
        }
        operand => invalid_unary_named("list tail", operand),
    }
}

/// Copy a shared list into a new allocation with room for `additional` more elements.
///
/// `EcoVec` detaches a shared vector on the first edit, but sizes the copy by its amortized
/// growth rule, which can double the capacity of a list that the evaluator's retained operands
/// keep shared and therefore copy on every edit anyway.
fn detached_copy(values: &[Value], additional: usize) -> EcoVec<Value> {
    let mut copy = EcoVec::with_capacity(values.len() + additional);
    copy.extend_from_slice(values);
    copy
}

/// The result of `List.filter`, built from predicate decisions made in element order.
///
/// The caller passes a clone of each [`pending`](Self::pending) element to its predicate and
/// reports the decision with [`decide`](Self::decide). Elements are read by reference, so a
/// shared source is not copied by iteration: its kept elements are copied only after a
/// predicate rejects one, and a unique source is compacted in place. When every element is kept
/// the result is the source list, and when none is kept it is an empty list. Either shortcut is
/// taken only by [`finish`](Self::finish), after every predicate has run.
pub(crate) struct ListFilter {
    source: EcoVec<Value>,
    unique: bool,
    /// Index of the next element awaiting a decision.
    next: usize,
    /// For a unique source: how many kept elements have been compacted to its front.
    compacted: usize,
    /// For a shared source: the kept elements, once some element has been rejected.
    kept: Option<EcoVec<Value>>,
}

impl ListFilter {
    pub(crate) fn new(mut source: EcoVec<Value>) -> Self {
        Self {
            unique: source.is_unique(),
            source,
            next: 0,
            compacted: 0,
            kept: None,
        }
    }

    /// The element whose decision is due, or `None` once every element has been decided.
    pub(crate) fn pending(&self) -> Option<&Value> {
        self.source.get(self.next)
    }

    /// Record whether the predicate keeps the element returned by [`pending`](Self::pending).
    pub(crate) fn decide(&mut self, keep: bool) {
        let index = self.next;
        debug_assert!(index < self.source.len());
        self.next += 1;
        if self.unique {
            if keep {
                if self.compacted != index {
                    // Unique: no element is cloned.
                    self.source.make_mut().swap(self.compacted, index);
                }
                self.compacted += 1;
            }
        } else if let Some(kept) = &mut self.kept {
            if keep {
                kept.push(self.source[index].clone());
            }
        } else if !keep {
            self.kept = Some(self.source[..index].into());
        }
    }

    /// The filtered list. Every element must have been decided.
    pub(crate) fn finish(self) -> EcoVec<Value> {
        debug_assert_eq!(self.next, self.source.len());
        if !self.unique {
            return self.kept.unwrap_or(self.source);
        }
        let mut source = self.source;
        match self.compacted {
            0 => EcoVec::new(),
            kept => {
                source.truncate(kept);
                source
            }
        }
    }
}

pub fn list_len(list: Value) -> Result<Value, ValueOpError> {
    match list {
        Value::List(values) => i64::try_from(values.len())
            .map(Value::Int)
            .map_err(|_| ValueOpError::ListLengthOverflow(values.len())),
        operand => invalid_unary_named("list length", operand),
    }
}

pub fn map_get(map: Value, key: &EcoString) -> Result<Value, ValueOpError> {
    match map {
        Value::Map(values) => values
            .get(key)
            .cloned()
            .ok_or_else(|| ValueOpError::MissingMapKey(key.clone())),
        operand => invalid_unary_named("map get", operand),
    }
}

pub fn map_insert(map: Value, key: &EcoString, value: Value) -> Result<Value, ValueOpError> {
    match map {
        Value::Map(mut values) => {
            values.insert(key.clone(), value);
            Ok(Value::Map(values))
        }
        map => invalid_binary_named("map insert", map, value),
    }
}

pub fn map_remove(map: Value, key: &EcoString) -> Result<Value, ValueOpError> {
    match map {
        Value::Map(mut values) => {
            values.remove(key);
            Ok(Value::Map(values))
        }
        operand => invalid_unary_named("map remove", operand),
    }
}

pub fn map_has_key(map: Value, key: &EcoString) -> Result<Value, ValueOpError> {
    match map {
        Value::Map(values) => Ok(Value::Bool(values.contains_key(key))),
        operand => invalid_unary_named("map has-key", operand),
    }
}

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

#[cfg(not(feature = "thread-safe-ast"))]
use std::marker::PhantomData;

#[cfg(feature = "thread-safe-ast")]
use std::sync::{Arc, LazyLock, Mutex};
#[cfg(not(feature = "thread-safe-ast"))]
use std::{cell::RefCell, rc::Rc};

use serde::{Deserialize, Serialize};

// Variable names are represented by compact usize identities. In the default
// configuration the interner remains thread-local, preserving the fast local
// path and deliberately keeping VarName out of Send and Sync values. The
// opt-in configuration replaces it with one process-global Mutex-protected
// interner so an identity can safely cross threads.
const HASH_INDEX_THRESHOLD: usize = 20;

#[cfg(feature = "thread-safe-ast")]
type InternedName = Arc<str>;
#[cfg(not(feature = "thread-safe-ast"))]
type InternedName = Rc<str>;

#[derive(Default)]
struct VarInterner {
    names: Vec<InternedName>,
    ids: Option<HashMap<InternedName, usize>>,
}

impl VarInterner {
    fn intern(&mut self, name: &str) -> usize {
        if let Some(ids) = &self.ids {
            if let Some(&id) = ids.get(name) {
                return id;
            }
        } else if let Some(id) = self.names.iter().position(|candidate| &**candidate == name) {
            return id;
        }

        let id = self.names.len();
        let name: InternedName = name.into();
        self.names.push(name.clone());
        if let Some(ids) = &mut self.ids {
            ids.insert(name, id);
        } else if self.names.len() > HASH_INDEX_THRESHOLD {
            self.ids = Some(
                self.names
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(id, name)| (name, id))
                    .collect(),
            );
        }
        id
    }

    fn name(&self, id: usize) -> String {
        self.names[id].to_string()
    }
}

#[cfg(feature = "thread-safe-ast")]
static VAR_INTERNER: LazyLock<Mutex<VarInterner>> =
    LazyLock::new(|| Mutex::new(VarInterner::default()));

#[cfg(not(feature = "thread-safe-ast"))]
thread_local! {
    static VAR_INTERNER: RefCell<VarInterner> = RefCell::new(VarInterner::default());
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VarName {
    index: usize,
    #[cfg(not(feature = "thread-safe-ast"))]
    _not_send_sync: PhantomData<Rc<()>>,
}

impl VarName {
    fn from_index(index: usize) -> Self {
        Self {
            index,
            #[cfg(not(feature = "thread-safe-ast"))]
            _not_send_sync: PhantomData,
        }
    }

    pub fn new(name: &str) -> Self {
        Self::from_index(intern_name(name))
    }

    pub fn name(&self) -> String {
        lookup_name(self.index)
    }
}

#[cfg(feature = "thread-safe-ast")]
fn intern_name(name: &str) -> usize {
    VAR_INTERNER
        .lock()
        .expect("variable interner mutex was poisoned")
        .intern(name)
}

#[cfg(not(feature = "thread-safe-ast"))]
fn intern_name(name: &str) -> usize {
    VAR_INTERNER.with(|interner| VarInterner::intern(&mut interner.borrow_mut(), name))
}

#[cfg(feature = "thread-safe-ast")]
fn lookup_name(index: usize) -> String {
    VAR_INTERNER
        .lock()
        .expect("variable interner mutex was poisoned")
        .name(index)
}

#[cfg(not(feature = "thread-safe-ast"))]
fn lookup_name(index: usize) -> String {
    VAR_INTERNER.with(|interner| interner.borrow().name(index))
}

impl From<&str> for VarName {
    fn from(s: &str) -> Self {
        VarName::new(s)
    }
}

impl From<String> for VarName {
    fn from(s: String) -> Self {
        VarName::new(&s)
    }
}

impl From<VarName> for String {
    fn from(var_name: VarName) -> String {
        var_name.name()
    }
}

impl Display for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl Debug for VarName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "VarName::new(\"{}\")", self.name())
    }
}

impl Serialize for VarName {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.name().serialize(serializer)
    }
}

impl<'a> Deserialize<'a> for VarName {
    fn deserialize<D: serde::Deserializer<'a>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(VarName::new(&name))
    }
}

impl From<&VarName> for String {
    fn from(var_name: &VarName) -> String {
        var_name.name()
    }
}

#[cfg(feature = "thread-safe-ast")]
static_assertions::assert_impl_all!(VarName: Send, Sync);
#[cfg(not(feature = "thread-safe-ast"))]
static_assertions::assert_not_impl_any!(VarName: Send, Sync);

#[cfg(test)]
mod tests {
    use std::thread;

    use super::{HASH_INDEX_THRESHOLD, VarInterner, VarName};

    #[test]
    fn interner_promotes_after_the_small_linear_range() {
        let mut interner = VarInterner::default();
        for index in 0..HASH_INDEX_THRESHOLD {
            assert_eq!(interner.intern(&format!("promotion_{index}")), index);
        }
        assert!(interner.ids.is_none());

        assert_eq!(interner.intern("promotion_trigger"), HASH_INDEX_THRESHOLD);
        assert!(interner.ids.is_some());
        for index in 0..HASH_INDEX_THRESHOLD {
            assert_eq!(interner.intern(&format!("promotion_{index}")), index);
        }
        assert_eq!(interner.intern("promotion_trigger"), HASH_INDEX_THRESHOLD);
    }

    #[test]
    fn interning_reuses_ids_and_preserves_insertion_order() {
        let first = VarName::new("variables_test_zeta");
        let duplicate = VarName::new("variables_test_zeta");
        let second = VarName::new("variables_test_alpha");

        assert_eq!(first, duplicate);
        assert_eq!(second.index, first.index + 1);
        assert!(first < second);
        assert_eq!(first.name(), "variables_test_zeta");
        assert_eq!(String::from(&second), "variables_test_alpha");
        assert_eq!(first.to_string(), "variables_test_zeta");
        assert_eq!(
            format!("{first:?}"),
            "VarName::new(\"variables_test_zeta\")"
        );
    }

    #[test]
    fn serde_round_trip_uses_the_variable_name() {
        let original = VarName::new("variables_test_serde");
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: VarName = serde_json::from_str(&serialized).unwrap();

        assert_eq!(serialized, "\"variables_test_serde\"");
        assert_eq!(deserialized, original);
        assert_eq!(deserialized.name(), "variables_test_serde");
    }

    #[cfg(not(feature = "thread-safe-ast"))]
    #[test]
    fn ids_are_thread_local() {
        let forward = thread::spawn(|| {
            let first = VarName::new("variables_test_thread_first");
            let second = VarName::new("variables_test_thread_second");
            assert_eq!(first.name(), "variables_test_thread_first");
            assert_eq!(second.name(), "variables_test_thread_second");
            (first.index, second.index)
        });
        let reverse = thread::spawn(|| {
            let second = VarName::new("variables_test_thread_second");
            let first = VarName::new("variables_test_thread_first");
            assert_eq!(first.name(), "variables_test_thread_first");
            assert_eq!(second.name(), "variables_test_thread_second");
            (first.index, second.index)
        });

        assert_eq!(forward.join().unwrap(), (0, 1));
        assert_eq!(reverse.join().unwrap(), (1, 0));
    }

    #[cfg(feature = "thread-safe-ast")]
    #[test]
    fn ids_are_process_global_and_can_cross_threads() {
        let expected_first = VarName::new("variables_test_thread_first");
        let expected_second = VarName::new("variables_test_thread_second");
        let forward = thread::spawn(|| {
            let first = VarName::new("variables_test_thread_first");
            let second = VarName::new("variables_test_thread_second");
            assert_eq!(first.name(), "variables_test_thread_first");
            assert_eq!(second.name(), "variables_test_thread_second");
            (first, second)
        });
        let reverse = thread::spawn(|| {
            let second = VarName::new("variables_test_thread_second");
            let first = VarName::new("variables_test_thread_first");
            (first, second)
        });

        assert_eq!(
            forward.join().unwrap(),
            (expected_first.clone(), expected_second.clone())
        );
        assert_eq!(
            reverse.join().unwrap(),
            (expected_first.clone(), expected_second.clone())
        );
    }
}

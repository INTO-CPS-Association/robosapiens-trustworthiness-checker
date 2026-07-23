use std::{
    cell::RefCell,
    collections::HashMap,
    fmt::{Debug, Display},
    rc::Rc,
};

use serde::{Deserialize, Serialize};

// Global list of all variables in the system. This is used
// to represent individual variables as indices in the runtime
// instead of strings. This makes variables very cheap to clone,
// order, or compare.
//
// Variable names are assumed to act like atoms in programming languages:
// the only permitted operations are equality, cloning, comparison,
// and hashing (the results of the latter two operations is arbitrary
// but consistent for a given program run). Variables are thread local.
// They can also be converted to and from strings. For all of these operations
// they should act indistinguishably from strings: any case in which this
// global state is observable within these constraints is a bug.
//
// This is related to: https://dl.acm.org/doi/10.5555/646066.756689
// and is a pretty standard technique in both programming languages
// implementations and computer algebra systems.
//
// Note that this means that we leak some memory for each unique
// variable encountered in the system: this is hopefully an acceptable
// trade-off given how significant this is for symbolic computations and
// how unwieldy any solution without global sharing is.
const HASH_INDEX_THRESHOLD: usize = 20;

#[derive(Default)]
struct VarInterner {
    names: Vec<Rc<str>>,
    ids: Option<HashMap<Rc<str>, usize>>,
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
        let name: Rc<str> = Rc::from(name);
        self.names.push(Rc::clone(&name));
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

thread_local! {
    static VAR_INTERNER: RefCell<VarInterner> = RefCell::new(VarInterner::default());
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VarName(usize);

impl VarName {
    pub fn new(name: &str) -> Self {
        VAR_INTERNER.with(|interner| VarName(interner.borrow_mut().intern(name)))
    }

    pub fn name(&self) -> String {
        VAR_INTERNER.with(|interner| interner.borrow().name(self.0))
    }
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
        assert_eq!(second.0, first.0 + 1);
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

    #[test]
    fn ids_are_thread_local() {
        let forward = thread::spawn(|| {
            let first = VarName::new("variables_test_thread_first");
            let second = VarName::new("variables_test_thread_second");
            assert_eq!(first.name(), "variables_test_thread_first");
            assert_eq!(second.name(), "variables_test_thread_second");
            (first.0, second.0)
        });
        let reverse = thread::spawn(|| {
            let second = VarName::new("variables_test_thread_second");
            let first = VarName::new("variables_test_thread_first");
            assert_eq!(first.name(), "variables_test_thread_first");
            assert_eq!(second.name(), "variables_test_thread_second");
            (first.0, second.0)
        });

        assert_eq!(forward.join().unwrap(), (0, 1));
        assert_eq!(reverse.join().unwrap(), (1, 0));
    }
}

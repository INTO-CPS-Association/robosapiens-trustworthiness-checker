use std::{cell::RefCell, fmt::Debug, fmt::Display};

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
thread_local! {
    static VAR_LIST: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VarName(usize);

impl VarName {
    pub fn new(name: &str) -> Self {
        VAR_LIST.with(|var_list| {
            if let Some(pos) = var_list.borrow().iter().position(|x| x == name) {
                VarName(pos)
            } else {
                let pos = var_list.borrow().len();
                var_list.borrow_mut().push(name.into());
                VarName(pos)
            }
        })
    }

    pub fn name(&self) -> String {
        VAR_LIST.with(|var_list| var_list.borrow()[self.0].clone())
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

use std::collections::BTreeMap;

use crate::{Value, VarName};

pub type UntimedInputFileData = BTreeMap<usize, BTreeMap<VarName, Value>>;

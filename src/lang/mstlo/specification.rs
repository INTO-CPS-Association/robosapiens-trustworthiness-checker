use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
};

use mstlo::{FormulaDefinition, SignalIdentifier};

use crate::{Specification, VarName, core::StreamType};

#[derive(Clone, Debug, PartialEq)]
pub struct MstloSpecification {
    formulae: BTreeMap<VarName, FormulaDefinition>,
    var_names: Vec<VarName>,
}

impl MstloSpecification {
    pub fn new(formulae: BTreeMap<VarName, FormulaDefinition>) -> Self {
        let var_names = Self::extract_var_names(formulae.values());
        Self {
            formulae,
            var_names,
        }
    }

    pub fn single(name: VarName, formula: FormulaDefinition) -> Self {
        Self::new(BTreeMap::from([(name, formula)]))
    }

    pub fn formulae(&self) -> &BTreeMap<VarName, FormulaDefinition> {
        &self.formulae
    }

    pub fn into_formulae(self) -> BTreeMap<VarName, FormulaDefinition> {
        self.formulae
    }

    pub fn var_names(&self) -> &[VarName] {
        &self.var_names
    }

    fn extract_var_names<'a>(
        formulae: impl IntoIterator<Item = &'a FormulaDefinition>,
    ) -> Vec<VarName> {
        let mut names = Vec::new();
        for formula in formulae {
            let mut formula = formula.clone();
            names.extend(formula.get_signal_identifiers());
        }
        names.sort();
        names.dedup();
        names.into_iter().map(VarName::new).collect()
    }
}

impl From<FormulaDefinition> for MstloSpecification {
    fn from(formula: FormulaDefinition) -> Self {
        Self::single(VarName::new("out"), formula)
    }
}

impl Specification for MstloSpecification {
    type Expr = FormulaDefinition;

    fn input_vars(&self) -> BTreeSet<VarName> {
        self.var_names.iter().cloned().collect()
    }

    fn output_vars(&self) -> BTreeSet<VarName> {
        self.formulae.keys().cloned().collect()
    }

    fn aux_vars(&self) -> BTreeSet<VarName> {
        BTreeSet::new()
    }

    fn var_expr(&self, var: &VarName) -> Option<Self::Expr> {
        self.formulae.get(var).cloned()
    }

    fn add_input_var(&mut self, var: VarName) {
        if !self.var_names.contains(&var) {
            self.var_names.push(var);
            self.var_names.sort();
        }
    }

    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        self.input_vars()
            .into_iter()
            .chain(self.output_vars())
            .map(|var| (var, StreamType::Any))
            .collect()
    }
}

impl Display for MstloSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, (name, formula)) in self.formulae.iter().enumerate() {
            if idx > 0 {
                writeln!(f)?;
            }
            write!(f, "{name}: {formula}")?;
        }
        Ok(())
    }
}

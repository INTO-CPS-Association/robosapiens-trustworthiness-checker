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
    formula_signals: BTreeMap<VarName, BTreeSet<&'static str>>,
}

impl MstloSpecification {
    pub fn new(formulae: BTreeMap<VarName, FormulaDefinition>) -> Self {
        let formula_signals = Self::extract_formula_signals(&formulae);
        let var_names = formula_signals
            .values()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(VarName::new)
            .collect();
        Self {
            formulae,
            var_names,
            formula_signals,
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

    /// Returns the signals referenced by each named formula.
    pub(crate) fn formula_signals(&self) -> &BTreeMap<VarName, BTreeSet<&'static str>> {
        &self.formula_signals
    }

    fn extract_formula_signals(
        formulae: &BTreeMap<VarName, FormulaDefinition>,
    ) -> BTreeMap<VarName, BTreeSet<&'static str>> {
        formulae
            .iter()
            .map(|(name, formula)| {
                let mut formula = formula.clone();
                let signals = formula.get_signal_identifiers().into_iter().collect();
                (name.clone(), signals)
            })
            .collect()
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

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
};

use anyhow::{Context, anyhow};
use mstlo::{FormulaDefinition, SignalIdentifier, parse_stl};

use crate::{DsrvSpecification, VarName, core::StreamType};

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

impl DsrvSpecification for MstloSpecification {
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

/// Parses an MSTLO property file.
///
/// Each non-empty line must define one named STL property using:
///
/// ```text
/// property_name: stl formula
/// ```
///
/// Lines may contain `#` comments. The STL formula part is parsed by
/// [`mstlo::parse_stl`], so it accepts the same runtime syntax as MSTLO itself,
/// e.g. `x > 5`, `G[0, 10](x > $threshold)`, and `x > 5 && y < 3`.
pub fn parse_named_properties(input: &str) -> anyhow::Result<MstloSpecification> {
    let mut formulae = BTreeMap::new();
    for (idx, raw_line) in input.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line
            .split_once('#')
            .map_or(raw_line, |(line, _)| line)
            .trim();
        if line.is_empty() {
            continue;
        }

        let (name, formula) = line.split_once(':').ok_or_else(|| {
            anyhow!("MSTLO property line {line_no} must have format `name: formula`")
        })?;
        let name = name.trim();
        if name.is_empty() {
            return Err(anyhow!(
                "MSTLO property line {line_no} has an empty property name"
            ));
        }

        let formula = parse_stl(formula.trim())
            .map_err(|err| anyhow!(err.to_string()))
            .with_context(|| {
                format!("Failed to parse MSTLO property `{name}` on line {line_no}")
            })?;
        if formulae.insert(VarName::new(name), formula).is_some() {
            return Err(anyhow!(
                "Duplicate MSTLO property name `{name}` on line {line_no}"
            ));
        }
    }

    if formulae.is_empty() {
        return Err(anyhow!(
            "MSTLO property file did not contain any properties"
        ));
    }
    Ok(MstloSpecification::new(formulae))
}

pub async fn parse_file(path: &str) -> anyhow::Result<MstloSpecification> {
    let input = smol::fs::read_to_string(path)
        .await
        .with_context(|| format!("MSTLO property file `{path}` could not be read"))?;
    parse_named_properties(&input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_named_properties_with_mstlo_parser() {
        let formula = parse_named_properties(
            r#"
            gt: x > 5
            temporal: G[0, 10](x > $threshold) && F[0, 5](y < 3)
            "#,
        )
        .unwrap();

        assert_eq!(
            formula.output_vars(),
            BTreeSet::from([VarName::new("gt"), VarName::new("temporal")])
        );
        assert_eq!(
            formula.input_vars(),
            BTreeSet::from([VarName::new("x"), VarName::new("y")])
        );
    }
}

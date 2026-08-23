use std::{collections::HashSet, sync::Arc};

use super::VarName;

/// An immutable, validated variable layout shared by packed rows.
///
/// The variable slice is shared when a layout is cloned. Layout validation
/// therefore happens once at the boundary where a packed representation is
/// constructed rather than while it is iterated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValidatedLayout {
    variables: Arc<[VarName]>,
}

impl ValidatedLayout {
    pub(crate) fn new(variables: impl IntoIterator<Item = VarName>) -> Result<Self, anyhow::Error> {
        let variables = Arc::<[VarName]>::from(variables.into_iter().collect::<Vec<_>>());
        Self::from_arc(variables)
    }

    pub(crate) fn from_arc(variables: Arc<[VarName]>) -> Result<Self, anyhow::Error> {
        anyhow::ensure!(
            !variables.is_empty(),
            "validated packed layouts must contain at least one variable"
        );
        let mut seen = HashSet::with_capacity(variables.len());
        for variable in variables.iter() {
            anyhow::ensure!(
                seen.insert(variable),
                "output packed layout contains duplicate variable `{variable}`"
            );
        }

        Ok(Self { variables })
    }

    pub(crate) fn variables(&self) -> &[VarName] {
        &self.variables
    }

    pub(crate) fn len(&self) -> usize {
        self.variables.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.variables.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_clones_its_validated_variables() {
        let layout =
            ValidatedLayout::new([VarName::new("layout_x"), VarName::new("layout_y")]).unwrap();
        let clone = layout.clone();

        assert_eq!(layout, clone);
        assert_eq!(layout.variables(), clone.variables());
    }

    #[test]
    fn layout_rejects_duplicate_variables() {
        let error = ValidatedLayout::new([
            VarName::new("layout_duplicate"),
            VarName::new("layout_duplicate"),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("duplicate"));
    }
}

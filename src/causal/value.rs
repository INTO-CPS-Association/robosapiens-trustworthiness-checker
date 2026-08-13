use serde::Serialize;

use crate::core::{DeferrableStreamData, StreamData, Value};

use super::CausalDomain;

/// An ordinary DSRV value accompanied by a causal-domain element.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CausalValue<D: CausalDomain> {
    pub value: Value,
    pub explanation: D,
}

impl<D: CausalDomain> CausalValue<D> {
    pub fn constant(value: Value) -> Self {
        Self {
            value,
            explanation: D::unit(),
        }
    }

    pub fn new(value: Value, explanation: D) -> Self {
        Self { value, explanation }
    }

    pub fn erase(self) -> Value {
        self.value
    }
}

impl<D: CausalDomain> StreamData for CausalValue<D> {
    fn is_no_val(&self) -> bool {
        self.value.is_no_val()
    }
}

impl<D: CausalDomain> DeferrableStreamData for CausalValue<D> {
    fn is_deferred(&self) -> bool {
        self.value.is_deferred()
    }

    fn deferred_value() -> Self {
        Self::constant(Value::Deferred)
    }

    fn no_val_value() -> Self {
        Self::constant(Value::NoVal)
    }
}

#[cfg(test)]
mod tests {
    use crate::causal::{
        CausalDomain, CausalRole, CausalSet, RoleCausalAntichain, RoleCausalSet, TimedAtom,
    };
    use crate::{Value, VarName};

    fn atom(name: &str, tick: u64) -> TimedAtom {
        TimedAtom::new(VarName::new(name), tick)
    }

    #[test]
    fn flat_domain_collapses_alternatives() {
        let explanation = CausalSet::atom(atom("x", 1)).alternative(CausalSet::atom(atom("y", 1)));
        assert_eq!(explanation.support().iter().count(), 2);
    }

    #[test]
    fn antichain_retains_incomparable_alternatives() {
        let explanation = RoleCausalAntichain::atom(atom("x", 1))
            .alternative(RoleCausalAntichain::atom(atom("y", 1)));
        assert_eq!(explanation.alternatives().len(), 2);
    }

    #[test]
    fn antichain_removes_dominated_explanation() {
        let x = RoleCausalAntichain::atom(atom("x", 1));
        let xy = x.clone().joint(RoleCausalAntichain::atom(atom("y", 1)));
        assert_eq!(x.clone().alternative(xy).alternatives(), x.alternatives());
    }

    #[test]
    fn role_distinct_alternatives_remain_incomparable() {
        let alternatives = RoleCausalAntichain::atom(atom("x", 0))
            .alternative(RoleCausalAntichain::atom(atom("x", 0)).used_as(CausalRole::Selection));
        assert_eq!(alternatives.alternatives().len(), 2);
        assert_eq!(
            alternatives.alternatives()[0].causes()[0]
                .roles
                .iter()
                .collect::<Vec<_>>(),
            [CausalRole::Direct]
        );
        assert_eq!(
            alternatives.alternatives()[1].causes()[0]
                .roles
                .iter()
                .collect::<Vec<_>>(),
            [CausalRole::Selection]
        );
    }

    #[test]
    fn reannotation_reclassifies_causes() {
        let explanation = RoleCausalSet::atom(atom("guard", 3)).used_as(CausalRole::Selection);
        assert_eq!(
            explanation.causes()[0].roles.iter().collect::<Vec<_>>(),
            [CausalRole::Selection]
        );
    }

    #[test]
    fn every_causal_use_maps_to_its_domain_annotation() {
        let uses = [
            (CausalRole::Direct, CausalRole::Direct),
            (CausalRole::Selection, CausalRole::Selection),
            (CausalRole::Retention, CausalRole::Retention),
            (CausalRole::Initialization, CausalRole::Initialization),
            (CausalRole::Activation, CausalRole::Activation),
        ];

        for (use_, role) in uses {
            let explanation = RoleCausalSet::atom(atom("x", 0)).used_as(use_);
            assert_eq!(
                explanation.causes()[0].roles.iter().collect::<Vec<_>>(),
                [role]
            );
        }
    }

    #[test]
    fn cause_and_selection_roles_are_preserved() {
        let x = RoleCausalSet::atom(atom("x", 0));
        let explanation = x.clone().joint(x.used_as(CausalRole::Selection));
        assert_eq!(
            explanation.causes()[0].roles.iter().collect::<Vec<_>>(),
            [CausalRole::Direct, CausalRole::Selection]
        );
    }

    #[test]
    fn causal_value_erases_to_ordinary_value() {
        let value = super::CausalValue::<CausalSet>::constant(Value::Bool(true));
        assert_eq!(value.erase(), Value::Bool(true));
    }
}

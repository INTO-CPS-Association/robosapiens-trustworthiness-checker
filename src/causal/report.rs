use std::collections::BTreeMap;

use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};

use crate::{Value, VarName};

use super::{CausalDomain, CausalRole, CausalValue, RoleExplanation, TimedAtom};

/// One external cause occurrence in a normalized causal explanation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct CausalCauseReport {
    pub input: String,
    pub logical_tick: u64,
    pub roles: Vec<CausalRole>,
}

impl From<&TimedAtom> for CausalCauseReport {
    fn from(atom: &TimedAtom) -> Self {
        Self {
            input: atom.input.to_string(),
            logical_tick: atom.logical_tick,
            roles: Vec::new(),
        }
    }
}

impl From<&super::RoleCause> for CausalCauseReport {
    fn from(cause: &super::RoleCause) -> Self {
        Self {
            input: cause.atom.input.to_string(),
            logical_tick: cause.atom.logical_tick,
            roles: cause.roles.iter().collect(),
        }
    }
}

/// One normalized alternative in the external reporting schema.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct CausalExplanationReport {
    pub causes: Vec<CausalCauseReport>,
}

impl From<RoleExplanation> for CausalExplanationReport {
    fn from(explanation: RoleExplanation) -> Self {
        let mut causes = explanation.iter().map(Into::into).collect::<Vec<_>>();
        causes.sort();
        Self { causes }
    }
}

/// A causal result containing all alternative explanations for one value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct CausalResultReport {
    pub alternatives: Vec<CausalExplanationReport>,
}

/// The value state used by the shared causal report schema.
///
/// `Value` is deliberately distinct from `NoVal`: the latter is a runtime
/// absence marker and needs an explicit external representation.
#[derive(Clone, Debug, PartialEq)]
pub enum CausalReportValue {
    Value(Value),
    Deferred { state: &'static str },
    NoVal { state: &'static str },
}

impl CausalReportValue {
    fn requires_json5_encoding(&self) -> bool {
        match self {
            Self::Value(value) => value.requires_json5_encoding(),
            Self::Deferred { .. } | Self::NoVal { .. } => false,
        }
    }
}

impl Serialize for CausalReportValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Value(value) => ReportValue(value).serialize(serializer),
            Self::Deferred { state } | Self::NoVal { state } => {
                serialize_runtime_state(serializer, state)
            }
        }
    }
}

struct ReportValue<'a>(&'a Value);

impl Serialize for ReportValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Value::Int(value) => serializer.serialize_i64(*value),
            Value::Float(value) => serializer.serialize_f64(*value),
            Value::Str(value) => serializer.serialize_str(value),
            Value::Bool(value) => serializer.serialize_bool(*value),
            Value::Unit => serializer.serialize_none(),
            Value::Function(value) => serializer.serialize_str(value.display_source()),
            Value::List(values) | Value::Tuple(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&ReportValue(value))?;
                }
                sequence.end()
            }
            Value::Map(values) => {
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values {
                    map.serialize_entry(key, &ReportValue(value))?;
                }
                map.end()
            }
            Value::Deferred => serialize_runtime_state(serializer, "deferred"),
            Value::NoVal => serialize_runtime_state(serializer, "no_val"),
        }
    }
}

fn serialize_runtime_state<S>(serializer: S, state: &str) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry("state", state)?;
    map.end()
}

/// A causal value converted to the shared external reporting schema.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CausalReport {
    pub value: CausalReportValue,
    pub causality: CausalResultReport,
}

impl<D: CausalDomain> From<CausalValue<D>> for CausalReport {
    fn from(value: CausalValue<D>) -> Self {
        let state = match value.value {
            Value::Deferred => CausalReportValue::Deferred { state: "deferred" },
            Value::NoVal => CausalReportValue::NoVal { state: "no_val" },
            value => CausalReportValue::Value(value),
        };
        Self {
            value: state,
            causality: causality_report(&value.explanation),
        }
    }
}

impl CausalReport {
    pub fn from_value<D: CausalDomain>(value: CausalValue<D>) -> Self {
        value.into()
    }
}

/// A deterministic causal output batch in the shared external schema.
///
/// Values and explanations are deliberately separate so causality cannot
/// collide with a model output variable name.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CausalReportBatch {
    pub values: BTreeMap<String, CausalReportValue>,
    pub causality: BTreeMap<String, CausalResultReport>,
}

impl CausalReportBatch {
    fn requires_json5_encoding(&self) -> bool {
        self.values
            .values()
            .any(CausalReportValue::requires_json5_encoding)
    }
}

/// Normalize one causal-domain element for external reporting.
pub fn causality_report<D: CausalDomain>(explanation: &D) -> CausalResultReport {
    let mut alternatives = explanation
        .report_explanations()
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();
    alternatives.sort();
    CausalResultReport { alternatives }
}

/// Convert a causal output batch to its shared report representation.
pub fn report_batch<D: CausalDomain>(
    values: BTreeMap<VarName, CausalValue<D>>,
) -> CausalReportBatch {
    let mut reports = CausalReportBatch {
        values: BTreeMap::new(),
        causality: BTreeMap::new(),
    };
    for (name, value) in values {
        let report = CausalReport::from_value(value);
        let name = name.to_string();
        reports.values.insert(name.clone(), report.value);
        reports.causality.insert(name, report.causality);
    }
    reports
}

/// Serialize one report batch as strict JSON when possible and JSON5 when a
/// value contains a non-finite float.
pub fn report_batch_json<D: CausalDomain>(
    values: BTreeMap<VarName, CausalValue<D>>,
) -> anyhow::Result<String> {
    let report = report_batch(values);
    crate::core::json::encode_json_or_json5(&report, report.requires_json5_encoding())
        .map_err(|error| error.context("failed to serialize causal report"))
}

/// Serialize one report batch as one deterministic JSON/JSON5 Lines record.
pub fn report_batch_json_line<D: CausalDomain>(
    values: BTreeMap<VarName, CausalValue<D>>,
) -> anyhow::Result<String> {
    report_batch_json(values).map(|json| format!("{json}\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::causal::{CausalSet, RoleCausalAntichain};

    fn atom(name: &str, tick: u64) -> TimedAtom {
        TimedAtom::new(name.into(), tick)
    }

    #[test]
    fn reports_runtime_states_without_using_value_serialization() {
        let no_val = CausalReport::from_value(CausalValue::<CausalSet>::new(
            Value::NoVal,
            CausalSet::atom(atom("missing", 3)),
        ));
        assert_eq!(no_val.value, CausalReportValue::NoVal { state: "no_val" });
        assert_eq!(
            serde_json::to_value(&no_val).unwrap(),
            serde_json::json!({
                "value": {"state": "no_val"},
                "causality": {
                    "alternatives": [{
                        "causes": [{
                            "input": "missing",
                            "logical_tick": 3,
                            "roles": []
                        }]
                    }]
                }
            })
        );

        let deferred =
            CausalReport::from_value(CausalValue::<CausalSet>::constant(Value::Deferred));
        assert_eq!(
            deferred.value,
            CausalReportValue::Deferred { state: "deferred" }
        );
    }

    #[test]
    fn normalizes_causal_domains_to_the_same_schema() {
        let set = CausalValue::new(
            Value::Bool(true),
            CausalSet::atom(atom("a", 0)).alternative(CausalSet::atom(atom("b", 1))),
        );
        let antichain = CausalValue::new(
            Value::Bool(true),
            RoleCausalAntichain::atom(atom("a", 0))
                .alternative(RoleCausalAntichain::atom(atom("b", 1))),
        );
        let set = CausalReport::from_value(set);
        let antichain = CausalReport::from_value(antichain);
        assert_eq!(set.causality.alternatives.len(), 1);
        assert_eq!(antichain.causality.alternatives.len(), 2);
        assert_eq!(
            antichain.causality.alternatives[0].causes[0],
            CausalCauseReport {
                input: "a".to_owned(),
                logical_tick: 0,
                roles: vec![CausalRole::Direct],
            }
        );
    }

    #[test]
    fn report_json_is_deterministic_and_preserves_unit_explanations() {
        let mut values = BTreeMap::new();
        values.insert(
            VarName::new("z"),
            CausalValue::<CausalSet>::constant(Value::Int(2)),
        );
        values.insert(
            VarName::new("a"),
            CausalValue::<CausalSet>::constant(Value::Int(1)),
        );
        let json = report_batch_json(values).unwrap();
        assert_eq!(
            json,
            r#"{"values":{"a":1,"z":2},"causality":{"a":{"alternatives":[{"causes":[]}]},"z":{"alternatives":[{"causes":[]}]}}}"#
        );
    }

    #[test]
    fn report_uses_json5_numbers_for_non_finite_floats() {
        let values = BTreeMap::from([
            (
                VarName::new("infinite"),
                CausalValue::<CausalSet>::constant(Value::Float(f64::INFINITY)),
            ),
            (
                VarName::new("nested"),
                CausalValue::<CausalSet>::constant(Value::List(
                    vec![Value::Float(f64::NEG_INFINITY), Value::Float(f64::NAN)].into(),
                )),
            ),
        ]);

        let encoded = report_batch_json(values).unwrap();
        assert!(encoded.contains("infinite:Infinity"), "{encoded}");
        assert!(encoded.contains("nested:[-Infinity,NaN,]"), "{encoded}");
        assert!(!encoded.contains(r#""Infinity""#));
        assert!(!encoded.contains(r#""NaN""#));
        assert!(!encoded.contains('\n'));
        assert!(serde_json::from_str::<serde_json::Value>(&encoded).is_err());
        assert!(json5::from_str::<serde_json::Value>(&encoded).is_ok());
    }

    #[test]
    fn report_serializes_nested_runtime_states_explicitly() {
        let values = BTreeMap::from([(
            VarName::new("states"),
            CausalValue::<CausalSet>::constant(Value::List(
                vec![Value::Deferred, Value::NoVal].into(),
            )),
        )]);

        let encoded = report_batch_json(values).unwrap();
        assert!(encoded.contains(r#""states":[{"state":"deferred"},{"state":"no_val"}]"#));
        assert!(serde_json::from_str::<serde_json::Value>(&encoded).is_ok());
    }

    #[test]
    fn report_atoms_are_sorted_by_external_names() {
        let z = TimedAtom::new(VarName::new("z"), 0);
        let a = TimedAtom::new(VarName::new("a"), 0);
        let explanation = CausalSet::atom(z).joint(CausalSet::atom(a));
        let report = CausalReport::from_value(CausalValue::new(Value::Bool(false), explanation));

        let names = report.causality.alternatives[0]
            .causes
            .iter()
            .map(|atom| atom.input.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["a", "z"]);
    }

    #[test]
    fn reports_both_roles_on_one_occurrence() {
        use crate::causal::{CausalRole, RoleCausalSet};

        let atom = atom("x", 1);
        let explanation = RoleCausalSet::atom(atom.clone())
            .joint(RoleCausalSet::atom(atom).used_as(CausalRole::Selection));
        let report = CausalReport::from_value(CausalValue::new(Value::Bool(true), explanation));
        assert_eq!(
            report.causality.alternatives[0].causes[0].roles,
            vec![CausalRole::Direct, CausalRole::Selection]
        );
    }
}

use serde::Serialize;

use crate::core::{DeferrableStreamData, JsonStreamValue, StreamData, Value};

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

/// Transport output uses the normalized causal report, rather than the
/// internal domain representation. Inputs are decoded as ordinary values and
/// annotated by the causal runtime with their variable and logical tick.
impl<D: CausalDomain> JsonStreamValue for CausalValue<D> {
    fn decode_json(_payload: &[u8]) -> anyhow::Result<Self> {
        anyhow::bail!("decode causal inputs as ordinary values before annotating them")
    }

    fn decode_mqtt_payload(payload: &[u8]) -> anyhow::Result<Self> {
        Self::decode_json(payload)
    }

    fn encode_json(&self) -> anyhow::Result<String> {
        let report = super::CausalReport::from_value(self.clone());
        crate::core::json::encode_json_or_json5(&report, self.value.requires_json5_encoding())
    }
}

#[cfg(feature = "ros")]
impl<D: CausalDomain> crate::core::RosStreamValue for CausalValue<D> {
    fn open_ros_input(
        _executor: std::rc::Rc<smol::LocalExecutor<'static>>,
        _mapping: std::collections::BTreeMap<String, (String, String)>,
    ) -> anyhow::Result<(
        crate::core::InputStream<Self>,
        crate::io::ros::RosInputControl,
    )> {
        anyhow::bail!("causal inputs must be decoded as ordinary values")
    }

    fn open_reconfigurable_ros_input(
        _executor: std::rc::Rc<smol::LocalExecutor<'static>>,
        _mapping: std::collections::BTreeMap<String, (String, String)>,
    ) -> anyhow::Result<(
        crate::io::ros::RosInputStream<Self>,
        crate::io::ros::RosInputControl,
    )> {
        anyhow::bail!("causal inputs must be decoded as ordinary values")
    }

    fn open_ros_output(
        _executor: std::rc::Rc<smol::LocalExecutor<'static>>,
        _node_name: String,
        _interface: crate::core::OutputInterface,
    ) -> futures::future::LocalBoxFuture<
        'static,
        Result<crate::core::OutputWriter<Self>, crate::core::OutputError>,
    > {
        Box::pin(async {
            Err(crate::core::OutputError::invalid(
                "causal reports do not have a ROS message mapping; use stdout, MQTT, or Redis output",
            ))
        })
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
        CausalDomain, CausalRole, CausalSet, CausalValue, RoleCausalAntichain, RoleCausalSet,
        TimedAtom,
    };
    use crate::core::JsonStreamValue;
    use crate::{Value, VarName};

    fn atom(name: &str, tick: u64) -> TimedAtom {
        TimedAtom::new(VarName::new(name), tick)
    }

    #[test]
    fn transport_reports_preserve_runtime_states_and_non_finite_values() {
        for (value, state) in [(Value::Deferred, "deferred"), (Value::NoVal, "no_val")] {
            let encoded = CausalValue::<CausalSet>::constant(value)
                .encode_json()
                .unwrap();
            let report: serde_json::Value = serde_json::from_str(&encoded).unwrap();
            assert_eq!(report["value"], serde_json::json!({"state": state}));
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let encoded = CausalValue::<CausalSet>::constant(Value::Float(value))
                .encode_json()
                .unwrap();
            #[derive(serde::Deserialize)]
            struct Report {
                value: f64,
            }
            let report: Report = json5::from_str(&encoded).unwrap();
            assert!(if value.is_nan() {
                report.value.is_nan()
            } else {
                report.value == value
            });
        }
    }

    #[cfg(feature = "ros")]
    #[test]
    fn causal_ros_output_reports_the_missing_message_mapping() {
        use crate::core::{OutputInterface, RosStreamValue};
        let error = smol::block_on(CausalValue::<CausalSet>::open_ros_output(
            std::rc::Rc::new(smol::LocalExecutor::new()),
            "causal_test".to_owned(),
            OutputInterface::outputs(std::collections::BTreeSet::new()).unwrap(),
        ))
        .err()
        .expect("causal reports need a ROS message mapping");
        assert!(
            error
                .to_string()
                .contains("causal reports do not have a ROS message mapping")
        );
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

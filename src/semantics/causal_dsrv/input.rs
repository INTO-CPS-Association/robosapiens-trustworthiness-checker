use std::collections::BTreeSet;

use async_stream::try_stream;
use futures::StreamExt;

use crate::causal::{CausalDomain, CausalValue, TimedAtom};
use crate::core::input::into_tick_stream;
use crate::{InputBatch, InputStream, InputUpdate, Specification, Value, VarName};

/// Label every input in a semi-synchronous logical step with `(variable, tick)`.
///
/// Missing declared inputs are made explicit as annotated `NoVal` observations.
/// This is important for stateful operators whose selection depends on the
/// absence of an update.
pub fn annotate_input<D: CausalDomain>(
    input: InputStream<Value>,
    declared_inputs: BTreeSet<VarName>,
) -> InputStream<CausalValue<D>> {
    let mut ticks = into_tick_stream(input);
    Box::pin(try_stream! {
        let mut logical_tick = 0_u64;
        while let Some(tick) = ticks.next().await {
            let tick = tick?;
            for event in &tick {
                if !declared_inputs.contains(&event.variable) {
                    Err(anyhow::anyhow!(
                        "causal input stream emitted undeclared variable `{}`",
                        event.variable
                    ))?;
                }
            }
            let present = tick
                .iter()
                .map(|event| event.variable.clone())
                .collect::<BTreeSet<_>>();
            let mut annotated = tick
                .into_iter()
                .map(|InputUpdate { variable, value }| {
                    let atom = TimedAtom::new(variable.clone(), logical_tick);
                    InputUpdate::new(variable, CausalValue::new(value, D::atom(atom)))
                })
                .collect::<Vec<_>>();
            for var in declared_inputs.difference(&present) {
                let atom = TimedAtom::new(var.clone(), logical_tick);
                annotated.push(InputUpdate::new(
                    var.clone(),
                    CausalValue::new(Value::NoVal, D::atom(atom)),
                ));
            }
            yield InputBatch::tick(annotated)?;
            logical_tick = logical_tick
                .checked_add(1)
                .expect("causal logical tick overflow");
        }
    })
}

/// Annotate an ordinary input stream using the input variables owned by a
/// specification.
pub fn annotate_input_for_spec<D: CausalDomain, S: Specification>(
    input: InputStream<Value>,
    spec: &S,
) -> InputStream<CausalValue<D>> {
    annotate_input::<D>(input, spec.input_vars())
}

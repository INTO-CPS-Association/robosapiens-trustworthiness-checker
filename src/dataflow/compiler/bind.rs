use super::super::plan::*;
use super::super::*;
use std::num::NonZeroU64;

pub(in crate::dataflow) use super::super::error::PlanValidationError;

impl UnboundPlanBody {
    fn for_each_dynamic_spec(&mut self, visit: &mut impl FnMut(&mut UnboundDynamicSpec)) {
        for op in &mut self.nodes {
            match op {
                UnboundOp::Dynamic(spec) => visit(spec),
                UnboundOp::If {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    then_branch.for_each_dynamic_spec(visit);
                    else_branch.for_each_dynamic_spec(visit);
                }
                _ => {}
            }
        }
    }

    pub(in crate::dataflow) fn configure_dynamic_context(
        &mut self,
        current_stream: &VarName,
        input_vars: &[VarName],
        stream_vars: &BTreeSet<VarName>,
    ) {
        self.for_each_dynamic_spec(&mut |spec| match &mut spec.scope {
            DataflowDynamicScope::Automatic => {
                spec.scope = DataflowDynamicScope::Restricted(
                    input_vars
                        .iter()
                        .chain(stream_vars)
                        .filter(|var| *var != current_stream)
                        .cloned()
                        .collect(),
                );
            }
            DataflowDynamicScope::Restricted(_) => {}
        });
    }

    pub(in crate::dataflow) fn inherit_dynamic_context(&mut self, allowed_vars: &[VarName]) {
        self.for_each_dynamic_spec(&mut |spec| {
            let vars = match &spec.scope {
                DataflowDynamicScope::Automatic => allowed_vars.iter().cloned().collect(),
                DataflowDynamicScope::Restricted(vars) => vars
                    .iter()
                    .filter(|var| allowed_vars.contains(var))
                    .cloned()
                    .collect(),
            };
            spec.scope = DataflowDynamicScope::Restricted(vars);
        });
    }

    pub(in crate::dataflow) fn bind(
        mut self,
        recursive_output: Option<VarName>,
        environment: Rc<EnvironmentLayout>,
    ) -> Result<Rc<ExecutablePlan>, PlanValidationError> {
        let environment_vars = environment.keys().cloned().collect::<Vec<_>>();
        self.inherit_dynamic_context(&environment_vars);
        self.validate(false)?;
        let body = bind_body(self, &environment, recursive_output.as_ref())?;
        body.debug_assert_valid(environment.len());
        Ok(Rc::new(Plan::new(body, environment)))
    }

    fn validate(&self, in_function: bool) -> Result<(), PlanValidationError> {
        for op in &self.nodes {
            if in_function && let Some(operator) = op.temporal_operator_name() {
                return Err(PlanValidationError::TemporalFunctionBody { operator });
            }
            match op {
                UnboundOp::If {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    then_branch.validate(in_function)?;
                    else_branch.validate(in_function)?;
                }
                UnboundOp::Function { func } | UnboundOp::DirectFixApply { func, .. } => {
                    func.plan.body.validate(true)?
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(in crate::dataflow) fn free_vars(
        &self,
        recursive_output: Option<&VarName>,
    ) -> BTreeSet<VarName> {
        let mut inputs = BTreeSet::new();
        self.collect_free_vars(&mut inputs, recursive_output);
        inputs
    }

    fn collect_free_vars(
        &self,
        inputs: &mut BTreeSet<VarName>,
        recursive_output: Option<&VarName>,
    ) {
        collect_ref_input(&self.output, inputs, recursive_output);
        for op in &self.nodes {
            op.for_each_operand(|operand| collect_ref_input(operand, inputs, recursive_output));
            match op {
                UnboundOp::If {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    then_branch.collect_free_vars(inputs, recursive_output);
                    else_branch.collect_free_vars(inputs, recursive_output);
                }
                UnboundOp::Function { func } | UnboundOp::DirectFixApply { func, .. } => {
                    let mut captures = func.plan.body.free_vars(recursive_output);
                    for param in &func.params {
                        captures.remove(param);
                    }
                    inputs.extend(captures);
                }
                _ => {}
            }
        }
    }
}

fn bind_body(
    body: UnboundPlanBody,
    environment: &EnvironmentLayout,
    recursive_output: Option<&VarName>,
) -> Result<BoundPlanBody, PlanValidationError> {
    let output = bind_ref(body.output, environment, recursive_output)?;
    let nodes = body
        .nodes
        .into_iter()
        .map(|op| bind_op(op, environment, recursive_output))
        .collect::<Result<Vec<_>, _>>()?;
    let recursive_delays = nodes
        .iter()
        .enumerate()
        .filter(|(_, op)| op.is_recursive_sindex())
        .map(|(index, _)| NodeId::new(index))
        .collect();
    Ok(BoundPlanBody {
        nodes,
        output,
        recursive_delays,
    })
}

fn bind_op(
    op: UnboundOp,
    environment: &EnvironmentLayout,
    recursive_output: Option<&VarName>,
) -> Result<BoundOp, PlanValidationError> {
    macro_rules! r {
        ($value:expr) => {
            bind_ref($value, environment, recursive_output)?
        };
    }
    macro_rules! rs {
        ($values:expr) => {
            $values
                .into_iter()
                .map(|value| bind_ref(value, environment, recursive_output))
                .collect::<Result<_, _>>()?
        };
    }
    macro_rules! body {
        ($value:expr) => {
            bind_body($value, environment, recursive_output)?
        };
    }
    macro_rules! function {
        ($value:expr) => {
            bind_function($value, environment, recursive_output)?
        };
    }

    Ok(match op {
        UnboundOp::Unary { op, arg } => BoundOp::Unary { op, arg: r!(arg) },
        UnboundOp::Binary { op, lhs, rhs } => BoundOp::Binary {
            op,
            lhs: r!(lhs),
            rhs: r!(rhs),
        },
        UnboundOp::If {
            cond,
            then_branch,
            else_branch,
        } => BoundOp::If {
            cond: r!(cond),
            then_branch: body!(then_branch),
            else_branch: body!(else_branch),
        },
        UnboundOp::SIndex { input, offset } if matches!(&input, UnboundRef::External(var) if Some(var) == recursive_output) =>
        {
            let offset =
                NonZeroU64::new(offset).ok_or(PlanValidationError::UnguardedRecursiveOutput)?;
            BoundOp::RecursiveSIndex { offset }
        }
        UnboundOp::SIndex { input, offset } => BoundOp::SIndex {
            input: r!(input),
            offset,
        },
        UnboundOp::RecursiveSIndex { offset } => BoundOp::RecursiveSIndex { offset },
        UnboundOp::Default { input, fallback } => BoundOp::Default {
            input: r!(input),
            fallback: r!(fallback),
        },
        UnboundOp::Init { input, initial } => BoundOp::Init {
            input: r!(input),
            initial: r!(initial),
        },
        UnboundOp::IsDefined { input } => BoundOp::IsDefined { input: r!(input) },
        UnboundOp::When { input } => BoundOp::When { input: r!(input) },
        UnboundOp::Update { base, update } => BoundOp::Update {
            base: r!(base),
            update: r!(update),
        },
        UnboundOp::Latch { value, trigger } => BoundOp::Latch {
            value: r!(value),
            trigger: r!(trigger),
        },
        UnboundOp::List(items) => BoundOp::List(rs!(items)),
        UnboundOp::Tuple(items) => BoundOp::Tuple(rs!(items)),
        UnboundOp::Map(items) => BoundOp::Map(
            items
                .into_iter()
                .map(|(key, value)| Ok((key, bind_ref(value, environment, recursive_output)?)))
                .collect::<Result<_, PlanValidationError>>()?,
        ),
        UnboundOp::LIndex { list, index } => BoundOp::LIndex {
            list: r!(list),
            index: r!(index),
        },
        UnboundOp::LAppend { list, value } => BoundOp::LAppend {
            list: r!(list),
            value: r!(value),
        },
        UnboundOp::LConcat { lhs, rhs } => BoundOp::LConcat {
            lhs: r!(lhs),
            rhs: r!(rhs),
        },
        UnboundOp::LHead { list } => BoundOp::LHead { list: r!(list) },
        UnboundOp::LTail { list } => BoundOp::LTail { list: r!(list) },
        UnboundOp::LLen { list } => BoundOp::LLen { list: r!(list) },
        UnboundOp::MGet { map, key } => BoundOp::MGet { map: r!(map), key },
        UnboundOp::MRemove { map, key } => BoundOp::MRemove { map: r!(map), key },
        UnboundOp::MInsert { map, key, value } => BoundOp::MInsert {
            map: r!(map),
            key,
            value: r!(value),
        },
        UnboundOp::MHasKey { map, key } => BoundOp::MHasKey { map: r!(map), key },
        UnboundOp::TGet { tuple, index } => BoundOp::TGet {
            tuple: r!(tuple),
            index,
        },
        UnboundOp::Dynamic(spec) => BoundOp::Dynamic(BoundDynamicSpec {
            input: r!(spec.input),
            scope: spec.scope,
            mode: spec.mode,
            typed: spec.typed,
        }),
        UnboundOp::Function { func } => BoundOp::Function {
            func: function!(func),
        },
        UnboundOp::Apply { func, args } => BoundOp::Apply {
            func: r!(func),
            args: rs!(args),
        },
        UnboundOp::DirectFixApply { func, args } => BoundOp::DirectFixApply {
            func: function!(func),
            args: rs!(args),
        },
        UnboundOp::RecursiveCall { args } => BoundOp::RecursiveCall { args: rs!(args) },
        UnboundOp::Partial {
            func,
            args,
            display,
        } => BoundOp::Partial {
            func: r!(func),
            args: rs!(args),
            display,
        },
        UnboundOp::Fix { func, display } => BoundOp::Fix {
            func: r!(func),
            display,
        },
        UnboundOp::ListMap { func, list } => BoundOp::ListMap {
            func: r!(func),
            list: r!(list),
        },
        UnboundOp::ListFilter { func, list } => BoundOp::ListFilter {
            func: r!(func),
            list: r!(list),
        },
        UnboundOp::ListFold { func, init, list } => BoundOp::ListFold {
            func: r!(func),
            init: r!(init),
            list: r!(list),
        },
    })
}

fn bind_function(
    function: UnboundFunctionDef,
    environment: &EnvironmentLayout,
    recursive_output: Option<&VarName>,
) -> Result<BoundFunctionDef, PlanValidationError> {
    let mut captures = function.plan.body.free_vars(None);
    for param in &function.params {
        captures.remove(param);
    }

    let capture_names = captures.into_iter().collect::<Vec<_>>();
    let capture_sources = capture_names
        .iter()
        .map(|name| {
            environment
                .get(name)
                .ok_or_else(|| PlanValidationError::UnknownVariable(name.clone()))
        })
        .collect::<Result<_, _>>()?;

    let local_ids = EnvironmentLayout::from_vars(
        capture_names
            .into_iter()
            .chain(function.params.iter().cloned()),
    );
    let body = bind_body(function.plan.body.clone(), &local_ids, recursive_output)?;
    Ok(BoundFunctionDef {
        params: function.params,
        plan: Rc::new(Plan::new(body, Rc::new(local_ids))),
        display: function.display,
        capture_sources,
    })
}

fn bind_ref(
    operand: UnboundRef,
    environment: &EnvironmentLayout,
    recursive_output: Option<&VarName>,
) -> Result<BoundRef, PlanValidationError> {
    Ok(match operand {
        UnboundRef::Const(value) => BoundRef::Const(value),
        UnboundRef::Node(id) => BoundRef::Node(id),
        UnboundRef::External(var) => {
            if Some(&var) == recursive_output {
                return Err(PlanValidationError::UnguardedRecursiveOutput);
            }
            let id = environment
                .get(&var)
                .ok_or_else(|| PlanValidationError::UnknownVariable(var.clone()))?;
            BoundRef::External(id)
        }
    })
}

fn collect_ref_input(
    operand: &UnboundRef,
    inputs: &mut BTreeSet<VarName>,
    recursive_output: Option<&VarName>,
) {
    if let UnboundRef::External(var) = operand
        && Some(var) != recursive_output
    {
        inputs.insert(var.clone());
    }
}

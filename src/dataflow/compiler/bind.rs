use super::super::ir::*;
use super::super::*;
use std::num::NonZeroU64;

pub(in crate::dataflow) use super::super::error::StreamProgramError;

impl UnboundEvaluationGraph {
    fn for_each_dynamic_expression(
        &mut self,
        visit: &mut impl FnMut(&mut UnboundDynamicExpressionSpec),
    ) {
        for op in &mut self.nodes {
            match op {
                UnboundOp::Dynamic(spec) => visit(spec),
                UnboundOp::If {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    then_branch.for_each_dynamic_expression(visit);
                    else_branch.for_each_dynamic_expression(visit);
                }
                _ => {}
            }
        }
    }

    pub(in crate::dataflow) fn resolve_automatic_dynamic_scopes(
        &mut self,
        current_stream: &VarName,
        input_vars: &[VarName],
        stream_vars: &BTreeSet<VarName>,
    ) {
        self.for_each_dynamic_expression(&mut |spec| match &mut spec.scope {
            DynamicExpressionScope::Automatic => {
                spec.scope = DynamicExpressionScope::Restricted {
                    allowed_variables: input_vars
                        .iter()
                        .chain(stream_vars)
                        .filter(|var| *var != current_stream)
                        .cloned()
                        .collect(),
                };
            }
            DynamicExpressionScope::Restricted { .. } => {}
        });
    }

    pub(in crate::dataflow) fn restrict_dynamic_scopes(&mut self, allowed_vars: &[VarName]) {
        self.for_each_dynamic_expression(&mut |spec| {
            let allowed_variables = match &spec.scope {
                DynamicExpressionScope::Automatic => allowed_vars.iter().cloned().collect(),
                DynamicExpressionScope::Restricted { allowed_variables } => allowed_variables
                    .iter()
                    .filter(|var| allowed_vars.contains(var))
                    .cloned()
                    .collect(),
            };
            spec.scope = DynamicExpressionScope::Restricted { allowed_variables };
        });
    }

    fn restrict_dynamic_scopes_to_environment(&mut self, environment: &EnvironmentLayout) {
        self.for_each_dynamic_expression(&mut |spec| {
            let allowed_variables = match &spec.scope {
                DynamicExpressionScope::Automatic => environment.variables().cloned().collect(),
                DynamicExpressionScope::Restricted { allowed_variables } => allowed_variables
                    .iter()
                    .filter(|var| environment.slot(var).is_some())
                    .cloned()
                    .collect(),
            };
            spec.scope = DynamicExpressionScope::Restricted { allowed_variables };
        });
    }

    pub(in crate::dataflow) fn bind_graph(
        mut self,
        recursive_output: Option<VarName>,
        environment: Rc<EnvironmentLayout>,
    ) -> Result<Rc<StreamProgram>, StreamProgramError> {
        self.restrict_dynamic_scopes_to_environment(&environment);
        self.validate(false)?;
        let body = bind_graph(self, &environment, recursive_output.as_ref())?;
        body.debug_assert_valid(environment.len());
        Ok(Rc::new(StreamProgram::new(body, environment)))
    }

    fn validate(&self, in_function: bool) -> Result<(), StreamProgramError> {
        for op in &self.nodes {
            let restricted_function = match op {
                UnboundOp::ListMap { func, .. }
                | UnboundOp::ListFilter { func, .. }
                | UnboundOp::ListFold { func, .. } => Some(func),
                UnboundOp::Partial { func, .. } => Some(func),
                _ => None,
            };
            if let Some(UnboundRef::Node(id)) = restricted_function
                && let Some(UnboundOp::Function { func }) = self.nodes.get(id.index())
                && func.graph.has_temporal_state()
            {
                return Err(StreamProgramError::TemporalFunctionBody {
                    operator: match op {
                        UnboundOp::Partial { .. } => "partial application",
                        _ => "collection callback",
                    },
                });
            }
            if in_function && let Some(operator) = op.temporal_operator_name() {
                return Err(StreamProgramError::TemporalFunctionBody { operator });
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
                UnboundOp::Function { func } => func.graph.validate_persistent_function()?,
                // A direct application owns a persistent nested evaluator, so temporal
                // operators retain their state across outer ticks.
                UnboundOp::DirectApply { func, .. } => func.graph.validate_persistent_function()?,
                UnboundOp::RecursiveApply { func, .. } => func.graph.validate(true)?,
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_persistent_function(&self) -> Result<(), StreamProgramError> {
        for op in &self.nodes {
            match op {
                // Runtime compilation has a fallible evaluation path which is not
                // yet exposed through nested function execution.
                UnboundOp::Dynamic(_) => {
                    return Err(StreamProgramError::TemporalFunctionBody {
                        operator: "dynamic/defer",
                    });
                }
                UnboundOp::If {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    then_branch.validate_persistent_function()?;
                    else_branch.validate_persistent_function()?;
                }
                UnboundOp::Function { func } | UnboundOp::RecursiveApply { func, .. } => {
                    func.graph.validate(true)?;
                }
                UnboundOp::DirectApply { func, .. } => {
                    func.graph.validate_persistent_function()?;
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

    pub(in crate::dataflow) fn same_tick_free_vars(
        &self,
        recursive_output: Option<&VarName>,
    ) -> BTreeSet<VarName> {
        let mut inputs = BTreeSet::new();
        self.collect_same_tick_free_vars(&mut inputs, recursive_output);
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
                UnboundOp::Function { func }
                | UnboundOp::DirectApply { func, .. }
                | UnboundOp::RecursiveApply { func, .. } => {
                    let mut captures = func.graph.free_vars(recursive_output);
                    for param in &func.parameters {
                        captures.remove(param);
                    }
                    inputs.extend(captures);
                }
                _ => {}
            }
        }
    }

    fn collect_same_tick_free_vars(
        &self,
        inputs: &mut BTreeSet<VarName>,
        recursive_output: Option<&VarName>,
    ) {
        collect_ref_input(&self.output, inputs, recursive_output);
        for op in &self.nodes {
            match op {
                UnboundOp::Delay { offset, .. } if *offset > 0 => {}
                _ => op.for_each_operand(|operand| {
                    collect_ref_input(operand, inputs, recursive_output)
                }),
            }
            match op {
                UnboundOp::If {
                    then_branch,
                    else_branch,
                    ..
                } => {
                    then_branch.collect_same_tick_free_vars(inputs, recursive_output);
                    else_branch.collect_same_tick_free_vars(inputs, recursive_output);
                }
                UnboundOp::DirectApply { func, .. } => {
                    let mut captures = func.graph.same_tick_free_vars(recursive_output);
                    for param in &func.parameters {
                        captures.remove(param);
                    }
                    inputs.extend(captures);
                }
                UnboundOp::Function { func } | UnboundOp::RecursiveApply { func, .. } => {
                    let mut captures = func.graph.free_vars(recursive_output);
                    for param in &func.parameters {
                        captures.remove(param);
                    }
                    inputs.extend(captures);
                }
                _ => {}
            }
        }
    }
}

fn bind_graph(
    body: UnboundEvaluationGraph,
    environment: &EnvironmentLayout,
    recursive_output: Option<&VarName>,
) -> Result<BoundEvaluationGraph, StreamProgramError> {
    let output = bind_ref(body.output, environment, recursive_output)?;
    let scalar_signatures = body.scalar_signatures;
    let nodes = body
        .nodes
        .into_iter()
        .map(|op| bind_op(op, environment, recursive_output))
        .collect::<Result<Vec<_>, _>>()?;
    let recursive_delays = nodes
        .iter()
        .enumerate()
        .filter(|(_, op)| op.is_recursive_delay())
        .map(|(index, _)| NodeId::new(index))
        .collect();
    Ok(BoundEvaluationGraph {
        nodes,
        scalar_signatures,
        output,
        recursive_delays,
    })
}

fn bind_op(
    op: UnboundOp,
    environment: &EnvironmentLayout,
    recursive_output: Option<&VarName>,
) -> Result<BoundOp, StreamProgramError> {
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
            bind_graph($value, environment, recursive_output)?
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
        UnboundOp::Delay { input, offset } if matches!(&input, UnboundRef::External(var) if Some(var) == recursive_output) =>
        {
            let offset =
                NonZeroU64::new(offset).ok_or(StreamProgramError::UnguardedRecursiveOutput)?;
            BoundOp::RecursiveDelay { offset }
        }
        UnboundOp::Delay { input, offset } => BoundOp::Delay {
            input: r!(input),
            offset,
        },
        UnboundOp::RecursiveDelay { offset } => BoundOp::RecursiveDelay { offset },
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
                .collect::<Result<_, StreamProgramError>>()?,
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
        UnboundOp::Dynamic(spec) => BoundOp::Dynamic(BoundDynamicExpressionSpec {
            input: r!(spec.input),
            scope: spec.scope,
            mode: spec.mode,
            typing: spec.typing,
        }),
        UnboundOp::Function { func } => BoundOp::Function {
            func: function!(func),
        },
        UnboundOp::Apply { func, args } => BoundOp::Apply {
            func: r!(func),
            args: rs!(args),
        },
        UnboundOp::DirectApply { func, args } => BoundOp::DirectApply {
            func: function!(func),
            args: rs!(args),
        },
        UnboundOp::RecursiveApply { func, args } => BoundOp::RecursiveApply {
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
    function: UnboundFunction,
    environment: &EnvironmentLayout,
    recursive_output: Option<&VarName>,
) -> Result<StreamFunction, StreamProgramError> {
    let mut captures = function.graph.free_vars(None);
    for param in &function.parameters {
        captures.remove(param);
    }

    let capture_names = captures.into_iter().collect::<Vec<_>>();
    let capture_slots = capture_names
        .iter()
        .map(|name| {
            environment
                .slot(name)
                .ok_or_else(|| StreamProgramError::UnknownVariable(name.clone()))
        })
        .collect::<Result<_, _>>()?;

    let local_ids = EnvironmentLayout::from_variables(
        capture_names
            .into_iter()
            .chain(function.parameters.iter().cloned()),
    );
    let body = bind_graph(function.graph.clone(), &local_ids, recursive_output)?;
    Ok(StreamFunction {
        parameters: function.parameters,
        program: Rc::new(StreamProgram::new(body, Rc::new(local_ids))),
        display: function.display,
        capture_slots,
    })
}

fn bind_ref(
    operand: UnboundRef,
    environment: &EnvironmentLayout,
    recursive_output: Option<&VarName>,
) -> Result<BoundRef, StreamProgramError> {
    Ok(match operand {
        UnboundRef::Const(value) => BoundRef::Const(value),
        UnboundRef::Node(id) => BoundRef::Node(id),
        UnboundRef::External(var) => {
            if Some(&var) == recursive_output {
                return Err(StreamProgramError::UnguardedRecursiveOutput);
            }
            let id = environment
                .slot(&var)
                .ok_or_else(|| StreamProgramError::UnknownVariable(var.clone()))?;
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

//! Lowering from DSRV expression views into evaluation graphs.
//!
//! Lowering walks a prepared checked expression. A graph that does not
//! specialise on types ignores them, but still takes each `dynamic` and
//! `defer` occurrence's prepared site.
//!
//! # Previous values where only some ticks run
//!
//! A lazy `if` branch runs only on the ticks that select it, and a `match`
//! arm or guard only on the ticks that select or try it, so a delay inside
//! one would see only those ticks. A previous value of a named stream,
//! `x[n]` with `n > 0`, must instead mean what it means outside them.
//! Lowering an equation therefore lifts each such read out of every lazy
//! branch, arm and guard, including a lambda written inside one, into a
//! stream of its own named `x[n]`, whose program is the delay alone, and the
//! code reads that stream. The lifted stream is evaluated on every tick like
//! any other, which is exactly what an author gets by writing `aux p = x[n]`
//! outside. Only the read moves: whatever the code computes from it,
//! including the history of a computed expression such as `(x + 1)[1]`,
//! stays on the timeline of the ticks that run it.
//!
//! A name a lambda parameter or a pattern binds is not a stream, so the
//! previous value of one inside such code cannot be lifted, and lowering
//! refuses it rather than give it the local timeline. Runtime text has no
//! equation to lift into, so it refuses a stream's previous value there too
//! ([`PreviousValues::refusing`]). Code that runs on every tick, a lambda
//! body outside these included, lifts and refuses nothing.

use super::super::ir::*;
use super::super::*;
use crate::core::UnaryOperator;
use crate::lang::dsrv::ast::{CheckedExpr, CheckedExprRef, ExprView};
use crate::lang::dsrv::patterns::{MatchArm, MatchPattern, PatternKind};
use crate::lang::dsrv::runtime_expression::RuntimeExpressionSite;
use crate::lang::dsrv::type_checker::TCType;

/// What lowering does with a stream's previous value read in code that runs
/// only on some ticks (see the [module documentation](self)): an equation
/// lifts it into a stream of its own, and runtime text refuses it.
pub(in crate::dataflow) struct PreviousValues {
    refuse: bool,
    /// The lifted streams in the order they were first read, so a lifted read
    /// of a lifted stream (`(x[1])[1]`) follows the stream it reads.
    streams: Vec<LiftedPreviousValue>,
    /// The first read that could not be lifted.
    refusal: Option<StreamProgramError>,
}

/// One lifted read: the stream `name` (`x[n]`) delays `source` (`x`).
pub(in crate::dataflow) struct LiftedPreviousValue {
    pub(in crate::dataflow) name: VarName,
    pub(in crate::dataflow) source: VarName,
    pub(in crate::dataflow) graph: UnboundEvaluationGraph,
}

impl PreviousValues {
    /// Lift previous values into streams, as an equation does.
    pub(in crate::dataflow) fn lifting() -> Self {
        Self {
            refuse: false,
            streams: Vec::new(),
            refusal: None,
        }
    }

    /// Refuse every previous value that would need lifting, as runtime text does.
    pub(in crate::dataflow) fn refusing() -> Self {
        Self {
            refuse: true,
            ..Self::lifting()
        }
    }

    /// Whether everything lowered so far could be lowered.
    pub(in crate::dataflow) fn check(&mut self) -> Result<(), StreamProgramError> {
        self.refusal.take().map_or(Ok(()), Err)
    }

    pub(in crate::dataflow) fn into_streams(self) -> Vec<LiftedPreviousValue> {
        self.streams
    }

    fn refuse(&mut self, error: StreamProgramError) {
        self.refusal.get_or_insert(error);
    }

    /// The stream holding `source[offset]`, lifting it on its first read, or
    /// `None` where it is refused. Every read of the same previous value
    /// shares one stream, since a delay evaluated on every tick depends on
    /// nothing but the stream it delays.
    fn read(
        &mut self,
        source: &VarName,
        offset: u64,
        expr: CheckedExprRef<'_>,
        specialise: bool,
    ) -> Option<UnboundRef> {
        if self.refuse {
            self.refuse(StreamProgramError::PreviousValueInRuntimeText {
                stream: source.clone(),
                offset,
            });
            return None;
        }
        let name = VarName::new(&format!("{source}[{offset}]"));
        if !self.streams.iter().any(|stream| stream.name == name) {
            let mut builder = EvaluationGraphBuilder::new(specialise, None);
            let output = builder.push(UnboundOp::Delay {
                input: UnboundRef::External(source.clone()),
                offset,
            });
            builder.sites.assign_from(0, expr);
            self.streams.push(LiftedPreviousValue {
                name: name.clone(),
                source: source.clone(),
                graph: builder.finish(output),
            });
        }
        Some(UnboundRef::External(name))
    }
}

struct EvaluationGraphBuilder<'p> {
    nodes: Vec<UnboundOp>,
    scalar_signatures: Vec<Option<ScalarSignature>>,
    /// Diagnostics-only source attribution, index-aligned with `nodes`.
    sites: super::super::diagnostics::NodeSites,
    /// Whether the graph consults the elaborated types to specialise scalar
    /// operations. An unspecialised graph still carries each `dynamic` and
    /// `defer` node's prepared site, typing included, so source arriving
    /// there is checked as it is everywhere else.
    specialise: bool,
    /// What becomes of a previous value read where only some ticks run.
    previous: Option<&'p mut PreviousValues>,
    /// Whether this graph runs only on some ticks: it is, or is inside, a
    /// lazy `if` branch or a `match` arm or guard.
    selective: bool,
    /// The names lambda parameters and patterns bind around this graph.
    bound: Vec<VarName>,
}

impl<'p> EvaluationGraphBuilder<'p> {
    fn new(specialise: bool, previous: Option<&'p mut PreviousValues>) -> Self {
        Self {
            nodes: Vec::new(),
            scalar_signatures: Vec::new(),
            sites: Default::default(),
            specialise,
            previous,
            selective: false,
            bound: Vec::new(),
        }
    }

    /// The builder of a graph nested in this one: an `if` branch, an arm, or a
    /// function body binding `binds`.
    fn nested(
        &mut self,
        selective: bool,
        binds: impl IntoIterator<Item = VarName>,
    ) -> EvaluationGraphBuilder<'_> {
        let mut bound = self.bound.clone();
        bound.extend(binds);
        EvaluationGraphBuilder {
            selective,
            bound,
            ..EvaluationGraphBuilder::new(self.specialise, self.previous.as_deref_mut())
        }
    }

    /// One nested graph, lowered from `expr`.
    fn lower_nested(
        &mut self,
        expr: CheckedExprRef<'_>,
        selective: bool,
        binds: impl IntoIterator<Item = VarName>,
    ) -> UnboundEvaluationGraph {
        let mut builder = self.nested(selective, binds);
        let output = lower_expression(expr, &mut builder);
        builder.finish(output)
    }

    /// `name[offset]` read where only some ticks run, as a lifted stream, or
    /// `None` where it stays a delay because lowering refused it.
    fn lift_previous_value(
        &mut self,
        name: &VarName,
        offset: u64,
        expr: CheckedExprRef<'_>,
    ) -> Option<UnboundRef> {
        let previous = self.previous.as_deref_mut()?;
        if self.bound.contains(name) {
            previous.refuse(StreamProgramError::PreviousValueOfBoundName {
                name: name.clone(),
                offset,
            });
            return None;
        }
        previous.read(name, offset, expr, self.specialise)
    }

    /// The elaborated type of `expr`, where this graph specialises on types.
    fn scalar_typ<'a>(&self, expr: CheckedExprRef<'a>) -> Option<&'a TCType> {
        self.specialise.then(|| expr.typ())
    }

    fn push(&mut self, op: UnboundOp) -> UnboundRef {
        self.push_with_signature(op, None)
    }

    fn push_with_signature(
        &mut self,
        op: UnboundOp,
        scalar_signature: Option<ScalarSignature>,
    ) -> UnboundRef {
        let id = self.nodes.len();
        self.nodes.push(op);
        self.scalar_signatures.push(scalar_signature);
        self.sites.push_unassigned();
        UnboundRef::Node(NodeId::new(id))
    }

    fn finish(self, output: UnboundRef) -> UnboundEvaluationGraph {
        UnboundEvaluationGraph::new(self.nodes, self.scalar_signatures, output)
            .with_sites(self.sites)
    }
}

/// One arm's guard or body, as a program of the names its pattern binds.
fn lower_arm(
    arm: &MatchArm,
    expr: CheckedExprRef<'_>,
    builder: &mut EvaluationGraphBuilder,
) -> UnboundFunction {
    let parameters: EcoVec<VarName> = arm.pattern.bound_names().into_iter().collect();
    UnboundFunction::new(
        parameters.clone(),
        builder.lower_nested(expr, true, parameters),
        format!("{} -> …", arm.pattern).into(),
    )
}

/// An arm whose body is a constant, which is what `matches` reports.
fn constant_arm(arm: &MatchArm, value: Value, specialise: bool) -> UnboundFunction {
    let builder = EvaluationGraphBuilder::new(specialise, None);
    let output = UnboundRef::Const(value.clone());
    UnboundFunction::new(
        arm.pattern.bound_names().into_iter().collect(),
        builder.finish(output),
        format!("{} -> {}", arm.pattern, value.dsrv_source()).into(),
    )
}

/// Lower runtime text, specialising on its types or not, as its occurrence
/// does. It refuses a previous value it would need to lift.
pub(in crate::dataflow) fn build_runtime_text_graph(
    expr: CheckedExpr,
    specialise: bool,
) -> Result<UnboundEvaluationGraph, StreamProgramError> {
    let mut previous = PreviousValues::refusing();
    let graph = build_equation_graph(expr, specialise, &mut previous);
    previous.check().map(|()| graph)
}

/// Lower an equation's expression, lifting into `previous` the previous
/// values it reads where only some ticks run (see the
/// [module documentation](self)). A graph that does not specialise on types
/// is what `untimed` evaluation runs.
pub(in crate::dataflow) fn build_equation_graph(
    expr: CheckedExpr,
    specialise: bool,
    previous: &mut PreviousValues,
) -> UnboundEvaluationGraph {
    let mut builder = EvaluationGraphBuilder::new(specialise, Some(previous));
    let output = lower_expression(expr.as_ref(), &mut builder);
    builder.finish(output)
}

fn lower_expression(expr: CheckedExprRef<'_>, builder: &mut EvaluationGraphBuilder) -> UnboundRef {
    let start = builder.nodes.len();
    let lowered = lower_expression_nodes(expr, builder);
    builder.sites.assign_from(start, expr);
    lowered
}

fn lower_expression_nodes(
    expr: CheckedExprRef<'_>,
    builder: &mut EvaluationGraphBuilder,
) -> UnboundRef {
    use ExprView::*;

    let result_kind = scalar_kind(builder.scalar_typ(expr));
    match expr.view() {
        Val(value) => UnboundRef::Const(value.clone().into_runtime_value()),
        Var(var) => UnboundRef::External(var.clone()),
        BinOp(lhs, rhs, op) => {
            let signature = scalar_binary_signature(
                builder.scalar_typ(lhs),
                builder.scalar_typ(rhs),
                result_kind,
            );
            let lhs = lower_expression(lhs, builder);
            let rhs = lower_expression(rhs, builder);
            builder.push_with_signature(UnboundOp::Binary { op, lhs, rhs }, signature)
        }
        Cast(value, target) => {
            let op = match target {
                crate::core::StreamType::Float => UnaryOperator::CastFloat,
                crate::core::StreamType::Str => UnaryOperator::CastStr,
                _ => return lower_expression(value, builder),
            };
            lower_unary(builder, op, value, result_kind)
        }
        Ascribe(value, target) => {
            let input = lower_expression(value, builder);
            builder.push(UnboundOp::Ascribe {
                input,
                target: target.clone(),
            })
        }
        Wrap(value, newtype) => {
            let input = lower_expression(value, builder);
            builder.push(UnboundOp::Newtype {
                input,
                operation: NewtypeOp::Wrap(newtype.clone()),
            })
        }
        Unwrap(value, newtype) => {
            let input = lower_expression(value, builder);
            builder.push(UnboundOp::Newtype {
                input,
                operation: NewtypeOp::Unwrap(newtype.clone()),
            })
        }
        Newtype(_, operation) => {
            unreachable!("`{operation}` reached the dataflow compiler before elaboration")
        }
        Not(arg) => lower_unary(builder, UnaryOperator::Not, arg, result_kind),
        Neg(arg) => lower_unary(builder, UnaryOperator::Negate, arg, result_kind),
        Sin(arg) => lower_unary(builder, UnaryOperator::Sin, arg, result_kind),
        Cos(arg) => lower_unary(builder, UnaryOperator::Cos, arg, result_kind),
        Tan(arg) => lower_unary(builder, UnaryOperator::Tan, arg, result_kind),
        Abs(arg) => lower_unary(builder, UnaryOperator::Absolute, arg, result_kind),
        Trunc(arg) => lower_unary(builder, UnaryOperator::Truncate, arg, result_kind),
        Floor(arg) => lower_unary(builder, UnaryOperator::Floor, arg, result_kind),
        Ceil(arg) => lower_unary(builder, UnaryOperator::Ceiling, arg, result_kind),
        Round(arg) => lower_unary(builder, UnaryOperator::Round, arg, result_kind),
        If(cond, then_value, else_value) => {
            let policy = expr.expr().if_policy();
            let cond = lower_expression(cond, builder);
            let selective = builder.selective || policy == IfPolicy::Lazy;
            let then_branch = builder.lower_nested(then_value, selective, []);
            let else_branch = builder.lower_nested(else_value, selective, []);
            builder.push(UnboundOp::If {
                policy,
                cond,
                then_branch,
                else_branch,
            })
        }
        SIndex(input, offset) => {
            let input = lower_expression(input, builder);
            if let UnboundRef::External(name) = &input
                && builder.selective
                && offset > 0
                && let Some(lifted) = builder.lift_previous_value(name, offset, expr)
            {
                return lifted;
            }
            builder.push(UnboundOp::Delay { input, offset })
        }
        Default(input, fallback) => {
            let input = lower_expression(input, builder);
            let fallback = lower_expression(fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        Update(base, update) => {
            let base = lower_expression(base, builder);
            let update = lower_expression(update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        IsDefined(input) => {
            let input = lower_expression(input, builder);
            builder.push(UnboundOp::IsDefined { input })
        }
        When(input) => {
            let input = lower_expression(input, builder);
            builder.push(UnboundOp::When { input })
        }
        Latch(value, trigger) => {
            let value = lower_expression(value, builder);
            let trigger = lower_expression(trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        Init(input, initial) => {
            let input = lower_expression(input, builder);
            let initial = lower_expression(initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        // Elaboration has settled the union, so the graph needs the tag and
        // the payload it is built from, and no schema.
        Constructor(payload, tag, _) => {
            let payload = payload
                .into_iter()
                .next()
                .map(|payload| lower_expression(payload, builder));
            builder.push(UnboundOp::Constructor {
                tag: tag.clone(),
                payload,
            })
        }
        List(items) => {
            let items = lower_expressions(items, builder);
            builder.push(UnboundOp::List(items))
        }
        Tuple(items) => {
            let items = lower_expressions(items, builder);
            builder.push(UnboundOp::Tuple(items))
        }
        Map(items) | Struct(items) | ObjectLiteral(items) => {
            let items = items
                .iter()
                .map(|(key, value)| (key.clone(), lower_expression(value, builder)))
                .collect();
            builder.push(UnboundOp::Map(items))
        }
        LIndex(list, index) => {
            let list = lower_expression(list, builder);
            let index = lower_expression(index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        LAppend(list, value) => {
            let list = lower_expression(list, builder);
            let value = lower_expression(value, builder);
            builder.push(UnboundOp::LAppend { list, value })
        }
        LConcat(lhs, rhs) => {
            let lhs = lower_expression(lhs, builder);
            let rhs = lower_expression(rhs, builder);
            builder.push(UnboundOp::LConcat { lhs, rhs })
        }
        LHead(list) => {
            let list = lower_expression(list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        LTail(list) => {
            let list = lower_expression(list, builder);
            builder.push(UnboundOp::LTail { list })
        }
        LLen(list) => {
            let list = lower_expression(list, builder);
            builder.push(UnboundOp::LLen { list })
        }
        MGet(map, key) => {
            let map = lower_expression(map, builder);
            builder.push(UnboundOp::MGet {
                map,
                key: key.clone(),
            })
        }
        SGet(value, key) => {
            let value = lower_expression(value, builder);
            if let Ok(index) = key.parse::<usize>() {
                builder.push(UnboundOp::TGet {
                    tuple: value,
                    index,
                })
            } else {
                builder.push(UnboundOp::MGet {
                    map: value,
                    key: key.clone(),
                })
            }
        }
        MRemove(map, key) => {
            let map = lower_expression(map, builder);
            builder.push(UnboundOp::MRemove {
                map,
                key: key.clone(),
            })
        }
        MInsert(map, key, value) => {
            let map = lower_expression(map, builder);
            let value = lower_expression(value, builder);
            builder.push(UnboundOp::MInsert {
                map,
                key: key.clone(),
                value,
            })
        }
        // An update is its base with each named field inserted in turn.
        StructUpdate(values, fields) => {
            let (fields, base) = crate::lang::dsrv::ast::struct_update_parts(values, fields);
            let mut map = lower_expression(base, builder);
            for (key, value) in fields {
                let value = lower_expression(value, builder);
                map = builder.push(UnboundOp::MInsert {
                    map,
                    key: key.clone(),
                    value,
                });
            }
            map
        }
        MHasKey(map, key) => {
            let map = lower_expression(map, builder);
            builder.push(UnboundOp::MHasKey {
                map,
                key: key.clone(),
            })
        }
        Dynamic(source, _, scope) => lower_reconfigurable_expression(
            builder,
            source,
            ReconfigurableExpressionScope::from_ast(scope.clone()),
            ReconfigurableExpressionKind::Dynamic,
            expr.runtime_expression().clone(),
        ),
        Defer(source, _, scope) => lower_reconfigurable_expression(
            builder,
            source,
            ReconfigurableExpressionScope::from_ast(scope.clone()),
            ReconfigurableExpressionKind::Deferred,
            expr.runtime_expression().clone(),
        ),
        Lambda(params, body) => {
            let func = lower_function(params.clone(), body, builder);
            builder.push(UnboundOp::Function { func })
        }
        Apply(func, args) => {
            if let Some(value) = try_lower_recursive_apply(func, args.clone(), builder) {
                return value;
            }
            if let Lambda(params, body) = func.view() {
                let function = lower_function(params.clone(), body, builder);
                let args = lower_expressions(args, builder);
                return builder.push(UnboundOp::DirectApply {
                    func: function,
                    args,
                });
            }
            let lowered_func = lower_expression(func, builder);
            let args = lower_expressions(args, builder);
            builder.push(UnboundOp::Apply {
                func: lowered_func,
                args,
            })
        }
        Partial(func, args) => {
            let display = format!("partial({}, ...)", func.expr()).into();
            let lowered_func = lower_expression(func, builder);
            let args = lower_expressions(args, builder);
            builder.push(UnboundOp::Partial {
                func: lowered_func,
                args,
                display,
            })
        }
        Fix(func) => {
            let display = format!("fix({})", func.expr()).into();
            let func = lower_expression(func, builder);
            builder.push(UnboundOp::Fix { func, display })
        }
        LMap(func, list) => {
            let func = lower_expression(func, builder);
            let list = lower_expression(list, builder);
            builder.push(UnboundOp::ListMap { func, list })
        }
        LFilter(func, list) => {
            let func = lower_expression(func, builder);
            let list = lower_expression(list, builder);
            builder.push(UnboundOp::ListFilter { func, list })
        }
        LFold(func, init, list) => {
            let func = lower_expression(func, builder);
            let init = lower_expression(init, builder);
            let list = lower_expression(list, builder);
            builder.push(UnboundOp::ListFold { func, init, list })
        }
        Match(scrutinee, arms, shape) => {
            let scrutinee = lower_expression(scrutinee, builder);
            let children: Vec<CheckedExprRef<'_>> = arms.into_iter().collect();
            let mut place = 0;
            let arms = shape
                .iter()
                .map(|arm| {
                    let guard = arm.guarded.then(|| {
                        let guard = lower_arm(arm, children[place], builder);
                        place += 1;
                        guard
                    });
                    let body = lower_arm(arm, children[place], builder);
                    place += 1;
                    MatchArmProgram {
                        pattern: arm.pattern.clone(),
                        guard,
                        body,
                    }
                })
                .collect();
            builder.push(UnboundOp::Match { scrutinee, arms })
        }
        // `matches` asks whether one arm is selected, so it compiles to a
        // `match` that reports that.
        Matches(scrutinee, guard, pattern) => {
            let span = pattern.span;
            let scrutinee = lower_expression(scrutinee, builder);
            let guard_expr = guard.into_iter().next();
            let arm = MatchArm {
                pattern: pattern.clone(),
                guarded: guard_expr.is_some(),
            };
            let guard = guard_expr.map(|guard| lower_arm(&arm, guard, builder));
            let arms = vec![
                MatchArmProgram {
                    pattern: pattern.clone(),
                    guard,
                    body: constant_arm(&arm, Value::Bool(true), builder.specialise),
                },
                MatchArmProgram {
                    pattern: MatchPattern::new(PatternKind::Wildcard, span),
                    guard: None,
                    body: constant_arm(
                        &MatchArm {
                            pattern: MatchPattern::new(PatternKind::Wildcard, span),
                            guarded: false,
                        },
                        Value::Bool(false),
                        builder.specialise,
                    ),
                },
            ];
            builder.push(UnboundOp::Match { scrutinee, arms })
        }
        MonitoredAt(_, _) | Dist(_, _) => {
            panic!("dataflow semantics does not support distributed AST operations")
        }
    }
}

fn lower_unary(
    builder: &mut EvaluationGraphBuilder,
    op: UnaryOperator,
    arg: CheckedExprRef<'_>,
    output: Option<ScalarKind>,
) -> UnboundRef {
    let signature = scalar_unary_signature(builder.scalar_typ(arg), output);
    let arg = lower_expression(arg, builder);
    builder.push_with_signature(UnboundOp::Unary { op, arg }, signature)
}

fn scalar_kind(typ: Option<&TCType>) -> Option<ScalarKind> {
    match typ? {
        TCType::Int => Some(ScalarKind::Int),
        TCType::Float => Some(ScalarKind::Float),
        TCType::Bool => Some(ScalarKind::Bool),
        _ => None,
    }
}

fn scalar_unary_signature(
    input: Option<&TCType>,
    output: Option<ScalarKind>,
) -> Option<ScalarSignature> {
    Some(ScalarSignature::Unary {
        input: scalar_kind(input)?,
        output: output?,
    })
}

fn scalar_binary_signature(
    left: Option<&TCType>,
    right: Option<&TCType>,
    output: Option<ScalarKind>,
) -> Option<ScalarSignature> {
    Some(ScalarSignature::Binary {
        left: scalar_kind(left)?,
        right: scalar_kind(right)?,
        output: output?,
    })
}

fn lower_expressions<'arena>(
    items: impl IntoIterator<Item = CheckedExprRef<'arena>>,
    builder: &mut EvaluationGraphBuilder,
) -> Vec<UnboundRef> {
    items
        .into_iter()
        .map(|item| lower_expression(item, builder))
        .collect()
}

fn lower_reconfigurable_expression(
    builder: &mut EvaluationGraphBuilder,
    input: CheckedExprRef<'_>,
    scope: ReconfigurableExpressionScope,
    kind: ReconfigurableExpressionKind,
    site: RuntimeExpressionSite,
) -> UnboundRef {
    let specialise = builder.specialise;
    let input = lower_expression(input, builder);
    builder.push(UnboundOp::Reconfigurable(
        UnboundReconfigurableExpressionSpec {
            input,
            scope,
            kind,
            site,
            specialise,
        },
    ))
}

fn lower_function(
    params: EcoVec<(VarName, crate::core::StreamTypeAscription)>,
    body: CheckedExprRef<'_>,
    builder: &mut EvaluationGraphBuilder,
) -> UnboundFunction {
    let params_display = params
        .iter()
        .map(|(name, ascription)| ascription.parameter_display(name))
        .collect::<Vec<_>>()
        .join(", ");
    let display = format!("\\{} -> {}", params_display, body.expr()).into();
    let params: EcoVec<VarName> = params.into_iter().map(|(name, _)| name).collect();
    // A body runs when it is called, which is on the ticks its caller runs.
    let selective = builder.selective;
    let body = builder.lower_nested(body, selective, params.iter().cloned());
    UnboundFunction::new(params, body, display)
}

fn try_lower_recursive_apply<'arena>(
    func: CheckedExprRef<'arena>,
    args: impl IntoIterator<Item = CheckedExprRef<'arena>>,
    builder: &mut EvaluationGraphBuilder,
) -> Option<UnboundRef> {
    use ExprView::*;

    let Fix(fixed_func) = func.view() else {
        return None;
    };
    let Lambda(params, body) = fixed_func.view() else {
        return None;
    };
    let (self_name, _) = params.first()?;

    let function_params = params
        .iter()
        .skip(1)
        .map(|(name, _)| name.clone())
        .collect();
    let selective = builder.selective;
    let binds = params.iter().map(|(name, _)| name.clone());
    let mut body = builder.lower_nested(body, selective, binds);
    specialize_recursive_self_calls(&mut body, self_name);
    let args = lower_expressions(args, builder);
    Some(lower_recursive_apply(
        function_params,
        body,
        func.expr().to_string().into(),
        args,
        builder,
    ))
}

fn lower_recursive_apply(
    params: EcoVec<VarName>,
    body: UnboundEvaluationGraph,
    display: EcoString,
    args: Vec<UnboundRef>,
    builder: &mut EvaluationGraphBuilder,
) -> UnboundRef {
    let function = UnboundFunction::new(params, body, display);
    builder.push(UnboundOp::RecursiveApply {
        func: function,
        args,
    })
}

fn specialize_recursive_self_calls(graph: &mut UnboundEvaluationGraph, self_name: &VarName) {
    for op in &mut graph.nodes {
        specialize_recursive_self_calls_in_op(op, self_name);
    }
}

fn specialize_recursive_self_calls_in_op(op: &mut UnboundOp, self_name: &VarName) {
    match op {
        UnboundOp::Apply { func, args } => {
            if matches!(func, UnboundRef::External(var) if var == self_name) {
                let args = std::mem::take(args);
                *op = UnboundOp::RecursiveCall { args };
            }
        }
        UnboundOp::If {
            then_branch,
            else_branch,
            ..
        } => {
            specialize_recursive_self_calls(then_branch, self_name);
            specialize_recursive_self_calls(else_branch, self_name);
        }
        _ => {}
    }
}

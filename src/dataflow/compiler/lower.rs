//! Lowering from DSRV expression views into evaluation graphs.
//!
//! Checked and unchecked inputs share an AST-owned cursor. Its child cursors
//! preserve the phase and expose checked types only when they are present.

use super::super::ir::*;
use super::super::*;
use crate::core::UnaryOperator;
use crate::lang::dsrv::ast::{CheckedExpr, ExprCursor, ExprView};

struct EvaluationGraphBuilder {
    nodes: Vec<UnboundOp>,
}

impl EvaluationGraphBuilder {
    fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    fn push(&mut self, op: UnboundOp) -> UnboundRef {
        let id = self.nodes.len();
        self.nodes.push(op);
        UnboundRef::Node(NodeId::new(id))
    }

    fn finish(self, output: UnboundRef) -> UnboundEvaluationGraph {
        UnboundEvaluationGraph::new(self.nodes, output)
    }
}

fn lower_branch(expr: ExprCursor<'_>) -> UnboundEvaluationGraph {
    let mut builder = EvaluationGraphBuilder::new();
    let output = lower_expression(expr, &mut builder);
    builder.finish(output)
}

pub(in crate::dataflow) fn build_expression_graph(expr: Expr) -> UnboundEvaluationGraph {
    build_graph_from_cursor(ExprCursor::unchecked(expr.as_ref()))
}

pub(in crate::dataflow) fn build_checked_expression_graph(
    expr: CheckedExpr,
) -> UnboundEvaluationGraph {
    build_graph_from_cursor(expr.as_ref().erased())
}

fn build_graph_from_cursor(expr: ExprCursor<'_>) -> UnboundEvaluationGraph {
    let mut builder = EvaluationGraphBuilder::new();
    let output = lower_expression(expr, &mut builder);
    builder.finish(output)
}

fn lower_expression(expr: ExprCursor<'_>, builder: &mut EvaluationGraphBuilder) -> UnboundRef {
    use ExprView::*;

    match expr.view() {
        Val(value) => UnboundRef::Const(value.clone()),
        Var(var) => UnboundRef::External(var.clone()),
        BinOp(lhs, rhs, op) => {
            let lhs = lower_expression(lhs, builder);
            let rhs = lower_expression(rhs, builder);
            builder.push(UnboundOp::Binary { op, lhs, rhs })
        }
        Not(arg) => lower_unary(builder, UnaryOperator::Not, arg),
        Neg(arg) => lower_unary(builder, UnaryOperator::Negate, arg),
        Sin(arg) => lower_unary(builder, UnaryOperator::Sin, arg),
        Cos(arg) => lower_unary(builder, UnaryOperator::Cos, arg),
        Tan(arg) => lower_unary(builder, UnaryOperator::Tan, arg),
        Abs(arg) => lower_unary(builder, UnaryOperator::Absolute, arg),
        If(cond, then_value, else_value) => {
            let cond = lower_expression(cond, builder);
            let then_branch = lower_branch(then_value);
            let else_branch = lower_branch(else_value);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        SIndex(input, offset) => {
            let input = lower_expression(input, builder);
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
        MHasKey(map, key) => {
            let map = lower_expression(map, builder);
            builder.push(UnboundOp::MHasKey {
                map,
                key: key.clone(),
            })
        }
        Dynamic(source, _, scope) => lower_dynamic_expression(
            builder,
            source,
            DynamicExpressionScope::from_ast(scope.clone()),
            DynamicExpressionMode::Dynamic,
            expr.shared_type_environment()
                .zip(expr.typ())
                .map(|(environment, expected_type)| DynamicExpressionTyping {
                    environment: Rc::clone(environment),
                    expected_type: expected_type.clone(),
                }),
        ),
        Defer(source, _, scope) => lower_dynamic_expression(
            builder,
            source,
            DynamicExpressionScope::from_ast(scope.clone()),
            DynamicExpressionMode::Defer,
            expr.shared_type_environment()
                .zip(expr.typ())
                .map(|(environment, expected_type)| DynamicExpressionTyping {
                    environment: Rc::clone(environment),
                    expected_type: expected_type.clone(),
                }),
        ),
        Lambda(params, body) => {
            let func = lower_function(params.clone(), body);
            builder.push(UnboundOp::Function { func })
        }
        Apply(func, args) => {
            if let Some(value) = try_lower_recursive_apply(func, args.clone(), builder) {
                return value;
            }
            if let Lambda(params, body) = func.view() {
                let function = lower_function(params.clone(), body);
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
        MonitoredAt(_, _) | Dist(_, _) => {
            panic!("dataflow semantics does not support distributed AST operations")
        }
    }
}

fn lower_unary(
    builder: &mut EvaluationGraphBuilder,
    op: UnaryOperator,
    arg: ExprCursor<'_>,
) -> UnboundRef {
    let arg = lower_expression(arg, builder);
    builder.push(UnboundOp::Unary { op, arg })
}

fn lower_expressions<'arena>(
    items: impl IntoIterator<Item = ExprCursor<'arena>>,
    builder: &mut EvaluationGraphBuilder,
) -> Vec<UnboundRef> {
    items
        .into_iter()
        .map(|item| lower_expression(item, builder))
        .collect()
}

fn lower_dynamic_expression(
    builder: &mut EvaluationGraphBuilder,
    input: ExprCursor<'_>,
    scope: DynamicExpressionScope,
    mode: DynamicExpressionMode,
    typing: Option<DynamicExpressionTyping>,
) -> UnboundRef {
    let input = lower_expression(input, builder);
    builder.push(UnboundOp::Dynamic(UnboundDynamicExpressionSpec {
        input,
        scope,
        mode,
        typing,
    }))
}

fn lower_function(params: EcoVec<(VarName, StreamType)>, body: ExprCursor<'_>) -> UnboundFunction {
    let params_display = params
        .iter()
        .map(|(name, typ)| format!("{}: {}", name, typ))
        .collect::<Vec<_>>()
        .join(", ");
    let display = format!("\\{} -> {}", params_display, body.expr()).into();
    let params = params.into_iter().map(|(name, _)| name).collect();
    let body = build_graph_from_cursor(body);
    UnboundFunction::new(params, body, display)
}

fn try_lower_recursive_apply<'arena>(
    func: ExprCursor<'arena>,
    args: impl IntoIterator<Item = ExprCursor<'arena>>,
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
    let mut body = build_graph_from_cursor(body);
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

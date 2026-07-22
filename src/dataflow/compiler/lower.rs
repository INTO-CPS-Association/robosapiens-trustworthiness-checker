//! Lowering from DSRV expression views into executable dataflow plans.
//!
//! Checked and unchecked inputs share an AST-owned cursor. Its child cursors
//! preserve the phase and expose annotations only when they are present.

use super::super::plan::*;
use super::super::*;
use crate::lang::dsrv::ast::{CheckedExpr, ExprCursor, ExprView, SBinOp};
use crate::lang::dsrv::type_checker::{TCType, TypeInfo};

struct PlanBuilder {
    nodes: Vec<UnboundOp>,
}

impl PlanBuilder {
    fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    fn push(&mut self, op: UnboundOp) -> UnboundRef {
        let id = self.nodes.len();
        self.nodes.push(op);
        UnboundRef::Node(NodeId::new(id))
    }

    fn finish(self, output: UnboundRef) -> UnboundPlanBody {
        UnboundPlanBody::new(self.nodes, output)
    }
}

fn lower_branch(expr: ExprCursor<'_>) -> UnboundPlanBody {
    let mut builder = PlanBuilder::new();
    let output = lower_expr(expr, &mut builder);
    builder.finish(output)
}

pub(in crate::dataflow) fn lower_expr_plan(expr: Expr) -> UnboundPlanBody {
    lower_expr_ref_plan(ExprCursor::unchecked(expr.as_ref()))
}

pub(in crate::dataflow) fn lower_checked_expr_plan(expr: CheckedExpr) -> UnboundPlanBody {
    lower_expr_ref_plan(expr.as_ref().erased())
}

fn lower_expr_ref_plan(expr: ExprCursor<'_>) -> UnboundPlanBody {
    let mut builder = PlanBuilder::new();
    let output = lower_expr(expr, &mut builder);
    builder.finish(output)
}

fn lower_expr(expr: ExprCursor<'_>, builder: &mut PlanBuilder) -> UnboundRef {
    use ExprView::*;

    match expr.view() {
        Val(value) => UnboundRef::Const(value.clone()),
        Var(var) => UnboundRef::External(var.clone()),
        BinOp(lhs, rhs, op) => {
            let lhs = lower_expr(lhs, builder);
            let rhs = lower_expr(rhs, builder);
            builder.push(UnboundOp::Binary {
                op: lower_binary_op(op.clone()),
                lhs,
                rhs,
            })
        }
        Not(arg) => lower_unary(builder, DataflowUnaryOp::Not, arg),
        Neg(arg) => lower_unary(builder, DataflowUnaryOp::Neg, arg),
        Sin(arg) => lower_unary(builder, DataflowUnaryOp::Sin, arg),
        Cos(arg) => lower_unary(builder, DataflowUnaryOp::Cos, arg),
        Tan(arg) => lower_unary(builder, DataflowUnaryOp::Tan, arg),
        Abs(arg) => lower_unary(builder, DataflowUnaryOp::Abs, arg),
        If(cond, then_value, else_value) => {
            let cond = lower_expr(cond, builder);
            let then_branch = lower_branch(then_value);
            let else_branch = lower_branch(else_value);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        SIndex(input, offset) => {
            let input = lower_expr(input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        Default(input, fallback) => {
            let input = lower_expr(input, builder);
            let fallback = lower_expr(fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        Update(base, update) => {
            let base = lower_expr(base, builder);
            let update = lower_expr(update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        IsDefined(input) => {
            let input = lower_expr(input, builder);
            builder.push(UnboundOp::IsDefined { input })
        }
        When(input) => {
            let input = lower_expr(input, builder);
            builder.push(UnboundOp::When { input })
        }
        Latch(value, trigger) => {
            let value = lower_expr(value, builder);
            let trigger = lower_expr(trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        Init(input, initial) => {
            let input = lower_expr(input, builder);
            let initial = lower_expr(initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        List(items) => {
            let items = lower_exprs(items, builder);
            builder.push(UnboundOp::List(items))
        }
        Tuple(items) => {
            let items = lower_exprs(items, builder);
            builder.push(UnboundOp::Tuple(items))
        }
        Map(items) | Struct(items) | ObjectLiteral(items) => {
            let items = items
                .iter()
                .map(|(key, value)| (key.clone(), lower_expr(value, builder)))
                .collect();
            builder.push(UnboundOp::Map(items))
        }
        LIndex(list, index) => {
            let list = lower_expr(list, builder);
            let index = lower_expr(index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        LAppend(list, value) => {
            let list = lower_expr(list, builder);
            let value = lower_expr(value, builder);
            builder.push(UnboundOp::LAppend { list, value })
        }
        LConcat(lhs, rhs) => {
            let lhs = lower_expr(lhs, builder);
            let rhs = lower_expr(rhs, builder);
            builder.push(UnboundOp::LConcat { lhs, rhs })
        }
        LHead(list) => {
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        LTail(list) => {
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::LTail { list })
        }
        LLen(list) => {
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::LLen { list })
        }
        MGet(map, key) => {
            let map = lower_expr(map, builder);
            builder.push(UnboundOp::MGet {
                map,
                key: key.clone(),
            })
        }
        SGet(value, key) => {
            let value = lower_expr(value, builder);
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
            let map = lower_expr(map, builder);
            builder.push(UnboundOp::MRemove {
                map,
                key: key.clone(),
            })
        }
        MInsert(map, key, value) => {
            let map = lower_expr(map, builder);
            let value = lower_expr(value, builder);
            builder.push(UnboundOp::MInsert {
                map,
                key: key.clone(),
                value,
            })
        }
        MHasKey(map, key) => {
            let map = lower_expr(map, builder);
            builder.push(UnboundOp::MHasKey {
                map,
                key: key.clone(),
            })
        }
        Dynamic(source, _, scope) => lower_dynamic_expr(
            builder,
            source,
            DataflowDynamicScope::from_ast(scope.clone()),
            DataflowDynamicMode::Dynamic,
            expr.shared_type_info()
                .zip(expr.typ())
                .map(|(info, typ)| (Rc::clone(info), typ.clone())),
        ),
        Defer(source, _, scope) => lower_dynamic_expr(
            builder,
            source,
            DataflowDynamicScope::from_ast(scope.clone()),
            DataflowDynamicMode::Defer,
            expr.shared_type_info()
                .zip(expr.typ())
                .map(|(info, typ)| (Rc::clone(info), typ.clone())),
        ),
        Lambda(params, body) => {
            let func = lower_function(params.clone(), body);
            builder.push(UnboundOp::Function { func })
        }
        Apply(func, args) => {
            if let Some(value) = lower_direct_fix_apply(func, args.clone(), builder) {
                return value;
            }
            if let Lambda(params, body) = func.view() {
                let function = lower_function(params.clone(), body);
                let args = lower_exprs(args, builder);
                return builder.push(UnboundOp::DirectApply {
                    func: function,
                    args,
                });
            }
            let lowered_func = lower_expr(func, builder);
            let args = lower_exprs(args, builder);
            builder.push(UnboundOp::Apply {
                func: lowered_func,
                args,
            })
        }
        Partial(func, args) => {
            let display = format!("partial({}, ...)", func.expr()).into();
            let lowered_func = lower_expr(func, builder);
            let args = lower_exprs(args, builder);
            builder.push(UnboundOp::Partial {
                func: lowered_func,
                args,
                display,
            })
        }
        Fix(func) => {
            let display = format!("fix({})", func.expr()).into();
            let func = lower_expr(func, builder);
            builder.push(UnboundOp::Fix { func, display })
        }
        LMap(func, list) => {
            let func = lower_expr(func, builder);
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::ListMap { func, list })
        }
        LFilter(func, list) => {
            let func = lower_expr(func, builder);
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::ListFilter { func, list })
        }
        LFold(func, init, list) => {
            let func = lower_expr(func, builder);
            let init = lower_expr(init, builder);
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::ListFold { func, init, list })
        }
        MonitoredAt(_, _) | Dist(_, _) => {
            panic!("dataflow semantics does not support distributed AST operations")
        }
    }
}

fn lower_unary(builder: &mut PlanBuilder, op: DataflowUnaryOp, arg: ExprCursor<'_>) -> UnboundRef {
    let arg = lower_expr(arg, builder);
    builder.push(UnboundOp::Unary { op, arg })
}

fn lower_exprs<'arena>(
    items: impl IntoIterator<Item = ExprCursor<'arena>>,
    builder: &mut PlanBuilder,
) -> Vec<UnboundRef> {
    items
        .into_iter()
        .map(|item| lower_expr(item, builder))
        .collect()
}

fn lower_dynamic_expr(
    builder: &mut PlanBuilder,
    input: ExprCursor<'_>,
    scope: DataflowDynamicScope,
    mode: DataflowDynamicMode,
    typed: Option<(Rc<TypeInfo>, TCType)>,
) -> UnboundRef {
    let input = lower_expr(input, builder);
    builder.push(UnboundOp::Dynamic(UnboundDynamicSpec {
        input,
        scope,
        mode,
        typed,
    }))
}

fn lower_function(
    params: EcoVec<(VarName, StreamType)>,
    body: ExprCursor<'_>,
) -> UnboundFunctionDef {
    let params_display = params
        .iter()
        .map(|(name, typ)| format!("{}: {}", name, typ))
        .collect::<Vec<_>>()
        .join(", ");
    let display = format!("\\{} -> {}", params_display, body.expr()).into();
    let params = params.into_iter().map(|(name, _)| name).collect();
    let body = lower_expr_ref_plan(body);
    UnboundFunctionDef::new(params, body, display)
}

fn lower_direct_fix_apply<'arena>(
    func: ExprCursor<'arena>,
    args: impl IntoIterator<Item = ExprCursor<'arena>>,
    builder: &mut PlanBuilder,
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
    let mut body = lower_expr_ref_plan(body);
    specialize_recursive_self_calls(&mut body, self_name);
    let args = lower_exprs(args, builder);
    Some(lower_direct_recursive_apply(
        function_params,
        body,
        func.expr().to_string().into(),
        args,
        builder,
    ))
}

fn lower_direct_recursive_apply(
    params: EcoVec<VarName>,
    body: UnboundPlanBody,
    display: EcoString,
    args: Vec<UnboundRef>,
    builder: &mut PlanBuilder,
) -> UnboundRef {
    let function = UnboundFunctionDef::new(params, body, display);
    builder.push(UnboundOp::DirectFixApply {
        func: function,
        args,
    })
}

fn specialize_recursive_self_calls(plan: &mut UnboundPlanBody, self_name: &VarName) {
    for op in &mut plan.nodes {
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

fn lower_binary_op(op: SBinOp) -> DataflowBinaryOp {
    use crate::lang::dsrv::ast::{BoolBinOp, CompBinOp, NumericalBinOp, SBinOp, StrBinOp};
    match op {
        SBinOp::NOp(NumericalBinOp::Add) => DataflowBinaryOp::Add,
        SBinOp::NOp(NumericalBinOp::Sub) => DataflowBinaryOp::Sub,
        SBinOp::NOp(NumericalBinOp::Mul) => DataflowBinaryOp::Mul,
        SBinOp::NOp(NumericalBinOp::Div) => DataflowBinaryOp::Div,
        SBinOp::NOp(NumericalBinOp::Mod) => DataflowBinaryOp::Mod,
        SBinOp::BOp(BoolBinOp::Or) => DataflowBinaryOp::Or,
        SBinOp::BOp(BoolBinOp::And) => DataflowBinaryOp::And,
        SBinOp::BOp(BoolBinOp::Impl) => DataflowBinaryOp::Impl,
        SBinOp::SOp(StrBinOp::Concat) => DataflowBinaryOp::Concat,
        SBinOp::COp(CompBinOp::Eq) => DataflowBinaryOp::Eq,
        SBinOp::COp(CompBinOp::Le) => DataflowBinaryOp::Le,
        SBinOp::COp(CompBinOp::Lt) => DataflowBinaryOp::Lt,
        SBinOp::COp(CompBinOp::Ge) => DataflowBinaryOp::Ge,
        SBinOp::COp(CompBinOp::Gt) => DataflowBinaryOp::Gt,
    }
}

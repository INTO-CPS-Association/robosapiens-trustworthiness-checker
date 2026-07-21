//! Lowering from DSRV expression views into executable dataflow plans.
//!
//! Checked and unchecked inputs share the same [`ExprRef`](crate::lang::dsrv::ast::ExprRef)
//! traversal; checked lowering additionally looks up immutable type annotations.

use super::super::plan::*;
use super::super::*;
use crate::lang::dsrv::ast::{CheckedExpr, ExprRef, ExprRefs, ExprView, SBinOp, TypeAnnotations};
use crate::lang::dsrv::type_checker::{TCType, TypeInfo};

struct PlanBuilder<'annotations> {
    nodes: Vec<UnboundOp>,
    annotations: Option<&'annotations TypeAnnotations>,
}

impl<'annotations> PlanBuilder<'annotations> {
    fn new(annotations: Option<&'annotations TypeAnnotations>) -> Self {
        Self {
            nodes: Vec::new(),
            annotations,
        }
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

fn lower_branch(
    expr: ExprRef<'_>,
    annotations: Option<&TypeAnnotations>,
    build: impl FnOnce(ExprRef<'_>, &mut PlanBuilder<'_>) -> UnboundRef,
) -> UnboundPlanBody {
    let mut builder = PlanBuilder::new(annotations);
    let output = build(expr, &mut builder);
    builder.finish(output)
}

pub(in crate::dataflow) fn lower_expr_plan(expr: Expr) -> UnboundPlanBody {
    lower_expr_ref_plan(expr.as_ref(), None)
}

pub(in crate::dataflow) fn lower_checked_expr_plan(expr: CheckedExpr) -> UnboundPlanBody {
    let (expr, checked) = expr.into_parts();
    lower_expr_ref_plan(expr.as_ref(), Some(&checked))
}

fn lower_expr_ref_plan(
    expr: ExprRef<'_>,
    annotations: Option<&TypeAnnotations>,
) -> UnboundPlanBody {
    let mut builder = PlanBuilder::new(annotations);
    let output = lower_expr(expr, &mut builder);
    builder.finish(output)
}

fn lower_expr(expr: ExprRef<'_>, builder: &mut PlanBuilder<'_>) -> UnboundRef {
    match expr.view() {
        ExprView::Val(value) => UnboundRef::Const(value.clone()),
        ExprView::Var(var) => UnboundRef::External(var.clone()),
        ExprView::BinOp(lhs, rhs, op) => {
            let lhs = lower_expr(lhs, builder);
            let rhs = lower_expr(rhs, builder);
            builder.push(UnboundOp::Binary {
                op: lower_binary_op(op.clone()),
                lhs,
                rhs,
            })
        }
        ExprView::Not(arg) => lower_unary(builder, DataflowUnaryOp::Not, arg),
        ExprView::Neg(arg) => lower_unary(builder, DataflowUnaryOp::Neg, arg),
        ExprView::Sin(arg) => lower_unary(builder, DataflowUnaryOp::Sin, arg),
        ExprView::Cos(arg) => lower_unary(builder, DataflowUnaryOp::Cos, arg),
        ExprView::Tan(arg) => lower_unary(builder, DataflowUnaryOp::Tan, arg),
        ExprView::Abs(arg) => lower_unary(builder, DataflowUnaryOp::Abs, arg),
        ExprView::If(cond, then_value, else_value) => {
            let cond = lower_expr(cond, builder);
            let then_branch = lower_branch(then_value, builder.annotations, lower_expr);
            let else_branch = lower_branch(else_value, builder.annotations, lower_expr);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        ExprView::SIndex(input, offset) => {
            let input = lower_expr(input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        ExprView::Default(input, fallback) => {
            let input = lower_expr(input, builder);
            let fallback = lower_expr(fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        ExprView::Update(base, update) => {
            let base = lower_expr(base, builder);
            let update = lower_expr(update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        ExprView::IsDefined(input) => {
            let input = lower_expr(input, builder);
            builder.push(UnboundOp::IsDefined { input })
        }
        ExprView::When(input) => {
            let input = lower_expr(input, builder);
            builder.push(UnboundOp::When { input })
        }
        ExprView::Latch(value, trigger) => {
            let value = lower_expr(value, builder);
            let trigger = lower_expr(trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        ExprView::Init(input, initial) => {
            let input = lower_expr(input, builder);
            let initial = lower_expr(initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        ExprView::List(items) => {
            let items = lower_exprs(items, builder);
            builder.push(UnboundOp::List(items))
        }
        ExprView::Tuple(items) => {
            let items = lower_exprs(items, builder);
            builder.push(UnboundOp::Tuple(items))
        }
        ExprView::Map(items) | ExprView::Struct(items) | ExprView::ObjectLiteral(items) => {
            let items = items
                .iter()
                .map(|(key, value)| (key.clone(), lower_expr(value, builder)))
                .collect();
            builder.push(UnboundOp::Map(items))
        }
        ExprView::LIndex(list, index) => {
            let list = lower_expr(list, builder);
            let index = lower_expr(index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        ExprView::LAppend(list, value) => {
            let list = lower_expr(list, builder);
            let value = lower_expr(value, builder);
            builder.push(UnboundOp::LAppend { list, value })
        }
        ExprView::LConcat(lhs, rhs) => {
            let lhs = lower_expr(lhs, builder);
            let rhs = lower_expr(rhs, builder);
            builder.push(UnboundOp::LConcat { lhs, rhs })
        }
        ExprView::LHead(list) => {
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        ExprView::LTail(list) => {
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::LTail { list })
        }
        ExprView::LLen(list) => {
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::LLen { list })
        }
        ExprView::MGet(map, key) => {
            let map = lower_expr(map, builder);
            builder.push(UnboundOp::MGet {
                map,
                key: key.clone(),
            })
        }
        ExprView::SGet(value, key) => {
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
        ExprView::MRemove(map, key) => {
            let map = lower_expr(map, builder);
            builder.push(UnboundOp::MRemove {
                map,
                key: key.clone(),
            })
        }
        ExprView::MInsert(map, key, value) => {
            let map = lower_expr(map, builder);
            let value = lower_expr(value, builder);
            builder.push(UnboundOp::MInsert {
                map,
                key: key.clone(),
                value,
            })
        }
        ExprView::MHasKey(map, key) => {
            let map = lower_expr(map, builder);
            builder.push(UnboundOp::MHasKey {
                map,
                key: key.clone(),
            })
        }
        ExprView::Dynamic(source, _, scope) => lower_dynamic_expr(
            builder,
            source,
            DataflowDynamicScope::from_ast(scope.clone()),
            DataflowDynamicMode::Dynamic,
            builder
                .annotations
                .map(|checked| (checked.shared_type_info(), checked.type_of(expr.id())))
                .map(|(info, typ)| (Rc::clone(info), typ.clone())),
        ),
        ExprView::Defer(source, _, scope) => lower_dynamic_expr(
            builder,
            source,
            DataflowDynamicScope::from_ast(scope.clone()),
            DataflowDynamicMode::Defer,
            builder
                .annotations
                .map(|checked| (checked.shared_type_info(), checked.type_of(expr.id())))
                .map(|(info, typ)| (Rc::clone(info), typ.clone())),
        ),
        ExprView::Lambda(params, body) => {
            let func = lower_function(params.clone(), body, builder.annotations);
            builder.push(UnboundOp::Function { func })
        }
        ExprView::Apply(func, args) => {
            if let Some(value) = lower_direct_fix_apply(func, args.clone(), builder) {
                return value;
            }
            if let ExprView::Lambda(params, body) = func.view() {
                let function = lower_function(params.clone(), body, builder.annotations);
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
        ExprView::Partial(func, args) => {
            let display = format!("partial({func}, ...)").into();
            let lowered_func = lower_expr(func, builder);
            let args = lower_exprs(args, builder);
            builder.push(UnboundOp::Partial {
                func: lowered_func,
                args,
                display,
            })
        }
        ExprView::Fix(func) => {
            let display = format!("fix({func})").into();
            let func = lower_expr(func, builder);
            builder.push(UnboundOp::Fix { func, display })
        }
        ExprView::LMap(func, list) => {
            let func = lower_expr(func, builder);
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::ListMap { func, list })
        }
        ExprView::LFilter(func, list) => {
            let func = lower_expr(func, builder);
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::ListFilter { func, list })
        }
        ExprView::LFold(func, init, list) => {
            let func = lower_expr(func, builder);
            let init = lower_expr(init, builder);
            let list = lower_expr(list, builder);
            builder.push(UnboundOp::ListFold { func, init, list })
        }
        ExprView::MonitoredAt(_, _) | ExprView::Dist(_, _) => {
            panic!("dataflow semantics does not support distributed AST operations")
        }
    }
}

fn lower_unary(builder: &mut PlanBuilder<'_>, op: DataflowUnaryOp, arg: ExprRef<'_>) -> UnboundRef {
    let arg = lower_expr(arg, builder);
    builder.push(UnboundOp::Unary { op, arg })
}

fn lower_exprs(items: ExprRefs<'_>, builder: &mut PlanBuilder<'_>) -> Vec<UnboundRef> {
    items
        .into_iter()
        .map(|item| lower_expr(item, builder))
        .collect()
}

fn lower_dynamic_expr(
    builder: &mut PlanBuilder<'_>,
    input: ExprRef<'_>,
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
    body: ExprRef<'_>,
    annotations: Option<&TypeAnnotations>,
) -> UnboundFunctionDef {
    let params_display = params
        .iter()
        .map(|(name, typ)| format!("{}: {}", name, typ))
        .collect::<Vec<_>>()
        .join(", ");
    let display = format!("\\{} -> {}", params_display, body).into();
    let params = params.into_iter().map(|(name, _)| name).collect();
    let body = lower_expr_ref_plan(body, annotations);
    UnboundFunctionDef::new(params, body, display)
}

fn lower_direct_fix_apply(
    func: ExprRef<'_>,
    args: ExprRefs<'_>,
    builder: &mut PlanBuilder<'_>,
) -> Option<UnboundRef> {
    let ExprView::Fix(fixed_func) = func.view() else {
        return None;
    };
    let ExprView::Lambda(params, body) = fixed_func.view() else {
        return None;
    };
    let (self_name, _) = params.first()?;

    let function_params = params
        .iter()
        .skip(1)
        .map(|(name, _)| name.clone())
        .collect();
    let mut body = lower_expr_ref_plan(body, builder.annotations);
    specialize_recursive_self_calls(&mut body, self_name);
    let args = lower_exprs(args, builder);
    Some(lower_direct_recursive_apply(
        function_params,
        body,
        func.to_string().into(),
        args,
        builder,
    ))
}

fn lower_direct_recursive_apply(
    params: EcoVec<VarName>,
    body: UnboundPlanBody,
    display: EcoString,
    args: Vec<UnboundRef>,
    builder: &mut PlanBuilder<'_>,
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

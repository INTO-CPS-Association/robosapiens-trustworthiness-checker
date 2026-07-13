use super::super::plan::*;
use super::super::*;
use crate::core::PartialStreamValue;
use crate::lang::dsrv::ast::{FloatBinOp, IntBinOp, SExpr, StrBinOp};
use crate::lang::dsrv::type_checker::{
    SExprAny, SExprBool, SExprFloat, SExprInt, SExprStr, SExprUnit, TypedApplyExpr, TypedFoldExpr,
    TypedFunctionExpr, TypedListExpr, TypedListExprKind, TypedMapExpr, TypedMapExprKind,
    TypedStructExpr, TypedStructExprKind, TypedTupleExpr, TypedTupleExprKind,
};

#[derive(Default)]
struct PlanBuilder {
    nodes: Vec<UnboundOp>,
}

impl PlanBuilder {
    fn push(&mut self, op: UnboundOp) -> UnboundRef {
        let id = self.nodes.len();
        self.nodes.push(op);
        UnboundRef::Node(NodeId::new(id))
    }

    fn finish(self, output: UnboundRef) -> UnboundPlanBody {
        UnboundPlanBody::new(self.nodes, output)
    }
}

fn lower_branch<T>(
    expr: T,
    build: impl FnOnce(T, &mut PlanBuilder) -> UnboundRef,
) -> UnboundPlanBody {
    let mut builder = PlanBuilder::default();
    let output = build(expr, &mut builder);
    builder.finish(output)
}

pub(in crate::dataflow) fn lower_untyped_expr_plan(expr: SpannedExpr) -> UnboundPlanBody {
    let mut builder = PlanBuilder::default();
    let output = lower_untyped_expr(expr, &mut builder);
    builder.finish(output)
}

pub(in crate::dataflow) fn lower_typed_expr_plan(expr: SExprTE) -> UnboundPlanBody {
    let mut builder = PlanBuilder::default();
    let output = lower_typed_expr(expr, &mut builder);
    builder.finish(output)
}

fn lower_typed_expr(expr: SExprTE, builder: &mut PlanBuilder) -> UnboundRef {
    match expr {
        SExprTE::Int(expr) => lower_typed_int(expr, builder),
        SExprTE::Float(expr) => lower_typed_float(expr, builder),
        SExprTE::Str(expr) => lower_typed_str(expr, builder),
        SExprTE::Bool(expr) => lower_typed_bool(expr, builder),
        SExprTE::Unit(expr) => lower_typed_unit(expr, builder),
        SExprTE::List(expr) => lower_typed_list(expr, builder),
        SExprTE::Map(expr) => lower_typed_map(expr, builder),
        SExprTE::Struct(expr) => lower_typed_struct(expr, builder),
        SExprTE::Tuple(expr) => lower_typed_tuple(expr, builder),
        SExprTE::Function(expr) => lower_typed_function(expr, builder),
        SExprTE::Fold(expr) => lower_typed_fold(expr, builder),
        SExprTE::Apply(expr) => lower_typed_apply(expr, builder),
        SExprTE::Any(expr) => lower_typed_any(expr, builder),
    }
}

fn typed_value_operand<T>(value: PartialStreamValue<T>) -> UnboundRef
where
    Value: From<PartialStreamValue<T>>,
{
    UnboundRef::Const(value.into())
}

fn lower_typed_int(expr: SExprInt, builder: &mut PlanBuilder) -> UnboundRef {
    match expr {
        SExprInt::Cast(expr) => lower_typed_expr(*expr, builder),
        SExprInt::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_int);
            let else_branch = lower_branch(*else_value, lower_typed_int);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        SExprInt::SIndex(input, offset) => {
            let input = lower_typed_int(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        SExprInt::Val(value) => typed_value_operand(value),
        SExprInt::BinOp(lhs, rhs, op) => {
            let lhs = lower_typed_int(*lhs, builder);
            let rhs = lower_typed_int(*rhs, builder);
            builder.push(UnboundOp::Binary {
                op: dataflow_from_int(op),
                lhs,
                rhs,
            })
        }
        SExprInt::Var(var) => UnboundRef::External(var),
        SExprInt::Default(input, fallback) => {
            let input = lower_typed_int(*input, builder);
            let fallback = lower_typed_int(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        SExprInt::Update(base, update) => {
            let base = lower_typed_int(*base, builder);
            let update = lower_typed_int(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        SExprInt::Latch(value, trigger) => {
            let value = lower_typed_int(*value, builder);
            let trigger = lower_typed_int(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        SExprInt::Abs(value) => {
            let arg = lower_typed_int(*value, builder);
            builder.push(UnboundOp::Unary {
                op: DataflowUnaryOp::Abs,
                arg,
            })
        }
        SExprInt::Init(input, initial) => {
            let input = lower_typed_int(*input, builder);
            let initial = lower_typed_int(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        SExprInt::Defer(input, type_info, vars) => lower_typed_dynamic_expr(
            *input,
            TCType::Int,
            type_info,
            DataflowDynamicScope::Restricted(vars),
            DataflowDynamicMode::Defer,
            builder,
        ),
        SExprInt::Dynamic(input, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Int,
            type_info,
            DataflowDynamicScope::Automatic,
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprInt::RestrictedDynamic(input, vars, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Int,
            type_info,
            DataflowDynamicScope::from_ast(vars),
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprInt::LLen(list) => {
            let list = lower_typed_list(list, builder);
            builder.push(UnboundOp::LLen { list })
        }
        SExprInt::LHeadList(list) => {
            let list = lower_typed_list(list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        SExprInt::LIndexList(list, index) => {
            let list = lower_typed_list(list, builder);
            let index = lower_typed_int(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        SExprInt::MGetMap(map, key) => {
            let map = lower_typed_map(map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprInt::SGetStruct(st, key) => {
            let map = lower_typed_struct(st, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprInt::SGetTuple(tuple, index) => {
            let tuple = lower_typed_tuple(tuple, builder);
            builder.push(UnboundOp::TGet { tuple, index })
        }
    }
}

fn lower_typed_float(expr: SExprFloat, builder: &mut PlanBuilder) -> UnboundRef {
    match expr {
        SExprFloat::Cast(expr) => lower_typed_expr(*expr, builder),
        SExprFloat::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_float);
            let else_branch = lower_branch(*else_value, lower_typed_float);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        SExprFloat::SIndex(input, offset) => {
            let input = lower_typed_float(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        SExprFloat::Val(value) => typed_value_operand(value),
        SExprFloat::BinOp(lhs, rhs, op) => {
            let lhs = lower_typed_float(*lhs, builder);
            let rhs = lower_typed_float(*rhs, builder);
            builder.push(UnboundOp::Binary {
                op: dataflow_from_float(op),
                lhs,
                rhs,
            })
        }
        SExprFloat::Var(var) => UnboundRef::External(var),
        SExprFloat::Default(input, fallback) => {
            let input = lower_typed_float(*input, builder);
            let fallback = lower_typed_float(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        SExprFloat::Update(base, update) => {
            let base = lower_typed_float(*base, builder);
            let update = lower_typed_float(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        SExprFloat::Latch(value, trigger) => {
            let value = lower_typed_float(*value, builder);
            let trigger = lower_typed_float(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        SExprFloat::Sin(value) => lower_typed_float_unary(DataflowUnaryOp::Sin, *value, builder),
        SExprFloat::Cos(value) => lower_typed_float_unary(DataflowUnaryOp::Cos, *value, builder),
        SExprFloat::Tan(value) => lower_typed_float_unary(DataflowUnaryOp::Tan, *value, builder),
        SExprFloat::Abs(value) => lower_typed_float_unary(DataflowUnaryOp::Abs, *value, builder),
        SExprFloat::Init(input, initial) => {
            let input = lower_typed_float(*input, builder);
            let initial = lower_typed_float(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        SExprFloat::Defer(input, type_info, vars) => lower_typed_dynamic_expr(
            *input,
            TCType::Float,
            type_info,
            DataflowDynamicScope::Restricted(vars),
            DataflowDynamicMode::Defer,
            builder,
        ),
        SExprFloat::Dynamic(input, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Float,
            type_info,
            DataflowDynamicScope::Automatic,
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprFloat::RestrictedDynamic(input, vars, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Float,
            type_info,
            DataflowDynamicScope::from_ast(vars),
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprFloat::LHeadList(list) => {
            let list = lower_typed_list(list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        SExprFloat::LIndexList(list, index) => {
            let list = lower_typed_list(list, builder);
            let index = lower_typed_int(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        SExprFloat::MGetMap(map, key) => {
            let map = lower_typed_map(map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprFloat::SGetStruct(st, key) => {
            let map = lower_typed_struct(st, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprFloat::SGetTuple(tuple, index) => {
            let tuple = lower_typed_tuple(tuple, builder);
            builder.push(UnboundOp::TGet { tuple, index })
        }
    }
}

fn lower_typed_str(expr: SExprStr, builder: &mut PlanBuilder) -> UnboundRef {
    match expr {
        SExprStr::Cast(expr) => lower_typed_expr(*expr, builder),
        SExprStr::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_str);
            let else_branch = lower_branch(*else_value, lower_typed_str);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        SExprStr::SIndex(input, offset) => {
            let input = lower_typed_str(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        SExprStr::BinOp(lhs, rhs, StrBinOp::Concat) => {
            let lhs = lower_typed_str(*lhs, builder);
            let rhs = lower_typed_str(*rhs, builder);
            builder.push(UnboundOp::Binary {
                op: DataflowBinaryOp::Concat,
                lhs,
                rhs,
            })
        }
        SExprStr::Val(value) => typed_value_operand(value),
        SExprStr::Var(var) => UnboundRef::External(var),
        SExprStr::Default(input, fallback) => {
            let input = lower_typed_str(*input, builder);
            let fallback = lower_typed_str(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        SExprStr::Update(base, update) => {
            let base = lower_typed_str(*base, builder);
            let update = lower_typed_str(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        SExprStr::Latch(value, trigger) => {
            let value = lower_typed_str(*value, builder);
            let trigger = lower_typed_str(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        SExprStr::Init(input, initial) => {
            let input = lower_typed_str(*input, builder);
            let initial = lower_typed_str(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        SExprStr::Defer(input, type_info, vars) => lower_typed_dynamic_expr(
            *input,
            TCType::Str,
            type_info,
            DataflowDynamicScope::Restricted(vars),
            DataflowDynamicMode::Defer,
            builder,
        ),
        SExprStr::Dynamic(input, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Str,
            type_info,
            DataflowDynamicScope::Automatic,
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprStr::RestrictedDynamic(input, vars, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Str,
            type_info,
            DataflowDynamicScope::from_ast(vars),
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprStr::LHeadList(list) => {
            let list = lower_typed_list(list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        SExprStr::LIndexList(list, index) => {
            let list = lower_typed_list(list, builder);
            let index = lower_typed_int(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        SExprStr::MGetMap(map, key) => {
            let map = lower_typed_map(map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprStr::SGetStruct(st, key) => {
            let map = lower_typed_struct(st, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprStr::SGetTuple(tuple, index) => {
            let tuple = lower_typed_tuple(tuple, builder);
            builder.push(UnboundOp::TGet { tuple, index })
        }
    }
}

fn lower_typed_bool(expr: SExprBool, builder: &mut PlanBuilder) -> UnboundRef {
    match expr {
        SExprBool::Val(value) => typed_value_operand(value),
        SExprBool::Cast(expr) => lower_typed_expr(*expr, builder),
        SExprBool::Cmp(op, lhs, rhs) => {
            let lhs = lower_typed_expr(*lhs, builder);
            let rhs = lower_typed_expr(*rhs, builder);
            builder.push(UnboundOp::Binary {
                op: dataflow_from_compare(op),
                lhs,
                rhs,
            })
        }
        SExprBool::BinOp(lhs, rhs, op) => {
            let lhs = lower_typed_bool(*lhs, builder);
            let rhs = lower_typed_bool(*rhs, builder);
            builder.push(UnboundOp::Binary {
                op: dataflow_from_bool(op),
                lhs,
                rhs,
            })
        }
        SExprBool::Not(value) => {
            let arg = lower_typed_bool(*value, builder);
            builder.push(UnboundOp::Unary {
                op: DataflowUnaryOp::Not,
                arg,
            })
        }
        SExprBool::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_bool);
            let else_branch = lower_branch(*else_value, lower_typed_bool);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        SExprBool::SIndex(input, offset) => {
            let input = lower_typed_bool(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        SExprBool::Var(var) => UnboundRef::External(var),
        SExprBool::Default(input, fallback) => {
            let input = lower_typed_bool(*input, builder);
            let fallback = lower_typed_bool(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        SExprBool::Update(base, update) => {
            let base = lower_typed_bool(*base, builder);
            let update = lower_typed_bool(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        SExprBool::Latch(value, trigger) => {
            let value = lower_typed_bool(*value, builder);
            let trigger = lower_typed_bool(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        SExprBool::Init(input, initial) => {
            let input = lower_typed_bool(*input, builder);
            let initial = lower_typed_bool(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        SExprBool::IsDefined(expr) => {
            let input = lower_typed_expr(*expr, builder);
            builder.push(UnboundOp::IsDefined { input })
        }
        SExprBool::When(expr) => {
            let input = lower_typed_expr(*expr, builder);
            builder.push(UnboundOp::When { input })
        }
        SExprBool::LHeadList(list) => {
            let list = lower_typed_list(list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        SExprBool::LIndexList(list, index) => {
            let list = lower_typed_list(list, builder);
            let index = lower_typed_int(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        SExprBool::MGetMap(map, key) => {
            let map = lower_typed_map(map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprBool::SGetStruct(st, key) => {
            let map = lower_typed_struct(st, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprBool::SGetTuple(tuple, index) => {
            let tuple = lower_typed_tuple(tuple, builder);
            builder.push(UnboundOp::TGet { tuple, index })
        }
        SExprBool::MHasKeyMap(map, key) => {
            let map = lower_typed_map(map, builder);
            builder.push(UnboundOp::MHasKey { map, key })
        }
        SExprBool::Defer(input, type_info, vars) => lower_typed_dynamic_expr(
            *input,
            TCType::Bool,
            type_info,
            DataflowDynamicScope::Restricted(vars),
            DataflowDynamicMode::Defer,
            builder,
        ),
        SExprBool::Dynamic(input, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Bool,
            type_info,
            DataflowDynamicScope::Automatic,
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprBool::RestrictedDynamic(input, vars, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Bool,
            type_info,
            DataflowDynamicScope::from_ast(vars),
            DataflowDynamicMode::Dynamic,
            builder,
        ),
    }
}

fn lower_typed_unit(expr: SExprUnit, builder: &mut PlanBuilder) -> UnboundRef {
    match expr {
        SExprUnit::Cast(expr) => lower_typed_expr(*expr, builder),
        SExprUnit::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_unit);
            let else_branch = lower_branch(*else_value, lower_typed_unit);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        SExprUnit::SIndex(input, offset) => {
            let input = lower_typed_unit(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        SExprUnit::Val(value) => typed_value_operand(value),
        SExprUnit::Var(var) => UnboundRef::External(var),
        SExprUnit::Default(input, fallback) => {
            let input = lower_typed_unit(*input, builder);
            let fallback = lower_typed_unit(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        SExprUnit::Update(base, update) => {
            let base = lower_typed_unit(*base, builder);
            let update = lower_typed_unit(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        SExprUnit::Latch(value, trigger) => {
            let value = lower_typed_unit(*value, builder);
            let trigger = lower_typed_unit(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        SExprUnit::Init(input, initial) => {
            let input = lower_typed_unit(*input, builder);
            let initial = lower_typed_unit(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        SExprUnit::Defer(input, type_info, vars) => lower_typed_dynamic_expr(
            *input,
            TCType::Unit,
            type_info,
            DataflowDynamicScope::Restricted(vars),
            DataflowDynamicMode::Defer,
            builder,
        ),
        SExprUnit::Dynamic(input, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Unit,
            type_info,
            DataflowDynamicScope::Automatic,
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprUnit::RestrictedDynamic(input, vars, type_info) => lower_typed_dynamic_expr(
            *input,
            TCType::Unit,
            type_info,
            DataflowDynamicScope::from_ast(vars),
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        SExprUnit::LHeadList(list) => {
            let list = lower_typed_list(list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        SExprUnit::LIndexList(list, index) => {
            let list = lower_typed_list(list, builder);
            let index = lower_typed_int(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        SExprUnit::MGetMap(map, key) => {
            let map = lower_typed_map(map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprUnit::SGetStruct(st, key) => {
            let map = lower_typed_struct(st, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExprUnit::SGetTuple(tuple, index) => {
            let tuple = lower_typed_tuple(tuple, builder);
            builder.push(UnboundOp::TGet { tuple, index })
        }
    }
}

fn lower_typed_list(expr: TypedListExpr, builder: &mut PlanBuilder) -> UnboundRef {
    let typ = expr.list_tc_type();
    match expr.kind {
        TypedListExprKind::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_list);
            let else_branch = lower_branch(*else_value, lower_typed_list);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        TypedListExprKind::SIndex(input, offset) => {
            let input = lower_typed_list(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        TypedListExprKind::Var(var) => UnboundRef::External(var),
        TypedListExprKind::Default(input, fallback) => {
            let input = lower_typed_list(*input, builder);
            let fallback = lower_typed_list(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        TypedListExprKind::Update(base, update) => {
            let base = lower_typed_list(*base, builder);
            let update = lower_typed_list(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        TypedListExprKind::Latch(value, trigger) => {
            let value = lower_typed_list(*value, builder);
            let trigger = lower_typed_list(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        TypedListExprKind::Init(input, initial) => {
            let input = lower_typed_list(*input, builder);
            let initial = lower_typed_list(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        TypedListExprKind::Defer(input, type_info, vars) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::Restricted(vars),
            DataflowDynamicMode::Defer,
            builder,
        ),
        TypedListExprKind::Dynamic(input, type_info) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::Automatic,
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        TypedListExprKind::RestrictedDynamic(input, vars, type_info) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::from_ast(vars),
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        TypedListExprKind::Literal(items) => {
            let items = items
                .into_iter()
                .map(|item| lower_typed_expr(item, builder))
                .collect();
            builder.push(UnboundOp::List(items))
        }
        TypedListExprKind::LTail(input) => {
            let list = lower_typed_list(*input, builder);
            builder.push(UnboundOp::LTail { list })
        }
        TypedListExprKind::LConcat(lhs, rhs) => {
            let lhs = lower_typed_list(*lhs, builder);
            let rhs = lower_typed_list(*rhs, builder);
            builder.push(UnboundOp::LConcat { lhs, rhs })
        }
        TypedListExprKind::LAppend(list, value) => {
            let list = lower_typed_list(*list, builder);
            let value = lower_typed_expr(*value, builder);
            builder.push(UnboundOp::LAppend { list, value })
        }
        TypedListExprKind::LMap(func, list) => {
            let func = lower_typed_function(*func, builder);
            let list = lower_typed_list(*list, builder);
            builder.push(UnboundOp::ListMap { func, list })
        }
        TypedListExprKind::LFilter(func, list) => {
            let func = lower_typed_function(*func, builder);
            let list = lower_typed_list(*list, builder);
            builder.push(UnboundOp::ListFilter { func, list })
        }
        TypedListExprKind::LHeadList(input) => {
            let list = lower_typed_list(*input, builder);
            builder.push(UnboundOp::LHead { list })
        }
        TypedListExprKind::LIndexList(list, index) => {
            let list = lower_typed_list(*list, builder);
            let index = lower_typed_int(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        TypedListExprKind::MGetMap(map, key) => {
            let map = lower_typed_map(*map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        TypedListExprKind::SGetStruct(st, key) => {
            let map = lower_typed_struct(*st, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        TypedListExprKind::SGetTuple(tuple, index) => {
            let tuple = lower_typed_tuple(*tuple, builder);
            builder.push(UnboundOp::TGet { tuple, index })
        }
    }
}

fn lower_typed_map(expr: TypedMapExpr, builder: &mut PlanBuilder) -> UnboundRef {
    let typ = expr.map_tc_type();
    match expr.kind {
        TypedMapExprKind::Var(var) => UnboundRef::External(var),
        TypedMapExprKind::Literal(entries) => {
            let entries = entries
                .into_iter()
                .map(|(key, value)| (key, lower_typed_expr(value, builder)))
                .collect();
            builder.push(UnboundOp::Map(entries))
        }
        TypedMapExprKind::Default(input, fallback) => {
            let input = lower_typed_map(*input, builder);
            let fallback = lower_typed_map(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        TypedMapExprKind::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_map);
            let else_branch = lower_branch(*else_value, lower_typed_map);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        TypedMapExprKind::Update(base, update) => {
            let base = lower_typed_map(*base, builder);
            let update = lower_typed_map(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        TypedMapExprKind::Latch(value, trigger) => {
            let value = lower_typed_map(*value, builder);
            let trigger = lower_typed_map(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        TypedMapExprKind::Init(input, initial) => {
            let input = lower_typed_map(*input, builder);
            let initial = lower_typed_map(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        TypedMapExprKind::SIndex(input, offset) => {
            let input = lower_typed_map(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        TypedMapExprKind::Defer(input, type_info, vars) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::Restricted(vars),
            DataflowDynamicMode::Defer,
            builder,
        ),
        TypedMapExprKind::Dynamic(input, type_info) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::Automatic,
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        TypedMapExprKind::RestrictedDynamic(input, vars, type_info) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::from_ast(vars),
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        TypedMapExprKind::MInsert(map, key, value) => {
            let map = lower_typed_map(*map, builder);
            let value = lower_typed_expr(*value, builder);
            builder.push(UnboundOp::MInsert { map, key, value })
        }
        TypedMapExprKind::MRemove(map, key) => {
            let map = lower_typed_map(*map, builder);
            builder.push(UnboundOp::MRemove { map, key })
        }
        TypedMapExprKind::MGetMap(map, key) => {
            let map = lower_typed_map(*map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        TypedMapExprKind::SGetStruct(st, key) => {
            let map = lower_typed_struct(*st, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        TypedMapExprKind::SGetTuple(tuple, index) => {
            let tuple = lower_typed_tuple(*tuple, builder);
            builder.push(UnboundOp::TGet { tuple, index })
        }
        TypedMapExprKind::LHeadList(list) => {
            let list = lower_typed_list(*list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        TypedMapExprKind::LIndexList(list, index) => {
            let list = lower_typed_list(*list, builder);
            let index = lower_typed_int(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
    }
}

fn lower_typed_struct(expr: TypedStructExpr, builder: &mut PlanBuilder) -> UnboundRef {
    let typ = TCType::Struct(expr.typ_map.clone(), expr.allow_extra_fields);
    match expr.kind {
        TypedStructExprKind::Var(var) => UnboundRef::External(var),
        TypedStructExprKind::Literal(entries) => {
            let entries = entries
                .into_iter()
                .map(|(key, value)| (key, lower_typed_expr(value, builder)))
                .collect();
            builder.push(UnboundOp::Map(entries))
        }
        TypedStructExprKind::Default(input, fallback) => {
            let input = lower_typed_struct(*input, builder);
            let fallback = lower_typed_struct(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        TypedStructExprKind::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_struct);
            let else_branch = lower_branch(*else_value, lower_typed_struct);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        TypedStructExprKind::Update(base, update) => {
            let base = lower_typed_struct(*base, builder);
            let update = lower_typed_struct(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        TypedStructExprKind::Latch(value, trigger) => {
            let value = lower_typed_struct(*value, builder);
            let trigger = lower_typed_struct(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        TypedStructExprKind::Init(input, initial) => {
            let input = lower_typed_struct(*input, builder);
            let initial = lower_typed_struct(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        TypedStructExprKind::SIndex(input, offset) => {
            let input = lower_typed_struct(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        TypedStructExprKind::Defer(input, type_info, vars) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::Restricted(vars),
            DataflowDynamicMode::Defer,
            builder,
        ),
        TypedStructExprKind::Dynamic(input, type_info) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::Automatic,
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        TypedStructExprKind::RestrictedDynamic(input, vars, type_info) => lower_typed_dynamic_expr(
            *input,
            typ,
            type_info,
            DataflowDynamicScope::from_ast(vars),
            DataflowDynamicMode::Dynamic,
            builder,
        ),
        TypedStructExprKind::SUpdate(st, key, value) => {
            let map = lower_typed_struct(*st, builder);
            let value = lower_typed_expr(*value, builder);
            builder.push(UnboundOp::MInsert { map, key, value })
        }
        TypedStructExprKind::SGet(st, key) => {
            let map = lower_typed_struct(*st, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        TypedStructExprKind::MGetMap(map, key) => {
            let map = lower_typed_map(*map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        TypedStructExprKind::LHeadList(list) => {
            let list = lower_typed_list(*list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        TypedStructExprKind::LIndexList(list, index) => {
            let list = lower_typed_list(*list, builder);
            let index = lower_typed_int(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
    }
}

fn lower_typed_tuple(expr: TypedTupleExpr, builder: &mut PlanBuilder) -> UnboundRef {
    match expr.kind {
        TypedTupleExprKind::Var(var) => UnboundRef::External(var),
        TypedTupleExprKind::Literal(items) => {
            let items = items
                .into_iter()
                .map(|item| lower_typed_expr(item, builder))
                .collect();
            builder.push(UnboundOp::Tuple(items))
        }
        TypedTupleExprKind::Default(input, fallback) => {
            let input = lower_typed_tuple(*input, builder);
            let fallback = lower_typed_tuple(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        TypedTupleExprKind::If(cond, then_value, else_value) => {
            let cond = lower_typed_bool(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_typed_tuple);
            let else_branch = lower_branch(*else_value, lower_typed_tuple);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        TypedTupleExprKind::Update(base, update) => {
            let base = lower_typed_tuple(*base, builder);
            let update = lower_typed_tuple(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        TypedTupleExprKind::Latch(value, trigger) => {
            let value = lower_typed_tuple(*value, builder);
            let trigger = lower_typed_tuple(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        TypedTupleExprKind::Init(input, initial) => {
            let input = lower_typed_tuple(*input, builder);
            let initial = lower_typed_tuple(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        TypedTupleExprKind::SIndex(input, offset) => {
            let input = lower_typed_tuple(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        TypedTupleExprKind::TGetTuple(tuple, index) => {
            let tuple = lower_typed_tuple(*tuple, builder);
            builder.push(UnboundOp::TGet { tuple, index })
        }
    }
}

fn lower_typed_function(func: TypedFunctionExpr, builder: &mut PlanBuilder) -> UnboundRef {
    if let Some(var) = typed_function_var(&func) {
        return UnboundRef::External(var.clone());
    }

    let mut params = EcoVec::new();
    if let Some(name) = &func.recursive_name {
        params.push(name.clone());
    }
    params.extend(func.params.iter().map(|(name, _)| name.clone()));
    let display = EcoString::from(format!("{}", func));
    let body = lower_typed_expr_plan(*func.body);
    let function = UnboundFunctionDef::new(params, body, display.clone());
    let function = builder.push(UnboundOp::Function { func: function });
    if func.recursive_name.is_some() {
        builder.push(UnboundOp::Fix {
            func: function,
            display: format!("fix({})", display).into(),
        })
    } else {
        function
    }
}

fn lower_typed_fold(expr: TypedFoldExpr, builder: &mut PlanBuilder) -> UnboundRef {
    let func = lower_typed_function(*expr.func, builder);
    let init = lower_typed_expr(*expr.init, builder);
    let list = lower_typed_list(*expr.list, builder);
    builder.push(UnboundOp::ListFold { func, init, list })
}

fn lower_typed_apply(expr: TypedApplyExpr, builder: &mut PlanBuilder) -> UnboundRef {
    if expr.func.recursive_name.is_some() && typed_function_var(&expr.func).is_none() {
        return lower_typed_direct_fix_apply(*expr.func, expr.args, builder);
    }

    let func = lower_typed_function(*expr.func, builder);
    let args = expr
        .args
        .into_iter()
        .map(|arg| lower_typed_expr(arg, builder))
        .collect();
    builder.push(UnboundOp::Apply { func, args })
}

fn typed_function_var(func: &TypedFunctionExpr) -> Option<&VarName> {
    if func.recursive_name.is_some() {
        return None;
    }
    let SExprTE::Any(SExprAny::Var(var)) = func.body.as_ref() else {
        return None;
    };
    if func
        .params
        .iter()
        .all(|(name, _)| name.name().starts_with('$') || name.name().starts_with("_f"))
    {
        Some(var)
    } else {
        None
    }
}

fn lower_typed_direct_fix_apply(
    func: TypedFunctionExpr,
    args: EcoVec<SExprTE>,
    builder: &mut PlanBuilder,
) -> UnboundRef {
    let self_name = func
        .recursive_name
        .as_ref()
        .expect("typed direct fix apply requires recursive function")
        .clone();
    let display = format!("fix({})", func).into();
    let params = func.params.iter().map(|(name, _)| name.clone()).collect();
    let mut body = lower_typed_expr_plan(*func.body);
    specialize_recursive_self_calls(&mut body, &self_name);
    let args = args
        .into_iter()
        .map(|arg| lower_typed_expr(arg, builder))
        .collect();
    lower_direct_recursive_apply(params, body, display, args, builder)
}

fn lower_typed_any(expr: SExprAny, builder: &mut PlanBuilder) -> UnboundRef {
    match expr {
        SExprAny::Var(var) => UnboundRef::External(var),
        SExprAny::Val(value) => UnboundRef::Const(value),
        SExprAny::Expr(expr) => lower_untyped_expr(ast(expr), builder),
    }
}

fn lower_typed_dynamic_expr(
    input: SExprStr,
    typ: TCType,
    type_info: TypeInfo,
    scope: DataflowDynamicScope,
    mode: DataflowDynamicMode,
    builder: &mut PlanBuilder,
) -> UnboundRef {
    let input = lower_typed_str(input, builder);
    builder.push(UnboundOp::Dynamic(UnboundDynamicSpec {
        input,
        scope,
        mode,
        typed: Some((type_info, typ)),
    }))
}

fn lower_typed_float_unary(
    op: DataflowUnaryOp,
    value: SExprFloat,
    builder: &mut PlanBuilder,
) -> UnboundRef {
    let arg = lower_typed_float(value, builder);
    builder.push(UnboundOp::Unary { op, arg })
}

fn dataflow_from_int(op: IntBinOp) -> DataflowBinaryOp {
    match op {
        IntBinOp::Add => DataflowBinaryOp::Add,
        IntBinOp::Sub => DataflowBinaryOp::Sub,
        IntBinOp::Mul => DataflowBinaryOp::Mul,
        IntBinOp::Div => DataflowBinaryOp::Div,
        IntBinOp::Mod => DataflowBinaryOp::Mod,
    }
}

fn dataflow_from_float(op: FloatBinOp) -> DataflowBinaryOp {
    match op {
        FloatBinOp::Add => DataflowBinaryOp::Add,
        FloatBinOp::Sub => DataflowBinaryOp::Sub,
        FloatBinOp::Mul => DataflowBinaryOp::Mul,
        FloatBinOp::Div => DataflowBinaryOp::Div,
        FloatBinOp::Mod => DataflowBinaryOp::Mod,
    }
}

fn dataflow_from_bool(op: crate::lang::dsrv::ast::BoolBinOp) -> DataflowBinaryOp {
    match op {
        crate::lang::dsrv::ast::BoolBinOp::Or => DataflowBinaryOp::Or,
        crate::lang::dsrv::ast::BoolBinOp::And => DataflowBinaryOp::And,
        crate::lang::dsrv::ast::BoolBinOp::Impl => DataflowBinaryOp::Impl,
    }
}

fn dataflow_from_compare(op: crate::lang::dsrv::ast::CompBinOp) -> DataflowBinaryOp {
    match op {
        crate::lang::dsrv::ast::CompBinOp::Eq => DataflowBinaryOp::Eq,
        crate::lang::dsrv::ast::CompBinOp::Le => DataflowBinaryOp::Le,
        crate::lang::dsrv::ast::CompBinOp::Lt => DataflowBinaryOp::Lt,
        crate::lang::dsrv::ast::CompBinOp::Ge => DataflowBinaryOp::Ge,
        crate::lang::dsrv::ast::CompBinOp::Gt => DataflowBinaryOp::Gt,
    }
}

fn ast(expr: SExpr) -> SpannedExpr {
    expr.into()
}

fn lower_untyped_expr(expr: SpannedExpr, builder: &mut PlanBuilder) -> UnboundRef {
    match expr.node {
        SExpr::Val(value) => UnboundRef::Const(value),
        SExpr::Var(var) => UnboundRef::External(var),
        SExpr::BinOp(lhs, rhs, op) => {
            let lhs = lower_untyped_expr(*lhs, builder);
            let rhs = lower_untyped_expr(*rhs, builder);
            builder.push(UnboundOp::Binary {
                op: lower_binary_op(op),
                lhs,
                rhs,
            })
        }
        SExpr::Not(arg) => lower_unary(builder, DataflowUnaryOp::Not, *arg),
        SExpr::Sin(arg) => lower_unary(builder, DataflowUnaryOp::Sin, *arg),
        SExpr::Cos(arg) => lower_unary(builder, DataflowUnaryOp::Cos, *arg),
        SExpr::Tan(arg) => lower_unary(builder, DataflowUnaryOp::Tan, *arg),
        SExpr::Abs(arg) => lower_unary(builder, DataflowUnaryOp::Abs, *arg),
        SExpr::If(cond, then_value, else_value) => {
            let cond = lower_untyped_expr(*cond, builder);
            let then_branch = lower_branch(*then_value, lower_untyped_expr);
            let else_branch = lower_branch(*else_value, lower_untyped_expr);
            builder.push(UnboundOp::If {
                cond,
                then_branch,
                else_branch,
            })
        }
        SExpr::SIndex(input, offset) => {
            let input = lower_untyped_expr(*input, builder);
            builder.push(UnboundOp::SIndex { input, offset })
        }
        SExpr::Default(input, fallback) => {
            let input = lower_untyped_expr(*input, builder);
            let fallback = lower_untyped_expr(*fallback, builder);
            builder.push(UnboundOp::Default { input, fallback })
        }
        SExpr::Update(base, update) => {
            let base = lower_untyped_expr(*base, builder);
            let update = lower_untyped_expr(*update, builder);
            builder.push(UnboundOp::Update { base, update })
        }
        SExpr::IsDefined(input) => {
            let input = lower_untyped_expr(*input, builder);
            builder.push(UnboundOp::IsDefined { input })
        }
        SExpr::When(input) => {
            let input = lower_untyped_expr(*input, builder);
            builder.push(UnboundOp::When { input })
        }
        SExpr::Latch(value, trigger) => {
            let value = lower_untyped_expr(*value, builder);
            let trigger = lower_untyped_expr(*trigger, builder);
            builder.push(UnboundOp::Latch { value, trigger })
        }
        SExpr::Init(input, initial) => {
            let input = lower_untyped_expr(*input, builder);
            let initial = lower_untyped_expr(*initial, builder);
            builder.push(UnboundOp::Init { input, initial })
        }
        SExpr::List(items) => {
            let items = lower_expr_vec(items, builder);
            builder.push(UnboundOp::List(items))
        }
        SExpr::Tuple(items) => {
            let items = lower_expr_vec(items, builder);
            builder.push(UnboundOp::Tuple(items))
        }
        SExpr::Map(items) | SExpr::Struct(items) | SExpr::ObjectLiteral(items) => {
            let items = items
                .into_iter()
                .map(|(key, value)| (key, lower_untyped_expr(value, builder)))
                .collect();
            builder.push(UnboundOp::Map(items))
        }
        SExpr::LIndex(list, index) => {
            let list = lower_untyped_expr(*list, builder);
            let index = lower_untyped_expr(*index, builder);
            builder.push(UnboundOp::LIndex { list, index })
        }
        SExpr::LAppend(list, value) => {
            let list = lower_untyped_expr(*list, builder);
            let value = lower_untyped_expr(*value, builder);
            builder.push(UnboundOp::LAppend { list, value })
        }
        SExpr::LConcat(lhs, rhs) => {
            let lhs = lower_untyped_expr(*lhs, builder);
            let rhs = lower_untyped_expr(*rhs, builder);
            builder.push(UnboundOp::LConcat { lhs, rhs })
        }
        SExpr::LHead(list) => {
            let list = lower_untyped_expr(*list, builder);
            builder.push(UnboundOp::LHead { list })
        }
        SExpr::LTail(list) => {
            let list = lower_untyped_expr(*list, builder);
            builder.push(UnboundOp::LTail { list })
        }
        SExpr::LLen(list) => {
            let list = lower_untyped_expr(*list, builder);
            builder.push(UnboundOp::LLen { list })
        }
        SExpr::MGet(map, key) => {
            let map = lower_untyped_expr(*map, builder);
            builder.push(UnboundOp::MGet { map, key })
        }
        SExpr::SGet(value, key) => {
            let value = lower_untyped_expr(*value, builder);
            if let Ok(index) = key.parse::<usize>() {
                builder.push(UnboundOp::TGet {
                    tuple: value,
                    index,
                })
            } else {
                builder.push(UnboundOp::MGet { map: value, key })
            }
        }
        SExpr::MRemove(map, key) => {
            let map = lower_untyped_expr(*map, builder);
            builder.push(UnboundOp::MRemove { map, key })
        }
        SExpr::MInsert(map, key, value) => {
            let map = lower_untyped_expr(*map, builder);
            let value = lower_untyped_expr(*value, builder);
            builder.push(UnboundOp::MInsert { map, key, value })
        }
        SExpr::MHasKey(map, key) => {
            let map = lower_untyped_expr(*map, builder);
            builder.push(UnboundOp::MHasKey { map, key })
        }
        SExpr::Dynamic(runtime) => lower_dynamic_expr(
            builder,
            *runtime.source,
            DataflowDynamicScope::from_ast(runtime.scope),
            DataflowDynamicMode::Dynamic,
        ),
        SExpr::Defer(runtime) => lower_dynamic_expr(
            builder,
            *runtime.source,
            DataflowDynamicScope::from_ast(runtime.scope),
            DataflowDynamicMode::Defer,
        ),
        SExpr::Lambda(params, body) => {
            let func = lower_untyped_function(params, *body);
            builder.push(UnboundOp::Function { func })
        }
        SExpr::Apply(func, args) => {
            if let Some(value) = lower_direct_fix_apply(&func, &args, builder) {
                return value;
            }
            let func = lower_untyped_expr(*func, builder);
            let args = lower_expr_vec(args, builder);
            builder.push(UnboundOp::Apply { func, args })
        }
        SExpr::Partial(func, args) => {
            let display = format!("partial({}, ...)", func).into();
            let func = lower_untyped_expr(*func, builder);
            let args = lower_expr_vec(args, builder);
            builder.push(UnboundOp::Partial {
                func,
                args,
                display,
            })
        }
        SExpr::Fix(func) => {
            let display = format!("fix({})", func).into();
            let func = lower_untyped_expr(*func, builder);
            builder.push(UnboundOp::Fix { func, display })
        }
        SExpr::LMap(func, list) => {
            let func = lower_untyped_expr(*func, builder);
            let list = lower_untyped_expr(*list, builder);
            builder.push(UnboundOp::ListMap { func, list })
        }
        SExpr::LFilter(func, list) => {
            let func = lower_untyped_expr(*func, builder);
            let list = lower_untyped_expr(*list, builder);
            builder.push(UnboundOp::ListFilter { func, list })
        }
        SExpr::LFold(func, init, list) => {
            let func = lower_untyped_expr(*func, builder);
            let init = lower_untyped_expr(*init, builder);
            let list = lower_untyped_expr(*list, builder);
            builder.push(UnboundOp::ListFold { func, init, list })
        }
        SExpr::MonitoredAt(_, _) | SExpr::Dist(_, _) => {
            panic!("dataflow semantics does not support distributed AST operations")
        }
    }
}

fn lower_unary(builder: &mut PlanBuilder, op: DataflowUnaryOp, arg: SpannedExpr) -> UnboundRef {
    let arg = lower_untyped_expr(arg, builder);
    builder.push(UnboundOp::Unary { op, arg })
}

fn lower_expr_vec(
    items: impl IntoIterator<Item = SpannedExpr>,
    builder: &mut PlanBuilder,
) -> Vec<UnboundRef> {
    items
        .into_iter()
        .map(|item| lower_untyped_expr(item, builder))
        .collect()
}

fn lower_dynamic_expr(
    builder: &mut PlanBuilder,
    input: SpannedExpr,
    scope: DataflowDynamicScope,
    mode: DataflowDynamicMode,
) -> UnboundRef {
    let input = lower_untyped_expr(input, builder);
    builder.push(UnboundOp::Dynamic(UnboundDynamicSpec {
        input,
        scope,
        mode,
        typed: None,
    }))
}

fn lower_untyped_function(
    params: EcoVec<(VarName, StreamType)>,
    body: SpannedExpr,
) -> UnboundFunctionDef {
    let params_display = params
        .iter()
        .map(|(name, typ)| format!("{}: {}", name, typ))
        .collect::<Vec<_>>()
        .join(", ");
    let display = format!("\\{} -> {}", params_display, body).into();
    let params = params.into_iter().map(|(name, _)| name).collect();
    let body = lower_untyped_expr_plan(body);
    UnboundFunctionDef::new(params, body, display)
}

fn lower_direct_fix_apply(
    func: &SpannedExpr,
    args: &EcoVec<SpannedExpr>,
    builder: &mut PlanBuilder,
) -> Option<UnboundRef> {
    let SExpr::Fix(fixed_func) = &func.node else {
        return None;
    };
    let SExpr::Lambda(params, body) = &fixed_func.node else {
        return None;
    };
    let Some((self_name, _)) = params.first() else {
        return None;
    };

    let function_params = params
        .iter()
        .skip(1)
        .map(|(name, _)| name.clone())
        .collect();
    let mut body = lower_untyped_expr_plan((**body).clone());
    specialize_recursive_self_calls(&mut body, self_name);
    let args = lower_expr_vec(args.iter().cloned(), builder);
    Some(lower_direct_recursive_apply(
        function_params,
        body,
        format!("{}", func).into(),
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

fn lower_binary_op(op: crate::lang::dsrv::ast::SBinOp) -> DataflowBinaryOp {
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

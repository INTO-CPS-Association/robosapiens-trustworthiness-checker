//! Untimed DSRV evaluation reused by distributed semantics.

use super::combinators as mc;
use crate::core::{OutputStream, Value};
use crate::lang::dsrv::ast::{
    BoolBinOp, CompBinOp, ExprRef, ExprView, NumericalBinOp, SBinOp, StrBinOp,
};

pub(crate) fn evaluate<'a>(
    node: ExprRef<'a>,
    evaluate: &impl Fn(ExprRef<'a>) -> OutputStream<Value>,
) -> Option<OutputStream<Value>> {
    use ExprView::*;

    let stream = match node.view() {
        Val(value) => mc::val(value.clone()),
        BinOp(left, right, operator) => {
            let left = evaluate(left);
            let right = evaluate(right);
            match operator {
                SBinOp::NOp(NumericalBinOp::Add) => mc::plus(left, right),
                SBinOp::NOp(NumericalBinOp::Sub) => mc::minus(left, right),
                SBinOp::NOp(NumericalBinOp::Mul) => mc::mult(left, right),
                SBinOp::NOp(NumericalBinOp::Div) => mc::div(left, right),
                SBinOp::NOp(NumericalBinOp::Mod) => mc::modulo(left, right),
                SBinOp::BOp(BoolBinOp::Or) => mc::or(left, right),
                SBinOp::BOp(BoolBinOp::And) => mc::and(left, right),
                SBinOp::BOp(BoolBinOp::Impl) => mc::implication(left, right),
                SBinOp::SOp(StrBinOp::Concat) => mc::concat(left, right),
                SBinOp::COp(CompBinOp::Eq) => mc::eq(left, right),
                SBinOp::COp(CompBinOp::Le) => mc::le(left, right),
                SBinOp::COp(CompBinOp::Lt) => mc::lt(left, right),
                SBinOp::COp(CompBinOp::Ge) => mc::ge(left, right),
                SBinOp::COp(CompBinOp::Gt) => mc::gt(left, right),
            }
        }
        Not(value) => mc::not(evaluate(value)),
        Neg(value) => mc::neg(evaluate(value)),
        Update(current, update) => mc::update(evaluate(current), evaluate(update)),
        Default(value, default) => mc::default(evaluate(value), evaluate(default)),
        IsDefined(value) => mc::is_defined(evaluate(value)),
        When(value) => mc::when(evaluate(value)),
        Latch(value, trigger) => mc::latch(evaluate(value), evaluate(trigger)),
        Init(initial, value) => mc::init(evaluate(initial), evaluate(value)),
        SIndex(value, index) => mc::sindex(evaluate(value), index),
        If(condition, then_value, else_value) => mc::if_stm(
            evaluate(condition),
            evaluate(then_value),
            evaluate(else_value),
        ),
        List(values) => mc::list(values.into_iter().map(evaluate).collect()),
        Tuple(values) => mc::tuple(values.into_iter().map(evaluate).collect()),
        LIndex(list, index) => mc::lindex(evaluate(list), evaluate(index)),
        LAppend(list, value) => mc::lappend(evaluate(list), evaluate(value)),
        LConcat(left, right) => mc::lconcat(evaluate(left), evaluate(right)),
        LHead(list) => mc::lhead(evaluate(list)),
        LTail(list) => mc::ltail(evaluate(list)),
        LLen(list) => mc::llen(evaluate(list)),
        MGet(map, key) => mc::mget(evaluate(map), key.clone()),
        MRemove(map, key) => mc::mremove(evaluate(map), key.clone()),
        MInsert(map, key, value) => mc::minsert(evaluate(map), key.clone(), evaluate(value)),
        MHasKey(map, key) => mc::mhas_key(evaluate(map), key.clone()),
        Sin(value) => mc::sin(evaluate(value)),
        Cos(value) => mc::cos(evaluate(value)),
        Tan(value) => mc::tan(evaluate(value)),
        Abs(value) => mc::abs(evaluate(value)),
        _ => return None,
    };
    Some(stream)
}

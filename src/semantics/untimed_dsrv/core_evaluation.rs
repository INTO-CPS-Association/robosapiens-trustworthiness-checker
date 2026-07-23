//! Untimed DSRV evaluation reused by distributed semantics.

use super::combinators as mc;
use crate::core::{BinaryOperator, OutputStream, Value};
use crate::lang::dsrv::ast::{ExprRef, ExprView};

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
                BinaryOperator::Add => mc::plus(left, right),
                BinaryOperator::Subtract => mc::minus(left, right),
                BinaryOperator::Multiply => mc::mult(left, right),
                BinaryOperator::Divide => mc::div(left, right),
                BinaryOperator::Modulo => mc::modulo(left, right),
                BinaryOperator::Or => mc::or(left, right),
                BinaryOperator::And => mc::and(left, right),
                BinaryOperator::Implication => mc::implication(left, right),
                BinaryOperator::Concatenate => mc::concat(left, right),
                BinaryOperator::Equal => mc::eq(left, right),
                BinaryOperator::LessEqual => mc::le(left, right),
                BinaryOperator::Less => mc::lt(left, right),
                BinaryOperator::GreaterEqual => mc::ge(left, right),
                BinaryOperator::Greater => mc::gt(left, right),
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

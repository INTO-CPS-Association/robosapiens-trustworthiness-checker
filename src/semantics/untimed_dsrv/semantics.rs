use std::{collections::BTreeMap, rc::Rc};

use super::combinators as mc;
use super::dynamic;
pub(crate) use super::functions::bind_expression_for_benchmark;
use super::functions::{
    ScopedExpr, eval_apply, eval_fix, eval_list_filter, eval_list_fold, eval_list_map,
    eval_partial, make_function,
};
use crate::core::OutputStream;
use crate::core::Value;
use crate::lang::core::parser::ExprParser;
use crate::lang::dsrv::ast::{
    BoolBinOp, CheckedExpr, CompBinOp, Expr, ExprRef, ExprView, NumericalBinOp, SBinOp, StrBinOp,
};
use crate::lang::dsrv::type_checker::TCType;
use crate::semantics::{AsyncConfig, MonitoringSemantics};
use tracing::debug;

#[cfg(test)]
use ecow::EcoVec;

#[derive(Clone)]
pub struct UntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<Expr> + 'static,
{
    _parser: std::marker::PhantomData<Parser>,
}

pub(crate) fn evaluate<Parser, AC>(expr: Expr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    evaluate_scope::<Parser, AC>(ScopedExpr::unchecked(expr), ctx)
}

pub(crate) fn evaluate_checked<Parser, AC>(expr: CheckedExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    super::typed_execution::evaluate_checked::<Parser, AC>(expr, ctx)
}

pub(super) fn evaluate_scope<Parser, AC>(expr: ScopedExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    evaluate_ref::<Parser, AC>(expr.as_ref(), &expr, ctx)
}

fn evaluate_ref<Parser, AC>(
    node: ExprRef<'_>,
    expr: &ScopedExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value>,
{
    debug!("Creating async stream for expression: {:?}", node);
    let own_child = |child| expr.scope(child);
    let evaluate = |child| evaluate_ref::<Parser, AC>(child, expr, ctx);
    match node.view() {
        ExprView::Val(v) => {
            debug!("Constant value: {:?}", v);
            mc::val(v.clone())
        }
        ExprView::BinOp(e1, e2, op) => {
            debug!("Binary operation: {:?} {:?} {:?}", e1, op, e2);
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            match op {
                SBinOp::NOp(NumericalBinOp::Add) => {
                    debug!("Performing addition operation");
                    mc::plus(e1, e2)
                }
                SBinOp::NOp(NumericalBinOp::Sub) => {
                    debug!("Performing subtraction operation");
                    mc::minus(e1, e2)
                }
                SBinOp::NOp(NumericalBinOp::Mul) => {
                    debug!("Performing multiplication operation");
                    mc::mult(e1, e2)
                }
                SBinOp::NOp(NumericalBinOp::Div) => {
                    debug!("Performing division operation");
                    mc::div(e1, e2)
                }
                SBinOp::NOp(NumericalBinOp::Mod) => {
                    debug!("Performing modulo operation");
                    mc::modulo(e1, e2)
                }
                SBinOp::BOp(BoolBinOp::Or) => {
                    debug!("Performing logical OR operation");
                    mc::or(e1, e2)
                }
                SBinOp::BOp(BoolBinOp::And) => {
                    debug!("Performing logical AND operation");
                    mc::and(e1, e2)
                }
                SBinOp::BOp(BoolBinOp::Impl) => {
                    debug!("Performing logical IMPLICATION operation");
                    mc::implication(e1, e2)
                }
                SBinOp::SOp(StrBinOp::Concat) => {
                    debug!("Performing string concatenation");
                    mc::concat(e1, e2)
                }
                SBinOp::COp(CompBinOp::Eq) => {
                    debug!("Performing equality comparison");
                    mc::eq(e1, e2)
                }
                SBinOp::COp(CompBinOp::Le) => {
                    debug!("Performing less than or equal comparison");
                    mc::le(e1, e2)
                }
                SBinOp::COp(CompBinOp::Lt) => {
                    debug!("Performing less than comparison");
                    mc::lt(e1, e2)
                }
                SBinOp::COp(CompBinOp::Ge) => {
                    debug!("Performing greater than or equal comparison");
                    mc::ge(e1, e2)
                }
                SBinOp::COp(CompBinOp::Gt) => {
                    debug!("Performing greater than comparison");
                    mc::gt(e1, e2)
                }
            }
        }
        ExprView::Not(x) => {
            debug!("Performing logical NOT operation");
            let x = evaluate(x);
            mc::not(x)
        }
        ExprView::Var(v) => {
            debug!("Accessing variable: {:?}", v);
            if let Some(stream) = expr.resolve_stream(v) {
                return stream;
            }
            match expr.resolve(v) {
                Some(bound) => evaluate_scope::<Parser, AC>(bound, ctx),
                None => mc::var::<AC>(ctx, v.clone()),
            }
        }
        ExprView::Dynamic(source, _, scope) => {
            let dynamic_type = expr.typ(node).cloned().and_then(|expected| {
                expr.shared_type_info()
                    .map(|info| (Rc::clone(info), expected))
            });
            let e = evaluate(source);
            dynamic::dynamic_checked::<AC, Parser>(
                ctx,
                e,
                scope.clone(),
                expr.owner().cloned(),
                1,
                dynamic_type,
            )
        }
        ExprView::Defer(source, _, scope) => {
            let dynamic_type = expr.typ(node).cloned().and_then(|expected| {
                expr.shared_type_info()
                    .map(|info| (Rc::clone(info), expected))
            });
            let e = evaluate(source);
            dynamic::defer_checked::<AC, Parser>(
                ctx,
                e,
                scope.clone(),
                expr.owner().cloned(),
                1,
                dynamic_type,
            )
        }
        ExprView::Update(e1, e2) => {
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            mc::update(e1, e2)
        }
        ExprView::Default(e, d) => {
            let e = evaluate(e);
            let d = evaluate(d);
            mc::default(e, d)
        }
        ExprView::IsDefined(e) => {
            let e = evaluate(e);
            mc::is_defined(e)
        }
        ExprView::When(e) => {
            let e = evaluate(e);
            mc::when(e)
        }
        ExprView::Latch(e1, e2) => {
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            mc::latch(e1, e2)
        }
        ExprView::Init(e1, e2) => {
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            mc::init(e1, e2)
        }
        ExprView::SIndex(e, i) => {
            let e = evaluate(e);
            mc::sindex(e, i)
        }
        ExprView::If(b, e1, e2) => {
            let b = evaluate(b);
            let e1 = evaluate(e1);
            let e2 = evaluate(e2);
            mc::if_stm(b, e1, e2)
        }
        ExprView::List(exprs) => {
            let exprs: Vec<_> = exprs.into_iter().map(|e| evaluate(e)).collect();
            mc::list(exprs)
        }
        ExprView::Tuple(exprs) => {
            let exprs: Vec<_> = exprs.into_iter().map(|e| evaluate(e)).collect();
            mc::tuple(exprs)
        }
        ExprView::LIndex(e, i) => {
            let e = evaluate(e);
            let i = evaluate(i);
            mc::lindex(e, i)
        }
        ExprView::LAppend(lst, el) => {
            let lst = evaluate(lst);
            let el = evaluate(el);
            mc::lappend(lst, el)
        }
        ExprView::LConcat(lst1, lst2) => {
            let lst1 = evaluate(lst1);
            let lst2 = evaluate(lst2);
            mc::lconcat(lst1, lst2)
        }
        ExprView::LHead(lst) => {
            let lst = evaluate(lst);
            mc::lhead(lst)
        }
        ExprView::LTail(lst) => {
            let lst = evaluate(lst);
            mc::ltail(lst)
        }
        ExprView::LLen(lst) => {
            let lst = evaluate(lst);
            mc::llen(lst)
        }
        ExprView::Lambda(params, body) => {
            let params_display = params
                .iter()
                .map(|(name, typ)| format!("{}: {}", name, typ))
                .collect::<Vec<_>>()
                .join(", ");
            let body = own_child(body);
            let display = format!("\\{} -> {}", params_display, body.expr).into();
            mc::val(make_function::<AC, Parser>(
                display,
                params.clone(),
                body,
                ctx,
            ))
        }
        ExprView::Apply(func, args) => eval_apply::<AC, Parser>(
            own_child(func),
            args.into_iter().map(&own_child).collect(),
            ctx,
        ),
        ExprView::Fix(func) => eval_fix::<AC, Parser>(own_child(func), ctx),
        ExprView::Partial(func, args) => eval_partial::<AC, Parser>(
            own_child(func),
            args.into_iter().map(&own_child).collect(),
            ctx,
        ),
        ExprView::LMap(func, list) => {
            eval_list_map::<AC, Parser>(own_child(func), own_child(list), ctx)
        }
        ExprView::LFilter(func, list) => {
            eval_list_filter::<AC, Parser>(own_child(func), own_child(list), ctx)
        }
        ExprView::LFold(func, init, list) => {
            eval_list_fold::<AC, Parser>(own_child(func), own_child(init), own_child(list), ctx)
        }
        ExprView::Map(map) | ExprView::Struct(map) | ExprView::ObjectLiteral(map) => {
            let checked_type = expr.typ(node);
            let map: BTreeMap<_, _> = map
                .iter()
                .filter(|(name, _)| field_survives_checked_projection(checked_type, name))
                .map(|(k, v)| (k.clone(), evaluate(v)))
                .collect();
            mc::map(map)
        }
        ExprView::MGet(map, k) => {
            let map = evaluate(map);
            mc::mget(map, k.clone())
        }
        ExprView::SGet(value, key) => {
            let value_type = expr.typ(value).cloned();
            let value = evaluate(value);
            match (value_type, key.parse::<usize>()) {
                (Some(TCType::Tuple(_)), Ok(index)) | (None, Ok(index)) => mc::tget(value, index),
                _ => mc::mget(value, key.clone()),
            }
        }
        ExprView::MRemove(map, k) => {
            let map = evaluate(map);
            mc::mremove(map, k.clone())
        }
        ExprView::MInsert(map, k, v) => {
            let map = evaluate(map);
            let v = evaluate(v);
            mc::minsert(map, k.clone(), v)
        }
        ExprView::MHasKey(map, k) => {
            let map = evaluate(map);
            mc::mhas_key(map, k.clone())
        }
        ExprView::MonitoredAt(_, _) => {
            unimplemented!("Function monitored_at only supported in distributed semantics")
        }
        ExprView::Dist(_, _) => {
            unimplemented!("Function dist only supported in distributed semantics")
        }
        ExprView::Sin(v) => {
            let v = evaluate(v);
            mc::sin(v)
        }
        ExprView::Cos(v) => {
            let v = evaluate(v);
            mc::cos(v)
        }
        ExprView::Tan(v) => {
            let v = evaluate(v);
            mc::tan(v)
        }
        ExprView::Abs(v) => {
            let v = evaluate(v);
            mc::abs(v)
        }
    }
}

/// Width-subtyping projects checked structs to their declared runtime shape.
/// Maps, object literals, and unchecked expressions retain every field.
fn field_survives_checked_projection(typ: Option<&TCType>, name: &str) -> bool {
    match typ {
        Some(TCType::Struct(fields, _)) => fields.iter().any(|(field, _)| field == name),
        _ => true,
    }
}

impl<Parser, AC> MonitoringSemantics<AC> for UntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = Expr>,
{
    fn to_async_stream(expr: Expr, ctx: &AC::Ctx) -> OutputStream<Value> {
        evaluate::<Parser, AC>(expr, ctx)
    }

    fn to_async_stream_for_var(
        var: &crate::VarName,
        expr: Expr,
        ctx: &AC::Ctx,
    ) -> OutputStream<Value> {
        evaluate_scope::<Parser, AC>(ScopedExpr::unchecked(expr).with_owner(var.clone()), ctx)
    }
}

/// Untimed semantics for checked DSRV expressions.
///
/// Immutable type annotations select specialised scalar streams where possible
/// and support type-checking of
/// expressions introduced by `dynamic` and `defer` at runtime.
#[derive(Clone)]
pub struct CheckedUntimedDsrvSemantics<Parser>(std::marker::PhantomData<Parser>)
where
    Parser: ExprParser<Expr> + 'static;

impl<Parser, AC> MonitoringSemantics<AC> for CheckedUntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<Expr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = CheckedExpr>,
{
    fn to_async_stream(expr: CheckedExpr, ctx: &AC::Ctx) -> OutputStream<Value> {
        evaluate_checked::<Parser, AC>(expr, ctx)
    }

    fn to_async_stream_for_var(
        var: &crate::VarName,
        expr: CheckedExpr,
        ctx: &AC::Ctx,
    ) -> OutputStream<Value> {
        super::typed_execution::evaluate_scoped::<Parser, AC>(
            ScopedExpr::checked(expr).with_owner(var.clone()),
            ctx,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test;
    use crate::core::StreamTypeAscription;
    use crate::dsrv_fixtures::TestConfig;
    use crate::lang::dsrv::ast::Expr;
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::lang::dsrv::type_checker::type_check;
    use crate::runtime::asynchronous::Context;
    use crate::semantics::StreamContext;
    use ecow::eco_vec;
    use futures::stream::{self, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    #[test]
    fn checked_lexical_frame_preserves_node_annotations() {
        let source =
            "in property: Str\nout result: Int\nresult = (\\p: Str -> dynamic(p : Int))(property)";
        let spec = crate::lang::dsrv::lalr_parser::parse_str(source).unwrap();
        let checked = type_check(spec).unwrap();
        let expression = ScopedExpr::checked(checked.var_expr(&"result".into()).unwrap());
        let ExprView::Apply(function, mut args) = expression.as_ref().view() else {
            panic!("expected application");
        };
        let argument = expression.scope(args.next().expect("application has an argument"));
        let function = expression.scope(function);
        let ExprView::Lambda(params, body) = function.as_ref().view() else {
            panic!("expected lambda");
        };

        let framed = function
            .scope(body)
            .bind(params, EcoVec::from([argument]))
            .unwrap();

        assert!(framed.typ(framed.as_ref()).is_some());
        assert!(framed.shared_type_info().is_some());
        let ExprView::Dynamic(source, _, _) = framed.as_ref().view() else {
            panic!("expected dynamic expression");
        };
        let source = framed.scope(source);
        assert!(source.typ(source.as_ref()).is_some());
        assert!(framed.resolve(&"p".into()).is_some());
    }

    #[apply(async_test)]
    async fn lexical_function_arguments_keep_temporal_state(executor: Rc<LocalExecutor<'static>>) {
        let mut source = "(\\f: (Int -> Int) -> f(x))(\\v: Int -> v[1])";
        let expression = LALRParser::parse(&mut source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["x".into()],
            vec![Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]))],
            2,
        );
        let output = evaluate::<LALRParser, TestConfig>(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 1.into(), 2.into(), 3.into()]
        );
    }

    #[apply(async_test)]
    async fn first_class_function_values_keep_per_call_site_state(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut source = "(if choose then \\v: Int -> v[1] else \\v: Int -> v[1])(x)";
        let expression = LALRParser::parse(&mut source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["choose".into(), "x".into()],
            vec![
                Box::pin(stream::iter(vec![true.into(), true.into(), true.into()])),
                Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()])),
            ],
            2,
        );
        let output = evaluate::<LALRParser, TestConfig>(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 1.into(), 2.into()]
        );
    }

    #[apply(async_test)]
    async fn first_class_functions_share_arguments_and_update_captures(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut source = "(if choose then \\v: Int -> v[1] + bias else \\v: Int -> v + v)(x)";
        let expression = LALRParser::parse(&mut source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["choose".into(), "x".into(), "bias".into()],
            vec![
                Box::pin(stream::iter(vec![true.into(), true.into(), true.into()])),
                Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()])),
                Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()])),
            ],
            2,
        );
        let output = evaluate::<LALRParser, TestConfig>(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 21.into(), 32.into()]
        );
    }

    #[apply(async_test)]
    async fn first_class_function_capture_ports_advance_each_tick(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut source = "(if choose then \\v: Int -> bias[1] else \\v: Int -> bias[1])(x)";
        let expression = LALRParser::parse(&mut source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["choose".into(), "x".into(), "bias".into()],
            vec![
                Box::pin(stream::iter(vec![true.into(), true.into(), true.into()])),
                Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()])),
                Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()])),
            ],
            2,
        );
        let output = evaluate::<LALRParser, TestConfig>(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 10.into(), 20.into()]
        );
    }

    #[apply(async_test)]
    async fn partial_functions_preserve_stream_bindings(executor: Rc<LocalExecutor<'static>>) {
        let mut source = "partial(\\a: Int, b: Int -> a[1] + b, x)(y)";
        let expression = LALRParser::parse(&mut source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["x".into(), "y".into()],
            vec![
                Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()])),
                Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()])),
            ],
            2,
        );
        let output = evaluate::<LALRParser, TestConfig>(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![Value::Deferred, 21.into(), 32.into()]
        );
    }

    #[apply(async_test)]
    async fn first_class_function_switching_starts_a_new_instance(
        executor: Rc<LocalExecutor<'static>>,
    ) {
        let mut source = "(if choose then \\v: Int -> v[1] else \\v: Int -> v[1])(x)";
        let expression = LALRParser::parse(&mut source).unwrap();
        let mut context = Context::<TestConfig>::new(
            executor,
            vec!["choose".into(), "x".into()],
            vec![
                Box::pin(stream::iter(vec![
                    true.into(),
                    true.into(),
                    false.into(),
                    false.into(),
                    true.into(),
                ])),
                Box::pin(stream::iter(vec![
                    1.into(),
                    2.into(),
                    3.into(),
                    4.into(),
                    5.into(),
                ])),
            ],
            2,
        );
        let output = evaluate::<LALRParser, TestConfig>(expression, &context);
        context.run().await;

        assert_eq!(
            output.collect::<Vec<_>>().await,
            vec![
                Value::Deferred,
                1.into(),
                Value::Deferred,
                3.into(),
                Value::Deferred,
            ]
        );
    }

    fn to_stream(expr: Expr, ctx: &Context<TestConfig>) -> OutputStream<Value> {
        <UntimedDsrvSemantics<LALRParser> as MonitoringSemantics<TestConfig>>::to_async_stream(
            expr, ctx,
        )
    }
    // ============================================================================
    // DEFER TESTS
    // ============================================================================

    #[apply(async_test)]
    async fn test_defer_int(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x + 1")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = Context::<TestConfig>::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![2.into(), 3.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_int_x_squared(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x * x")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = Context::<TestConfig>::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_bool(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x && y")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into(), "y".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(true)]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Bool(true), Value::Bool(false)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_with_deferred_value(executor: Rc<LocalExecutor<'static>>) {
        // Use a variable stream carrying Deferred instead of Val(Deferred),
        // because Val produces an infinite repeating stream via stream::repeat.
        let expr = Expr::Defer(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
            eco_vec!["e".into(), "x".into()],
        );

        let e = Box::pin(stream::iter(vec![Value::Deferred]));
        let x = Box::pin(stream::iter(vec![2.into()]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Deferred];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_float(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x + 1.5")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = Context::<TestConfig>::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Float(2.5), Value::Float(3.5)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_str(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Defer(
            Box::new(Expr::Val("x ++ y")),
            StreamTypeAscription::Unascribed,
            eco_vec!["x".into(), "y".into()],
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("hello".into()),
            Value::Str("hi".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str(" world".into()),
            Value::Str(" there".into()),
        ]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![
            Value::Str("hello world".into()),
            Value::Str("hi there".into()),
        ];
        assert_eq!(res, exp);
    }

    // ============================================================================
    // DYNAMIC TESTS
    // ============================================================================

    #[apply(async_test)]
    async fn test_dynamic_int(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![2.into(), 4.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_int_x_squared(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x * x".into()),
            Value::Str("x * x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![4.into(), 9.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_start_deferred(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Str("x + 1".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Deferred, 3.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_mid_deferred(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Deferred,
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![2.into(), Value::Deferred, 5.into()];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_bool(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x && y".into()),
            Value::Str("x || y".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(false), Value::Bool(true)]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["e".into(), "x".into(), "y".into()],
            vec![e, x, y],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Bool(false), Value::Bool(true)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_float(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1.5".into()),
            Value::Str("x * 2.0".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![Value::Float(2.5), Value::Float(4.0)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_str(executor: Rc<LocalExecutor<'static>>) {
        let expr = Expr::Dynamic(
            Box::new(Expr::Var("e".into())),
            StreamTypeAscription::Unascribed,
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x ++ y".into()),
            Value::Str("y ++ x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![
            Value::Str("hello ".into()),
            Value::Str("hi ".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("world".into()),
            Value::Str("there".into()),
        ]));
        let mut ctx = Context::<TestConfig>::new(
            executor.clone(),
            vec!["e".into(), "x".into(), "y".into()],
            vec![e, x, y],
            10,
        );

        let res_stream = to_stream(expr, &ctx);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;

        let exp: Vec<Value> = vec![
            Value::Str("hello world".into()),
            Value::Str("therehi ".into()),
        ];
        assert_eq!(res, exp);
    }
}

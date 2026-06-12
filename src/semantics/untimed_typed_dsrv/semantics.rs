use super::combinators as mc;
use crate::core::OutputStream;
use crate::core::Value;
use crate::core::stream_casting::{from_typed_stream, to_typed_partial_stream, to_typed_stream};
use crate::core::values::StreamType;
use crate::lang::core::parser::ExprParser;
use crate::lang::dsrv::ast::SExpr;
use crate::lang::dsrv::ast::{BoolBinOp, CompBinOp, FloatBinOp, IntBinOp, StrBinOp};
use crate::lang::dsrv::type_checker::{
    PartialStreamValue, SExprBool, SExprDyn, SExprFloat, SExprInt, SExprStr, SExprTE, SExprUnit,
    TypedListExpr, TypedListExprKind, TypedMapExpr, TypedMapExprKind, TypedStructExpr,
    TypedStructExprKind,
};
use crate::semantics::untimed_untyped_dsrv::combinators as uc;
use crate::semantics::{AsyncConfig, MonitoringSemantics, StreamContext};
use ecow::EcoVec;

#[derive(Clone)]
pub struct TypedUntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<SExpr> + 'static,
{
    _parser: std::marker::PhantomData<Parser>,
}

impl<Parser, AC> MonitoringSemantics<AC> for TypedUntimedDsrvSemantics<Parser>
where
    Parser: ExprParser<SExpr> + 'static,
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
{
    fn to_async_stream(expr: AC::Expr, ctx: &AC::Ctx) -> OutputStream<Value> {
        match expr {
            SExprTE::Int(e) => from_typed_stream::<PartialStreamValue<i64>>(to_async_stream_int::<
                AC,
                Parser,
            >(e, ctx)),
            SExprTE::Float(e) => from_typed_stream::<PartialStreamValue<f64>>(
                to_async_stream_float::<AC, Parser>(e, ctx),
            ),
            SExprTE::Str(e) => from_typed_stream::<PartialStreamValue<String>>(
                to_async_stream_str::<AC, Parser>(e, ctx),
            ),
            SExprTE::Bool(e) => from_typed_stream::<PartialStreamValue<bool>>(
                to_async_stream_bool::<AC, Parser>(e, ctx),
            ),
            SExprTE::Unit(e) => from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(e, ctx),
            ),
            SExprTE::List(tl) => from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(
                eval_typed_list::<AC, Parser>(tl, ctx),
            ),
            SExprTE::Map(tm) => eval_typed_map::<AC, Parser>(tm, ctx),
            SExprTE::Struct(st) => eval_typed_struct::<AC, Parser>(st, ctx),
            SExprTE::Dyn(e) => eval_dyn::<AC, Parser>(e, ctx),
        }
    }
}

fn eval_dyn<AC, Parser>(expr: SExprDyn, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprDyn::Var(v) => ctx.var(&v).unwrap(),
        SExprDyn::Val(v) => uc::val(v),
        SExprDyn::Expr(e) => {
            let _ = e;
            panic!("Gradual dynamic expression fallback reached runtime without a concrete cast")
        }
    }
}

fn eval_typed_list<AC, Parser>(typed_list: TypedListExpr, ctx: &AC::Ctx) -> mc::ListStream
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    let list_stream_type = typed_list
        .list_tc_type()
        .to_stream_type()
        .expect("list_tc_type should be a concrete type at runtime");

    match typed_list.kind {
        TypedListExprKind::Var(v) => to_typed_partial_stream::<EcoVec<Value>>(ctx.var(&v).unwrap()),
        TypedListExprKind::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = eval_typed_list::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_list::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        TypedListExprKind::SIndex(e, i) => {
            let e = eval_typed_list::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        TypedListExprKind::Default(e1, e2) => {
            let e1 = eval_typed_list::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_list::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        TypedListExprKind::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(eval_typed_list::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(eval_typed_list::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<EcoVec<Value>>(uc::update(e1, e2))
        }
        TypedListExprKind::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(eval_typed_list::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<EcoVec<Value>>>(eval_typed_list::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<EcoVec<Value>>(uc::latch(e1, e2))
        }
        TypedListExprKind::Init(e1, e2) => {
            let e1 = eval_typed_list::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_list::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        TypedListExprKind::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, EcoVec<Value>>(ctx, e, vs, 1, &type_ctx, list_stream_type)
        }
        TypedListExprKind::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, EcoVec<Value>>(ctx, e, None, 1, &type_ctx, list_stream_type)
        }
        TypedListExprKind::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, EcoVec<Value>>(
                ctx,
                e,
                Some(vs),
                1,
                &type_ctx,
                list_stream_type,
            )
        }
        TypedListExprKind::Literal(exprs) => {
            let streams: Vec<OutputStream<Value>> = exprs
                .into_iter()
                .map(|e| {
                    <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                        e, ctx,
                    )
                })
                .collect();
            mc::list(streams, list_stream_type)
        }
        TypedListExprKind::LTail(inner) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner, ctx);
            mc::ltail(inner_stream)
        }
        TypedListExprKind::LConcat(a, b) => {
            let a_stream = eval_typed_list::<AC, Parser>(*a, ctx);
            let b_stream = eval_typed_list::<AC, Parser>(*b, ctx);
            mc::lconcat(a_stream, b_stream, list_stream_type)
        }
        TypedListExprKind::LAppend(list, elem) => {
            let list_stream = eval_typed_list::<AC, Parser>(*list, ctx);
            let elem_stream =
                <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                    *elem, ctx,
                );
            let elem_partial = mc::to_partial_value_stream(elem_stream);
            mc::lappend(list_stream, elem_partial, list_stream_type)
        }
        TypedListExprKind::LHeadList(inner_list) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            mc::lhead::<EcoVec<Value>>(inner_stream)
        }
        TypedListExprKind::LIndexList(inner_list, idx) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<EcoVec<Value>>(inner_stream, idx_stream)
        }
        TypedListExprKind::MGetMap(map, key) => to_typed_partial_stream::<EcoVec<Value>>(uc::mget(
            eval_typed_map::<AC, Parser>(*map, ctx),
            key,
        )),
        TypedListExprKind::SGetStruct(st, key) => to_typed_partial_stream::<EcoVec<Value>>(
            uc::mget(eval_typed_struct::<AC, Parser>(*st, ctx), key),
        ),
    }
}

fn eval_typed_struct<AC, Parser>(
    typed_struct: TypedStructExpr,
    ctx: &AC::Ctx,
) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    let typed_struct_stream_type = typed_struct
        .to_stream_type()
        .expect("struct type should be concrete at runtime");

    match typed_struct.kind {
        TypedStructExprKind::Var(v) => ctx.var(&v).unwrap(),
        TypedStructExprKind::Literal(entries) => {
            let streams = entries
                .into_iter()
                .map(|(k, e)| {
                    (
                        k,
                        <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                            e, ctx,
                        ),
                    )
                })
                .collect();
            uc::map(streams)
        }
        TypedStructExprKind::Default(e1, e2) => {
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::default(e1, e2)
        }
        TypedStructExprKind::Update(e1, e2) => {
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::update(e1, e2)
        }
        TypedStructExprKind::Latch(e1, e2) => {
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::latch(e1, e2)
        }
        TypedStructExprKind::If(b, e1, e2) => {
            let b = from_typed_stream::<PartialStreamValue<bool>>(
                to_async_stream_bool::<AC, Parser>(*b, ctx),
            );
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::if_stm(b, e1, e2)
        }
        TypedStructExprKind::Init(e1, e2) => {
            let e1 = eval_typed_struct::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_struct::<AC, Parser>(*e2, ctx);
            uc::init(e1, e2)
        }
        TypedStructExprKind::SIndex(e, i) => {
            let e = eval_typed_struct::<AC, Parser>(*e, ctx);
            uc::sindex(e, i)
        }
        TypedStructExprKind::SUpdate(st, key, value) => {
            let struct_stream = eval_typed_struct::<AC, Parser>(*st, ctx);
            let value_stream =
                <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                    *value, ctx,
                );
            uc::minsert(struct_stream, key, value_stream)
        }
        TypedStructExprKind::SGet(st, key) => {
            uc::mget(eval_typed_struct::<AC, Parser>(*st, ctx), key)
        }
        TypedStructExprKind::MGetMap(map, key) => {
            uc::mget(eval_typed_map::<AC, Parser>(*map, ctx), key)
        }
        TypedStructExprKind::LHeadList(inner_list) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::lhead::<Value>(inner_stream))
        }
        TypedStructExprKind::LIndexList(inner_list, idx) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::lindex::<Value>(
                inner_stream,
                idx_stream,
            ))
        }
        TypedStructExprKind::Defer(e, type_ctx, vs) => {
            let stream_type = typed_struct_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::defer::<AC, Parser, Value>(
                ctx,
                e,
                vs,
                1,
                &type_ctx,
                stream_type,
            ))
        }
        TypedStructExprKind::Dynamic(e, type_ctx) => {
            let stream_type = typed_struct_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::dynamic::<AC, Parser, Value>(
                ctx,
                e,
                None,
                1,
                &type_ctx,
                stream_type,
            ))
        }
        TypedStructExprKind::RestrictedDynamic(e, vs, type_ctx) => {
            let stream_type = typed_struct_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::dynamic::<AC, Parser, Value>(
                ctx,
                e,
                Some(vs),
                1,
                &type_ctx,
                stream_type,
            ))
        }
    }
}

fn eval_typed_map<AC, Parser>(typed_map: TypedMapExpr, ctx: &AC::Ctx) -> OutputStream<Value>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    let typed_map_stream_type = typed_map
        .map_tc_type()
        .to_stream_type()
        .expect("map type should be concrete at runtime");

    match typed_map.kind {
        TypedMapExprKind::Var(v) => ctx.var(&v).unwrap(),
        TypedMapExprKind::Literal(entries) => {
            let streams = entries
                .into_iter()
                .map(|(k, e)| {
                    (
                        k,
                        <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                            e, ctx,
                        ),
                    )
                })
                .collect();
            uc::map(streams)
        }
        TypedMapExprKind::Default(e1, e2) => {
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::default(e1, e2)
        }
        TypedMapExprKind::Update(e1, e2) => {
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::update(e1, e2)
        }
        TypedMapExprKind::Latch(e1, e2) => {
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::latch(e1, e2)
        }
        TypedMapExprKind::If(b, e1, e2) => {
            let b = from_typed_stream::<PartialStreamValue<bool>>(
                to_async_stream_bool::<AC, Parser>(*b, ctx),
            );
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::if_stm(b, e1, e2)
        }
        TypedMapExprKind::Init(e1, e2) => {
            let e1 = eval_typed_map::<AC, Parser>(*e1, ctx);
            let e2 = eval_typed_map::<AC, Parser>(*e2, ctx);
            uc::init(e1, e2)
        }
        TypedMapExprKind::SIndex(e, i) => {
            let e = eval_typed_map::<AC, Parser>(*e, ctx);
            uc::sindex(e, i)
        }
        TypedMapExprKind::MInsert(map, key, value) => {
            let map_stream = eval_typed_map::<AC, Parser>(*map, ctx);
            let value_stream =
                <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                    *value, ctx,
                );
            uc::minsert(map_stream, key, value_stream)
        }
        TypedMapExprKind::MRemove(map, key) => {
            let map_stream = eval_typed_map::<AC, Parser>(*map, ctx);
            uc::mremove(map_stream, key)
        }
        TypedMapExprKind::MGetMap(map, key) => {
            uc::mget(eval_typed_map::<AC, Parser>(*map, ctx), key)
        }
        TypedMapExprKind::SGetStruct(st, key) => {
            uc::mget(eval_typed_struct::<AC, Parser>(*st, ctx), key)
        }
        TypedMapExprKind::LHeadList(inner_list) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::lhead::<Value>(inner_stream))
        }
        TypedMapExprKind::LIndexList(inner_list, idx) => {
            let inner_stream = eval_typed_list::<AC, Parser>(*inner_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::lindex::<Value>(
                inner_stream,
                idx_stream,
            ))
        }
        TypedMapExprKind::Defer(e, type_ctx, vs) => {
            let stream_type = typed_map_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::defer::<AC, Parser, Value>(
                ctx,
                e,
                vs,
                1,
                &type_ctx,
                stream_type,
            ))
        }
        TypedMapExprKind::Dynamic(e, type_ctx) => {
            let stream_type = typed_map_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::dynamic::<AC, Parser, Value>(
                ctx,
                e,
                None,
                1,
                &type_ctx,
                stream_type,
            ))
        }
        TypedMapExprKind::RestrictedDynamic(e, vs, type_ctx) => {
            let stream_type = typed_map_stream_type.clone();
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            from_typed_stream::<PartialStreamValue<Value>>(mc::dynamic::<AC, Parser, Value>(
                ctx,
                e,
                Some(vs),
                1,
                &type_ctx,
                stream_type,
            ))
        }
    }
}

fn to_async_stream_int<AC, Parser>(
    expr: SExprInt,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<i64>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprInt::Cast(e) => to_typed_partial_stream::<i64>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprInt::Val(v) => mc::val(v),
        SExprInt::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            match op {
                IntBinOp::Add => mc::plus(e1, e2),
                IntBinOp::Sub => mc::minus(e1, e2),
                IntBinOp::Mul => mc::mult(e1, e2),
                IntBinOp::Div => mc::div(e1, e2),
                IntBinOp::Mod => mc::modulo(e1, e2),
            }
        }
        SExprInt::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprInt::SIndex(e, i) => {
            let e = to_async_stream_int::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprInt::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprInt::Default(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprInt::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<i64>>(
                to_async_stream_int::<AC, Parser>(*e1, ctx),
            );
            let e2 = from_typed_stream::<PartialStreamValue<i64>>(
                to_async_stream_int::<AC, Parser>(*e2, ctx),
            );
            to_typed_partial_stream::<i64>(uc::update(e1, e2))
        }
        SExprInt::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<i64>>(
                to_async_stream_int::<AC, Parser>(*e1, ctx),
            );
            let e2 = from_typed_stream::<PartialStreamValue<i64>>(
                to_async_stream_int::<AC, Parser>(*e2, ctx),
            );
            to_typed_partial_stream::<i64>(uc::latch(e1, e2))
        }
        SExprInt::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, i64>(ctx, e, vs, 1, &type_ctx, StreamType::Int)
        }
        SExprInt::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, i64>(ctx, e, None, 1, &type_ctx, StreamType::Int)
        }
        SExprInt::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, i64>(ctx, e, Some(vs), 1, &type_ctx, StreamType::Int)
        }
        SExprInt::Abs(e) => {
            let e = to_async_stream_int::<AC, Parser>(*e, ctx);
            mc::abs_int(e)
        }
        SExprInt::Init(e1, e2) => {
            let e1 = to_async_stream_int::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_int::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprInt::LLen(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::llen(list_stream)
        }
        SExprInt::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<i64>(list_stream)
        }
        SExprInt::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<i64>(list_stream, idx_stream)
        }
        SExprInt::MGetMap(map, key) => {
            to_typed_partial_stream::<i64>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprInt::SGetStruct(st, key) => {
            to_typed_partial_stream::<i64>(uc::mget(eval_typed_struct::<AC, Parser>(st, ctx), key))
        }
    }
}

fn to_async_stream_float<AC, Parser>(
    expr: SExprFloat,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<f64>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprFloat::Cast(e) => to_typed_partial_stream::<f64>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprFloat::Val(v) => mc::val(v),
        SExprFloat::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            match op {
                FloatBinOp::Add => mc::plus(e1, e2),
                FloatBinOp::Sub => mc::minus(e1, e2),
                FloatBinOp::Mul => mc::mult(e1, e2),
                FloatBinOp::Div => mc::div(e1, e2),
                FloatBinOp::Mod => mc::modulo(e1, e2),
            }
        }
        SExprFloat::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprFloat::SIndex(e, i) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprFloat::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprFloat::Default(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprFloat::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<f64>(uc::update(e1, e2))
        }
        SExprFloat::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<f64>>(to_async_stream_float::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<f64>(uc::latch(e1, e2))
        }
        SExprFloat::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, f64>(ctx, e, vs, 1, &type_ctx, StreamType::Float)
        }
        SExprFloat::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, f64>(ctx, e, None, 1, &type_ctx, StreamType::Float)
        }
        SExprFloat::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, f64>(ctx, e, Some(vs), 1, &type_ctx, StreamType::Float)
        }
        SExprFloat::Sin(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::sin(e)
        }
        SExprFloat::Cos(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::cos(e)
        }
        SExprFloat::Tan(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::tan(e)
        }
        SExprFloat::Abs(e) => {
            let e = to_async_stream_float::<AC, Parser>(*e, ctx);
            mc::abs_float(e)
        }
        SExprFloat::Init(e1, e2) => {
            let e1 = to_async_stream_float::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_float::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprFloat::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<f64>(list_stream)
        }
        SExprFloat::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<f64>(list_stream, idx_stream)
        }
        SExprFloat::MGetMap(map, key) => {
            to_typed_partial_stream::<f64>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprFloat::SGetStruct(st, key) => {
            to_typed_partial_stream::<f64>(uc::mget(eval_typed_struct::<AC, Parser>(st, ctx), key))
        }
    }
}

fn to_async_stream_str<AC, Parser>(
    expr: SExprStr,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<String>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprStr::Cast(e) => to_typed_partial_stream::<String>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprStr::Val(v) => mc::val(v),
        SExprStr::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprStr::SIndex(e, i) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprStr::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprStr::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, String>(ctx, e, None, 1, &type_ctx, StreamType::Str)
        }
        SExprStr::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, String>(ctx, e, Some(vs), 1, &type_ctx, StreamType::Str)
        }
        SExprStr::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            match op {
                StrBinOp::Concat => mc::concat(e1, e2),
            }
        }
        SExprStr::Default(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprStr::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<String>(uc::update(e1, e2))
        }
        SExprStr::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<String>>(to_async_stream_str::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<String>(uc::latch(e1, e2))
        }
        SExprStr::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, String>(ctx, e, vs, 1, &type_ctx, StreamType::Str)
        }
        SExprStr::Init(e1, e2) => {
            let e1 = to_async_stream_str::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_str::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprStr::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<String>(list_stream)
        }
        SExprStr::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<String>(list_stream, idx_stream)
        }
        SExprStr::MGetMap(map, key) => {
            to_typed_partial_stream::<String>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprStr::SGetStruct(st, key) => to_typed_partial_stream::<String>(uc::mget(
            eval_typed_struct::<AC, Parser>(st, ctx),
            key,
        )),
    }
}

/// Evaluate a `Cmp` node by dispatching on the operand type and comparison operator.
fn eval_cmp<AC, Parser>(
    op: CompBinOp,
    e1: SExprTE,
    e2: SExprTE,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match (e1, e2) {
        (SExprTE::Int(a), SExprTE::Int(b)) => {
            let a = to_async_stream_int::<AC, Parser>(a, ctx);
            let b = to_async_stream_int::<AC, Parser>(b, ctx);
            match op {
                CompBinOp::Eq => mc::eq(a, b),
                CompBinOp::Le => mc::le(a, b),
                CompBinOp::Lt => mc::lt(a, b),
                CompBinOp::Ge => mc::ge(a, b),
                CompBinOp::Gt => mc::gt(a, b),
            }
        }
        (SExprTE::Float(a), SExprTE::Float(b)) => {
            let a = to_async_stream_float::<AC, Parser>(a, ctx);
            let b = to_async_stream_float::<AC, Parser>(b, ctx);
            match op {
                CompBinOp::Eq => mc::eq_partial(a, b),
                CompBinOp::Le => mc::le_partial(a, b),
                CompBinOp::Lt => mc::lt(a, b),
                CompBinOp::Ge => mc::ge(a, b),
                CompBinOp::Gt => mc::gt(a, b),
            }
        }
        (SExprTE::Str(a), SExprTE::Str(b)) => {
            let a = to_async_stream_str::<AC, Parser>(a, ctx);
            let b = to_async_stream_str::<AC, Parser>(b, ctx);
            match op {
                CompBinOp::Eq => mc::eq(a, b),
                CompBinOp::Le => mc::le_partial(a, b),
                CompBinOp::Lt => mc::lt(a, b),
                CompBinOp::Ge => mc::ge(a, b),
                CompBinOp::Gt => mc::gt(a, b),
            }
        }
        (SExprTE::Bool(a), SExprTE::Bool(b)) => {
            let a = to_async_stream_bool::<AC, Parser>(a, ctx);
            let b = to_async_stream_bool::<AC, Parser>(b, ctx);
            // Only Eq is valid for Bool (enforced by the type checker)
            mc::eq(a, b)
        }
        (SExprTE::Unit(a), SExprTE::Unit(b)) => {
            let a = to_async_stream_unit::<AC, Parser>(a, ctx);
            let b = to_async_stream_unit::<AC, Parser>(b, ctx);
            // Only Eq is valid for Unit (enforced by the type checker)
            mc::eq(a, b)
        }
        (SExprTE::Dyn(a), SExprTE::Dyn(b)) if op == CompBinOp::Eq => {
            let a = eval_dyn::<AC, Parser>(a, ctx);
            let b = eval_dyn::<AC, Parser>(b, ctx);
            to_typed_partial_stream::<bool>(uc::eq(a, b))
        }
        _ => panic!("eval_cmp: type checker should have ensured operand types match"),
    }
}

/// Evaluate an `IsDefined` node by dispatching on the inner expression type.
fn eval_is_defined<AC, Parser>(
    inner: SExprTE,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match inner {
        SExprTE::Int(e) => mc::is_defined(to_async_stream_int::<AC, Parser>(e, ctx)),
        SExprTE::Float(e) => mc::is_defined(to_async_stream_float::<AC, Parser>(e, ctx)),
        SExprTE::Str(e) => mc::is_defined(to_async_stream_str::<AC, Parser>(e, ctx)),
        SExprTE::Bool(e) => mc::is_defined(to_async_stream_bool::<AC, Parser>(e, ctx)),
        SExprTE::Unit(e) => mc::is_defined(to_async_stream_unit::<AC, Parser>(e, ctx)),
        SExprTE::List(tl) => mc::is_defined_list(eval_typed_list::<AC, Parser>(tl, ctx)),
        SExprTE::Map(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_typed_map::<AC, Parser>(e, ctx)))
        }
        SExprTE::Struct(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_typed_struct::<AC, Parser>(e, ctx)))
        }
        SExprTE::Dyn(e) => {
            to_typed_partial_stream::<bool>(uc::is_defined(eval_dyn::<AC, Parser>(e, ctx)))
        }
    }
}

/// Evaluate a `When` node by dispatching on the inner expression type.
fn eval_when<AC, Parser>(inner: SExprTE, ctx: &AC::Ctx) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match inner {
        SExprTE::Int(e) => mc::when(to_async_stream_int::<AC, Parser>(e, ctx)),
        SExprTE::Float(e) => mc::when(to_async_stream_float::<AC, Parser>(e, ctx)),
        SExprTE::Str(e) => mc::when(to_async_stream_str::<AC, Parser>(e, ctx)),
        SExprTE::Bool(e) => mc::when(to_async_stream_bool::<AC, Parser>(e, ctx)),
        SExprTE::Unit(e) => mc::when(to_async_stream_unit::<AC, Parser>(e, ctx)),
        SExprTE::List(tl) => mc::when_list(eval_typed_list::<AC, Parser>(tl, ctx)),
        SExprTE::Map(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_typed_map::<AC, Parser>(e, ctx)))
        }
        SExprTE::Struct(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_typed_struct::<AC, Parser>(e, ctx)))
        }
        SExprTE::Dyn(e) => {
            to_typed_partial_stream::<bool>(uc::when(eval_dyn::<AC, Parser>(e, ctx)))
        }
    }
}

fn to_async_stream_bool<AC, Parser>(
    expr: SExprBool,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<bool>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprBool::Cast(e) => to_typed_partial_stream::<bool>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprBool::Val(b) => mc::val(b),
        SExprBool::Cmp(op, e1, e2) => eval_cmp::<AC, Parser>(op, *e1, *e2, ctx),
        SExprBool::BinOp(e1, e2, op) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            match op {
                BoolBinOp::And => mc::and(e1, e2),
                BoolBinOp::Or => mc::or(e1, e2),
                BoolBinOp::Impl => mc::implication(e1, e2),
            }
        }
        SExprBool::Not(b) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            mc::not(b)
        }
        SExprBool::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprBool::SIndex(e, i) => {
            let e = to_async_stream_bool::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprBool::If(b, e1, e2) => {
            let b2 = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::if_stm(b2, e1, e2)
        }
        SExprBool::Default(e1, e2) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprBool::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<bool>(uc::update(e1, e2))
        }
        SExprBool::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<
                AC,
                Parser,
            >(*e1, ctx));
            let e2 = from_typed_stream::<PartialStreamValue<bool>>(to_async_stream_bool::<
                AC,
                Parser,
            >(*e2, ctx));
            to_typed_partial_stream::<bool>(uc::latch(e1, e2))
        }
        SExprBool::Defer(e, type_ctx, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, bool>(ctx, e, vs, 1, &type_ctx, StreamType::Bool)
        }
        SExprBool::Dynamic(e, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, bool>(ctx, e, None, 1, &type_ctx, StreamType::Bool)
        }
        SExprBool::RestrictedDynamic(e, vs, type_ctx) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, bool>(ctx, e, Some(vs), 1, &type_ctx, StreamType::Bool)
        }
        SExprBool::IsDefined(inner) => eval_is_defined::<AC, Parser>(*inner, ctx),
        SExprBool::When(inner) => eval_when::<AC, Parser>(*inner, ctx),
        SExprBool::Init(e1, e2) => {
            let e1 = to_async_stream_bool::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_bool::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprBool::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<bool>(list_stream)
        }
        SExprBool::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<bool>(list_stream, idx_stream)
        }
        SExprBool::MGetMap(map, key) => {
            to_typed_partial_stream::<bool>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprBool::SGetStruct(st, key) => {
            to_typed_partial_stream::<bool>(uc::mget(eval_typed_struct::<AC, Parser>(st, ctx), key))
        }
        SExprBool::MHasKeyMap(map, key) => to_typed_partial_stream::<bool>(uc::mhas_key(
            eval_typed_map::<AC, Parser>(map, ctx),
            key,
        )),
    }
}

fn to_async_stream_unit<AC, Parser>(
    expr: SExprUnit,
    ctx: &AC::Ctx,
) -> OutputStream<PartialStreamValue<()>>
where
    AC: AsyncConfig<Val = Value, Expr = SExprTE>,
    Parser: ExprParser<SExpr> + 'static,
{
    match expr {
        SExprUnit::Cast(e) => to_typed_partial_stream::<()>(
            <TypedUntimedDsrvSemantics<Parser> as MonitoringSemantics<AC>>::to_async_stream(
                *e, ctx,
            ),
        ),
        SExprUnit::Val(v) => mc::val(v),
        SExprUnit::Var(v) => to_typed_stream(ctx.var(&v).unwrap()),
        SExprUnit::SIndex(e, i) => {
            let e = to_async_stream_unit::<AC, Parser>(*e, ctx);
            mc::sindex(e, i)
        }
        SExprUnit::If(b, e1, e2) => {
            let b = to_async_stream_bool::<AC, Parser>(*b, ctx);
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::if_stm(b, e1, e2)
        }
        SExprUnit::Default(e1, e2) => {
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::default(e1, e2)
        }
        SExprUnit::Update(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(*e1, ctx),
            );
            let e2 = from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(*e2, ctx),
            );
            to_typed_partial_stream::<()>(uc::update(e1, e2))
        }
        SExprUnit::Latch(e1, e2) => {
            let e1 = from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(*e1, ctx),
            );
            let e2 = from_typed_stream::<PartialStreamValue<()>>(
                to_async_stream_unit::<AC, Parser>(*e2, ctx),
            );
            to_typed_partial_stream::<()>(uc::latch(e1, e2))
        }
        SExprUnit::Defer(e, type_info, vs) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::defer::<AC, Parser, ()>(ctx, e, vs, 1, &type_info, StreamType::Unit)
        }
        SExprUnit::Dynamic(e, type_info) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, ()>(ctx, e, None, 1, &type_info, StreamType::Unit)
        }
        SExprUnit::RestrictedDynamic(e, vs, type_info) => {
            let e = to_async_stream_str::<AC, Parser>(*e, ctx);
            mc::dynamic::<AC, Parser, ()>(ctx, e, Some(vs), 1, &type_info, StreamType::Unit)
        }
        SExprUnit::Init(e1, e2) => {
            let e1 = to_async_stream_unit::<AC, Parser>(*e1, ctx);
            let e2 = to_async_stream_unit::<AC, Parser>(*e2, ctx);
            mc::init(e1, e2)
        }
        SExprUnit::LHeadList(typed_list) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            mc::lhead::<()>(list_stream)
        }
        SExprUnit::LIndexList(typed_list, idx) => {
            let list_stream = eval_typed_list::<AC, Parser>(typed_list, ctx);
            let idx_stream = to_async_stream_int::<AC, Parser>(*idx, ctx);
            mc::lindex::<()>(list_stream, idx_stream)
        }
        SExprUnit::MGetMap(map, key) => {
            to_typed_partial_stream::<()>(uc::mget(eval_typed_map::<AC, Parser>(map, ctx), key))
        }
        SExprUnit::SGetStruct(st, key) => {
            to_typed_partial_stream::<()>(uc::mget(eval_typed_struct::<AC, Parser>(st, ctx), key))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::StreamType;
    use crate::dsrv_fixtures::TestTypedConfig;
    use crate::lang::dsrv::type_checker::TypeInfo;
    use crate::runtime::asynchronous::Context;
    use crate::{async_test, lang::dsrv::lalr_parser::LALRParser};
    use ecow::eco_vec;
    use futures::stream::{self, StreamExt};
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    type TestCtx = Context<TestTypedConfig>;

    fn type_info(vars: &[(&str, StreamType)]) -> TypeInfo {
        vars.iter().map(|(v, t)| ((*v).into(), t.clone())).collect()
    }

    #[apply(async_test)]
    async fn test_defer_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1".into())));
        let defer_expr = SExprInt::Defer(
            e_str,
            type_info(&[("x", StreamType::Int)]),
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(3)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_int_x_squared_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x * x".into())));
        let defer_expr = SExprInt::Defer(
            e_str,
            type_info(&[("x", StreamType::Int)]),
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x && y".into())));
        let defer_expr = SExprBool::Defer(
            e_str,
            type_info(&[("x", StreamType::Bool), ("y", StreamType::Bool)]),
            eco_vec!["x".into(), "y".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(true)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_with_deferred_value_runtime(executor: Rc<LocalExecutor<'static>>) {
        // Use a variable stream carrying Deferred instead of Val(Deferred),
        // because Val produces an infinite repeating stream via stream::repeat.
        let e_str = Box::new(SExprStr::Var("e".into()));
        let defer_expr = SExprInt::Defer(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
            eco_vec!["x".into(), "e".into()],
        );

        let e = Box::pin(stream::iter(vec![Value::Deferred]));
        let x = Box::pin(stream::iter(vec![2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![PartialStreamValue::Deferred];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(2), PartialStreamValue::Known(4)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_int_x_squared_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x * x".into()),
            Value::Str("x * x".into()),
        ]));
        let x = Box::pin(stream::iter(vec![2.into(), 3.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Known(4), PartialStreamValue::Known(9)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_start_deferred_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Str("x + 1".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> =
            vec![PartialStreamValue::Deferred, PartialStreamValue::Known(3)];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_with_mid_deferred_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprInt::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Int)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1".into()),
            Value::Deferred,
            Value::Str("x + 2".into()),
        ]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(2),
            PartialStreamValue::Deferred,
            PartialStreamValue::Known(5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprBool::Dynamic(
            e_str,
            type_info(&[
                ("e", StreamType::Str),
                ("x", StreamType::Bool),
                ("y", StreamType::Bool),
            ]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x && y".into()),
            Value::Str("x || y".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let y = Box::pin(stream::iter(vec![Value::Bool(false), Value::Bool(true)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into(), "y".into()],
            vec![e, x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x + 1.5".into())));
        let defer_expr = SExprFloat::Defer(
            e_str,
            type_info(&[("x", StreamType::Float)]),
            eco_vec!["x".into()],
        );

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(3.5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_defer_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Val(PartialStreamValue::Known("x ++ y".into())));
        let defer_expr = SExprStr::Defer(
            e_str,
            type_info(&[("x", StreamType::Str), ("y", StreamType::Str)]),
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
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(defer_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("hi there".into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprStr::Dynamic(
            e_str,
            type_info(&[
                ("e", StreamType::Str),
                ("x", StreamType::Str),
                ("y", StreamType::Str),
            ]),
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
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into(), "y".into()],
            vec![e, x, y],
            10,
        );

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("hello world".into()),
            PartialStreamValue::Known("therehi ".into()),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_dynamic_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let e_str = Box::new(SExprStr::Var("e".into()));
        let dynamic_expr = SExprFloat::Dynamic(
            e_str,
            type_info(&[("e", StreamType::Str), ("x", StreamType::Float)]),
        );

        let e = Box::pin(stream::iter(vec![
            Value::Str("x + 1.5".into()),
            Value::Str("x * 2.0".into()),
        ]));
        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["e".into(), "x".into()],
            vec![e, x],
            10,
        );

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(dynamic_expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(4.0),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for new comparison operators ==========

    #[apply(async_test)]
    async fn test_lt_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Lt,
            Box::new(SExprTE::Int(SExprInt::Var("x".into()))),
            Box::new(SExprTE::Int(SExprInt::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Ge,
            Box::new(SExprTE::Int(SExprInt::Var("x".into()))),
            Box::new(SExprTE::Int(SExprInt::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Gt,
            Box::new(SExprTE::Int(SExprInt::Var("x".into()))),
            Box::new(SExprTE::Int(SExprInt::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 5.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![2.into(), 5.into(), 1.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_eq_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Eq,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(3.0)]));
        let y = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Float(2.0)]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_le_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Le,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_lt_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Lt,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Ge,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Gt,
            Box::new(SExprTE::Float(SExprFloat::Var("x".into()))),
            Box::new(SExprTE::Float(SExprFloat::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Float(1.0),
            Value::Float(3.0),
            Value::Float(2.0),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Float(1.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_le_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Le,
            Box::new(SExprTE::Str(SExprStr::Var("x".into()))),
            Box::new(SExprTE::Str(SExprStr::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("banana".into()),
            Value::Str("cherry".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_lt_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Lt,
            Box::new(SExprTE::Str(SExprStr::Var("x".into()))),
            Box::new(SExprTE::Str(SExprStr::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("banana".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_ge_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Ge,
            Box::new(SExprTE::Str(SExprStr::Var("x".into()))),
            Box::new(SExprTE::Str(SExprStr::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("apple".into()),
            Value::Str("cherry".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_gt_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Cmp(
            CompBinOp::Gt,
            Box::new(SExprTE::Str(SExprStr::Var("x".into()))),
            Box::new(SExprTE::Str(SExprStr::Var("y".into()))),
        );

        let x = Box::pin(stream::iter(vec![
            Value::Str("cherry".into()),
            Value::Str("apple".into()),
        ]));
        let y = Box::pin(stream::iter(vec![
            Value::Str("banana".into()),
            Value::Str("banana".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "y".into()],
            vec![x, y],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for trigonometric functions ==========

    #[apply(async_test)]
    async fn test_sin_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Sin(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::FRAC_PI_2),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    #[apply(async_test)]
    async fn test_cos_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Cos(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::PI),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - (-1.0)).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    #[apply(async_test)]
    async fn test_tan_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Tan(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(0.0),
            Value::Float(std::f64::consts::FRAC_PI_4),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 2);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 1.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    // ========== Tests for abs ==========

    #[apply(async_test)]
    async fn test_abs_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Abs(Box::new(SExprInt::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Int(-5),
            Value::Int(3),
            Value::Int(0),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(5),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(0),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_abs_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Abs(Box::new(SExprFloat::Var("x".into())));

        let x = Box::pin(stream::iter(vec![
            Value::Float(-5.5),
            Value::Float(3.2),
            Value::Float(0.0),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        assert_eq!(res.len(), 3);
        match &res[0] {
            PartialStreamValue::Known(v) => assert!((v - 5.5).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[1] {
            PartialStreamValue::Known(v) => assert!((v - 3.2).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
        match &res[2] {
            PartialStreamValue::Known(v) => assert!((v - 0.0).abs() < 1e-10),
            other => panic!("Expected Known, got {:?}", other),
        }
    }

    // ========== Tests for is_defined ==========

    #[apply(async_test)]
    async fn test_is_defined_int_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefined(Box::new(SExprTE::Int(SExprInt::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Int(1),
            Value::Deferred,
            Value::Int(3),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefined(Box::new(SExprTE::Float(SExprFloat::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![Value::Float(1.0), Value::Deferred]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefined(Box::new(SExprTE::Bool(SExprBool::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Bool(true),
            Value::Deferred,
            Value::Bool(false),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_is_defined_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::IsDefined(Box::new(SExprTE::Str(SExprStr::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Str("hello".into()),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for when ==========

    #[apply(async_test)]
    async fn test_when_int_never_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::When(Box::new(SExprTE::Int(SExprInt::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Deferred,
            Value::Deferred,
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_when_int_eventually_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::When(Box::new(SExprTE::Int(SExprInt::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![
            Value::Deferred,
            Value::Int(10),
            Value::Int(20),
        ]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_when_bool_immediately_defined_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::When(Box::new(SExprTE::Bool(SExprBool::Var("x".into()))));

        let x = Box::pin(stream::iter(vec![Value::Bool(true), Value::Bool(false)]));
        let mut ctx = TestCtx::new(executor.clone(), vec!["x".into()], vec![x], 10);

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(true),
        ];
        assert_eq!(res, exp);
    }

    // ========== Tests for init ==========

    #[apply(async_test)]
    async fn test_init_int_no_noval_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Init(
            Box::new(SExprInt::Var("x".into())),
            Box::new(SExprInt::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let d = Box::pin(stream::iter(vec![10.into(), 20.into(), 30.into()]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(1),
            PartialStreamValue::Known(2),
            PartialStreamValue::Known(3),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_int_starts_with_noval_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprInt::Init(
            Box::new(SExprInt::Var("x".into())),
            Box::new(SExprInt::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::NoVal,
            Value::Int(3),
            Value::Int(4),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Int(10),
            Value::Int(20),
            Value::Int(30),
            Value::Int(40),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_int::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<i64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<i64>> = vec![
            PartialStreamValue::Known(10),
            PartialStreamValue::Known(20),
            PartialStreamValue::Known(3),
            PartialStreamValue::Known(4),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_float_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprFloat::Init(
            Box::new(SExprFloat::Var("x".into())),
            Box::new(SExprFloat::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::Float(2.5),
            Value::Float(3.5),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Float(10.0),
            Value::Float(20.0),
            Value::Float(30.0),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_float::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<f64>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<f64>> = vec![
            PartialStreamValue::Known(10.0),
            PartialStreamValue::Known(2.5),
            PartialStreamValue::Known(3.5),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_bool_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprBool::Init(
            Box::new(SExprBool::Var("x".into())),
            Box::new(SExprBool::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![
            Value::NoVal,
            Value::Bool(true),
            Value::Bool(false),
        ]));
        let d = Box::pin(stream::iter(vec![
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_bool::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<bool>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<bool>> = vec![
            PartialStreamValue::Known(false),
            PartialStreamValue::Known(true),
            PartialStreamValue::Known(false),
        ];
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_init_str_runtime(executor: Rc<LocalExecutor<'static>>) {
        let expr = SExprStr::Init(
            Box::new(SExprStr::Var("x".into())),
            Box::new(SExprStr::Var("d".into())),
        );

        let x = Box::pin(stream::iter(vec![Value::NoVal, Value::Str("hello".into())]));
        let d = Box::pin(stream::iter(vec![
            Value::Str("default".into()),
            Value::Str("default".into()),
        ]));
        let mut ctx = TestCtx::new(
            executor.clone(),
            vec!["x".into(), "d".into()],
            vec![x, d],
            10,
        );

        let res_stream = to_async_stream_str::<TestTypedConfig, LALRParser>(expr, &ctx);
        ctx.run().await;
        let res: Vec<PartialStreamValue<String>> = res_stream.collect().await;

        let exp: Vec<PartialStreamValue<String>> = vec![
            PartialStreamValue::Known("default".into()),
            PartialStreamValue::Known("hello".into()),
        ];
        assert_eq!(res, exp);
    }
}

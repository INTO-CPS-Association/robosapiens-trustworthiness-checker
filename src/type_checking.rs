use futures::future::TryFlattenStream;
use futures::Stream;
use futures::{stream::BoxStream, StreamExt};

use crate::{OutputStream, StreamExpr};
use crate::{
    ast::{BExpr, SBinOp, SExpr, StreamType},
    core::{
        SemanticError, StreamTransformationFn, TypeCheckable, TypeCheckableHelper, TypeSystem,
        Value,
    },
    ConcreteStreamData, VarName,
};
use std::fmt::{Debug, Display};
use std::ops::DerefMut;
use std::pin::Pin;
use std::{collections::BTreeMap, ops::Deref};

pub struct LOLATypeSystem;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LOLATypedValue {
    Int(i64),
    Str(String),
    Bool(bool),
    Unit,
}

impl Display for LOLATypedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LOLATypedValue::Int(i) => write!(f, "{}", i),
            LOLATypedValue::Str(s) => write!(f, "{}", s),
            LOLATypedValue::Bool(b) => write!(f, "{}", b),
            LOLATypedValue::Unit => write!(f, "()"),
        }
    }
}

impl StreamExpr<StreamType> for SExprTE<VarName> {
    fn var(typ: StreamType, var: &VarName) -> Self {
        match typ {
            StreamType::Int => SExprTE::Int(SExprT::Var(var.clone())),
            StreamType::Str => SExprTE::Str(SExprT::Var(var.clone())),
            StreamType::Bool => SExprTE::Bool(SExprT::Var(var.clone())),
            StreamType::Unit => SExprTE::Unit(SExprT::Var(var.clone())),
        }
    }
}

impl TypeSystem for LOLATypeSystem {
    type Type = StreamType;
    type TypedValue = LOLATypedValue;
    type TypedExpr = SExprTE<VarName>;
    type TypedStream = LOLAStream;

    fn type_of_expr(expr: &Self::TypedExpr) -> Self::Type {
        match expr {
            SExprTE::Int(_) => StreamType::Int,
            SExprTE::Str(_) => StreamType::Str,
            SExprTE::Bool(_) => StreamType::Bool,
            SExprTE::Unit(_) => StreamType::Unit,
        }
    }

    fn type_of_stream(value: &Self::TypedStream) -> Self::Type {
        match value {
            LOLAStream::Int(_) => StreamType::Int,
            LOLAStream::Str(_) => StreamType::Str,
            LOLAStream::Bool(_) => StreamType::Bool,
            LOLAStream::Unit(_) => StreamType::Unit,
        }
    }

    fn transform_stream(
        transformation: impl StreamTransformationFn,
        stream: <Self as TypeSystem>::TypedStream,
    ) -> <Self as TypeSystem>::TypedStream {
        match stream {
            LOLAStream::Int(pin) => {
                let new_stream = transformation.transform(pin);
                LOLAStream::Int(new_stream)
            }
            LOLAStream::Str(pin) => {
                let new_stream = transformation.transform(pin);
                LOLAStream::Str(new_stream)
            }
            LOLAStream::Bool(pin) => {
                let new_stream = transformation.transform(pin);
                LOLAStream::Bool(new_stream)
            }
            LOLAStream::Unit(pin) => {
                let new_stream = transformation.transform(pin);
                LOLAStream::Unit(new_stream)
            }
        }
    }

    fn type_of_value(value: &Self::TypedValue) -> Self::Type {
        match value {
            LOLATypedValue::Int(_) => StreamType::Int,
            LOLATypedValue::Str(_) => StreamType::Str,
            LOLATypedValue::Bool(_) => StreamType::Bool,
            LOLATypedValue::Unit => StreamType::Unit,
        }
    }

    fn to_typed_stream(
        typ: Self::Type,
        stream: OutputStream<Self::TypedValue>,
    ) -> Self::TypedStream {
        match typ {
            StreamType::Int => LOLAStream::Int(Box::pin(stream.map(|v| match v {
                LOLATypedValue::Int(i) => i,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Str => LOLAStream::Str(Box::pin(stream.map(|v| match v {
                LOLATypedValue::Str(s) => s,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Bool => LOLAStream::Bool(Box::pin(stream.map(|v| match v {
                LOLATypedValue::Bool(b) => b,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Unit => LOLAStream::Unit(Box::pin(stream.map(|v| match v {
                LOLATypedValue::Unit => (),
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
        }
    }
}

pub enum LOLAStream {
    Int(BoxStream<'static, i64>),
    Str(BoxStream<'static, String>),
    Bool(BoxStream<'static, bool>),
    Unit(BoxStream<'static, ()>),
}

impl Stream for LOLAStream {
    type Item = LOLATypedValue;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            LOLAStream::Int(pin) => pin
                .poll_next_unpin(cx)
                .map(|opt| opt.map(|v| LOLATypedValue::Int(v))),
            LOLAStream::Str(pin) => pin
                .poll_next_unpin(cx)
                .map(|opt| opt.map(|v| LOLATypedValue::Str(v))),
            LOLAStream::Bool(pin) => pin
                .poll_next_unpin(cx)
                .map(|opt| opt.map(|v| LOLATypedValue::Bool(v))),
            LOLAStream::Unit(pin) => pin
                .poll_next_unpin(cx)
                .map(|opt| opt.map(|_| LOLATypedValue::Unit)),
        }
    }
}

impl From<(StreamType, OutputStream<LOLATypedValue>)> for LOLAStream {
    fn from((typ, x): (StreamType, OutputStream<LOLATypedValue>)) -> Self {
        match typ {
            StreamType::Int => LOLAStream::Int(Box::pin(x.map(|v| match v {
                LOLATypedValue::Int(i) => i,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Str => LOLAStream::Str(Box::pin(x.map(|v| match v {
                LOLATypedValue::Str(s) => s,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Bool => LOLAStream::Bool(Box::pin(x.map(|v| match v {
                LOLATypedValue::Bool(b) => b,
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
            StreamType::Unit => LOLAStream::Unit(Box::pin(x.map(|v| match v {
                LOLATypedValue::Unit => (),
                _ => panic!("Invalid stream type specialization in runtime"),
            }))),
        }
    }
}

impl LOLATypeSystem {}

// Trait defining the allowed types for expression values
impl Value<LOLATypeSystem> for i64 {
    fn type_of(&self) -> <LOLATypeSystem as TypeSystem>::Type {
        StreamType::Int
    }

    fn to_typed_value(&self) -> <LOLATypeSystem as TypeSystem>::TypedValue {
        LOLATypedValue::Int(*self)
    }
}
impl Value<LOLATypeSystem> for String {
    fn type_of(&self) -> <LOLATypeSystem as TypeSystem>::Type {
        StreamType::Str
    }

    fn to_typed_value(&self) -> <LOLATypeSystem as TypeSystem>::TypedValue {
        LOLATypedValue::Str(self.clone())
    }
}
impl Value<LOLATypeSystem> for bool {
    fn type_of(&self) -> <LOLATypeSystem as TypeSystem>::Type {
        StreamType::Bool
    }

    fn to_typed_value(&self) -> <LOLATypeSystem as TypeSystem>::TypedValue {
        LOLATypedValue::Bool(*self)
    }
}
impl Value<LOLATypeSystem> for () {
    fn type_of(&self) -> <LOLATypeSystem as TypeSystem>::Type {
        StreamType::Int
    }

    fn to_typed_value(&self) -> <LOLATypeSystem as TypeSystem>::TypedValue {
        LOLATypedValue::Unit
    }
}

// Stream expressions - now with types
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SExprT<ValT: Value<LOLATypeSystem>, VarT: Debug> {
    If(Box<BExpr<VarT>>, Box<Self>, Box<Self>),

    // Stream indexing
    Index(
        // Inner SExpr e
        Box<Self>,
        // Index i
        isize,
        // Default c
        ValT,
    ),

    // Arithmetic Stream expression
    Val(ValT),

    BinOp(Box<Self>, Box<Self>, SBinOp),

    Var(VarT),

    // Eval
    Eval(Box<Self>),
}

// Stream expression typed enum
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SExprTE<VarT: Debug> {
    Int(SExprT<i64, VarT>),
    Str(SExprT<String, VarT>),
    Bool(SExprT<bool, VarT>),
    Unit(SExprT<(), VarT>),
}

impl TypeCheckableHelper<LOLATypeSystem> for ConcreteStreamData {
    fn type_check_raw(
        &self,
        _: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE<VarName>, ()> {
        match self {
            ConcreteStreamData::Int(v) => Ok(SExprTE::Int(SExprT::Val(*v))),
            ConcreteStreamData::Str(v) => Ok(SExprTE::Str(SExprT::Val(v.clone()))),
            ConcreteStreamData::Bool(v) => Ok(SExprTE::Bool(SExprT::Val(*v))),
            ConcreteStreamData::Unit => Ok(SExprTE::Unit(SExprT::Val(()))),
            ConcreteStreamData::Unknown => {
                errs.push(crate::core::SemanticError::UnknownError(
                    format!(
                        "Stream expression {:?} not assigned a type before semantic analysis",
                        self
                    )
                    .into(),
                ));
                Err(())
            }
        }
    }
}

// Type check a binary operation
impl TypeCheckableHelper<LOLATypeSystem> for (SBinOp, SExpr<VarName>, SExpr<VarName>) {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE<VarName>, ()> {
        let (op, se1, se2) = self;
        let se1_check = se1.type_check_raw(ctx, errs);
        let se2_check = se2.type_check_raw(ctx, errs);

        match (se1_check, se2_check) {
            (Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => Ok(SExprTE::Int(SExprT::BinOp(
                Box::new(se1.clone()),
                Box::new(se2.clone()),
                *op,
            ))),
            (Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) if *op == SBinOp::Plus => {
                Ok(SExprTE::Str(SExprT::BinOp(
                    Box::new(se1.clone()),
                    Box::new(se2.clone()),
                    *op,
                )))
            }
            // Any other case where sub-expressions are Ok, but `op` is not supported
            (Ok(ste1), Ok(ste2)) => {
                errs.push(SemanticError::TypeError(
                    format!(
                        "Cannot apply binary function {:?} to expressions of type {:?} and {:?}",
                        op, ste1, ste2
                    )
                    .into(),
                ));
                Err(())
            }
            // If the underlying values already result in an error then simply propagate
            (Ok(_), Err(_)) | (Err(_), Ok(_)) | (Err(_), Err(_)) => Err(()),
        }
    }
}

// Type check an if expression
impl TypeCheckableHelper<LOLATypeSystem> for (&BExpr<VarName>, &SExpr<VarName>, &SExpr<VarName>) {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE<VarName>, ()> {
        let (b, se1, se2) = self;
        let se1_check = se1.type_check_raw(ctx, errs);
        let se2_check = se2.type_check_raw(ctx, errs);

        match (se1_check, se2_check) {
            (Ok(ste1), Ok(ste2)) => {
                // Matching on type-checked expressions. If same then Ok, else error.
                match (ste1, ste2) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprTE::Int(SExprT::If(
                        Box::new((*b).clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprTE::Str(SExprT::If(
                        Box::new((*b).clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprTE::Bool(SExprT::If(
                        Box::new((*b).clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprTE::Unit(SExprT::If(
                        Box::new((*b).clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (stenum1, stenum2) => {
                        errs.push(SemanticError::TypeError(
                            format!(
                                "Cannot create if-expression with two different types: {:?} and {:?}",
                                stenum1, stenum2
                            )
                            .into(),
                        ));
                        Err(())
                    }
                }
            }
            // If there's already an error in any branch, propagate the error
            (Ok(_), Err(_)) | (Err(_), Ok(_)) | (Err(_), Err(_)) => Err(()),
        }
    }
}

// Type check an index expression
impl TypeCheckableHelper<LOLATypeSystem> for (&SExpr<VarName>, isize, ConcreteStreamData) {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE<VarName>, ()> {
        let (inner, idx, default) = self;
        let inner_check = inner.type_check_raw(ctx, errs);

        match inner_check {
            Ok(ste) => match (ste, default) {
                (SExprTE::Int(se), ConcreteStreamData::Int(def)) => Ok(SExprTE::Int(
                    SExprT::Index(Box::new(se.clone()), *idx, *def),
                )),
                (SExprTE::Str(se), ConcreteStreamData::Str(def)) => Ok(SExprTE::Str(
                    SExprT::Index(Box::new(se.clone()), *idx, def.clone()),
                )),
                (SExprTE::Bool(se), ConcreteStreamData::Bool(def)) => Ok(SExprTE::Bool(
                    SExprT::Index(Box::new(se.clone()), *idx, *def),
                )),
                (SExprTE::Unit(se), ConcreteStreamData::Unit) => {
                    Ok(SExprTE::Unit(SExprT::Index(Box::new(se.clone()), *idx, ())))
                }
                (se, def) => {
                    errs.push(SemanticError::TypeError(
                            format!(
                                "Mismatched type in Index expression, expression and default does not match: {:?}",
                                (se, def)
                            )
                            .into(),
                        ));
                    Err(())
                }
            },
            // If there's already an error just propagate it
            Err(_) => Err(()),
        }
    }
}

// Type check a variable
impl TypeCheckableHelper<LOLATypeSystem> for VarName {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE<VarName>, ()> {
        let type_opt = ctx.get(self);
        match type_opt {
            Some(t) => match t {
                StreamType::Int => Ok(SExprTE::Int(SExprT::Var(self.clone()))),
                StreamType::Str => Ok(SExprTE::Str(SExprT::Var(self.clone()))),
                StreamType::Bool => Ok(SExprTE::Bool(SExprT::Var(self.clone()))),
                StreamType::Unit => Ok(SExprTE::Unit(SExprT::Var(self.clone()))),
            },
            None => {
                errs.push(SemanticError::UndeclaredVariable(
                    format!("Usage of undeclared variable: {:?}", self).into(),
                ));
                Err(())
            }
        }
    }
}

// Type check an expression
impl TypeCheckableHelper<LOLATypeSystem> for SExpr<VarName> {
    fn type_check_raw(
        &self,
        ctx: &mut crate::core::TypeContext<LOLATypeSystem>,
        errs: &mut crate::core::SemanticErrors,
    ) -> Result<SExprTE<VarName>, ()> {
        match self {
            SExpr::Val(sdata) => sdata.type_check_raw(ctx, errs),
            SExpr::BinOp(se1, se2, op) => {
                (op.clone(), *se1.clone(), *se2.clone()).type_check_raw(ctx, errs)
            }
            SExpr::If(b, se1, se2) => {
                (b.deref(), se1.deref(), se2.deref()).type_check_raw(ctx, errs)
            }
            SExpr::Index(inner, idx, default) => {
                (inner.deref(), *idx, default.clone()).type_check_raw(ctx, errs)
            }
            SExpr::Var(id) => id.type_check_raw(ctx, errs),
            SExpr::Eval(_) => todo!("Implement support for Eval (to be renamed)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::zip, mem::discriminant};

    use crate::core::{SemanticResult, TypeContext};

    use super::*;

    type SExprStr = SExpr<VarName>;
    type SemantResultStr = SemanticResult<SExprTE<VarName>>;
    type SExprTStr<Val> = SExprT<Val, VarName>;
    type BExprStr = BExpr<VarName>;

    trait AsExpr<Expr> {
        fn binop_expr(lhs: Expr, rhs: Expr, op: SBinOp) -> Self;
        fn if_expr(b: Box<BExprStr>, texpr: Expr, fexpr: Expr) -> Self;
    }

    impl AsExpr<Box<SExprStr>> for SExprStr {
        fn binop_expr(lhs: Box<SExprStr>, rhs: Box<SExprStr>, op: SBinOp) -> Self {
            SExprStr::BinOp(lhs, rhs, op)
        }
        fn if_expr(b: Box<BExprStr>, texpr: Box<SExprStr>, fexpr: Box<SExprStr>) -> Self {
            SExprStr::If(b, texpr, fexpr)
        }
    }

    impl<Val: Value<LOLATypeSystem>> AsExpr<Box<SExprTStr<Val>>> for SExprTStr<Val> {
        fn binop_expr(lhs: Box<SExprTStr<Val>>, rhs: Box<SExprTStr<Val>>, op: SBinOp) -> Self {
            SExprTStr::BinOp(lhs, rhs, op)
        }
        fn if_expr(
            b: Box<BExprStr>,
            texpr: Box<SExprTStr<Val>>,
            fexpr: Box<SExprTStr<Val>>,
        ) -> Self {
            SExprTStr::If(b, texpr, fexpr)
        }
    }

    fn check_correct_error_type(result: &SemantResultStr, expected: &SemantResultStr) {
        // Checking that error type is correct but not the specific message
        if let (Err(res_errs), Err(exp_errs)) = (&result, &expected) {
            assert_eq!(res_errs.len(), exp_errs.len());
            let mut errs = zip(res_errs, exp_errs);
            assert!(
                errs.all(|(res, exp)| discriminant(res) == discriminant(exp)),
                "Error variants do not match: got {:?}, expected {:?}",
                res_errs,
                exp_errs
            );
        } else {
            // We didn't receive error - make assertion fail with nice output
            let msg = format!(
                "Expected error: {:?}. Received result: {:?}",
                expected, result
            );
            assert!(false, "{}", msg);
        }
    }

    fn check_correct_error_types(results: &Vec<SemantResultStr>, expected: &Vec<SemantResultStr>) {
        assert_eq!(
            results.len(),
            expected.len(),
            "Result and expected vectors must have the same length"
        );

        // Iterate over both vectors and call check_correct_error_type on each pair
        for (result, exp) in results.iter().zip(expected.iter()) {
            check_correct_error_type(result, exp);
        }
    }

    // Helper function that returns all the sbinop variants at the time of writing these tests
    // (Not guaranteed to be maintained)
    fn all_sbinop_variants() -> Vec<SBinOp> {
        vec![SBinOp::Plus, SBinOp::Minus, SBinOp::Mult]
    }

    // Function to generate combinations to use in tests, e.g., for binops
    fn generate_combinations<T, Expr, F>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        generate_expr: F,
    ) -> Vec<T>
    where
        T: AsExpr<Box<Expr>>,
        Expr: Clone,
        F: Fn(Box<Expr>, Box<Expr>) -> T,
    {
        let mut vals = Vec::new();

        for a in variants_a.iter() {
            for b in variants_b.iter() {
                vals.push(generate_expr(Box::new(a.clone()), Box::new(b.clone())));
            }
        }

        vals
    }

    // Example usage for binary operations
    fn generate_binop_combinations<T, Expr>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        sbinops: Vec<SBinOp>,
    ) -> Vec<T>
    where
        T: AsExpr<Box<Expr>>,
        Expr: Clone,
    {
        let mut vals = Vec::new();

        for op in sbinops {
            vals.extend(generate_combinations(variants_a, variants_b, |lhs, rhs| {
                T::binop_expr(lhs, rhs, op)
            }));
        }

        vals
    }

    // Example usage for if-expressions
    fn generate_if_combinations<T, Expr>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        b_expr: Box<BExprStr>,
    ) -> Vec<T>
    where
        T: AsExpr<Box<Expr>>,
        Expr: Clone,
    {
        generate_combinations(variants_a, variants_b, |lhs, rhs| {
            T::if_expr(b_expr.clone(), lhs, rhs)
        })
    }

    #[test]
    fn test_vals_ok() {
        // Checks that vals returns the expected typed AST after semantic analysis
        let vals = vec![
            SExprStr::Val(ConcreteStreamData::Int(1)),
            SExprStr::Val(ConcreteStreamData::Str("".into())),
            SExprStr::Val(ConcreteStreamData::Bool(true)),
            SExprStr::Val(ConcreteStreamData::Unit),
        ];
        let results = vals
            .iter()
            .map(TypeCheckable::<LOLATypeSystem>::type_check_with_default);
        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprT::Val(1))),
            Ok(SExprTE::Str(SExprT::Val("".into()))),
            Ok(SExprTE::Bool(SExprT::Val(true))),
            Ok(SExprTE::Unit(SExprT::Val(()))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_unknown_err() {
        // Checks that if a Val is unknown during semantic analysis it produces a UnknownError
        let val = SExprStr::Val(ConcreteStreamData::Unknown);
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::UnknownError("".into())]);
        check_correct_error_type(&result, &expected);
    }

    #[test]
    fn test_plus_err_ident_types() {
        // Checks that if we add two identical types together that are not addable,
        let vals = vec![
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Bool(false))),
                Box::new(SExprStr::Val(ConcreteStreamData::Bool(false))),
                SBinOp::Plus,
            ),
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Unit)),
                Box::new(SExprStr::Val(ConcreteStreamData::Unit)),
                SBinOp::Plus,
            ),
        ];
        let results = vals
            .iter()
            .map(TypeCheckable::type_check_with_default)
            .collect();
        let expected: Vec<SemantResultStr> = vec![
            Err(vec![SemanticError::TypeError("".into())]),
            Err(vec![SemanticError::TypeError("".into())]),
        ];
        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_binop_err_diff_types() {
        // Checks that calling a BinOp on two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Unknown)
        let variants = vec![
            SExprStr::Val(ConcreteStreamData::Int(0)),
            SExprStr::Val(ConcreteStreamData::Str("".into())),
            SExprStr::Val(ConcreteStreamData::Bool(true)),
            SExprStr::Val(ConcreteStreamData::Unit),
        ];

        // Create a vector of all SBinOp variants
        let sbinops = all_sbinop_variants();

        let vals_tmp = generate_binop_combinations(&variants, &variants, sbinops);
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprStr::BinOp(left, right, _) => {
                    // Only keep values where left != right
                    left != right
                }
                _ => true, // Keep non-BinOps (unused in this case)
            }
        });

        let results = vals
            .map(|x| TypeCheckable::type_check_with_default(&x))
            .collect::<Vec<_>>();

        // Since all combinations of different types should yield an error,
        // we'll expect each result to be an Err with a type error.
        let expected: Vec<SemantResultStr> = results
            .iter()
            .map(|_| Err(vec![SemanticError::TypeError("".into())]))
            .collect();

        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_plus_err_unknown() {
        // Checks that if either value is unknown then Plus does not generate further errors
        let vals = vec![
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Int(0))),
                Box::new(SExprStr::Val(ConcreteStreamData::Unknown)),
                SBinOp::Plus,
            ),
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Unknown)),
                Box::new(SExprStr::Val(ConcreteStreamData::Int(0))),
                SBinOp::Plus,
            ),
            SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Unknown)),
                Box::new(SExprStr::Val(ConcreteStreamData::Unknown)),
                SBinOp::Plus,
            ),
        ];
        let results = vals.iter().map(TypeCheckable::type_check_with_default);
        let expected_err_lens = vec![1, 1, 2];

        // For each result, check that we got errors and that we got the correct amount:
        for (res, exp_err_len) in zip(results, expected_err_lens) {
            match res {
                Err(errs) => {
                    assert_eq!(
                        errs.len(),
                        exp_err_len,
                        "Expected {} errors but got {}: {:?}",
                        exp_err_len,
                        errs.len(),
                        errs
                    );
                    // TODO: Check that it is actually UnknownErrors
                }
                Ok(_) => {
                    assert!(
                        false,
                        "Expected an error but got a successful result: {:?}",
                        res
                    );
                }
            }
        }
    }

    #[test]
    fn test_int_binop_ok() {
        // Checks that if we BinOp two Ints together it results in typed AST after semantic analysis
        let int_val = vec![SExprStr::Val(ConcreteStreamData::Int(0))];
        let sbinops = all_sbinop_variants();
        let vals: Vec<SExpr<VarName>> =
            generate_binop_combinations(&int_val, &int_val, sbinops.clone());
        let results = vals
            .iter()
            .map(TypeCheckable::<LOLATypeSystem>::type_check_with_default);

        let int_t_val = vec![SExprTStr::<i64>::Val(0)];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprTStr<i64>> =
            generate_binop_combinations(&int_t_val, &int_t_val, sbinops);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Int(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_str_plus_ok() {
        // Checks that if we add two Strings together it results in typed AST after semantic analysis
        let str_val = vec![SExprStr::Val(ConcreteStreamData::Str("".into()))];
        let sbinops = vec![SBinOp::Plus];
        let vals: Vec<SExpr<VarName>> =
            generate_binop_combinations(&str_val, &str_val, sbinops.clone());
        let results = vals.iter().map(TypeCheckable::type_check_with_default);

        let str_t_val = vec![SExprTStr::<String>::Val("".into())];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprTStr<String>> =
            generate_binop_combinations(&str_t_val, &str_t_val, sbinops);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Str(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_ok() {
        // Checks that typechecking if-statements with identical types for if- and else- part results in correct typed AST

        // Create a vector of all ConcreteStreamData variants (except Unknown)
        let val_variants = vec![
            SExprStr::Val(ConcreteStreamData::Int(0)),
            SExprStr::Val(ConcreteStreamData::Str("".into())),
            SExprStr::Val(ConcreteStreamData::Bool(true)),
            SExprStr::Val(ConcreteStreamData::Unit),
        ];

        // Create a vector of all SBinOp variants
        let bexpr = Box::new(BExprStr::Val(true));

        let vals_tmp = generate_if_combinations(&val_variants, &val_variants, bexpr.clone());

        // Only consider cases where true and false cases are equal
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprStr::If(_, t, f) => t == f,
                _ => true, // Keep non-ifs (unused in this case)
            }
        });
        let results = vals.map(|x| x.type_check_with_default());

        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprT::If(
                bexpr.clone(),
                Box::new(SExprT::Val(0)),
                Box::new(SExprT::Val(0)),
            ))),
            Ok(SExprTE::Str(SExprT::If(
                bexpr.clone(),
                Box::new(SExprT::Val("".into())),
                Box::new(SExprT::Val("".into())),
            ))),
            Ok(SExprTE::Bool(SExprT::If(
                bexpr.clone(),
                Box::new(SExprT::Val(true)),
                Box::new(SExprT::Val(true)),
            ))),
            Ok(SExprTE::Unit(SExprT::If(
                bexpr.clone(),
                Box::new(SExprT::Val(())),
                Box::new(SExprT::Val(())),
            ))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_err() {
        // Checks that creating an if-expression with two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Unknown)
        let variants = vec![
            SExprStr::Val(ConcreteStreamData::Int(0)),
            SExprStr::Val(ConcreteStreamData::Str("".into())),
            SExprStr::Val(ConcreteStreamData::Bool(true)),
            SExprStr::Val(ConcreteStreamData::Unit),
        ];

        let bexpr = Box::new(BExprStr::Val(true));

        let vals_tmp = generate_if_combinations(&variants, &variants, bexpr.clone());
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprStr::If(_, t, f) => t != f,
                _ => true, // Keep non-BinOps (unused in this case)
            }
        });

        let results = vals
            .map(|x| x.type_check_with_default())
            .collect::<Vec<_>>();

        // Since all combinations of different types should yield an error,
        // we'll expect each result to be an Err with a type error.
        let expected: Vec<SemantResultStr> = results
            .iter()
            .map(|_| Err(vec![SemanticError::TypeError("".into())]))
            .collect();

        check_correct_error_types(&results, &expected);
    }

    #[test]
    fn test_var_ok() {
        // Checks that Vars are correctly typechecked if they exist in the context

        let variant_names = vec!["int", "str", "bool", "unit"];
        let variant_types = vec![
            StreamType::Int,
            StreamType::Str,
            StreamType::Bool,
            StreamType::Unit,
        ];
        let vals = variant_names
            .clone()
            .into_iter()
            .map(|n| SExprStr::Var(VarName(n.into())));

        // Fake context/environment that simulates type-checking context
        let mut ctx = TypeContext::<LOLATypeSystem>::new();
        for (n, t) in variant_names.into_iter().zip(variant_types.into_iter()) {
            ctx.insert(VarName(n.into()), t);
        }

        let results = vals.into_iter().map(|sexpr| sexpr.type_check(&mut ctx));

        let expected = vec![
            Ok(SExprTE::Int(SExprT::Var(VarName("int".into())))),
            Ok(SExprTE::Str(SExprT::Var(VarName("str".into())))),
            Ok(SExprTE::Bool(SExprT::Var(VarName("bool".into())))),
            Ok(SExprTE::Unit(SExprT::Var(VarName("unit".into())))),
        ];

        assert!(results.eq(expected));
    }

    #[test]
    fn test_var_err() {
        // Checks that Vars produce UndeclaredVariable errors if they do not exist in the context

        let val = SExprStr::Var(VarName("undeclared_name".into()));
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::UndeclaredVariable("".into())]);
        check_correct_error_type(&result, &expected);
    }
    // TODO: Test that any SExpr leaf is a Val. If not it should return a Type-Error

    #[test]
    fn test_dodgy_if() {
        let dodgy_bexpr = BExpr::Eq(
            Box::new(SExprStr::Val(ConcreteStreamData::Int(0))),
            Box::new(SExprStr::BinOp(
                Box::new(SExprStr::Val(ConcreteStreamData::Int(3))),
                Box::new(SExprStr::Val(ConcreteStreamData::Str("Banana".into()))),
                SBinOp::Plus,
            )),
        );
        let sexpr = SExprStr::If(
            Box::new(dodgy_bexpr),
            Box::new(SExprStr::Val(ConcreteStreamData::Int(1))),
            Box::new(SExprStr::Val(ConcreteStreamData::Int(2))),
        );
        if let Ok(_) = sexpr.type_check_with_default() {
            assert!(false, "Expected type error but got a successful result");
        }
    }
}

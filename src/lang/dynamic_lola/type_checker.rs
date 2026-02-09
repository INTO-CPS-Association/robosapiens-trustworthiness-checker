use ecow::EcoVec;

use super::ast::{BoolBinOp, CompBinOp, FloatBinOp, IntBinOp, SBinOp, SExpr, StrBinOp};
use crate::core::{StreamData, StreamType, StreamTypeAscription};
use crate::{LOLASpecification, Specification};
use crate::{Value, VarName};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::Deref;

#[derive(Debug, PartialEq, Eq)]
pub enum SemanticError {
    TypeError(String),
    DeferredError(String),
    UndeclaredVariable(String),
}

pub type SemanticErrors = Vec<SemanticError>;
pub type TypeContext = BTreeMap<VarName, StreamType>;

pub type SemanticResult<Expected> = Result<Expected, SemanticErrors>;

pub trait TypeCheckableHelper<TypedExpr> {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<TypedExpr, ()>;
}
impl<TypedExpr, R: TypeCheckableHelper<TypedExpr>> TypeCheckable<TypedExpr> for R {
    fn type_check(&self, context: &mut TypeContext) -> SemanticResult<TypedExpr> {
        let mut errors = Vec::new();
        let res = self.type_check_raw(context, &mut errors);
        match res {
            Ok(se) => Ok(se),
            Err(()) => Err(errors),
        }
    }
}
pub trait TypeCheckable<TypedExpr> {
    fn type_check_with_default(&self) -> SemanticResult<TypedExpr> {
        let mut context = TypeContext::new();
        self.type_check(&mut context)
    }

    fn type_check(&self, context: &mut TypeContext) -> SemanticResult<TypedExpr>;
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PartialStreamValue<T> {
    Known(T),
    NoVal,
    Deferred,
}

impl StreamData for PartialStreamValue<bool> {}
impl StreamData for PartialStreamValue<i64> {}
impl StreamData for PartialStreamValue<f64> {}
impl StreamData for PartialStreamValue<String> {}
impl StreamData for PartialStreamValue<()> {}

fn extract_type(expr: &SExprTE) -> StreamType {
    match expr {
        SExprTE::Bool(_) => StreamType::Bool,
        SExprTE::Int(_) => StreamType::Int,
        SExprTE::Float(_) => StreamType::Float,
        SExprTE::Str(_) => StreamType::Str,
        SExprTE::Unit(_) => StreamType::Unit,
    }
}

impl TryFrom<Value> for PartialStreamValue<i64> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(PartialStreamValue::Known(i)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for PartialStreamValue<f64> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(x) => Ok(PartialStreamValue::Known(x)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for PartialStreamValue<String> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Str(i) => Ok(PartialStreamValue::Known(i.to_string())),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for PartialStreamValue<bool> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(i) => Ok(PartialStreamValue::Known(i)),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}
impl TryFrom<Value> for PartialStreamValue<()> {
    type Error = ();

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Unit => Ok(PartialStreamValue::Known(())),
            Value::NoVal => Ok(PartialStreamValue::NoVal),
            Value::Deferred => Ok(PartialStreamValue::Deferred),
            _ => Err(()),
        }
    }
}

impl From<PartialStreamValue<i64>> for Value {
    fn from(value: PartialStreamValue<i64>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Int(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}

impl From<PartialStreamValue<f64>> for Value {
    fn from(value: PartialStreamValue<f64>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Float(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<String>> for Value {
    fn from(value: PartialStreamValue<String>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Str(v.into()),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<bool>> for Value {
    fn from(value: PartialStreamValue<bool>) -> Self {
        match value {
            PartialStreamValue::Known(v) => Value::Bool(v),
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}
impl From<PartialStreamValue<()>> for Value {
    fn from(value: PartialStreamValue<()>) -> Self {
        match value {
            PartialStreamValue::Known(_) => Value::Unit,
            PartialStreamValue::NoVal => Value::NoVal,
            PartialStreamValue::Deferred => Value::Deferred,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprBool {
    Val(PartialStreamValue<bool>),

    // Equality comparisons
    EqInt(SExprInt, SExprInt),
    EqFloat(SExprFloat, SExprFloat),
    EqStr(SExprStr, SExprStr),
    EqBool(Box<Self>, Box<Self>),
    EqUnit(SExprUnit, SExprUnit),

    // Ordering comparisons
    LeInt(SExprInt, SExprInt),
    LeFloat(SExprFloat, SExprFloat),
    LeStr(SExprStr, SExprStr),
    LtInt(SExprInt, SExprInt),
    LtFloat(SExprFloat, SExprFloat),
    LtStr(SExprStr, SExprStr),
    GeInt(SExprInt, SExprInt),
    GeFloat(SExprFloat, SExprFloat),
    GeStr(SExprStr, SExprStr),
    GtInt(SExprInt, SExprInt),
    GtFloat(SExprFloat, SExprFloat),
    GtStr(SExprStr, SExprStr),

    BinOp(Box<Self>, Box<Self>, BoolBinOp),
    Not(Box<Self>),
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Boolean-producing unary operators on typed streams
    IsDefinedInt(SExprInt),
    IsDefinedFloat(SExprFloat),
    IsDefinedStr(SExprStr),
    IsDefinedBool(Box<SExprBool>),
    IsDefinedUnit(SExprUnit),
    WhenInt(SExprInt),
    WhenFloat(SExprFloat),
    WhenStr(SExprStr),
    WhenBool(Box<SExprBool>),
    WhenUnit(SExprUnit),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeContext),
    Dynamic(Box<SExprStr>, TypeContext),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeContext),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprInt {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    // Arithmetic Stream expression
    Val(PartialStreamValue<i64>),

    BinOp(Box<Self>, Box<Self>, IntBinOp),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Math functions
    Abs(Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeContext),
    Dynamic(Box<SExprStr>, TypeContext),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeContext),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprFloat {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    // Arithmetic Stream expression
    Val(PartialStreamValue<f64>),

    BinOp(Box<Self>, Box<Self>, FloatBinOp),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Trigonometric and math functions
    Sin(Box<Self>),
    Cos(Box<Self>),
    Tan(Box<Self>),
    Abs(Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeContext),
    Dynamic(Box<SExprStr>, TypeContext),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeContext),
}

// Stream expressions - now with types
#[derive(Clone, PartialEq, Debug)]
pub enum SExprUnit {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    // Arithmetic Stream expression
    Val(PartialStreamValue<()>),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeContext),
    Dynamic(Box<SExprStr>, TypeContext),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeContext),
}

#[derive(Clone, PartialEq, Debug)]
pub enum SExprStr {
    If(Box<SExprBool>, Box<Self>, Box<Self>),

    // Stream indexing
    SIndex(
        // Inner SExpr e
        Box<Self>,
        // Index i
        u64,
    ),

    BinOp(Box<Self>, Box<Self>, StrBinOp),

    // Arithmetic Stream expression
    Val(PartialStreamValue<String>),

    Var(VarName),

    Default(Box<Self>, Box<Self>),

    // Async operators
    Init(Box<Self>, Box<Self>),

    // Deferred and dynamic expressions
    Defer(Box<SExprStr>, TypeContext),
    Dynamic(Box<SExprStr>, TypeContext),
    RestrictedDynamic(Box<SExprStr>, EcoVec<VarName>, TypeContext),
}

// Stream expression typed enum
#[derive(Debug, PartialEq, Clone)]
pub enum SExprTE {
    Int(SExprInt),
    Float(SExprFloat),
    Str(SExprStr),
    Bool(SExprBool),
    Unit(SExprUnit),
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedLOLASpecification {
    pub input_vars: Vec<VarName>,
    pub output_vars: Vec<VarName>,
    pub exprs: BTreeMap<VarName, SExprTE>,
    pub type_annotations: BTreeMap<VarName, StreamType>,
}

impl Specification for TypedLOLASpecification {
    type Expr = SExprTE;

    fn input_vars(&self) -> Vec<VarName> {
        self.input_vars.clone()
    }

    fn output_vars(&self) -> Vec<VarName> {
        self.output_vars.clone()
    }

    fn var_expr(&self, var: &VarName) -> Option<SExprTE> {
        self.exprs.get(var).cloned()
    }

    fn add_input_var(&mut self, var: VarName) {
        // TODO: How to add type info?
        self.input_vars = self
            .input_vars
            .iter()
            .cloned()
            .chain(std::iter::once(var))
            .collect();
    }
}

pub fn type_check(spec: LOLASpecification) -> SemanticResult<TypedLOLASpecification> {
    let type_context = spec.type_annotations.clone();
    let mut typed_exprs = BTreeMap::new();
    let mut errors = vec![];
    for (var, expr) in spec.exprs.iter() {
        let mut ctx = type_context.clone();
        let typed_expr = expr.type_check_raw(&mut ctx, &mut errors);
        typed_exprs.insert(var, typed_expr);
    }
    if errors.is_empty() {
        Ok(TypedLOLASpecification {
            input_vars: spec.input_vars.clone(),
            output_vars: spec.output_vars.clone(),
            exprs: typed_exprs
                .into_iter()
                .map(|(k, v)| (k.clone(), v.unwrap()))
                .collect(),
            type_annotations: spec.type_annotations.clone(),
        })
    } else {
        Err(errors)
    }
}

impl TypeCheckableHelper<SExprTE> for Value {
    fn type_check_raw(
        &self,
        _: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        match self {
            Value::Int(v) => Ok(SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(*v)))),
            Value::Float(v) => Ok(SExprTE::Float(SExprFloat::Val(PartialStreamValue::Known(
                *v,
            )))),
            Value::Str(v) => Ok(SExprTE::Str(SExprStr::Val(PartialStreamValue::Known(
                v.into(),
            )))),
            Value::Bool(v) => Ok(SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(*v)))),
            Value::List(_) => todo!(),
            Value::Map(_) => todo!(),
            Value::Unit => Ok(SExprTE::Unit(SExprUnit::Val(PartialStreamValue::Known(())))),
            Value::Deferred => {
                errs.push(SemanticError::DeferredError(format!(
                    "Stream expression {:?} not assigned a type before semantic analysis",
                    self
                )));
                Err(())
            }
            // Not sure how the type-checking should deal with a value not provided
            // Something like skipping type-checking for this value but not the next
            Value::NoVal => todo!(),
        }
    }
}

// Type check a binary operation
impl TypeCheckableHelper<SExprTE> for (SBinOp, &SExpr, &SExpr) {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (op, se1, se2) = self;
        let se1_check = se1.type_check_raw(ctx, errs);
        let se2_check = se2.type_check_raw(ctx, errs);

        match (op, se1_check, se2_check) {
            // Integer operations
            (SBinOp::NOp(op), Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => {
                match op.clone().try_into() {
                    Ok(op) => Ok(SExprTE::Int(SExprInt::BinOp(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                        op,
                    ))),
                    Err(_) => {
                        errs.push(SemanticError::TypeError(
                            "Numerical operation not valid on integers".into(),
                        ));
                        Err(())
                    }
                }
            }
            // Pure float operations
            (SBinOp::NOp(op), Ok(SExprTE::Float(se1)), Ok(SExprTE::Float(se2))) => {
                match op.clone().try_into() {
                    Ok(op) => Ok(SExprTE::Float(SExprFloat::BinOp(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                        op,
                    ))),
                    Err(_) => {
                        errs.push(SemanticError::TypeError(
                            "Numerical operation not valid on integers".into(),
                        ));
                        Err(())
                    }
                }
            }

            // TODO: add casts for mixed integer/float operations

            // Boolean operations
            (SBinOp::BOp(op), Ok(SExprTE::Bool(se1)), Ok(SExprTE::Bool(se2))) => Ok(SExprTE::Bool(
                SExprBool::BinOp(Box::new(se1.clone()), Box::new(se2.clone()), op.clone()),
            )),
            // String operations
            (SBinOp::SOp(op), Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) => Ok(SExprTE::Str(
                SExprStr::BinOp(Box::new(se1.clone()), Box::new(se2.clone()), op.clone()),
            )),

            // Comparison operations
            // Equality
            (SBinOp::COp(CompBinOp::Eq), Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => {
                Ok(SExprTE::Bool(SExprBool::EqInt(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Eq), Ok(SExprTE::Float(se1)), Ok(SExprTE::Float(se2))) => {
                Ok(SExprTE::Bool(SExprBool::EqFloat(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Eq), Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) => {
                Ok(SExprTE::Bool(SExprBool::EqStr(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Eq), Ok(SExprTE::Bool(se1)), Ok(SExprTE::Bool(se2))) => Ok(
                SExprTE::Bool(SExprBool::EqBool(Box::new(se1), Box::new(se2))),
            ),
            (SBinOp::COp(CompBinOp::Eq), Ok(SExprTE::Unit(se1)), Ok(SExprTE::Unit(se2))) => {
                Ok(SExprTE::Bool(SExprBool::EqUnit(se1, se2)))
            }

            // Less than or equal
            (SBinOp::COp(CompBinOp::Le), Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => {
                Ok(SExprTE::Bool(SExprBool::LeInt(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Le), Ok(SExprTE::Float(se1)), Ok(SExprTE::Float(se2))) => {
                Ok(SExprTE::Bool(SExprBool::LeFloat(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Le), Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) => {
                Ok(SExprTE::Bool(SExprBool::LeStr(se1, se2)))
            }

            // Less than
            (SBinOp::COp(CompBinOp::Lt), Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => {
                Ok(SExprTE::Bool(SExprBool::LtInt(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Lt), Ok(SExprTE::Float(se1)), Ok(SExprTE::Float(se2))) => {
                Ok(SExprTE::Bool(SExprBool::LtFloat(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Lt), Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) => {
                Ok(SExprTE::Bool(SExprBool::LtStr(se1, se2)))
            }

            // Greater than or equal
            (SBinOp::COp(CompBinOp::Ge), Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => {
                Ok(SExprTE::Bool(SExprBool::GeInt(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Ge), Ok(SExprTE::Float(se1)), Ok(SExprTE::Float(se2))) => {
                Ok(SExprTE::Bool(SExprBool::GeFloat(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Ge), Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) => {
                Ok(SExprTE::Bool(SExprBool::GeStr(se1, se2)))
            }

            // Greater than
            (SBinOp::COp(CompBinOp::Gt), Ok(SExprTE::Int(se1)), Ok(SExprTE::Int(se2))) => {
                Ok(SExprTE::Bool(SExprBool::GtInt(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Gt), Ok(SExprTE::Float(se1)), Ok(SExprTE::Float(se2))) => {
                Ok(SExprTE::Bool(SExprBool::GtFloat(se1, se2)))
            }
            (SBinOp::COp(CompBinOp::Gt), Ok(SExprTE::Str(se1)), Ok(SExprTE::Str(se2))) => {
                Ok(SExprTE::Bool(SExprBool::GtStr(se1, se2)))
            }

            // Any other case where sub-expressions are Ok, but `op` is not supported
            (_, Ok(ste1), Ok(ste2)) => {
                errs.push(SemanticError::TypeError(format!(
                    "Cannot apply binary function {:?} to expressions of type {:?} and {:?}",
                    op, ste1, ste2
                )));
                Err(())
            }
            // If the underlying values already result in an error then simply propagate
            _ => Err(()),
        }
    }
}

// Type check a default operation
impl TypeCheckableHelper<SExprTE> for (&SExpr, &SExpr) {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (se1, se2) = *self;
        let se1_check = se1.type_check_raw(ctx, errs);
        let se2_check = se2.type_check_raw(ctx, errs);

        match (se1_check, se2_check) {
            (Ok(ste1), Ok(ste2)) => {
                // Matching on type-checked expressions. If same then Ok, else error.
                match (ste1, ste2) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprTE::Int(SExprInt::Default(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprTE::Str(SExprStr::Default(
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprTE::Bool(
                        SExprBool::Default(Box::new(se1.clone()), Box::new(se2.clone())),
                    )),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprTE::Unit(
                        SExprUnit::Default(Box::new(se1.clone()), Box::new(se2.clone())),
                    )),
                    (stenum1, stenum2) => {
                        errs.push(SemanticError::TypeError(format!(
                            "Cannot create if-expression with two different types: {:?} and {:?}",
                            stenum1, stenum2
                        )));
                        Err(())
                    }
                }
            }
            // If there's already an error in any branch, propagate the error
            _ => Err(()),
        }
    }
}

// Type check an if expression
impl TypeCheckableHelper<SExprTE> for (&SExpr, &SExpr, &SExpr) {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (b, se1, se2) = *self;
        let b_check = b.type_check_raw(ctx, errs);
        let se1_check = se1.type_check_raw(ctx, errs);
        let se2_check = se2.type_check_raw(ctx, errs);

        match (b_check, se1_check, se2_check) {
            (Ok(SExprTE::Bool(b)), Ok(ste1), Ok(ste2)) => {
                // Matching on type-checked expressions. If same then Ok, else error.
                match (ste1, ste2) {
                    (SExprTE::Int(se1), SExprTE::Int(se2)) => Ok(SExprTE::Int(SExprInt::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Str(se1), SExprTE::Str(se2)) => Ok(SExprTE::Str(SExprStr::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Bool(se1), SExprTE::Bool(se2)) => Ok(SExprTE::Bool(SExprBool::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (SExprTE::Unit(se1), SExprTE::Unit(se2)) => Ok(SExprTE::Unit(SExprUnit::If(
                        Box::new(b.clone()),
                        Box::new(se1.clone()),
                        Box::new(se2.clone()),
                    ))),
                    (stenum1, stenum2) => {
                        errs.push(SemanticError::TypeError(format!(
                            "Cannot create if-expression with two different types: {:?} and {:?}",
                            stenum1, stenum2
                        )));
                        Err(())
                    }
                }
            }
            (Ok(_), Ok(_), Ok(_)) => {
                errs.push(SemanticError::TypeError(
                    "If expression condition must be a boolean".into(),
                ));
                Err(())
            }
            // If there's already an error in any branch, propagate the error
            _ => Err(()),
        }
    }
}

// Type check an index expression
impl TypeCheckableHelper<SExprTE> for (&SExpr, u64) {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (inner, idx) = *self;
        let inner_check = inner.type_check_raw(ctx, errs);

        match inner_check {
            Ok(ste) => match ste {
                SExprTE::Int(se) => Ok(SExprTE::Int(SExprInt::SIndex(Box::new(se.clone()), idx))),
                SExprTE::Str(se) => Ok(SExprTE::Str(SExprStr::SIndex(Box::new(se.clone()), idx))),
                SExprTE::Bool(se) => {
                    Ok(SExprTE::Bool(SExprBool::SIndex(Box::new(se.clone()), idx)))
                }
                SExprTE::Unit(se) => {
                    Ok(SExprTE::Unit(SExprUnit::SIndex(Box::new(se.clone()), idx)))
                }
                se => {
                    errs.push(SemanticError::TypeError(
                        format!(
                            "Mismatched type in Stream Index expression, expression and default does not match: {:?}",
                            se
                        ),
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
impl TypeCheckableHelper<SExprTE> for VarName {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let type_opt = ctx.get(self);
        match type_opt {
            Some(t) => match t {
                StreamType::Int => Ok(SExprTE::Int(SExprInt::Var(self.clone()))),
                StreamType::Float => Ok(SExprTE::Float(SExprFloat::Var(self.clone()))),
                StreamType::Str => Ok(SExprTE::Str(SExprStr::Var(self.clone()))),
                StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Var(self.clone()))),
                StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Var(self.clone()))),
            },
            None => {
                errs.push(SemanticError::UndeclaredVariable(format!(
                    "Usage of undeclared variable: {:?}",
                    self
                )));
                Err(())
            }
        }
    }
}

impl TypeCheckableHelper<SExprTE> for (SExpr, StreamTypeAscription) {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        let (expr, ascription) = self;
        let expr_te = expr.type_check_raw(ctx, errs)?;

        match ascription {
            StreamTypeAscription::Unascribed => Ok(expr_te),
            StreamTypeAscription::Ascribed(expected_ty) => {
                let actual_ty = extract_type(&expr_te);
                if actual_ty == *expected_ty {
                    Ok(expr_te)
                } else {
                    errs.push(SemanticError::TypeError(format!(
                        "Type mismatch: expected {:?}, got {:?}",
                        expected_ty, actual_ty
                    )));
                    Err(())
                }
            }
        }
    }
}

// Type check an expression
impl TypeCheckableHelper<SExprTE> for SExpr {
    fn type_check_raw(
        &self,
        ctx: &mut TypeContext,
        errs: &mut SemanticErrors,
    ) -> Result<SExprTE, ()> {
        match self {
            SExpr::Val(sdata) => sdata.type_check_raw(ctx, errs),
            SExpr::BinOp(se1, se2, op) => {
                (op.clone(), se1.deref(), se2.deref()).type_check_raw(ctx, errs)
            }
            SExpr::If(b, se1, se2) => {
                (b.deref(), se1.deref(), se2.deref()).type_check_raw(ctx, errs)
            }
            SExpr::SIndex(inner, idx) => (inner.deref(), *idx).type_check_raw(ctx, errs),
            SExpr::Var(id) => id.type_check_raw(ctx, errs),
            SExpr::Dynamic(e, type_ascription) => {
                let e_check = e.type_check_raw(ctx, errs)?;

                // Ascriptions are required for defers in strictly-typed expressions
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::TypeError(format!(
                            "Type ascription required for defer"
                        )));
                        return Err(());
                    }
                };

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::TypeError(format!(
                            "Expected Dynamic to be applied to a Str, got {:?}",
                            ty
                        )));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => Ok(SExprTE::Int(SExprInt::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Str => Ok(SExprTE::Str(SExprStr::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Dynamic(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                }
            }
            SExpr::RestrictedDynamic(e, type_ascription, vs) => {
                let e_check = e.type_check_raw(ctx, errs)?;

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::TypeError(format!(
                            "Expected RestrictedDynamic to be applied to a Str, got {:?}",
                            ty
                        )));
                        return Err(());
                    }
                };

                // Verify type ascription if provided - RestrictedDynamic only supports Str output
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::TypeError(format!(
                            "Type ascription required for dynamic"
                        )));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => Ok(SExprTE::Int(SExprInt::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Str => Ok(SExprTE::Str(SExprStr::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::RestrictedDynamic(
                        Box::new(e_str),
                        vs.clone(),
                        ctx.clone(),
                    ))),
                }
            }
            SExpr::Defer(e, type_ascription) => {
                let e_check = e.type_check_raw(ctx, errs)?;

                // Ascriptions are required for defer in strictly-typed expressions
                let type_ascription = match type_ascription {
                    StreamTypeAscription::Ascribed(ta) => ta,
                    StreamTypeAscription::Unascribed => {
                        errs.push(SemanticError::TypeError(format!(
                            "Type ascription required for defer"
                        )));
                        return Err(());
                    }
                };

                // Inner stream type must be Str
                let e_str = match e_check {
                    SExprTE::Str(e_str) => e_str,
                    ty => {
                        errs.push(SemanticError::TypeError(format!(
                            "Expected Defer to be applied to a Str, got {:?}",
                            ty
                        )));
                        return Err(());
                    }
                };

                // Use the type ascription to determine the output type
                match &type_ascription {
                    StreamType::Int => {
                        Ok(SExprTE::Int(SExprInt::Defer(Box::new(e_str), ctx.clone())))
                    }
                    StreamType::Float => Ok(SExprTE::Float(SExprFloat::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Str => {
                        Ok(SExprTE::Str(SExprStr::Defer(Box::new(e_str), ctx.clone())))
                    }
                    StreamType::Bool => Ok(SExprTE::Bool(SExprBool::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                    StreamType::Unit => Ok(SExprTE::Unit(SExprUnit::Defer(
                        Box::new(e_str),
                        ctx.clone(),
                    ))),
                }
            }
            SExpr::Update(_, _) => todo!("Implement support for Update"),
            SExpr::Default(se, d) => (se.deref(), d.deref()).type_check_raw(ctx, errs),
            SExpr::Not(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(ctx, errs)?;
                match sexpr_check {
                    SExprTE::Bool(se) => Ok(SExprTE::Bool(SExprBool::Not(Box::new(se)))),
                    _ => {
                        errs.push(SemanticError::TypeError(
                            "Not can only be applied to boolean expressions".into(),
                        ));
                        Err(())
                    }
                }
            }
            SExpr::List(_) => todo!("Implement support for typed List"),
            SExpr::LIndex(_, _) => todo!("Implement support for typed LIndex"),
            SExpr::LAppend(_, _) => todo!("Implement support for typed LAppend"),
            SExpr::LConcat(_, _) => todo!("Implement support for typed LConcat"),
            SExpr::LHead(_) => todo!("Implement support for typed LHead"),
            SExpr::LTail(_) => todo!("Implement support for typed LTail"),
            SExpr::LLen(_) => todo!("Implement support for typed LLen"),
            SExpr::IsDefined(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(ctx, errs)?;
                match sexpr_check {
                    SExprTE::Int(se) => Ok(SExprTE::Bool(SExprBool::IsDefinedInt(se))),
                    SExprTE::Float(se) => Ok(SExprTE::Bool(SExprBool::IsDefinedFloat(se))),
                    SExprTE::Str(se) => Ok(SExprTE::Bool(SExprBool::IsDefinedStr(se))),
                    SExprTE::Bool(se) => Ok(SExprTE::Bool(SExprBool::IsDefinedBool(Box::new(se)))),
                    SExprTE::Unit(se) => Ok(SExprTE::Bool(SExprBool::IsDefinedUnit(se))),
                }
            }
            SExpr::When(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(ctx, errs)?;
                match sexpr_check {
                    SExprTE::Int(se) => Ok(SExprTE::Bool(SExprBool::WhenInt(se))),
                    SExprTE::Float(se) => Ok(SExprTE::Bool(SExprBool::WhenFloat(se))),
                    SExprTE::Str(se) => Ok(SExprTE::Bool(SExprBool::WhenStr(se))),
                    SExprTE::Bool(se) => Ok(SExprTE::Bool(SExprBool::WhenBool(Box::new(se)))),
                    SExprTE::Unit(se) => Ok(SExprTE::Bool(SExprBool::WhenUnit(se))),
                }
            }
            SExpr::Latch(_, _) => todo!("Implement support for typed Latch"),
            SExpr::Init(se1, se2) => {
                let se1_check = se1.type_check_raw(ctx, errs);
                let se2_check = se2.type_check_raw(ctx, errs);
                match (se1_check, se2_check) {
                    (Ok(SExprTE::Int(e1)), Ok(SExprTE::Int(e2))) => {
                        Ok(SExprTE::Int(SExprInt::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Float(e1)), Ok(SExprTE::Float(e2))) => {
                        Ok(SExprTE::Float(SExprFloat::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Str(e1)), Ok(SExprTE::Str(e2))) => {
                        Ok(SExprTE::Str(SExprStr::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Bool(e1)), Ok(SExprTE::Bool(e2))) => {
                        Ok(SExprTE::Bool(SExprBool::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(SExprTE::Unit(e1)), Ok(SExprTE::Unit(e2))) => {
                        Ok(SExprTE::Unit(SExprUnit::Init(Box::new(e1), Box::new(e2))))
                    }
                    (Ok(ste1), Ok(ste2)) => {
                        errs.push(SemanticError::TypeError(format!(
                            "Init requires both arguments to have the same type, got {:?} and {:?}",
                            ste1, ste2
                        )));
                        Err(())
                    }
                    _ => Err(()),
                }
            }
            SExpr::Sin(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Sin(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "Sin can only be applied to float expressions, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::Cos(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Cos(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "Cos can only be applied to float expressions, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::Tan(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(ctx, errs)?;
                match sexpr_check {
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Tan(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "Tan can only be applied to float expressions, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::Abs(sexpr) => {
                let sexpr_check = sexpr.type_check_raw(ctx, errs)?;
                match sexpr_check {
                    SExprTE::Int(se) => Ok(SExprTE::Int(SExprInt::Abs(Box::new(se)))),
                    SExprTE::Float(se) => Ok(SExprTE::Float(SExprFloat::Abs(Box::new(se)))),
                    other => {
                        errs.push(SemanticError::TypeError(format!(
                            "Abs can only be applied to numeric expressions, got {:?}",
                            other
                        )));
                        Err(())
                    }
                }
            }
            SExpr::MonitoredAt(_, _) => todo!("Implement support for typed MonitoredAt"),
            SExpr::Dist(_, _) => todo!("Implement support for typed Dist"),
            SExpr::Map(_) => todo!("Implement support for typed Map"),
            SExpr::MGet(_, _) => todo!("Implement support for typed MGet"),
            SExpr::MInsert(_, _, _) => todo!("Implement support for typed MInsert"),
            SExpr::MRemove(_, _) => todo!("Implement support for typed MRemove"),
            SExpr::MHasKey(_, _) => todo!("Implement support for typed MHasKey"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::zip, mem::discriminant};

    use crate::lang::dynamic_lola::ast::{NumericalBinOp, StrBinOp};

    use super::{SemanticResult, TypeCheckable, TypeContext};

    use super::*;
    use test_log::test;

    type SExprV = SExpr;
    type SemantResultStr = SemanticResult<SExprTE>;

    trait BinOpExpr<Expr> {
        fn binop_expr(lhs: Expr, rhs: Expr, op: SBinOp) -> Self;
    }

    trait IfExpr<Expr, BoolExpr> {
        fn if_expr(b: BoolExpr, t: Expr, f: Expr) -> Self;
    }

    impl BinOpExpr<Box<SExpr>> for SExpr {
        fn binop_expr(lhs: Box<SExpr>, rhs: Box<SExpr>, op: SBinOp) -> Self {
            SExpr::BinOp(lhs, rhs, op)
        }
    }

    impl BinOpExpr<Box<SExprInt>> for SExprInt {
        fn binop_expr(lhs: Box<SExprInt>, rhs: Box<SExprInt>, op: SBinOp) -> Self {
            match op {
                SBinOp::NOp(op) => SExprInt::BinOp(lhs, rhs, op.try_into().unwrap()),
                _ => panic!("Invalid operation for SExprInt: {:?}", op),
            }
        }
    }

    impl IfExpr<Box<SExpr>, Box<SExpr>> for SExpr {
        fn if_expr(b: Box<SExpr>, t: Box<SExpr>, f: Box<SExpr>) -> Self {
            SExpr::If(b, t, f)
        }
    }

    impl IfExpr<Box<SExprInt>, Box<SExprBool>> for SExprInt {
        fn if_expr(b: Box<SExprBool>, t: Box<SExprInt>, f: Box<SExprInt>) -> Self {
            SExprInt::If(b, t, f)
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

    // // Helper function that returns all the sbinop variants at the time of writing these tests
    // // (Not guaranteed to be maintained)
    fn all_sbinop_variants() -> Vec<SBinOp> {
        vec![
            SBinOp::NOp(NumericalBinOp::Add),
            SBinOp::NOp(NumericalBinOp::Sub),
            SBinOp::NOp(NumericalBinOp::Mul),
            SBinOp::NOp(NumericalBinOp::Div),
        ]
    }

    // Function to generate combinations to use in tests, e.g., for binops
    fn generate_combinations<T, Expr, F>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        generate_expr: F,
    ) -> Vec<T>
    where
        // T: AsExpr<Box<Expr>>,
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
        T: BinOpExpr<Box<Expr>>,
        Expr: Clone,
    {
        let mut vals = Vec::new();

        for op in sbinops {
            vals.extend(generate_combinations(variants_a, variants_b, |lhs, rhs| {
                T::binop_expr(lhs, rhs, op.clone())
            }));
        }

        vals
    }

    fn generate_concat_combinations(
        variants_a: &[SExprStr],
        variants_b: &[SExprStr],
    ) -> Vec<SExprStr> {
        generate_combinations(variants_a, variants_b, |lhs, rhs| {
            SExprStr::BinOp(Box::new(*lhs), Box::new(*rhs), StrBinOp::Concat)
        })
    }

    // // Example usage for if-expressions
    fn generate_if_combinations<T, Expr, BoolExpr: Clone>(
        variants_a: &[Expr],
        variants_b: &[Expr],
        b_expr: Box<BoolExpr>,
    ) -> Vec<T>
    where
        T: IfExpr<Box<Expr>, Box<BoolExpr>>,
        Expr: Clone,
    {
        generate_combinations(variants_a, variants_b, |lhs, rhs| {
            T::if_expr(b_expr.clone(), lhs, rhs)
        })
    }

    #[test]
    fn test_vals_ok() {
        // Checks that vals returns the expected typed AST after semantic analysis
        let vals = [
            SExprV::Val(Value::Int(1)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];
        let results = vals.iter().map(TypeCheckable::type_check_with_default);
        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprInt::Val(PartialStreamValue::Known(1)))),
            Ok(SExprTE::Str(SExprStr::Val(PartialStreamValue::Known(
                "".into(),
            )))),
            Ok(SExprTE::Bool(SExprBool::Val(PartialStreamValue::Known(
                true,
            )))),
            Ok(SExprTE::Unit(SExprUnit::Val(PartialStreamValue::Known(())))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_deferred_err() {
        // Checks that if a Val is deferred during semantic analysis it produces a DeferredError
        let val = SExprV::Val(Value::Deferred);
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::DeferredError("".into())]);
        check_correct_error_type(&result, &expected);
    }

    #[test]
    fn test_plus_err_ident_types() {
        // Checks that if we add two identical types together that are not addable,
        let vals = [
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Bool(false))),
                Box::new(SExprV::Val(Value::Bool(false))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Unit)),
                Box::new(SExprV::Val(Value::Unit)),
                SBinOp::NOp(NumericalBinOp::Add),
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

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        // Create a vector of all SBinOp variants
        let sbinops = all_sbinop_variants();

        let vals_tmp = generate_binop_combinations(&variants, &variants, sbinops);
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::BinOp(left, right, _) => {
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
    fn test_plus_err_deferred() {
        // Checks that if either value is deferred then Plus does not generate further errors
        let vals = [
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Int(0))),
                Box::new(SExprV::Val(Value::Deferred)),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Deferred)),
                Box::new(SExprV::Val(Value::Int(0))),
                SBinOp::NOp(NumericalBinOp::Add),
            ),
            SExprV::BinOp(
                Box::new(SExprV::Val(Value::Deferred)),
                Box::new(SExprV::Val(Value::Deferred)),
                SBinOp::NOp(NumericalBinOp::Add),
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
                    // TODO: Check that it is actually DeferredErrors
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
        let int_val = vec![SExprV::Val(Value::Int(0))];
        let sbinops = all_sbinop_variants();
        let vals: Vec<SExpr> = generate_binop_combinations(&int_val, &int_val, sbinops.clone());
        let results = vals.iter().map(TypeCheckable::type_check_with_default);

        let int_t_val = vec![SExprInt::Val(PartialStreamValue::Known(0))];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprInt> =
            generate_binop_combinations(&int_t_val, &int_t_val, sbinops);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Int(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_str_plus_ok() {
        // Checks that if we add two Strings together it results in typed AST after semantic analysis
        let str_val = vec![SExprV::Val(Value::Str("".into()))];
        let sbinops = vec![SBinOp::SOp(StrBinOp::Concat)];
        let vals: Vec<SExpr> = generate_binop_combinations(&str_val, &str_val, sbinops.clone());
        let results = vals.iter().map(TypeCheckable::type_check_with_default);

        let str_t_val = vec![SExprStr::Val(PartialStreamValue::Known("".into()))];

        // Generate the different combinations and turn them into "Ok" results
        let expected_tmp: Vec<SExprStr> = generate_concat_combinations(&str_t_val, &str_t_val);
        let expected = expected_tmp.into_iter().map(|v| Ok(SExprTE::Str(v)));
        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_ok() {
        // Checks that typechecking if-statements with identical types for if- and else- part results in correct typed AST

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let val_variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        // Create a vector of all SBinOp variants
        let bexpr = Box::new(SExpr::Val(true.into()));
        let bexpr_checked = Box::new(SExprBool::Val(PartialStreamValue::Known(true)));

        let vals_tmp = generate_if_combinations(&val_variants, &val_variants, bexpr.clone());

        // Only consider cases where true and false cases are equal
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::If(_, t, f) => t == f,
                _ => true, // Keep non-ifs (unused in this case)
            }
        });
        let results = vals.map(|x| x.type_check_with_default());

        let expected: Vec<SemantResultStr> = vec![
            Ok(SExprTE::Int(SExprInt::If(
                bexpr_checked.clone(),
                Box::new(SExprInt::Val(PartialStreamValue::Known(0))),
                Box::new(SExprInt::Val(PartialStreamValue::Known(0))),
            ))),
            Ok(SExprTE::Str(SExprStr::If(
                bexpr_checked.clone(),
                Box::new(SExprStr::Val(PartialStreamValue::Known("".into()))),
                Box::new(SExprStr::Val(PartialStreamValue::Known("".into()))),
            ))),
            Ok(SExprTE::Bool(SExprBool::If(
                bexpr_checked.clone(),
                Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
                Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            ))),
            Ok(SExprTE::Unit(SExprUnit::If(
                bexpr_checked.clone(),
                Box::new(SExprUnit::Val(PartialStreamValue::Known(()))),
                Box::new(SExprUnit::Val(PartialStreamValue::Known(()))),
            ))),
        ];

        assert!(results.eq(expected.into_iter()));
    }

    #[test]
    fn test_if_err() {
        // Checks that creating an if-expression with two different types results in a TypeError

        // Create a vector of all ConcreteStreamData variants (except Deferred)
        let variants = vec![
            SExprV::Val(Value::Int(0)),
            SExprV::Val(Value::Str("".into())),
            SExprV::Val(Value::Bool(true)),
            SExprV::Val(Value::Unit),
        ];

        let bexpr = Box::new(SExpr::Val(true.into()));

        let vals_tmp = generate_if_combinations(&variants, &variants, bexpr.clone());
        let vals = vals_tmp.into_iter().filter(|bin_op| {
            match bin_op {
                SExprV::If(_, t, f) => t != f,
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
            .map(|n| SExprV::Var(n.into()));

        // Fake context/environment that simulates type-checking context
        let mut ctx = TypeContext::new();
        for (n, t) in variant_names.into_iter().zip(variant_types.into_iter()) {
            ctx.insert(n.into(), t);
        }

        let results = vals.into_iter().map(|sexpr| sexpr.type_check(&mut ctx));

        let expected = vec![
            Ok(SExprTE::Int(SExprInt::Var("int".into()))),
            Ok(SExprTE::Str(SExprStr::Var("str".into()))),
            Ok(SExprTE::Bool(SExprBool::Var("bool".into()))),
            Ok(SExprTE::Unit(SExprUnit::Var("unit".into()))),
        ];

        assert!(results.eq(expected));
    }

    #[test]
    fn test_var_err() {
        // Checks that Vars produce UndeclaredVariable errors if they do not exist in the context

        let val = SExprV::Var("undeclared_name".into());
        let result = val.type_check_with_default();
        let expected: SemantResultStr = Err(vec![SemanticError::UndeclaredVariable("".into())]);
        check_correct_error_type(&result, &expected);
    }
    // TODO: Test that any SExpr leaf is a Val. If not it should return a Type-Error

    #[test]
    fn test_dodgy_if() {
        let dodgy_bexpr = SExpr::BinOp(
            Box::new(SExprV::Val(Value::Int(0))),
            Box::new(SExprV::BinOp(
                Box::new(SExprV::Val(Value::Int(3))),
                Box::new(SExprV::Val(Value::Str("Banana".into()))),
                SBinOp::NOp(NumericalBinOp::Add),
            )),
            SBinOp::COp(CompBinOp::Eq),
        );
        let sexpr = SExprV::If(
            Box::new(dodgy_bexpr),
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Val(Value::Int(2))),
        );
        if let Ok(_) = sexpr.type_check_with_default() {
            assert!(false, "Expected type error but got a successful result");
        }
    }

    #[test]
    fn test_defer_nested_int_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeContext::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_ascription_no_eval() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeContext::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_ascription_incorrect_inner_type() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Bool);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_defer_nested_bool_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Bool(true))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
            )),
            SBinOp::BOp(BoolBinOp::And),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeContext::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Bool(SExprBool::BinOp(
            Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            Box::new(SExprBool::Defer(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            BoolBinOp::And,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_defer_nested_int_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_defer_nested_int_missing_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Defer(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_dynamic_nested_int_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Int),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeContext::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Int(SExprInt::BinOp(
            Box::new(SExprInt::Val(PartialStreamValue::Known(1))),
            Box::new(SExprInt::Dynamic(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            IntBinOp::Add,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dynamic_nested_bool_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Bool(true))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
            )),
            SBinOp::BOp(BoolBinOp::And),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeContext::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Bool(SExprBool::BinOp(
            Box::new(SExprBool::Val(PartialStreamValue::Known(true))),
            Box::new(SExprBool::Dynamic(
                Box::new(SExprStr::Var("x".into())),
                expected_ctx.clone(),
            )),
            BoolBinOp::And,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dynamic_nested_int_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_dynamic_nested_int_unascribed() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::Dynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Int);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("hello".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Str),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        let mut expected_ctx = TypeContext::new();
        expected_ctx.insert("x".into(), StreamType::Str);
        let expected = Ok(SExprTE::Str(SExprStr::BinOp(
            Box::new(SExprStr::Val(PartialStreamValue::Known("hello".into()))),
            Box::new(SExprStr::RestrictedDynamic(
                Box::new(SExprStr::Var("x".into())),
                EcoVec::from(vec!["x".into(), "y".into()]),
                expected_ctx.clone(),
            )),
            StrBinOp::Concat,
        )));
        assert_eq!(result, expected);
    }

    #[test]
    fn test_restricted_dynamic_nested_int_ascription_error() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Int(1))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::NOp(NumericalBinOp::Add),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_bool_false_ascription() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("test".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Ascribed(StreamType::Bool),
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        // Type ascription mismatch should cause error
        assert!(result.is_err());
    }

    #[test]
    fn test_restricted_dynamic_nested_str_unascribed() {
        let expr = SExprV::BinOp(
            Box::new(SExprV::Val(Value::Str("hello".into()))),
            Box::new(SExprV::RestrictedDynamic(
                Box::new(SExprV::Var("x".into())),
                StreamTypeAscription::Unascribed,
                EcoVec::from(vec!["x".into(), "y".into()]),
            )),
            SBinOp::SOp(StrBinOp::Concat),
        );
        let mut ctx = TypeContext::new();
        ctx.insert("x".into(), StreamType::Str);
        let result = expr.type_check(&mut ctx);
        assert!(result.is_err());
    }
}

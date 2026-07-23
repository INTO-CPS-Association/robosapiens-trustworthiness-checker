use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use ecow::{EcoString, EcoVec};

use crate::core::values::operations as value_operations;
use crate::core::{BinaryOperator, UnaryOperator};
use crate::distributed::distribution_graphs::NodeName;
use crate::lang::dsrv::ast::{ExprRef, ExprView};
use crate::lang::dsrv::span::Span;
use crate::{DsrvSpecification, Value, VarName};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintProfile {
    CompactEvaluator,
    Sat,
}

#[derive(Debug, Clone)]
pub struct ConstraintExpr {
    pub kind: ConstraintExprKind,
    pub span: Span,
    pub display: String,
}

#[derive(Debug, Clone)]
pub enum ConstraintExprKind {
    Value(Value),
    Variable(VarName),
    MonitoredAt(VarName, NodeName),
    If(
        Box<ConstraintExpr>,
        Box<ConstraintExpr>,
        Box<ConstraintExpr>,
    ),
    Binary(Box<ConstraintExpr>, Box<ConstraintExpr>, BinaryOperator),
    Not(Box<ConstraintExpr>),
    Neg(Box<ConstraintExpr>),
    Abs(Box<ConstraintExpr>),
    List(EcoVec<ConstraintExpr>),
    ListIndex(Box<ConstraintExpr>, Box<ConstraintExpr>),
    ListAppend(Box<ConstraintExpr>, Box<ConstraintExpr>),
    ListConcat(Box<ConstraintExpr>, Box<ConstraintExpr>),
    ListHead(Box<ConstraintExpr>),
    ListTail(Box<ConstraintExpr>),
    ListLen(Box<ConstraintExpr>),
    Map(BTreeMap<EcoString, ConstraintExpr>),
    MapGet(Box<ConstraintExpr>, EcoString),
    MapInsert(Box<ConstraintExpr>, EcoString, Box<ConstraintExpr>),
    MapRemove(Box<ConstraintExpr>, EcoString),
    MapHasKey(Box<ConstraintExpr>, EcoString),
}

#[derive(Debug, Clone)]
pub struct DistributionConstraintPlan {
    roots: Vec<VarName>,
    definitions: BTreeMap<VarName, ConstraintExpr>,
    input_dependencies: BTreeSet<VarName>,
    monitored_streams: BTreeSet<VarName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintLoweringError {
    MissingDefinition {
        variable: VarName,
    },
    CyclicDefinition {
        variable: VarName,
    },
    UnsupportedExpression {
        profile: ConstraintProfile,
        kind: String,
        span: Span,
        display: String,
        reason: Option<&'static str>,
    },
}

impl fmt::Display for ConstraintLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingDefinition { variable } => {
                write!(f, "Distribution constraint `{variable}` has no expression")
            }
            Self::CyclicDefinition { variable } => {
                write!(
                    f,
                    "Cycle while lowering distribution constraint variable `{variable}`"
                )
            }
            Self::UnsupportedExpression {
                profile,
                kind,
                span,
                display,
                reason,
            } => {
                let consumer = match profile {
                    ConstraintProfile::CompactEvaluator => {
                        "compact distribution constraint evaluator"
                    }
                    ConstraintProfile::Sat => "SAT monitored_at solver",
                };
                if let Some(reason) = reason {
                    write!(f, "{reason} at {}..{}: `{display}`", span.start, span.end)
                } else {
                    write!(
                        f,
                        "Unsupported expression in {consumer}: {kind} at {}..{}: `{display}`",
                        span.start, span.end
                    )
                }
            }
        }
    }
}

impl std::error::Error for ConstraintLoweringError {}

impl DistributionConstraintPlan {
    pub fn lower(
        spec: &DsrvSpecification,
        roots: impl IntoIterator<Item = VarName>,
        profile: ConstraintProfile,
    ) -> Result<Self, ConstraintLoweringError> {
        let roots = roots.into_iter().collect::<Vec<_>>();
        let mut lowerer = Lowerer {
            spec,
            profile,
            definitions: BTreeMap::new(),
            input_dependencies: BTreeSet::new(),
            monitored_streams: BTreeSet::new(),
            visiting: BTreeSet::new(),
        };
        for root in &roots {
            lowerer.lower_definition(root)?;
        }
        Ok(Self {
            roots,
            definitions: lowerer.definitions,
            input_dependencies: lowerer.input_dependencies,
            monitored_streams: lowerer.monitored_streams,
        })
    }

    pub fn roots(&self) -> &[VarName] {
        &self.roots
    }

    pub fn expression(&self, variable: &VarName) -> Option<&ConstraintExpr> {
        self.definitions.get(variable)
    }

    pub fn definitions(&self) -> &BTreeMap<VarName, ConstraintExpr> {
        &self.definitions
    }

    pub fn input_dependencies(&self) -> &BTreeSet<VarName> {
        &self.input_dependencies
    }

    pub fn monitored_streams(&self) -> &BTreeSet<VarName> {
        &self.monitored_streams
    }

    pub fn evaluate_var(
        &self,
        variable: &VarName,
        bindings: &BTreeMap<VarName, Value>,
        monitored_at: Option<&dyn Fn(&VarName, &NodeName) -> bool>,
    ) -> Option<Value> {
        let mut stack = BTreeSet::new();
        self.evaluate_variable(variable, bindings, monitored_at, &mut stack)
    }

    pub fn evaluate_expr(
        &self,
        expression: &ConstraintExpr,
        bindings: &BTreeMap<VarName, Value>,
        monitored_at: Option<&dyn Fn(&VarName, &NodeName) -> bool>,
    ) -> Option<Value> {
        let mut stack = BTreeSet::new();
        self.evaluate(expression, bindings, monitored_at, &mut stack)
    }

    fn evaluate_variable(
        &self,
        variable: &VarName,
        bindings: &BTreeMap<VarName, Value>,
        monitored_at: Option<&dyn Fn(&VarName, &NodeName) -> bool>,
        stack: &mut BTreeSet<VarName>,
    ) -> Option<Value> {
        if let Some(value) = bindings.get(variable) {
            return Some(value.clone());
        }
        if !stack.insert(variable.clone()) {
            return None;
        }
        let value = self
            .definitions
            .get(variable)
            .and_then(|expr| self.evaluate(expr, bindings, monitored_at, stack));
        stack.remove(variable);
        value
    }

    fn evaluate(
        &self,
        expression: &ConstraintExpr,
        bindings: &BTreeMap<VarName, Value>,
        monitored_at: Option<&dyn Fn(&VarName, &NodeName) -> bool>,
        stack: &mut BTreeSet<VarName>,
    ) -> Option<Value> {
        use ConstraintExprKind as E;
        match &expression.kind {
            E::Value(value) => Some(value.clone()),
            E::Variable(variable) => {
                self.evaluate_variable(variable, bindings, monitored_at, stack)
            }
            E::MonitoredAt(variable, node) => {
                monitored_at.map(|predicate| Value::Bool(predicate(variable, node)))
            }
            E::If(condition, then_expr, else_expr) => {
                match self.evaluate(condition, bindings, monitored_at, stack)? {
                    Value::Bool(true) => self.evaluate(then_expr, bindings, monitored_at, stack),
                    Value::Bool(false) => self.evaluate(else_expr, bindings, monitored_at, stack),
                    _ => None,
                }
            }
            E::Binary(left, right, op) => {
                let left = self.evaluate(left, bindings, monitored_at, stack)?;
                let right = self.evaluate(right, bindings, monitored_at, stack)?;
                eval_binary(left, right, op)
            }
            E::Not(value) => value_operations::unary(
                UnaryOperator::Not,
                self.evaluate(value, bindings, monitored_at, stack)?,
            )
            .ok(),
            E::Neg(value) => value_operations::unary(
                UnaryOperator::Negate,
                self.evaluate(value, bindings, monitored_at, stack)?,
            )
            .ok(),
            E::Abs(value) => value_operations::unary(
                UnaryOperator::Absolute,
                self.evaluate(value, bindings, monitored_at, stack)?,
            )
            .ok(),
            E::List(items) => items
                .iter()
                .map(|item| self.evaluate(item, bindings, monitored_at, stack))
                .collect::<Option<EcoVec<_>>>()
                .map(Value::List),
            E::ListIndex(list, index) => value_operations::list_index(
                self.evaluate(list, bindings, monitored_at, stack)?,
                self.evaluate(index, bindings, monitored_at, stack)?,
            )
            .ok(),
            E::ListAppend(list, value) => value_operations::list_append(
                self.evaluate(list, bindings, monitored_at, stack)?,
                self.evaluate(value, bindings, monitored_at, stack)?,
            )
            .ok(),
            E::ListConcat(left, right) => value_operations::list_concat(
                self.evaluate(left, bindings, monitored_at, stack)?,
                self.evaluate(right, bindings, monitored_at, stack)?,
            )
            .ok(),
            E::ListHead(list) => {
                value_operations::list_head(self.evaluate(list, bindings, monitored_at, stack)?)
                    .ok()
            }
            E::ListTail(list) => {
                value_operations::list_tail(self.evaluate(list, bindings, monitored_at, stack)?)
                    .ok()
            }
            E::ListLen(list) => {
                value_operations::list_len(self.evaluate(list, bindings, monitored_at, stack)?).ok()
            }
            E::Map(entries) => entries
                .iter()
                .map(|(key, value)| {
                    self.evaluate(value, bindings, monitored_at, stack)
                        .map(|value| (key.clone(), value))
                })
                .collect::<Option<BTreeMap<_, _>>>()
                .map(Value::Map),
            E::MapGet(map, key) => {
                value_operations::map_get(self.evaluate(map, bindings, monitored_at, stack)?, key)
                    .ok()
            }
            E::MapInsert(map, key, value) => value_operations::map_insert(
                self.evaluate(map, bindings, monitored_at, stack)?,
                key,
                self.evaluate(value, bindings, monitored_at, stack)?,
            )
            .ok(),
            E::MapRemove(map, key) => value_operations::map_remove(
                self.evaluate(map, bindings, monitored_at, stack)?,
                key,
            )
            .ok(),
            E::MapHasKey(map, key) => value_operations::map_has_key(
                self.evaluate(map, bindings, monitored_at, stack)?,
                key,
            )
            .ok(),
        }
    }
}

struct Lowerer<'a> {
    spec: &'a DsrvSpecification,
    profile: ConstraintProfile,
    definitions: BTreeMap<VarName, ConstraintExpr>,
    input_dependencies: BTreeSet<VarName>,
    monitored_streams: BTreeSet<VarName>,
    visiting: BTreeSet<VarName>,
}

impl Lowerer<'_> {
    fn lower_definition(&mut self, variable: &VarName) -> Result<(), ConstraintLoweringError> {
        if self.definitions.contains_key(variable) {
            return Ok(());
        }
        if !self.visiting.insert(variable.clone()) {
            return Err(ConstraintLoweringError::CyclicDefinition {
                variable: variable.clone(),
            });
        }
        let expression = self.spec.var_expr_ref(variable).ok_or_else(|| {
            ConstraintLoweringError::MissingDefinition {
                variable: variable.clone(),
            }
        })?;
        let lowered = self.lower_expr(expression)?;
        self.visiting.remove(variable);
        self.definitions.insert(variable.clone(), lowered);
        Ok(())
    }

    fn lower_expr(
        &mut self,
        expression: ExprRef<'_>,
    ) -> Result<ConstraintExpr, ConstraintLoweringError> {
        use ExprView::*;

        let span = expression.span();
        let display = format!("{expression}");
        let kind = match expression.view() {
            Val(value) => ConstraintExprKind::Value(value.clone()),
            Var(variable) => {
                if self.spec.input_vars().contains(variable) {
                    self.input_dependencies.insert(variable.clone());
                } else if self.spec.var_expr_ref(variable).is_some() {
                    self.lower_definition(variable)?;
                } else {
                    return Err(ConstraintLoweringError::MissingDefinition {
                        variable: variable.clone(),
                    });
                }
                ConstraintExprKind::Variable(variable.clone())
            }
            MonitoredAt(variable, node) => {
                self.monitored_streams.insert(variable.clone());
                ConstraintExprKind::MonitoredAt(variable.clone(), node.clone())
            }
            If(condition, then_expr, else_expr) => ConstraintExprKind::If(
                Box::new(self.lower_expr(condition)?),
                Box::new(self.lower_expr(then_expr)?),
                Box::new(self.lower_expr(else_expr)?),
            ),
            BinOp(left, right, operator) => ConstraintExprKind::Binary(
                Box::new(self.lower_expr(left)?),
                Box::new(self.lower_expr(right)?),
                operator,
            ),
            Not(value) => ConstraintExprKind::Not(Box::new(self.lower_expr(value)?)),
            Neg(value) => ConstraintExprKind::Neg(Box::new(self.lower_expr(value)?)),
            Abs(value) => ConstraintExprKind::Abs(Box::new(self.lower_expr(value)?)),
            MGet(map, key) | SGet(map, key) => {
                ConstraintExprKind::MapGet(Box::new(self.lower_expr(map)?), key.clone())
            }
            List(items) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::List(items.map(|item| self.lower_expr(item)).collect::<Result<
                    EcoVec<_>,
                    _,
                >>(
                )?)
            }
            LIndex(list, index) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::ListIndex(
                    Box::new(self.lower_expr(list)?),
                    Box::new(self.lower_expr(index)?),
                )
            }
            LAppend(list, value) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::ListAppend(
                    Box::new(self.lower_expr(list)?),
                    Box::new(self.lower_expr(value)?),
                )
            }
            LConcat(left, right) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::ListConcat(
                    Box::new(self.lower_expr(left)?),
                    Box::new(self.lower_expr(right)?),
                )
            }
            LHead(list) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::ListHead(Box::new(self.lower_expr(list)?))
            }
            LTail(list) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::ListTail(Box::new(self.lower_expr(list)?))
            }
            LLen(list) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::ListLen(Box::new(self.lower_expr(list)?))
            }
            Map(entries) | Struct(entries) | ObjectLiteral(entries)
                if self.profile == ConstraintProfile::Sat =>
            {
                ConstraintExprKind::Map(
                    entries
                        .iter()
                        .map(|(key, value)| Ok((key.clone(), self.lower_expr(value)?)))
                        .collect::<Result<BTreeMap<_, _>, ConstraintLoweringError>>()?,
                )
            }
            MInsert(map, key, value) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::MapInsert(
                    Box::new(self.lower_expr(map)?),
                    key.clone(),
                    Box::new(self.lower_expr(value)?),
                )
            }
            MRemove(map, key) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::MapRemove(Box::new(self.lower_expr(map)?), key.clone())
            }
            MHasKey(map, key) if self.profile == ConstraintProfile::Sat => {
                ConstraintExprKind::MapHasKey(Box::new(self.lower_expr(map)?), key.clone())
            }
            Dist(_, _) => {
                return Err(self.unsupported(
                    expression,
                    Some(match self.profile {
                        ConstraintProfile::CompactEvaluator => {
                            "dist(...) is unsupported in compact distribution constraints"
                        }
                        ConstraintProfile::Sat => {
                            "SAT monitored_at solver supports only monitored_at(...) + boolean logic; dist(...) is unsupported"
                        }
                    }),
                ));
            }
            SIndex(_, _) if self.profile == ConstraintProfile::Sat => {
                return Err(self.unsupported(
                    expression,
                    Some("SAT monitored_at solver supports only timeless constraints; time-indexed expressions are unsupported"),
                ));
            }
            _ => return Err(self.unsupported(expression, None)),
        };
        Ok(ConstraintExpr {
            kind,
            span,
            display,
        })
    }

    fn unsupported(
        &self,
        expression: ExprRef<'_>,
        reason: Option<&'static str>,
    ) -> ConstraintLoweringError {
        ConstraintLoweringError::UnsupportedExpression {
            profile: self.profile,
            kind: format!("{:?}", expression.kind()),
            span: expression.span(),
            display: format!("{expression}"),
            reason,
        }
    }
}

fn eval_binary(left: Value, right: Value, operator: &BinaryOperator) -> Option<Value> {
    value_operations::binary(*operator, left, right).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowering_collects_transitive_inputs_and_monitored_streams_once() {
        let spec = ("in gate\nout x\nout constraint\nhelper = gate && monitored_at(x, A)\nconstraint = helper").parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

        let plan = DistributionConstraintPlan::lower(
            &spec,
            [VarName::new("constraint")],
            ConstraintProfile::CompactEvaluator,
        )
        .unwrap();

        assert_eq!(
            plan.input_dependencies(),
            &BTreeSet::from([VarName::new("gate")])
        );
        assert_eq!(
            plan.monitored_streams(),
            &BTreeSet::from([VarName::new("x")])
        );
        assert!(plan.definitions().contains_key(&VarName::new("helper")));
    }

    #[test]
    fn lowering_reports_unsupported_form_with_span_and_display() {
        let spec = ("out constraint\nconstraint = dist(A, B) == 1")
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

        let error = DistributionConstraintPlan::lower(
            &spec,
            [VarName::new("constraint")],
            ConstraintProfile::CompactEvaluator,
        )
        .unwrap_err();

        let ConstraintLoweringError::UnsupportedExpression { span, display, .. } = error else {
            panic!("expected unsupported-expression diagnostic");
        };
        assert!(!span.is_empty());
        assert!(display.contains("dist"));
    }

    #[test]
    fn sat_profile_accepts_constant_collections_but_compact_profile_rejects_them() {
        let spec = ("out constraint\nconstraint = List.len(List(1, 2)) == 2")
            .parse::<DsrvSpecification>()
            .expect("test DSRV specification should parse");

        assert!(
            DistributionConstraintPlan::lower(
                &spec,
                [VarName::new("constraint")],
                ConstraintProfile::Sat,
            )
            .is_ok()
        );
        assert!(
            DistributionConstraintPlan::lower(
                &spec,
                [VarName::new("constraint")],
                ConstraintProfile::CompactEvaluator,
            )
            .is_err()
        );
    }
}

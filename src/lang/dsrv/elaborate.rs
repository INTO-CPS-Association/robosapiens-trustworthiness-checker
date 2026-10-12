//! Elaboration: the checked tree becomes the tree runtimes run.
//!
//! Elaboration rewrites every equation of a checked specification with the
//! contiguous-tree rewrite support, one destination subtree per checked node.
//! Each node it emits keeps the span and source context of the node it came
//! from and carries a type. The rewrite is the identity on every node kind
//! but the newtype ones, so every other node's type is the one checking gave
//! it and a specification without newtypes elaborates to the tree it was.
//!
//! Newtypes are what elaboration lowers, because checking is what settles
//! which instance each operation names:
//!
//! - `Name::new(x)` becomes `Wrap`, which only brands its operand. Where
//!   checking did not prove the operand to be exactly the representation (it
//!   was `Any`, or an `Int` promoted to a `Float`), the operand is first
//!   ascribed the representation, so a malformed operand fails before any
//!   value is taken for one of the newtype.
//! - `Name::value(x)` becomes `Unwrap`, which refuses a value of any other
//!   brand.
//! - A cast whose target contains a newtype becomes a checked ascription:
//!   casting only validates an identity a value already carries.
//! - Where a value whose type is only known at run time meets a position
//!   typed with a newtype (an equation's declared type, a function argument,
//!   a branch, a field or an element), the value is ascribed exactly the
//!   nominal part of that type, so the identity is checked where the
//!   gradual type was accepted rather than wherever it is next used.
//!
//! No frontend-only newtype node survives, so no runtime sees one.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};

use contiguous_tree::TreeCursorExt;

use super::ast::{
    AstShared, CheckedDsrvSpecification, CheckedExpr, CheckedExprRef, DsrvSpecification,
    ExprBuilder, ExprForestMap, ExprId, ExprKind, ExprView, NewtypeOperationKind, NewtypeTarget,
};
use super::diagnostics::SemanticAnalysisReport;
use super::parser::DsrvParseError;
use super::pipeline::{TypeCheckMode, TypeCheckOptions};
use super::type_checker::{TCType, mentions_newtype};
use crate::core::{
    RuntimeCapabilities, RuntimeCapabilityRequirement, Specification, StreamType, VarName,
};

/// A checked specification after elaboration: the tree every runtime runs,
/// with a type for each of its nodes. Only elaboration constructs one, from a
/// specification that passed strict or gradual checking, and every runtime
/// requires one; an evaluation strategy may ignore the types. Printing it
/// prints the source-level specification it was elaborated from.
#[derive(Clone, Debug)]
pub struct ElaboratedDsrvSpecification {
    source: CheckedDsrvSpecification,
    elaborated: CheckedDsrvSpecification,
}

impl ElaboratedDsrvSpecification {
    /// Parse, check and elaborate a specification. A parse failure stops
    /// before checking; otherwise the check is reported.
    pub fn parse_with(
        source: &str,
        options: TypeCheckOptions,
    ) -> Result<SemanticAnalysisReport<Self>, DsrvParseError> {
        CheckedDsrvSpecification::parse_with(source, options)
            .map(|report| report.map_checked(CheckedDsrvSpecification::elaborate))
    }

    /// The checked specification as written, before elaboration.
    pub fn source(&self) -> &CheckedDsrvSpecification {
        &self.source
    }

    /// The elaborated tree with the type of each of its nodes.
    pub fn checked(&self) -> &CheckedDsrvSpecification {
        &self.elaborated
    }

    /// The policy the specification was checked with, which elaboration and
    /// every later rewrite keep.
    pub fn check_mode(&self) -> TypeCheckMode {
        self.elaborated.check_mode()
    }

    pub fn var_expr_ref(&self, var: &VarName) -> Option<CheckedExprRef<'_>> {
        self.elaborated.var_expr_ref(var)
    }

    pub fn var_expr(&self, var: &VarName) -> Option<CheckedExpr> {
        self.elaborated.var_expr(var)
    }

    pub fn input_vars(&self) -> &BTreeSet<VarName> {
        self.elaborated.input_vars()
    }

    pub fn output_vars(&self) -> &BTreeSet<VarName> {
        self.elaborated.output_vars()
    }

    pub fn aux_vars(&self) -> &BTreeSet<VarName> {
        self.elaborated.aux_vars()
    }

    pub fn type_annotations(&self) -> &BTreeMap<VarName, StreamType> {
        self.elaborated.type_annotations()
    }
}

impl ElaboratedDsrvSpecification {
    /// A rewrite of an elaborated tree that keeps every node's type, such as
    /// distributed localisation, is itself elaborated; it has no other source.
    pub(crate) fn from_rewritten(elaborated: CheckedDsrvSpecification) -> Self {
        let elaborated = elaborated.prepare_sites();
        Self {
            source: elaborated.clone(),
            elaborated,
        }
    }
}

impl CheckedDsrvSpecification {
    /// Elaborate this checked specification into the tree runtimes run.
    pub fn elaborate(self) -> ElaboratedDsrvSpecification {
        let elaborated = elaborate_tree(&self).prepare_sites();
        ElaboratedDsrvSpecification {
            source: self,
            elaborated,
        }
    }
}

impl DsrvSpecification {
    /// Check this specification with the requested policy, then elaborate it.
    pub fn check_and_elaborate(
        self,
        options: TypeCheckOptions,
    ) -> SemanticAnalysisReport<ElaboratedDsrvSpecification> {
        self.check(options)
            .map_checked(CheckedDsrvSpecification::elaborate)
    }
}

impl CheckedExpr {
    /// Elaborate a standalone checked expression, as text supplied at run
    /// time to `dynamic` and `defer` is before it runs. Its sites are not
    /// prepared.
    pub(crate) fn elaborate(self) -> CheckedExpr {
        let root = self.as_ref();
        let boundaries = nominal_boundaries([(root, None)]);
        let mut builder = ExprBuilder::with_capacity(root.postorder().count());
        let mut types = Vec::new();
        let root = builder
            .try_rewrite_forest([root], |node| elaborate_node(node, &boundaries, &mut types))
            .expect("elaboration keeps every node's children")[0];
        let expr = builder
            .finish(root)
            .expect("elaboration emits one complete tree");
        let mut expr_types = expr.annotations_builder();
        assert_eq!(types.len(), expr.as_ref().postorder().count());
        for (node, typ) in expr.as_ref().postorder().zip(types) {
            expr_types
                .insert(node, typ)
                .expect("an emitted node belongs to the elaborated tree");
        }
        let expr_types = expr_types
            .finish()
            .expect("elaboration types every node it emits");
        CheckedExpr::new(expr, expr_types, AstShared::clone(self.environment()))
    }
}

/// Where a value whose type checking left gradual flows into a position
/// typed with a newtype, by the node that flows, with the part of that type
/// to check it against at run time.
type Boundaries = BTreeMap<ExprId, StreamType>;

/// Rewrite one checked node into its elaborated subtree, pushing the type of
/// each node it emits in allocation order.
fn elaborate_node(
    mut node: super::ast::ExprRewriteNode<'_, '_, CheckedExprRef<'_>>,
    boundaries: &Boundaries,
    types: &mut Vec<TCType>,
) -> Result<ExprId, contiguous_tree::BuildError<ExprId>> {
    let source = node.source();
    let metadata = source.expr().metadata().clone();
    let id = match source.view() {
        ExprView::Newtype(value, operation) => {
            let operand = node.child(value.expr().id());
            match operation.kind {
                NewtypeOperationKind::New => {
                    let TCType::Newtype(schema) = source.typ() else {
                        unreachable!("checking types `{operation}` as its newtype")
                    };
                    let schema = to_stream_newtype(schema);
                    let operand = if exactly(value.typ(), schema.repr()) {
                        operand
                    } else {
                        let ascribed = node.alloc(
                            ExprKind::Ascribe(operand, schema.repr().clone()),
                            metadata.clone(),
                        )?;
                        types.push(TCType::from_stream_type(schema.repr()));
                        ascribed
                    };
                    node.alloc(ExprKind::Wrap(operand, schema), metadata.clone())?
                }
                NewtypeOperationKind::Value => {
                    let schema = match (value.typ(), &operation.target) {
                        (TCType::Newtype(schema), _) => to_stream_newtype(schema),
                        (_, NewtypeTarget::Instance(schema)) => schema.clone(),
                        (other, NewtypeTarget::Generic { .. }) => unreachable!(
                            "checking refuses `{operation}` of an unsettled instance, got {other}"
                        ),
                    };
                    node.alloc(ExprKind::Unwrap(operand, schema), metadata.clone())?
                }
            }
        }
        // A cast into a newtype validates the identity the value carries,
        // which every runtime does for an ascription.
        ExprView::Cast(value, target) if contains_newtype(target) => {
            let operand = node.child(value.expr().id());
            node.alloc(ExprKind::Ascribe(operand, target.clone()), metadata.clone())?
        }
        _ => {
            let kind = node.source_node().rebuild(source.kind().clone());
            node.alloc(kind, metadata.clone())?
        }
    };
    types.push(source.typ().clone());
    match boundaries.get(&source.expr().id()) {
        Some(target) => {
            let checked = node.alloc(ExprKind::Ascribe(id, target.clone()), metadata)?;
            types.push(source.typ().clone());
            Ok(checked)
        }
        None => Ok(id),
    }
}

fn to_stream_newtype(
    schema: &crate::core::NewtypeSchema<TCType>,
) -> crate::core::NewtypeSchema<StreamType> {
    schema
        .try_map(|ty| ty.to_stream_type().ok_or(()))
        .expect("a checked newtype instance has a concrete representation")
}

/// Whether checking proved a value of type `actual` to already be exactly
/// `repr`, so branding it needs no check.
fn exactly(actual: &TCType, repr: &StreamType) -> bool {
    actual.to_stream_type().as_ref() == Some(repr) && !contains_gradual(actual)
}

fn contains_gradual(typ: &TCType) -> bool {
    match typ {
        TCType::Any | TCType::Unknown | TCType::EmptyList | TCType::EmptyMap => true,
        TCType::List(inner) | TCType::Map(inner) | TCType::Expr(inner) => contains_gradual(inner),
        TCType::Tuple(items) => items.iter().any(contains_gradual),
        TCType::Struct(fields, _) => fields.iter().any(|(_, typ)| contains_gradual(typ)),
        TCType::Function(arguments, result) => {
            arguments.iter().any(contains_gradual) || contains_gradual(result)
        }
        TCType::Union(schema) => schema.alternatives().iter().any(|alternative| {
            matches!(alternative.payload(), crate::core::UnionPayload::Of(payload) if contains_gradual(payload))
        }),
        TCType::Newtype(schema) => contains_gradual(schema.repr()),
        TCType::Int | TCType::Float | TCType::Str | TCType::Bool | TCType::Unit => false,
    }
}

fn contains_newtype(typ: &StreamType) -> bool {
    typ.mentions_newtype()
}

/// The part of `expected` a value of static type `actual` must be checked
/// against at run time: each newtype `actual` left gradual, inside the
/// containers that hold it, and nothing else. `None` where checking already
/// proved every newtype `expected` holds.
///
/// Only the nominal part is checked, so an ascription never judges what the
/// gradual type system already admits (an `Int` where a `Float` is expected,
/// say). A union or function type is not looked inside: a newtype there is
/// checked where it is next used, as N8 has every unchecked input be.
fn nominal_check(expected: &TCType, actual: &TCType) -> Option<StreamType> {
    let gradual = matches!(actual, TCType::Any);
    match expected {
        TCType::Newtype(_) if gradual => expected.to_stream_type(),
        TCType::List(inner) => {
            let actual = match actual {
                TCType::List(actual) => actual.as_ref(),
                _ if gradual => &TCType::Any,
                _ => return None,
            };
            nominal_check(inner, actual).map(|inner| StreamType::List(Box::new(inner)))
        }
        TCType::Map(inner) => {
            let actual = match actual {
                TCType::Map(actual) => actual.as_ref(),
                _ if gradual => &TCType::Any,
                _ => return None,
            };
            nominal_check(inner, actual).map(|inner| StreamType::Map(Box::new(inner)))
        }
        TCType::Tuple(items) => {
            let actuals: Vec<&TCType> = match actual {
                TCType::Tuple(actuals) if actuals.len() == items.len() => actuals.iter().collect(),
                _ if gradual => vec![&TCType::Any; items.len()],
                _ => return None,
            };
            let checks: Vec<_> = items
                .iter()
                .zip(actuals)
                .map(|(item, actual)| nominal_check(item, actual))
                .collect();
            checks.iter().any(Option::is_some).then(|| {
                StreamType::Tuple(
                    checks
                        .into_iter()
                        .map(|check| check.unwrap_or(StreamType::Any))
                        .collect(),
                )
            })
        }
        TCType::Struct(fields, _) => {
            let checked: ecow::EcoVec<_> = fields
                .iter()
                .filter_map(|(name, field)| {
                    let actual = match actual {
                        TCType::Struct(actuals, _) => actuals
                            .iter()
                            .find(|(actual, _)| actual == name)
                            .map(|(_, typ)| typ)?,
                        _ if gradual => &TCType::Any,
                        _ => return None,
                    };
                    nominal_check(field, actual).map(|check| (name.clone(), check))
                })
                .collect();
            // The other fields are left to what checking proved of them.
            (!checked.is_empty()).then(|| StreamType::Struct(checked, true))
        }
        _ => None,
    }
}

/// The type checking required of each child of `node` that stands where a
/// value of that type is used.
fn expected_children<'a>(node: CheckedExprRef<'a>) -> Vec<(CheckedExprRef<'a>, TCType)> {
    use ExprView::*;
    let typ = node.typ();
    let same = |children: &[CheckedExprRef<'a>]| {
        children
            .iter()
            .map(|child| (*child, typ.clone()))
            .collect::<Vec<_>>()
    };
    match node.view() {
        If(_, then_value, else_value) => same(&[then_value, else_value]),
        Default(a, b) | Update(a, b) | Init(a, b) | LConcat(a, b) => same(&[a, b]),
        Latch(value, _) | SIndex(value, _) => same(&[value]),
        Ascribe(value, target) => vec![(value, TCType::from_stream_type(target))],
        List(items) => match typ {
            TCType::List(element) => items
                .into_iter()
                .map(|item| (item, (**element).clone()))
                .collect(),
            _ => Vec::new(),
        },
        LAppend(list, value) => {
            let mut children = same(&[list]);
            if let TCType::List(element) = typ {
                children.push((value, (**element).clone()));
            }
            children
        }
        Tuple(items) => match typ {
            TCType::Tuple(types) => items.into_iter().zip(types.iter().cloned()).collect(),
            _ => Vec::new(),
        },
        Struct(fields) | ObjectLiteral(fields) | Map(fields) => fields
            .iter()
            .filter_map(|(name, value)| {
                let expected = match typ {
                    TCType::Struct(types, _) => types
                        .iter()
                        .find(|(field, _)| field == name)
                        .map(|(_, typ)| typ.clone())?,
                    TCType::Map(value) => (**value).clone(),
                    _ => return None,
                };
                Some((value, expected))
            })
            .collect(),
        MInsert(_, key, value) => match typ {
            TCType::Map(element) => vec![(value, (**element).clone())],
            TCType::Struct(fields, _) => fields
                .iter()
                .find(|(field, _)| field == key)
                .map(|(_, field)| vec![(value, field.clone())])
                .unwrap_or_default(),
            _ => Vec::new(),
        },
        Apply(function, arguments) => match function.typ() {
            TCType::Function(parameters, _) => arguments
                .into_iter()
                .zip(parameters.iter().cloned())
                .collect(),
            _ => Vec::new(),
        },
        Constructor(payload, tag, _) => match (payload.into_iter().next(), typ) {
            (Some(payload), TCType::Union(schema)) => schema
                .alternative(tag)
                .and_then(|(_, alternative)| match alternative.payload() {
                    crate::core::UnionPayload::Of(payload_type) => {
                        Some(vec![(payload, payload_type.clone())])
                    }
                    crate::core::UnionPayload::Nullary => None,
                })
                .unwrap_or_default(),
            _ => Vec::new(),
        },
        Match(_, arms, shape) => {
            let children: Vec<_> = arms.into_iter().collect();
            let mut place = 0;
            let mut bodies = Vec::new();
            for arm in shape.iter() {
                if arm.guarded {
                    place += 1;
                }
                bodies.push((children[place], typ.clone()));
                place += 1;
            }
            bodies
        }
        _ => Vec::new(),
    }
}

/// Every boundary among the trees rooted at `roots`, each root with the type
/// its stream was declared to have, if any.
fn nominal_boundaries<'a>(
    roots: impl IntoIterator<Item = (CheckedExprRef<'a>, Option<TCType>)>,
) -> Boundaries {
    let mut boundaries = Boundaries::new();
    for (root, declared) in roots {
        if let Some(declared) = declared
            && let Some(target) = nominal_check(&declared, root.typ())
        {
            boundaries.insert(root.expr().id(), target);
        }
        for node in root.postorder() {
            for (child, expected) in expected_children(node) {
                if let Some(target) = nominal_check(&expected, child.typ()) {
                    boundaries.insert(child.expr().id(), target);
                }
            }
        }
    }
    boundaries
}

/// Rewrite every equation of `checked` into fresh storage, recording the type
/// of each emitted node in allocation order.
fn elaborate_tree(checked: &CheckedDsrvSpecification) -> CheckedDsrvSpecification {
    let spec = checked.unchecked();
    let node_count = spec.exprs.nodes().len();
    let mut builder = ExprBuilder::with_capacities(node_count, spec.exprs.len());
    let mut types: Vec<TCType> = Vec::with_capacity(node_count);
    let roots = || {
        spec.exprs.keys().map(|name| {
            checked
                .var_expr_ref(name)
                .expect("every equation of a checked specification is typed")
        })
    };
    // Only a specification that mentions a newtype has boundaries to find;
    // any other is rewritten as the identity without looking for them.
    let nominal = spec
        .type_annotations
        .values()
        .any(StreamType::mentions_newtype)
        || roots().any(|root| root.postorder().any(|node| mentions_newtype(node.typ())));
    let boundaries = if nominal {
        nominal_boundaries(spec.exprs.keys().zip(roots()).map(|(name, root)| {
            (
                root,
                spec.type_annotations
                    .get(name)
                    .map(TCType::from_stream_type),
            )
        }))
    } else {
        Boundaries::new()
    };
    let roots = builder
        .try_rewrite_forest(roots(), |node| {
            elaborate_node(node, &boundaries, &mut types)
        })
        .expect("elaboration keeps every node's children");
    let forest = builder
        .finish_forest(roots)
        .expect("elaboration emits one complete tree per equation");
    let exprs = ExprForestMap::new(spec.exprs.keys().cloned(), forest)
        .expect("elaboration keeps the sorted, unique equation names");

    let mut expr_types = exprs.annotations_builder();
    assert_eq!(
        types.len(),
        exprs.nodes().len(),
        "one type per emitted node"
    );
    for (node, typ) in exprs.nodes().zip(types) {
        expr_types
            .insert(node, typ)
            .expect("an emitted node belongs to the elaborated forest");
    }
    let expr_types = expr_types
        .finish()
        .expect("elaboration types every node it emits");

    let mut elaborated = DsrvSpecification::from_expression_forest_with_entries(
        spec.input_vars.clone(),
        spec.output_vars.clone(),
        exprs,
        spec.type_annotations.clone(),
        spec.aux_vars.iter().cloned(),
        spec.declarations.clone(),
    );
    elaborated.source_context = spec.source_context.clone();
    elaborated.sources = AstShared::clone(&spec.sources);
    CheckedDsrvSpecification::new(elaborated, expr_types, checked.check_mode())
}

impl Display for ElaboratedDsrvSpecification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.source, formatter)
    }
}

impl Specification for ElaboratedDsrvSpecification {
    type Expr = CheckedExpr;

    fn first_unsupported_construct(
        &self,
        supported: RuntimeCapabilities,
    ) -> Option<RuntimeCapabilityRequirement> {
        Specification::first_unsupported_construct(&self.elaborated, supported)
    }

    fn input_vars(&self) -> BTreeSet<VarName> {
        self.elaborated.input_vars().clone()
    }
    fn input_vars_in_order(&self) -> Vec<VarName> {
        self.elaborated.input_vars_in_order()
    }
    fn output_vars(&self) -> BTreeSet<VarName> {
        self.elaborated.output_vars().clone()
    }
    fn output_vars_in_order(&self) -> Vec<VarName> {
        self.elaborated.output_vars_in_order()
    }
    fn aux_vars(&self) -> BTreeSet<VarName> {
        self.elaborated.aux_vars().clone()
    }
    fn aux_vars_in_order(&self) -> Vec<VarName> {
        self.elaborated.aux_vars_in_order()
    }
    fn stream_vars(&self) -> BTreeSet<VarName> {
        self.elaborated.stream_vars().clone()
    }
    fn stream_vars_in_order(&self) -> Vec<VarName> {
        self.elaborated.stream_vars_in_order()
    }
    fn var_expr(&self, var: &VarName) -> Option<CheckedExpr> {
        self.elaborated.var_expr(var)
    }
    fn type_annotations(&self) -> BTreeMap<VarName, StreamType> {
        self.elaborated.type_annotations().clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::dsrv_fixtures::WithoutWarnings;
    use contiguous_tree::TreeCursorExt;

    use super::*;
    use crate::lang::dsrv::ast::AstShared;

    const SOURCE: &str = "type Pair = Struct<a: Int, b: Int>\n\
        in x: Int\n\
        in p: Pair\n\
        out y: Int\n\
        out z\n\
        aux w: Int\n\
        y = if x > 0 then p.a + w[1] else default(x[1], 0)\n\
        z = List.map(\\v -> v * 2, [x, p.b])\n\
        w = x + 1";

    #[test]
    fn elaboration_keeps_every_node_with_its_span_context_and_type() {
        for options in [TypeCheckOptions::STRICT, TypeCheckOptions::GRADUAL] {
            let source = SOURCE.replace("out z\n", "out z: List<Int>\n");
            let checked = crate::dsrv_fixtures::checked_with(&source, options);
            let elaborated = checked.clone().elaborate();

            assert_eq!(elaborated.checked().unchecked(), checked.unchecked());
            for name in checked.unchecked().roots().map(|(name, _)| name) {
                let original = checked.var_expr_ref(name).unwrap();
                let rewritten = elaborated.var_expr_ref(name).unwrap();
                assert!(
                    !original.expr().shares_storage_with(rewritten.expr()),
                    "{name} is rewritten into fresh storage"
                );
                assert_eq!(original.postorder().count(), rewritten.postorder().count());
                for (before, after) in original.postorder().zip(rewritten.postorder()) {
                    assert!(before.kind().same_payload(after.kind()));
                    assert_eq!(before.expr().span(), after.expr().span());
                    let (before_context, after_context) = (
                        before.expr().metadata().context.as_ref().unwrap(),
                        after.expr().metadata().context.as_ref().unwrap(),
                    );
                    assert!(AstShared::ptr_eq(before_context, after_context));
                    assert_eq!(before.typ(), after.typ());
                }
            }
            assert!(AstShared::ptr_eq(
                elaborated.checked().unchecked().source_context(),
                checked.unchecked().source_context()
            ));
        }
    }

    #[test]
    fn printing_uses_the_source_level_specification() {
        let checked = crate::dsrv_fixtures::checked_with(SOURCE, TypeCheckOptions::GRADUAL);
        let printed = checked.to_string();
        assert_eq!(checked.elaborate().to_string(), printed);
    }

    #[test]
    fn only_a_checked_specification_is_elaborated() {
        let ill_typed = "out y: Bool\ny = 1".parse::<DsrvSpecification>().unwrap();
        assert!(
            ill_typed
                .clone()
                .check_and_elaborate(TypeCheckOptions::GRADUAL)
                .without_warnings()
                .is_err()
        );
        assert!(
            ill_typed
                .check_and_elaborate(TypeCheckOptions::STRICT)
                .without_warnings()
                .is_err()
        );
    }
}

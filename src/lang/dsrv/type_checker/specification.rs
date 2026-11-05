//! Dependency ordering and a single concrete checking visit per root.
use super::{
    StreamTypes,
    checking::check,
    diagnostics::{WarningCollector, missing_annotation},
    type_contexts::TypeContext,
};
use crate::{
    VarName,
    lang::dsrv::{
        ast::{DsrvSpecification, ExprRef, ExprTypes},
        diagnostics::SemanticError,
    },
};
use contiguous_tree::TreeCursorExt;
use std::collections::BTreeSet;

pub(super) fn check_streams(
    spec: &DsrvSpecification,
    environment: &mut StreamTypes,
    warnings: &mut WarningCollector,
) -> Result<ExprTypes, Vec<SemanticError>> {
    let mut errors = Vec::new();
    let mut failed = BTreeSet::new();
    for input in &spec.input_vars {
        if !environment.contains_key(input) {
            errors.push(missing_annotation(spec, input));
            failed.insert(input.clone());
        }
    }
    let mut pending: Vec<_> = spec
        .roots()
        .map(|(name, expr)| {
            let reads = expr.free_variables();
            let dependencies = reads
                .iter()
                .filter(|read| spec.exprs.get(read).is_some())
                .cloned()
                .collect();
            PendingRoot {
                name: name.clone(),
                expr,
                reads,
                dependencies,
            }
        })
        .collect();
    let mut succeeded = BTreeSet::new();
    let mut annotations = spec.exprs.annotations_builder();
    while !pending.is_empty() {
        // Propagate failures before choosing another root, including through
        // annotated streams: an annotation does not prove its equation valid.
        let before = pending.len();
        pending.retain(|root| {
            if root.reads.iter().any(|read| failed.contains(read)) {
                failed.insert(root.name.clone());
                false
            } else {
                true
            }
        });
        if pending.len() != before {
            continue;
        }
        let ready = pending
            .iter()
            .position(|root| {
                root.dependencies
                    .iter()
                    .all(|read| succeeded.contains(read))
            })
            .or_else(|| {
                // Only a blocked cycle may start from declared dependency types.
                // Check one root, then resume priority for successful dependencies.
                pending.iter().position(|root| {
                    root.dependencies
                        .iter()
                        .all(|read| environment.contains_key(read))
                })
            });
        let Some(ready) = ready else {
            errors.extend(
                pending
                    .drain(..)
                    .map(|root| missing_annotation(spec, &root.name)),
            );
            break;
        };
        let root = pending.remove(ready);
        let mut context = TypeContext::new(environment, warnings);
        match check(root.expr, environment.get(&root.name), &mut context) {
            Ok(typ) => {
                for node in root.expr.postorder() {
                    annotations
                        .insert(node, context.expr_types[&node.id()].clone())
                        .expect("root annotation belongs to forest");
                }
                drop(context);
                environment.entry(root.name.clone()).or_insert(typ);
                succeeded.insert(root.name);
            }
            Err(error) => {
                errors.push(error);
                failed.insert(root.name);
            }
        }
    }
    if errors.is_empty() {
        Ok(annotations
            .finish()
            .expect("every successful root records all nodes"))
    } else {
        Err(errors)
    }
}

/// Cache source reads once; scheduling only examines stream names thereafter.
struct PendingRoot<'arena> {
    name: VarName,
    expr: ExprRef<'arena>,
    reads: BTreeSet<VarName>,
    dependencies: Vec<VarName>,
}

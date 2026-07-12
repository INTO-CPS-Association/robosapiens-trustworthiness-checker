use static_assertions::assert_obj_safe;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Debug;

use tracing::debug;

use crate::lang::dsrv::ast::{SpannedExpr, UntypedDsrvSpecification};

use crate::distributed::distribution_graphs::{GenericLabelledDistributionGraph, NodeName};
use crate::{SExpr, Specification, VarName};

pub trait LocalitySpec: Debug {
    fn local_vars(&self) -> Vec<VarName>;
}

assert_obj_safe!(LocalitySpec);

impl LocalitySpec for Vec<VarName> {
    fn local_vars(&self) -> Vec<VarName> {
        self.clone()
    }
}
impl<W: Debug> LocalitySpec for (NodeName, &GenericLabelledDistributionGraph<W>) {
    /// Returns the local variables of the node.
    /// Panics if the node does not exist in the graph.
    fn local_vars(&self) -> Vec<VarName> {
        let node_index = self.1.get_node_index_by_name(&self.0).unwrap();
        self.1
            .monitors_at_node(node_index)
            .unwrap_or_else(|| panic!("Node index {:?} does not exist in the graph", node_index))
            .clone()
    }
}
impl<W: Debug + Clone> LocalitySpec for (NodeName, GenericLabelledDistributionGraph<W>) {
    fn local_vars(&self) -> Vec<VarName> {
        (self.0.clone(), &self.1).local_vars()
    }
}

impl LocalitySpec for Box<dyn LocalitySpec> {
    fn local_vars(&self) -> Vec<VarName> {
        self.as_ref().local_vars()
    }
}

pub trait Localisable {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self;
}

#[cfg_attr(not(test), allow(dead_code))]
fn replace_var(var: &VarName, var_expr: &SpannedExpr, repl_expr: &SpannedExpr) -> SpannedExpr {
    // Replaces all occurrences of var in the repl_expr with var_expr
    if matches!(&repl_expr.node, SExpr::Var(v) if v == var) {
        return var_expr.clone();
    }

    let node = match &repl_expr.node {
        SExpr::Var(_) => repl_expr.node.clone(),
        SExpr::BinOp(lhs, rhs, op) => SExpr::BinOp(
            Box::new(replace_var(var, var_expr, lhs)),
            Box::new(replace_var(var, var_expr, rhs)),
            op.clone(),
        ),
        SExpr::If(sexpr, sexpr1, sexpr2) => SExpr::If(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
            Box::new(replace_var(var, var_expr, sexpr2)),
        ),
        SExpr::SIndex(sexpr, idx) => {
            SExpr::SIndex(Box::new(replace_var(var, var_expr, sexpr)), *idx)
        }
        SExpr::Val(value) => SExpr::Val(value.clone()),
        SExpr::Dynamic(runtime) => {
            let mut runtime = runtime.clone();
            runtime.source = Box::new(replace_var(var, var_expr, &runtime.source));
            SExpr::Dynamic(runtime)
        }
        SExpr::Defer(runtime) => {
            let mut runtime = runtime.clone();
            runtime.source = Box::new(replace_var(var, var_expr, &runtime.source));
            SExpr::Defer(runtime)
        }
        SExpr::Update(sexpr, sexpr1) => SExpr::Update(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::Default(sexpr, sexpr1) => SExpr::Default(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::IsDefined(sexpr) => SExpr::IsDefined(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::When(sexpr) => SExpr::When(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Latch(sexpr, sexpr1) => SExpr::Latch(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::Init(sexpr, sexpr1) => SExpr::Init(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::Not(sexpr) => SExpr::Not(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Lambda(params, body) => {
            SExpr::Lambda(params.clone(), Box::new(replace_var(var, var_expr, body)))
        }
        SExpr::Apply(func, args) => SExpr::Apply(
            Box::new(replace_var(var, var_expr, func)),
            args.iter()
                .map(|arg| replace_var(var, var_expr, arg))
                .collect(),
        ),
        SExpr::Fix(func) => SExpr::Fix(Box::new(replace_var(var, var_expr, func))),
        SExpr::Partial(func, args) => SExpr::Partial(
            Box::new(replace_var(var, var_expr, func)),
            args.iter()
                .map(|arg| replace_var(var, var_expr, arg))
                .collect(),
        ),
        SExpr::List(eco_vec) => {
            let vec = eco_vec
                .iter()
                .map(|e| replace_var(var, var_expr, e))
                .collect();
            SExpr::List(vec)
        }
        SExpr::Tuple(eco_vec) => {
            let vec = eco_vec
                .iter()
                .map(|e| replace_var(var, var_expr, e))
                .collect();
            SExpr::Tuple(vec)
        }
        SExpr::LIndex(sexpr, sexpr1) => SExpr::LIndex(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::LAppend(sexpr, sexpr1) => SExpr::LAppend(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::LConcat(sexpr, sexpr1) => SExpr::LConcat(
            Box::new(replace_var(var, var_expr, sexpr)),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::LHead(sexpr) => SExpr::LHead(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::LTail(sexpr) => SExpr::LTail(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::LLen(sexpr) => SExpr::LLen(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::LMap(func, list) => SExpr::LMap(
            Box::new(replace_var(var, var_expr, func)),
            Box::new(replace_var(var, var_expr, list)),
        ),
        SExpr::LFilter(func, list) => SExpr::LFilter(
            Box::new(replace_var(var, var_expr, func)),
            Box::new(replace_var(var, var_expr, list)),
        ),
        SExpr::LFold(func, init, list) => SExpr::LFold(
            Box::new(replace_var(var, var_expr, func)),
            Box::new(replace_var(var, var_expr, init)),
            Box::new(replace_var(var, var_expr, list)),
        ),
        SExpr::Map(btree_map) => SExpr::Map(
            btree_map
                .iter()
                .map(|(k, v)| (k.clone(), replace_var(var, var_expr, v)))
                .collect(),
        ),
        SExpr::Struct(btree_map) => SExpr::Struct(
            btree_map
                .iter()
                .map(|(k, v)| (k.clone(), replace_var(var, var_expr, v)))
                .collect(),
        ),
        SExpr::ObjectLiteral(btree_map) => SExpr::ObjectLiteral(
            btree_map
                .iter()
                .map(|(k, v)| (k.clone(), replace_var(var, var_expr, v)))
                .collect(),
        ),
        SExpr::MGet(sexpr, eco_string) => SExpr::MGet(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
        ),
        SExpr::SGet(sexpr, eco_string) => SExpr::SGet(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
        ),
        SExpr::MInsert(sexpr, eco_string, sexpr1) => SExpr::MInsert(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
            Box::new(replace_var(var, var_expr, sexpr1)),
        ),
        SExpr::MRemove(sexpr, eco_string) => SExpr::MRemove(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
        ),
        SExpr::MHasKey(sexpr, eco_string) => SExpr::MHasKey(
            Box::new(replace_var(var, var_expr, sexpr)),
            eco_string.clone(),
        ),
        SExpr::Sin(sexpr) => SExpr::Sin(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Cos(sexpr) => SExpr::Cos(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Tan(sexpr) => SExpr::Tan(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::Abs(sexpr) => SExpr::Abs(Box::new(replace_var(var, var_expr, sexpr))),
        SExpr::MonitoredAt(var_name, node_name) => {
            if var_name == var {
                panic!("Localisation of monitored_at expression with aux variable not allowed")
            } else {
                SExpr::MonitoredAt(var_name.clone(), node_name.clone())
            }
        }
        SExpr::Dist(_, _) => {
            unimplemented!("Dist currently unsupported")
        }
    };

    SpannedExpr {
        node,
        span: repl_expr.span,
    }
}

fn expand_aux_var(
    aux: &VarName,
    aux_defs: &BTreeMap<VarName, SpannedExpr>,
    expanded_aux_defs: &mut BTreeMap<VarName, SpannedExpr>,
    visiting: &mut BTreeSet<VarName>,
) -> SpannedExpr {
    if let Some(expr) = expanded_aux_defs.get(aux) {
        return expr.clone();
    }

    if !visiting.insert(aux.clone()) {
        panic!(
            "Recursive/cyclic aux definition detected while localising at aux variable {:?}",
            aux
        );
    }

    let aux_expr = aux_defs.get(aux).unwrap_or_else(|| {
        panic!(
            "Aux variable {:?} does not have a definition in the expressions",
            aux
        )
    });
    let expanded = replace_aux_refs(aux_expr, aux_defs, expanded_aux_defs, visiting);
    visiting.remove(aux);
    expanded_aux_defs.insert(aux.clone(), expanded.clone());

    expanded
}

fn replace_aux_refs(
    repl_expr: &SpannedExpr,
    aux_defs: &BTreeMap<VarName, SpannedExpr>,
    expanded_aux_defs: &mut BTreeMap<VarName, SpannedExpr>,
    visiting: &mut BTreeSet<VarName>,
) -> SpannedExpr {
    let node = match &repl_expr.node {
        SExpr::Var(v) if aux_defs.contains_key(v) => {
            return expand_aux_var(v, aux_defs, expanded_aux_defs, visiting);
        }
        SExpr::Var(_) => repl_expr.node.clone(),
        SExpr::BinOp(lhs, rhs, op) => SExpr::BinOp(
            Box::new(replace_aux_refs(lhs, aux_defs, expanded_aux_defs, visiting)),
            Box::new(replace_aux_refs(rhs, aux_defs, expanded_aux_defs, visiting)),
            op.clone(),
        ),
        SExpr::If(sexpr, sexpr1, sexpr2) => SExpr::If(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr2,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::SIndex(sexpr, idx) => SExpr::SIndex(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            *idx,
        ),
        SExpr::Val(value) => SExpr::Val(value.clone()),
        SExpr::Dynamic(runtime) => {
            let mut runtime = runtime.clone();
            runtime.source = Box::new(replace_aux_refs(
                &runtime.source,
                aux_defs,
                expanded_aux_defs,
                visiting,
            ));
            SExpr::Dynamic(runtime)
        }
        SExpr::Defer(runtime) => {
            let mut runtime = runtime.clone();
            runtime.source = Box::new(replace_aux_refs(
                &runtime.source,
                aux_defs,
                expanded_aux_defs,
                visiting,
            ));
            SExpr::Defer(runtime)
        }
        SExpr::Update(sexpr, sexpr1) => SExpr::Update(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::Default(sexpr, sexpr1) => SExpr::Default(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::IsDefined(sexpr) => SExpr::IsDefined(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::When(sexpr) => SExpr::When(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::Latch(sexpr, sexpr1) => SExpr::Latch(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::Init(sexpr, sexpr1) => SExpr::Init(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::Not(sexpr) => SExpr::Not(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::Lambda(params, body) => SExpr::Lambda(
            params.clone(),
            Box::new(replace_aux_refs(
                body,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::Apply(func, args) => SExpr::Apply(
            Box::new(replace_aux_refs(
                func,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            args.iter()
                .map(|arg| replace_aux_refs(arg, aux_defs, expanded_aux_defs, visiting))
                .collect(),
        ),
        SExpr::Fix(func) => SExpr::Fix(Box::new(replace_aux_refs(
            func,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::Partial(func, args) => SExpr::Partial(
            Box::new(replace_aux_refs(
                func,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            args.iter()
                .map(|arg| replace_aux_refs(arg, aux_defs, expanded_aux_defs, visiting))
                .collect(),
        ),
        SExpr::List(eco_vec) => SExpr::List(
            eco_vec
                .iter()
                .map(|e| replace_aux_refs(e, aux_defs, expanded_aux_defs, visiting))
                .collect(),
        ),
        SExpr::Tuple(eco_vec) => SExpr::Tuple(
            eco_vec
                .iter()
                .map(|e| replace_aux_refs(e, aux_defs, expanded_aux_defs, visiting))
                .collect(),
        ),
        SExpr::LIndex(sexpr, sexpr1) => SExpr::LIndex(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::LAppend(sexpr, sexpr1) => SExpr::LAppend(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::LConcat(sexpr, sexpr1) => SExpr::LConcat(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::LHead(sexpr) => SExpr::LHead(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::LTail(sexpr) => SExpr::LTail(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::LLen(sexpr) => SExpr::LLen(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::LMap(func, list) => SExpr::LMap(
            Box::new(replace_aux_refs(
                func,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                list,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::LFilter(func, list) => SExpr::LFilter(
            Box::new(replace_aux_refs(
                func,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                list,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::LFold(func, init, list) => SExpr::LFold(
            Box::new(replace_aux_refs(
                func,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                init,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            Box::new(replace_aux_refs(
                list,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::Map(btree_map) => SExpr::Map(
            btree_map
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        replace_aux_refs(v, aux_defs, expanded_aux_defs, visiting),
                    )
                })
                .collect(),
        ),
        SExpr::Struct(btree_map) => SExpr::Struct(
            btree_map
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        replace_aux_refs(v, aux_defs, expanded_aux_defs, visiting),
                    )
                })
                .collect(),
        ),
        SExpr::ObjectLiteral(btree_map) => SExpr::ObjectLiteral(
            btree_map
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        replace_aux_refs(v, aux_defs, expanded_aux_defs, visiting),
                    )
                })
                .collect(),
        ),
        SExpr::MGet(sexpr, eco_string) => SExpr::MGet(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            eco_string.clone(),
        ),
        SExpr::SGet(sexpr, eco_string) => SExpr::SGet(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            eco_string.clone(),
        ),
        SExpr::MInsert(sexpr, eco_string, sexpr1) => SExpr::MInsert(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            eco_string.clone(),
            Box::new(replace_aux_refs(
                sexpr1,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
        ),
        SExpr::MRemove(sexpr, eco_string) => SExpr::MRemove(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            eco_string.clone(),
        ),
        SExpr::MHasKey(sexpr, eco_string) => SExpr::MHasKey(
            Box::new(replace_aux_refs(
                sexpr,
                aux_defs,
                expanded_aux_defs,
                visiting,
            )),
            eco_string.clone(),
        ),
        SExpr::Sin(sexpr) => SExpr::Sin(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::Cos(sexpr) => SExpr::Cos(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::Tan(sexpr) => SExpr::Tan(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::Abs(sexpr) => SExpr::Abs(Box::new(replace_aux_refs(
            sexpr,
            aux_defs,
            expanded_aux_defs,
            visiting,
        ))),
        SExpr::MonitoredAt(var_name, node_name) => {
            if aux_defs.contains_key(var_name) {
                panic!("Localisation of monitored_at expression with aux variable not allowed")
            } else {
                SExpr::MonitoredAt(var_name.clone(), node_name.clone())
            }
        }
        SExpr::Dist(_, _) => {
            unimplemented!("Dist currently unsupported")
        }
    };

    SpannedExpr {
        node,
        span: repl_expr.span,
    }
}

fn dependency_closure(
    spec: &UntypedDsrvSpecification,
    roots: impl IntoIterator<Item = VarName>,
) -> BTreeSet<VarName> {
    let mut reachable = BTreeSet::new();
    let mut pending = roots.into_iter().collect::<Vec<_>>();

    while let Some(var) = pending.pop() {
        if !reachable.insert(var.clone()) {
            continue;
        }

        if let Some(expr) = spec.exprs.get(&var) {
            pending.extend(expr.inputs());
        }
    }

    reachable
}

fn prune_to_dependency_closure(
    mut spec: UntypedDsrvSpecification,
    roots: &[VarName],
) -> UntypedDsrvSpecification {
    let reachable = dependency_closure(&spec, roots.iter().cloned());
    let root_set = roots.iter().cloned().collect::<BTreeSet<_>>();

    spec.output_vars.retain(|v| root_set.contains(v));
    spec.aux_vars.retain(|v| reachable.contains(v));
    spec.exprs.retain(|v, _| reachable.contains(v));
    spec.type_annotations.retain(|v, _| reachable.contains(v));

    spec
}

fn inline_aux(spec: UntypedDsrvSpecification) -> UntypedDsrvSpecification {
    // Inline auxiliary variables transitively, while rejecting recursive/cyclic definitions.
    let aux_vars: BTreeSet<VarName> = spec.aux_vars();

    // Build aux definition map and ensure every aux has a definition.
    let aux_defs: BTreeMap<VarName, SpannedExpr> = aux_vars
        .iter()
        .map(|aux| {
            let aux_expr = spec.exprs.get(aux).unwrap_or_else(|| {
                panic!(
                    "Aux variable {:?} does not have a definition in the expressions",
                    aux
                )
            });
            (aux.clone(), aux_expr.clone())
        })
        .collect();

    let mut expanded_aux_defs = BTreeMap::new();
    for aux in &aux_vars {
        expand_aux_var(aux, &aux_defs, &mut expanded_aux_defs, &mut BTreeSet::new());
    }

    let mut visiting = BTreeSet::new();
    let replaced_exprs = spec
        .exprs
        .into_iter()
        .map(|(name, repl_expr)| {
            (
                name,
                replace_aux_refs(&repl_expr, &aux_defs, &mut expanded_aux_defs, &mut visiting),
            )
        })
        .collect::<BTreeMap<_, _>>();

    // Remove aux declarations/definitions from final spec and rebuild via constructor.
    let filtered_exprs: BTreeMap<VarName, SpannedExpr> = replaced_exprs
        .into_iter()
        .filter(|(name, _)| !aux_vars.contains(name))
        .collect();

    let filtered_output_vars: BTreeSet<VarName> = spec
        .output_vars
        .into_iter()
        .filter(|v| !aux_vars.contains(v))
        .collect();

    let filtered_type_annotations: BTreeMap<VarName, crate::core::StreamType> = spec
        .type_annotations
        .into_iter()
        .filter(|(name, _)| !aux_vars.contains(name))
        .collect();

    UntypedDsrvSpecification::new(
        spec.input_vars,
        filtered_output_vars,
        filtered_exprs,
        filtered_type_annotations,
        vec![],
    )
}

impl Localisable for UntypedDsrvSpecification {
    fn localise(&self, locality_spec: &impl LocalitySpec) -> Self {
        let local_vars = locality_spec.local_vars();
        let spec = inline_aux(prune_to_dependency_closure(self.clone(), &local_vars));
        let mut exprs = spec.exprs.clone();
        let mut output_vars = spec.output_vars.clone();
        let mut aux_vars = spec.aux_vars.clone();
        let input_vars = spec.input_vars.clone();

        let mut to_remove = vec![];
        for v in output_vars.iter() {
            if !local_vars.contains(v) {
                to_remove.push(v.clone());
            }
        }
        for v in exprs.keys() {
            if !local_vars.contains(v) {
                to_remove.push(v.clone());
            }
        }
        output_vars.retain(|v| local_vars.contains(v));
        aux_vars.retain(|v| local_vars.contains(v));
        exprs.retain(|v, _| local_vars.contains(v));
        let expr_input_vars: HashSet<_> = exprs.values().flat_map(|e| e.inputs()).collect();
        debug!("Expr input vars: {:?}", expr_input_vars);
        // We keep the order from the original input vars,
        // but remove variable that are not needed locally
        let new_input_vars: BTreeSet<_> = input_vars
            .iter()
            .cloned()
            .chain(to_remove)
            .filter(|v| expr_input_vars.contains(v))
            .collect();
        debug!("Old input vars: {:?}", input_vars);
        debug!("New input vars: {:?}", new_input_vars);

        UntypedDsrvSpecification::new(
            new_input_vars,
            output_vars,
            exprs,
            spec.type_annotations.clone(),
            aux_vars,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::vec;

    use crate::dsrv_fixtures::spec_simple_add_decomposable;
    use crate::dsrv_specification;
    use crate::lang::dsrv::ast::SpannedExpr;
    use crate::lang::dsrv::span::strip_span;
    use proptest::prelude::*;
    use test_log::test;
    use winnow::Parser;

    use crate::lang::dsrv::ast::generation::arb_boolean_dsrv_spec;
    type SExpr = SpannedExpr;
    use super::*;

    fn assert_specs_eq_ignoring_spans(
        actual: &UntypedDsrvSpecification,
        expected: &UntypedDsrvSpecification,
    ) {
        assert_eq!(actual.input_vars, expected.input_vars);
        assert_eq!(actual.output_vars, expected.output_vars);
        assert_eq!(actual.aux_vars, expected.aux_vars);
        assert_eq!(actual.stream_vars, expected.stream_vars);
        assert_eq!(actual.type_annotations, expected.type_annotations);

        let actual_exprs = actual
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span(expr)))
            .collect::<BTreeMap<_, _>>();
        let expected_exprs = expected
            .exprs
            .iter()
            .map(|(name, expr)| (name.clone(), strip_span(expr)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual_exprs, expected_exprs);
    }

    #[test]
    fn test_localise_specification_1() {
        let spec = UntypedDsrvSpecification::new(
            BTreeSet::from(["a".into(), "b".into()]),
            BTreeSet::from(["c".into(), "d".into(), "e".into()]),
            vec![
                ("c".into(), SExpr::Var("a".into())),
                ("d".into(), SExpr::Not(Box::new(SExpr::Var("a".into())))),
                ("e".into(), SExpr::Not(Box::new(SExpr::Var("d".into())))),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![],
        );
        let restricted_vars = vec!["c".into(), "e".into()];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            UntypedDsrvSpecification::new(
                BTreeSet::from(["a".into(), "d".into()]),
                BTreeSet::from(["c".into(), "e".into()]),
                vec![
                    ("c".into(), SExpr::Var("a".into())),
                    ("e".into(), SExpr::Not(Box::new(SExpr::Var("d".into())))),
                ]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            )
        )
    }

    #[test]
    fn test_localise_specification_2() {
        let spec = UntypedDsrvSpecification::new(
            BTreeSet::from(["a".into()]),
            BTreeSet::from(["i".into()]),
            BTreeMap::<VarName, SExpr>::new(),
            BTreeMap::new(),
            vec![],
        );
        let restricted_vars = vec![];
        let localised_spec = spec.localise(&restricted_vars);
        assert_eq!(
            localised_spec,
            UntypedDsrvSpecification::new(
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeMap::<VarName, SExpr>::new(),
                BTreeMap::new(),
                vec![],
            )
        )
    }

    #[test]
    fn test_localise_specification_simple_add() {
        let spec = dsrv_specification
            .parse(spec_simple_add_decomposable())
            .expect("Failed to parse specification");

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_specs_eq_ignoring_spans(
            &local_spec1,
            &UntypedDsrvSpecification::new(
                BTreeSet::from(["x".into(), "y".into()]),
                BTreeSet::from(["w".into()]),
                vec![(
                    "w".into(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var("x".into())),
                        Box::new(SExpr::Var("y".into())),
                        "+".into(),
                    ),
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );

        assert_specs_eq_ignoring_spans(
            &local_spec2,
            &UntypedDsrvSpecification::new(
                BTreeSet::from(["z".into(), "w".into()]),
                BTreeSet::from(["v".into()]),
                vec![(
                    "v".into(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var("z".into())),
                        Box::new(SExpr::Var("w".into())),
                        "+".into(),
                    ),
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );
    }

    #[test]
    fn test_localise_spec_with_aux() {
        // Tests that localisation correctly handles auxiliary variables
        // Note that these must be specified similarly to output variables
        let spec = dsrv_specification
            .parse(
                "   in x
                    in y
                    in z
                    out w
                    out v
                    aux tmp
                    w = x + y
                    tmp = z + w
                    v = tmp",
            )
            .expect("Failed to parse specification");

        let local_spec1 = spec.localise(&vec!["w".into()]);
        let local_spec2 = spec.localise(&vec!["v".into()]);

        assert_specs_eq_ignoring_spans(
            &local_spec1,
            &UntypedDsrvSpecification::new(
                BTreeSet::from(["x".into(), "y".into()]),
                BTreeSet::from(["w".into()]),
                vec![(
                    "w".into(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var("x".into())),
                        Box::new(SExpr::Var("y".into())),
                        "+".into(),
                    ),
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );

        assert_specs_eq_ignoring_spans(
            &local_spec2,
            &UntypedDsrvSpecification::new(
                BTreeSet::from(["z".into(), "w".into()]),
                BTreeSet::from(["v".into()]),
                vec![(
                    "v".into(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var("z".into())),
                        Box::new(SExpr::Var("w".into())),
                        "+".into(),
                    ),
                )]
                .into_iter()
                .collect(),
                BTreeMap::new(),
                vec![],
            ),
        );
    }

    #[test]
    fn test_replace_var_simple() {
        let x: VarName = "x".into();
        let expr = SExpr::Var(x.clone());
        let replacement = SExpr::Val(42);

        let result = replace_var(&x, &replacement, &expr);

        assert_eq!(result, replacement);
    }

    #[test]
    fn test_replace_var_nested() {
        let x: VarName = "x".into();
        let y: VarName = "y".into();

        let expr = SExpr::BinOp(
            Box::new(SExpr::Var(x.clone())),
            Box::new(SExpr::BinOp(
                Box::new(SExpr::Var(y.clone())),
                Box::new(SExpr::Var(x.clone())),
                "+".into(),
            )),
            "*".into(),
        );

        let replacement = SExpr::Val(1);

        let result = replace_var(&x, &replacement, &expr);

        let expected = SExpr::BinOp(
            Box::new(SExpr::Val(1)),
            Box::new(SExpr::BinOp(
                Box::new(SExpr::Var(y)),
                Box::new(SExpr::Val(1)),
                "+".into(),
            )),
            "*".into(),
        );

        assert_eq!(result, expected);
    }

    #[test]
    fn test_inline_aux_single_aux() {
        let x: VarName = "x".into();
        let y: VarName = "y".into();
        let tmp: VarName = "tmp".into();
        let z: VarName = "z".into();

        let spec = UntypedDsrvSpecification::new(
            BTreeSet::from(["x".into(), "y".into()]),
            BTreeSet::from([tmp.clone(), z.clone()]),
            vec![
                (
                    tmp.clone(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var(x.clone())),
                        Box::new(SExpr::Var(y.clone())),
                        "+".into(),
                    ),
                ),
                (
                    z.clone(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var(tmp.clone())),
                        Box::new(SExpr::Var(x.clone())),
                        "*".into(),
                    ),
                ),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![tmp.clone()],
        );

        let result = inline_aux(spec);

        let expected_exprs = vec![(
            z.clone(),
            SExpr::BinOp(
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Var(x.clone())),
                    Box::new(SExpr::Var(y.clone())),
                    "+".into(),
                )),
                Box::new(SExpr::Var(x.clone())),
                "*".into(),
            ),
        )]
        .into_iter()
        .collect();

        assert_eq!(
            result,
            UntypedDsrvSpecification::new(
                BTreeSet::from([x, y]),
                BTreeSet::from([z]),
                expected_exprs,
                BTreeMap::new(),
                vec![]
            )
        );
    }

    #[test]
    fn test_inline_aux_transitive_chain() {
        let i: VarName = "i".into();
        let h1: VarName = "h1".into();
        let h2: VarName = "h2".into();
        let h3: VarName = "h3".into();
        let out: VarName = "out".into();

        let spec = UntypedDsrvSpecification::new(
            BTreeSet::from([i.clone()]),
            BTreeSet::from([h1.clone(), h2.clone(), h3.clone(), out.clone()]),
            vec![
                (h1.clone(), SExpr::Var(i.clone())),
                (
                    h2.clone(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var(h1.clone())),
                        Box::new(SExpr::Val(1)),
                        "+".into(),
                    ),
                ),
                (
                    h3.clone(),
                    SExpr::BinOp(
                        Box::new(SExpr::Var(h2.clone())),
                        Box::new(SExpr::Val(2)),
                        "+".into(),
                    ),
                ),
                (out.clone(), SExpr::Var(h3.clone())),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![h1.clone(), h2.clone(), h3.clone()],
        );

        let result = inline_aux(spec);

        // Canonicalize expected to match shape generated by replace_var expansion:
        let expected_exprs = vec![(
            out.clone(),
            SExpr::BinOp(
                Box::new(SExpr::BinOp(
                    Box::new(SExpr::Var(i.clone())),
                    Box::new(SExpr::Val(1)),
                    "+".into(),
                )),
                Box::new(SExpr::Val(2)),
                "+".into(),
            ),
        )]
        .into_iter()
        .collect();

        assert_eq!(
            result,
            UntypedDsrvSpecification::new(
                BTreeSet::from([i]),
                BTreeSet::from([out]),
                expected_exprs,
                BTreeMap::new(),
                vec![]
            )
        );
    }

    #[test]
    #[should_panic(expected = "Recursive/cyclic aux definition detected")]
    fn test_inline_aux_cycle_panics() {
        let h1: VarName = "h1".into();
        let h2: VarName = "h2".into();
        let out: VarName = "out".into();

        let spec = UntypedDsrvSpecification::new(
            BTreeSet::new(),
            BTreeSet::from([h1.clone(), h2.clone(), out.clone()]),
            vec![
                (h1.clone(), SExpr::Var(h2.clone())),
                (h2.clone(), SExpr::Var(h1.clone())),
                (out.clone(), SExpr::Var(h1.clone())),
            ]
            .into_iter()
            .collect(),
            BTreeMap::new(),
            vec![h1, h2],
        );

        let _ = inline_aux(spec);
    }

    proptest! {
        #[test]
        fn test_localise_specification_prop(
            spec in arb_boolean_dsrv_spec(),
            restricted_vars in prop::collection::hash_set("[a-z]", 0..5)
        ) {
            let restricted_vars: Vec<VarName> = restricted_vars.into_iter().map(|s| s.into()).collect();
            let localised_spec = spec.localise(&restricted_vars);

            for var in localised_spec.output_vars.iter() {
                assert!(restricted_vars.contains(var));
            }
            for var in localised_spec.exprs.keys() {
                assert!(restricted_vars.contains(var));
            }
            for var in localised_spec.exprs.keys() {
                assert!(spec.exprs.contains_key(var));
            }
            for var in localised_spec.input_vars.iter() {
                assert!(spec.input_vars.contains(var)
                    || spec.output_vars.contains(var));
            }
        }
    }
}

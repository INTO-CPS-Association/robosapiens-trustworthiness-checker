use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use async_stream::stream;
use sat_solver::sat::cnf::Cnf;
use sat_solver::sat::literal::PackedLiteral;
use sat_solver::sat::solver::{Solver, SolverImpls};
use tracing::info;

use crate::{
    DsrvSpecification, Specification, Value, VarName,
    distributed::distribution_graphs::{
        DistributionGraph, LabelledDistGraphStream, LabelledDistributionGraph, NodeName,
    },
    lang::dsrv::{ast::SExpr, parser::dsrv_specification},
    semantics::{AsyncConfig, MonitoringSemantics, distributed::localisation::Localisable},
};

/// SAT-backed solver specialized for distribution constraints expressible
/// using `monitored_at(var, node)` and boolean connectives.
///
/// This solver intentionally does **not** support:
/// - `dist(...)`
/// - time-indexed expressions
///
/// If used with unsupported constraints/specs, it panics.
pub struct SatMonitoredAtDistConstraintSolver<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value>,
    AC::Spec: Localisable,
{
    pub dist_constraints: Vec<VarName>,
    pub output_vars: Vec<VarName>,
    pub parsed_spec: DsrvSpecification,
    pub dist_constraint_set: HashSet<VarName>,
    pub replay_history: Option<crate::io::replay_history::ReplayHistory>,
    _semantics: std::marker::PhantomData<S>,
    _config: std::marker::PhantomData<AC>,
}

impl<S, AC> SatMonitoredAtDistConstraintSolver<S, AC>
where
    S: MonitoringSemantics<AC>,
    AC: AsyncConfig<Val = Value>,
    AC::Spec: Localisable,
{
    pub fn new(
        dist_constraints: Vec<VarName>,
        output_vars: Vec<VarName>,
        spec_text: String,
        replay_history: Option<crate::io::replay_history::ReplayHistory>,
    ) -> Self {
        let mut spec_src = spec_text.as_str();
        let parsed_spec = dsrv_specification(&mut spec_src)
            .unwrap_or_else(|e| panic!("Failed to parse DSRV spec text for SAT solver: {e}"));
        let dist_constraint_set = dist_constraints.iter().cloned().collect::<HashSet<_>>();

        Self {
            dist_constraints,
            output_vars,
            parsed_spec,
            dist_constraint_set,
            replay_history,
            _semantics: std::marker::PhantomData,
            _config: std::marker::PhantomData,
        }
    }

    pub fn possible_labelled_dist_graph_stream(
        self: Rc<Self>,
        graph: Rc<DistributionGraph>,
    ) -> LabelledDistGraphStream {
        info!("Starting SAT monitored_at-only distribution solving");
        let spec = self.parsed_spec.localise(&self.dist_constraints);

        let mut state = CnfCompilerState::new(&graph);
        state.declared_dist_constraints = self.dist_constraint_set.clone();

        let snapshot = self.replay_history.as_ref().and_then(|h| h.snapshot());

        // Build replay bindings from the full (unlocalised) parsed spec first, then
        // project those bindings onto localised inputs. This mirrors brute-force flow
        // where replay values feed evaluation before/through localisation.
        let mut replay_bindings_full_spec = BTreeMap::<VarName, Value>::new();

        if let Some(snap) = &snapshot {
            for (_t, row) in snap {
                for (k, v) in row {
                    if !self.dist_constraint_set.contains(k) && *v != Value::NoVal {
                        replay_bindings_full_spec.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // Close replay bindings over helper vars that can be computed from other replay values.
        // We do this on the original parsed spec so helper vars that become inputs after
        // localisation can still be derived.
        let mut changed = true;
        while changed {
            changed = false;

            let full_spec_input_vars = self.parsed_spec.input_vars();
            for v in full_spec_input_vars {
                if replay_bindings_full_spec.contains_key(&v) {
                    continue;
                }
                if let Some(expr) = self.parsed_spec.var_expr(&v) {
                    let mut st_full = CnfCompilerState::new(&graph);
                    st_full.value_bindings = replay_bindings_full_spec.clone();
                    if let Some(val) = eval_const_expr(expr, &self.parsed_spec, &st_full) {
                        if val != Value::NoVal {
                            replay_bindings_full_spec.insert(v.clone(), val);
                            changed = true;
                        }
                    }
                }
            }

            for v in self.parsed_spec.output_vars() {
                if replay_bindings_full_spec.contains_key(&v) {
                    continue;
                }
                if let Some(expr) = self.parsed_spec.var_expr(&v) {
                    let mut st_full = CnfCompilerState::new(&graph);
                    st_full.value_bindings = replay_bindings_full_spec.clone();
                    if let Some(val) = eval_const_expr(expr, &self.parsed_spec, &st_full) {
                        if val != Value::NoVal {
                            replay_bindings_full_spec.insert(v.clone(), val);
                            changed = true;
                        }
                    }
                }
            }

            for v in self.parsed_spec.aux_vars() {
                if replay_bindings_full_spec.contains_key(&v) {
                    continue;
                }
                if let Some(expr) = self.parsed_spec.var_expr(&v) {
                    let mut st_full = CnfCompilerState::new(&graph);
                    st_full.value_bindings = replay_bindings_full_spec.clone();
                    if let Some(val) = eval_const_expr(expr, &self.parsed_spec, &st_full) {
                        if val != Value::NoVal {
                            replay_bindings_full_spec.insert(v.clone(), val);
                            changed = true;
                        }
                    }
                }
            }
        }

        // Keep only bindings needed by localised constraint evaluation inputs.
        let local_input_set = spec.input_vars.iter().cloned().collect::<HashSet<_>>();
        for (k, v) in replay_bindings_full_spec {
            if local_input_set.contains(&k) {
                state.value_bindings.insert(k, v);
            }
        }

        let mut top_lits = Vec::new();

        for c in &self.dist_constraints {
            let expr = spec.var_expr(c).unwrap_or_else(|| {
                panic!(
                    "Missing expression for distribution constraint variable `{}`",
                    c
                )
            });
            let lit = compile_expr_to_lit(expr, &spec, &mut state)
                .unwrap_or_else(|e| panic!("Failed to compile SAT constraint `{}`: {}", c, e));
            top_lits.push(lit);
        }

        // Enforce all top-level constraints to hold.
        for lit in top_lits {
            state.add_clause1(lit);
        }

        // Exactly-one-node placement for all streams appearing in monitored_at(...)
        state.emit_exactly_one_constraints();

        let sat_assignment = solve_with_sat_solver(&state).unwrap_or_else(|| {
            panic!("No valid assignment exists for given monitored_at constraints")
        });

        let labelled =
            build_labelled_graph_from_solution(&graph, &self.output_vars, &state, &sat_assignment);

        let labelled = Rc::new(labelled);

        Box::pin(stream! {
            yield labelled;
        })
    }
}

type Lit = i32;
type Clause = Vec<Lit>;
type RawCnf = Vec<Clause>;

#[derive(Default)]
struct CnfCompilerState {
    cnf: RawCnf,
    next_var_id: usize,
    // (stream, node) -> SAT var
    var_node_to_atom: BTreeMap<(VarName, NodeName), usize>,
    // SAT var -> (stream, node)
    atoms_to_var_node: BTreeMap<usize, (VarName, NodeName)>,
    // bound substitutions from replay history for non-constraint variables
    value_bindings: BTreeMap<VarName, Value>,
    // streams appearing in monitored_at(...)
    constrained_streams: BTreeSet<VarName>,
    // declared distribution-constraint variables passed to the solver
    declared_dist_constraints: HashSet<VarName>,
    // Graph node names
    nodes: Vec<NodeName>,
}

impl CnfCompilerState {
    fn new(graph: &DistributionGraph) -> Self {
        let nodes = graph.locations();
        Self {
            nodes,
            next_var_id: 1,
            ..Self::default()
        }
    }

    fn fresh_var(&mut self) -> usize {
        let id = self.next_var_id;
        self.next_var_id = self.next_var_id.saturating_add(1);
        id
    }

    fn atom_for_monitored_at(&mut self, v: VarName, n: NodeName) -> Lit {
        if !self.nodes.contains(&n) {
            panic!("Constraint references unknown node `{}` in monitored_at", n);
        }
        self.constrained_streams.insert(v.clone());
        let key = (v.clone(), n.clone());
        let id = if let Some(id) = self.var_node_to_atom.get(&key) {
            *id
        } else {
            let id = self.fresh_var();
            self.var_node_to_atom.insert(key.clone(), id);
            self.atoms_to_var_node.insert(id, key);
            id
        };
        i32::try_from(id).unwrap_or_else(|_| panic!("SAT variable id {} exceeds i32 range", id))
    }

    fn add_clause1(&mut self, a: Lit) {
        self.cnf.push(vec![a]);
    }

    fn add_clause2(&mut self, a: Lit, b: Lit) {
        self.cnf.push(vec![a, b]);
    }

    fn add_clause3(&mut self, a: Lit, b: Lit, c: Lit) {
        self.cnf.push(vec![a, b, c]);
    }

    // Use Tseitin operations to compile logical ops into CNF conjuncts
    fn tseitin_and(&mut self, a: Lit, b: Lit) -> Lit {
        let x = i32::try_from(self.fresh_var())
            .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
        // x <-> (a ∧ b)
        self.add_clause2(-x, a);
        self.add_clause2(-x, b);
        self.add_clause3(x, -a, -b);
        x
    }

    fn tseitin_or(&mut self, a: Lit, b: Lit) -> Lit {
        let x = i32::try_from(self.fresh_var())
            .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
        // x <-> (a ∨ b)
        self.add_clause2(-a, x);
        self.add_clause2(-b, x);
        self.add_clause3(-x, a, b);
        x
    }

    fn tseitin_impl(&mut self, a: Lit, b: Lit) -> Lit {
        // a => b == (!a ∨ b)
        self.tseitin_or(-a, b)
    }

    fn tseitin_eq(&mut self, a: Lit, b: Lit) -> Lit {
        // (a <-> b) == (a=>b) ∧ (b=>a)
        let i1 = self.tseitin_impl(a, b);
        let i2 = self.tseitin_impl(b, a);
        self.tseitin_and(i1, i2)
    }

    fn tseitin_ite(&mut self, c: Lit, t: Lit, e: Lit) -> Lit {
        let x = i32::try_from(self.fresh_var())
            .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
        // x <-> (c ? t : e)
        // (c -> (x <-> t)) ∧ (!c -> (x <-> e))
        self.add_clause3(-c, -x, t); // c -> (!x \/ t)
        self.add_clause3(-c, x, -t); // c -> (x \/ !t)
        self.add_clause3(c, -x, e); // !c -> (!x \/ e)
        self.add_clause3(c, x, -e); // !c -> (x \/ !e)
        x
    }

    fn emit_exactly_one_constraints(&mut self) {
        let streams: Vec<_> = self.constrained_streams.iter().cloned().collect();
        for s in streams {
            let mut atoms = Vec::with_capacity(self.nodes.len());
            for n in self.nodes.clone() {
                let lit = self.atom_for_monitored_at(s.clone(), n);
                atoms.push(lit);
            }

            // At least one
            self.cnf.push(atoms.clone());

            // At most one (pairwise)
            for i in 0..atoms.len() {
                for j in (i + 1)..atoms.len() {
                    self.add_clause2(-atoms[i], -atoms[j]);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum SatCompileError {
    UnsupportedExpr(String),
    UnknownConstraintVar(VarName),
}

impl std::fmt::Display for SatCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SatCompileError::UnsupportedExpr(msg) => write!(f, "{}", msg),
            SatCompileError::UnknownConstraintVar(v) => {
                write!(
                    f,
                    "Unknown variable `{}` encountered while compiling constraints",
                    v
                )
            }
        }
    }
}

impl std::error::Error for SatCompileError {}

fn eval_const_expr(
    expr: SExpr,
    spec: &crate::DsrvSpecification,
    st: &CnfCompilerState,
) -> Option<Value> {
    match expr {
        SExpr::Val(v) => Some(v),
        SExpr::Var(v) => {
            if let Some(bound) = st.value_bindings.get(&v) {
                return Some(bound.clone());
            }
            let inner = spec.var_expr(&v)?;
            eval_const_expr(inner, spec, st)
        }
        SExpr::List(items) => {
            let mut out = ecow::EcoVec::new();
            for item in items {
                out.push(eval_const_expr(item, spec, st)?);
            }
            Some(Value::List(out))
        }
        SExpr::LIndex(list, idx) => {
            let list_v = eval_const_expr(*list, spec, st)?;
            let idx_v = eval_const_expr(*idx, spec, st)?;
            match (list_v, idx_v) {
                (Value::List(l), Value::Int(i)) if i >= 0 => l.get(i as usize).cloned(),
                _ => None,
            }
        }
        SExpr::LAppend(list, el) => {
            let list_v = eval_const_expr(*list, spec, st)?;
            let el_v = eval_const_expr(*el, spec, st)?;
            match list_v {
                Value::List(mut l) => {
                    l.push(el_v);
                    Some(Value::List(l))
                }
                _ => None,
            }
        }
        SExpr::LConcat(list1, list2) => {
            let list1_v = eval_const_expr(*list1, spec, st)?;
            let list2_v = eval_const_expr(*list2, spec, st)?;
            match (list1_v, list2_v) {
                (Value::List(mut l1), Value::List(l2)) => {
                    l1.extend(l2);
                    Some(Value::List(l1))
                }
                _ => None,
            }
        }
        SExpr::LHead(list) => {
            let list_v = eval_const_expr(*list, spec, st)?;
            match list_v {
                Value::List(l) => l.first().cloned(),
                _ => None,
            }
        }
        SExpr::LTail(list) => {
            let list_v = eval_const_expr(*list, spec, st)?;
            match list_v {
                Value::List(l) => {
                    let tail = l.get(1..)?;
                    Some(Value::List(tail.into()))
                }
                _ => None,
            }
        }
        SExpr::LLen(list) => {
            let list_v = eval_const_expr(*list, spec, st)?;
            match list_v {
                Value::List(l) => Some(Value::Int(l.len() as i64)),
                _ => None,
            }
        }
        SExpr::Map(map) => {
            let mut out = BTreeMap::new();
            for (k, v) in map {
                out.insert(k, eval_const_expr(v, spec, st)?);
            }
            Some(Value::Map(out))
        }
        SExpr::MGet(map, k) => {
            let map_v = eval_const_expr(*map, spec, st)?;
            match map_v {
                Value::Map(m) => m.get(&k).cloned(),
                _ => None,
            }
        }
        SExpr::MRemove(map, k) => {
            let map_v = eval_const_expr(*map, spec, st)?;
            match map_v {
                Value::Map(mut m) => {
                    m.remove(&k);
                    Some(Value::Map(m))
                }
                _ => None,
            }
        }
        SExpr::MInsert(map, k, v) => {
            let map_v = eval_const_expr(*map, spec, st)?;
            let val_v = eval_const_expr(*v, spec, st)?;
            match map_v {
                Value::Map(mut m) => {
                    m.insert(k, val_v);
                    Some(Value::Map(m))
                }
                _ => None,
            }
        }
        SExpr::MHasKey(map, k) => {
            let map_v = eval_const_expr(*map, spec, st)?;
            match map_v {
                Value::Map(m) => Some(Value::Bool(m.contains_key(&k))),
                _ => None,
            }
        }
        SExpr::If(c, t, e) => {
            let c_v = eval_const_expr(*c, spec, st)?;
            match c_v {
                Value::Bool(true) => eval_const_expr(*t, spec, st),
                Value::Bool(false) => eval_const_expr(*e, spec, st),
                _ => None,
            }
        }
        SExpr::Not(e) => {
            let v = eval_const_expr(*e, spec, st)?;
            match v {
                Value::Bool(b) => Some(Value::Bool(!b)),
                _ => None,
            }
        }
        SExpr::BinOp(lhs, rhs, op) => {
            let l = eval_const_expr(*lhs, spec, st)?;
            let r = eval_const_expr(*rhs, spec, st)?;
            match (op, l, r) {
                (
                    crate::lang::dsrv::ast::SBinOp::BOp(crate::lang::dsrv::ast::BoolBinOp::And),
                    Value::Bool(a),
                    Value::Bool(b),
                ) => Some(Value::Bool(a && b)),
                (
                    crate::lang::dsrv::ast::SBinOp::BOp(crate::lang::dsrv::ast::BoolBinOp::Or),
                    Value::Bool(a),
                    Value::Bool(b),
                ) => Some(Value::Bool(a || b)),
                (
                    crate::lang::dsrv::ast::SBinOp::BOp(crate::lang::dsrv::ast::BoolBinOp::Impl),
                    Value::Bool(a),
                    Value::Bool(b),
                ) => Some(Value::Bool(!a || b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Eq),
                    a,
                    b,
                ) => Some(Value::Bool(a == b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Le),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Bool(a <= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Le),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Bool((a as f64) <= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Le),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Bool(a <= (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Le),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Bool(a <= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Le),
                    Value::Bool(a),
                    Value::Bool(b),
                ) => Some(Value::Bool(a <= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Le),
                    Value::Str(a),
                    Value::Str(b),
                ) => Some(Value::Bool(a <= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Lt),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Bool(a < b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Lt),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Bool((a as f64) < b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Lt),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Bool(a < (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Lt),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Bool(a < b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Lt),
                    Value::Bool(a),
                    Value::Bool(b),
                ) => Some(Value::Bool(!a & b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Lt),
                    Value::Str(a),
                    Value::Str(b),
                ) => Some(Value::Bool(a < b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Ge),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Bool(a >= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Ge),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Bool((a as f64) >= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Ge),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Bool(a >= (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Ge),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Bool(a >= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Ge),
                    Value::Bool(a),
                    Value::Bool(b),
                ) => Some(Value::Bool(a >= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Ge),
                    Value::Str(a),
                    Value::Str(b),
                ) => Some(Value::Bool(a >= b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Gt),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Bool(a > b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Gt),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Bool((a as f64) > b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Gt),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Bool(a > (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Gt),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Bool(a > b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Gt),
                    Value::Bool(a),
                    Value::Bool(b),
                ) => Some(Value::Bool(a & !b)),
                (
                    crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Gt),
                    Value::Str(a),
                    Value::Str(b),
                ) => Some(Value::Bool(a > b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Add,
                    ),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Int(a + b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Add,
                    ),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Float((a as f64) + b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Add,
                    ),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Float(a + (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Add,
                    ),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Float(a + b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Sub,
                    ),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Int(a - b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Sub,
                    ),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Float((a as f64) - b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Sub,
                    ),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Float(a - (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Sub,
                    ),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Float(a - b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Mul,
                    ),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Int(a * b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Mul,
                    ),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Float((a as f64) * b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Mul,
                    ),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Float(a * (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Mul,
                    ),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Float(a * b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Div,
                    ),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Int(a / b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Div,
                    ),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Float((a as f64) / b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Div,
                    ),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Float(a / (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Div,
                    ),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Float(a / b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Mod,
                    ),
                    Value::Int(a),
                    Value::Int(b),
                ) => Some(Value::Int(a % b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Mod,
                    ),
                    Value::Int(a),
                    Value::Float(b),
                ) => Some(Value::Float((a as f64) % b)),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Mod,
                    ),
                    Value::Float(a),
                    Value::Int(b),
                ) => Some(Value::Float(a % (b as f64))),
                (
                    crate::lang::dsrv::ast::SBinOp::NOp(
                        crate::lang::dsrv::ast::NumericalBinOp::Mod,
                    ),
                    Value::Float(a),
                    Value::Float(b),
                ) => Some(Value::Float(a % b)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn compile_expr_to_lit(
    expr: SExpr,
    spec: &crate::DsrvSpecification,
    st: &mut CnfCompilerState,
) -> Result<Lit, SatCompileError> {
    match expr {
        SExpr::MonitoredAt(v, n) => Ok(st.atom_for_monitored_at(v, n)),

        SExpr::Val(Value::Bool(true)) => {
            let x = i32::try_from(st.fresh_var())
                .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
            st.cnf.push(vec![x]);
            Ok(x)
        }
        SExpr::Val(Value::Bool(false)) => {
            let x = i32::try_from(st.fresh_var())
                .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
            st.cnf.push(vec![-x]);
            Ok(x)
        }

        SExpr::Not(e) => {
            let a = compile_expr_to_lit(*e, spec, st)?;
            Ok(-a)
        }

        SExpr::BinOp(
            lhs,
            rhs,
            crate::lang::dsrv::ast::SBinOp::BOp(crate::lang::dsrv::ast::BoolBinOp::And),
        ) => {
            let a = compile_expr_to_lit(*lhs, spec, st)?;
            let b = compile_expr_to_lit(*rhs, spec, st)?;
            Ok(st.tseitin_and(a, b))
        }

        SExpr::BinOp(
            lhs,
            rhs,
            crate::lang::dsrv::ast::SBinOp::BOp(crate::lang::dsrv::ast::BoolBinOp::Or),
        ) => {
            let a = compile_expr_to_lit(*lhs, spec, st)?;
            let b = compile_expr_to_lit(*rhs, spec, st)?;
            Ok(st.tseitin_or(a, b))
        }

        SExpr::BinOp(
            lhs,
            rhs,
            crate::lang::dsrv::ast::SBinOp::BOp(crate::lang::dsrv::ast::BoolBinOp::Impl),
        ) => {
            let a = compile_expr_to_lit(*lhs, spec, st)?;
            let b = compile_expr_to_lit(*rhs, spec, st)?;
            Ok(st.tseitin_impl(a, b))
        }

        SExpr::BinOp(
            lhs,
            rhs,
            crate::lang::dsrv::ast::SBinOp::COp(crate::lang::dsrv::ast::CompBinOp::Eq),
        ) => {
            let lhs_expr = *lhs;
            let rhs_expr = *rhs;
            if let (Some(l), Some(r)) = (
                eval_const_expr(lhs_expr.clone(), spec, st),
                eval_const_expr(rhs_expr.clone(), spec, st),
            ) {
                let x = i32::try_from(st.fresh_var())
                    .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
                if l == r {
                    st.add_clause1(x);
                } else {
                    st.add_clause1(-x);
                }
                Ok(x)
            } else {
                let a = compile_expr_to_lit(lhs_expr, spec, st)?;
                let b = compile_expr_to_lit(rhs_expr, spec, st)?;
                Ok(st.tseitin_eq(a, b))
            }
        }

        SExpr::If(cond, then_e, else_e) => {
            let c = compile_expr_to_lit(*cond, spec, st)?;
            let t = compile_expr_to_lit(*then_e, spec, st)?;
            let e = compile_expr_to_lit(*else_e, spec, st)?;
            Ok(st.tseitin_ite(c, t, e))
        }

        SExpr::Var(v) => {
            if let Some(const_v) = eval_const_expr(SExpr::Var(v.clone()), spec, st) {
                let x = i32::try_from(st.fresh_var())
                    .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
                match const_v {
                    Value::Bool(true) => {
                        st.add_clause1(x);
                        return Ok(x);
                    }
                    Value::Bool(false) => {
                        st.add_clause1(-x);
                        return Ok(x);
                    }
                    Value::NoVal | Value::Deferred => {
                        st.add_clause1(-x);
                        return Ok(x);
                    }
                    _ => {}
                }
            }

            let is_dist_constraint = spec.var_expr(&v).is_some_and(|e| {
                matches!(
                    e,
                    SExpr::MonitoredAt(_, _)
                        | SExpr::If(_, _, _)
                        | SExpr::Not(_)
                        | SExpr::BinOp(_, _, _)
                        | SExpr::Val(Value::Bool(_))
                )
            });
            if is_dist_constraint {
                let inlined = spec
                    .var_expr(&v)
                    .ok_or_else(|| SatCompileError::UnknownConstraintVar(v.clone()))?;
                compile_expr_to_lit(inlined, spec, st)
            } else {
                // After localisation, helper vars can appear as bare inputs without
                // corresponding expressions. Align with brute-force behavior by
                // treating unknown bare vars as false in SAT constraints.
                let x = i32::try_from(st.fresh_var())
                    .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
                st.add_clause1(-x);
                Ok(x)
            }
        }

        SExpr::Dist(_, _) => Err(SatCompileError::UnsupportedExpr(
            "SAT monitored_at solver supports only monitored_at(...) + boolean logic; dist(...) is unsupported".to_string(),
        )),
        SExpr::SIndex(_, _) => Err(SatCompileError::UnsupportedExpr(
            "SAT monitored_at solver supports only timeless constraints; time-indexed expressions are unsupported".to_string(),
        )),

        other => {
            if let Some(v) = eval_const_expr(other.clone(), spec, st) {
                let x = i32::try_from(st.fresh_var())
                    .unwrap_or_else(|_| panic!("SAT variable id exceeds i32 range"));
                match v {
                    Value::Bool(true) => {
                        st.add_clause1(x);
                        Ok(x)
                    }
                    Value::Bool(false) | Value::NoVal | Value::Deferred => {
                        st.add_clause1(-x);
                        Ok(x)
                    }
                    _ => Err(SatCompileError::UnsupportedExpr(format!(
                        "Unsupported non-boolean constant expression in SAT constraint compilation: {:?}",
                        v
                    ))),
                }
            } else {
                Err(SatCompileError::UnsupportedExpr(format!(
                    "Unsupported distribution-constraint expression for SAT monitored_at solver: {:?}",
                    other
                )))
            }
        },
    }
}

fn solve_with_sat_solver(state: &CnfCompilerState) -> Option<HashMap<usize, bool>> {
    let cnf: Cnf<PackedLiteral> = Cnf::new(state.cnf.clone());
    let mut solver: SolverImpls = SolverImpls::new(cnf);

    let model = solver.solve()?;

    let mut assignment: HashMap<usize, bool> = HashMap::new();
    for lit in model.iter() {
        let val = lit.get();
        let abs = val.unsigned_abs();
        let var = usize::try_from(abs)
            .unwrap_or_else(|_| panic!("Model variable id {} exceeds usize range", abs));
        let is_true = val > 0;
        assignment.insert(var, is_true);
    }

    // Ensure every monitored_at atom is present in the map
    // (default false if solver does not emit a literal explicitly).
    for atom in state.atoms_to_var_node.keys() {
        assignment.entry(*atom).or_insert(false);
    }

    Some(assignment)
}

fn build_labelled_graph_from_solution(
    graph: &Rc<DistributionGraph>,
    output_vars: &[VarName],
    st: &CnfCompilerState,
    assignment: &HashMap<usize, bool>,
) -> LabelledDistributionGraph {
    let mut node_labels: BTreeMap<_, Vec<VarName>> = graph
        .graph
        .node_indices()
        .map(|idx| (idx, Vec::new()))
        .collect();

    let constrained: HashSet<VarName> = st.constrained_streams.iter().cloned().collect();

    // Only schedule non-constraint output variables as actual work assignments.
    // Exclude declared distribution-constraint vars directly.
    let declared_dist_constraints: HashSet<VarName> =
        st.declared_dist_constraints.iter().cloned().collect();

    let assignment_vars: Vec<VarName> = output_vars
        .iter()
        .filter(|v| !declared_dist_constraints.contains(*v))
        .cloned()
        .collect();
    let assignment_var_set: HashSet<VarName> = assignment_vars.iter().cloned().collect();

    // Place assignment vars that are constrained by monitored_at(...) from SAT model.
    for (atom, (v, n)) in &st.atoms_to_var_node {
        if !assignment_var_set.contains(v) {
            continue;
        }
        let val = assignment.get(atom).copied().unwrap_or(false);
        if val {
            let idx = graph
                .get_node_index_by_name(n)
                .unwrap_or_else(|| panic!("Node `{}` missing in graph", n));
            node_labels.entry(idx).or_default().push(v.clone());
        }
    }

    // Place all non-constrained assignment vars deterministically at central monitor.
    for v in &assignment_vars {
        if constrained.contains(v) {
            continue;
        }
        node_labels
            .entry(graph.central_monitor)
            .or_default()
            .push(v.clone());
    }

    // Deduplicate per node.
    for vars in node_labels.values_mut() {
        let mut seen = BTreeSet::new();
        vars.retain(|v| seen.insert(v.clone()));
    }

    // Sanity check: each constrained assignment var must be placed exactly once.
    for s in constrained
        .iter()
        .filter(|s| assignment_var_set.contains(*s))
    {
        let mut count = 0usize;
        for vars in node_labels.values() {
            if vars.contains(s) {
                count = count.saturating_add(1);
            }
        }
        if count != 1 {
            panic!(
                "Internal solver inconsistency: constrained assignment stream `{}` placed {} times",
                s, count
            );
        }
    }

    LabelledDistributionGraph {
        dist_graph: graph.clone(),
        var_names: assignment_vars,
        node_labels,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::interfaces::AbstractMonitorBuilder;
    use crate::dsrv_fixtures::TestDistConfig;
    use crate::io::replay_history::ReplayHistory;
    use crate::lang::dsrv::lalr_parser::LALRParser;
    use crate::lang::dsrv::parser::dsrv_specification;
    use crate::runtime::distributed::DistAsyncMonitorBuilder;
    use crate::semantics::distributed::semantics::DistributedSemantics;
    use macro_rules_attribute::apply;
    use petgraph::graph::DiGraph;
    use proptest::prelude::*;
    use smol::LocalExecutor;
    use std::panic::AssertUnwindSafe;

    fn simple_dist_graph() -> Rc<DistributionGraph> {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        })
    }

    #[apply(crate::async_test)]
    async fn sat_solver_finds_valid_assignment(_executor: Rc<LocalExecutor<'static>>) {
        let spec = "in x\nout c\nc = monitored_at(x, A)";
        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["c".into()],
            vec!["x".into(), "c".into()],
            spec.to_string(),
            None,
        ));

        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment");

        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        let b_idx = graph.get_node_index_by_name(&"B".into()).unwrap();

        assert!(labelled.node_labels[&a_idx].contains(&"x".into()));
        assert!(!labelled.node_labels[&b_idx].contains(&"x".into()));
    }

    #[test]
    fn sat_solver_panics_on_unsat_constraints() {
        let spec = "in x\nout c\nc = (monitored_at(x, A) && monitored_at(x, B))";
        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["c".into()],
            vec!["x".into(), "c".into()],
            spec.to_string(),
            None,
        ));

        let graph = simple_dist_graph();

        let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = solver.possible_labelled_dist_graph_stream(graph);
        }));

        assert!(res.is_err());
    }

    #[apply(crate::async_test)]
    async fn sat_solver_handles_if_then_else_monitored_at(_executor: Rc<LocalExecutor<'static>>) {
        let spec =
            "in c\nout w\nout distW\ndistW = if c then monitored_at(w, A) else monitored_at(w, B)";
        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["distW".into()],
            vec!["w".into(), "distW".into()],
            spec.to_string(),
            None,
        ));

        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment for if-then-else monitored_at constraint");

        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        let b_idx = graph.get_node_index_by_name(&"B".into()).unwrap();

        let w_on_a = labelled.node_labels[&a_idx].contains(&"w".into());
        let w_on_b = labelled.node_labels[&b_idx].contains(&"w".into());

        assert!(
            w_on_a ^ w_on_b,
            "w should be placed on exactly one of A or B"
        );
    }

    #[test]
    fn sat_solver_panics_on_unsupported_dist_constraint() {
        let spec = "in x\nout c\nc = dist(A, B)";
        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["c".into()],
            vec!["x".into(), "c".into()],
            spec.to_string(),
            None,
        ));

        let graph = simple_dist_graph();

        let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = solver.possible_labelled_dist_graph_stream(graph);
        }));

        assert!(res.is_err(), "expected panic for unsupported dist(...)");
    }

    #[apply(crate::async_test)]
    async fn sat_solver_supports_const_map_operations_in_constraints(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec = r#"in m
out x
out c1
out c2
out c3
out c4
out c5
c1 = Map.has_key(Map("k": 1), "k")
c2 = (Map.get(Map("k": 2), "k") == 2)
c3 = Map.has_key(Map.insert(Map("a": 1), "b", 3), "b")
c4 = !Map.has_key(Map.remove(Map("z": 7), "z"), "z")
c5 = monitored_at(x, A)"#;
        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec![
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
            ],
            vec![
                "x".into(),
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
            ],
            spec.to_string(),
            None,
        ));
        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment");
        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        assert!(labelled.node_labels[&a_idx].contains(&"x".into()));
    }

    #[apply(crate::async_test)]
    async fn sat_solver_supports_const_list_operations_in_constraints(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec = r#"in xs
out x
out c1
out c2
out c3
out c4
out c5
out c6
c1 = (List.get(List(1, 2, 3), 1) == 2)
c2 = (List.append(List(1, 2), 3) == List(1, 2, 3))
c3 = (List.concat(List(1), List(2, 3)) == List(1, 2, 3))
c4 = (List.head(List(7, 8)) == 7)
c5 = (List.tail(List(7, 8)) == List(8))
c6 = (List.len(List(9, 10, 11)) == 3) && monitored_at(x, A)"#;
        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec![
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
                "c6".into(),
            ],
            vec![
                "x".into(),
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
                "c6".into(),
            ],
            spec.to_string(),
            None,
        ));
        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment");
        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        assert!(labelled.node_labels[&a_idx].contains(&"x".into()));
    }

    #[apply(crate::async_test)]
    async fn sat_solver_supports_replay_bound_map_variables_in_constraints(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

        use crate::io::replay_history::{ReplayHistory, StoreAllReplayHistory};

        let spec = r#"in m
out x
out c
c = (Map.get(m, "k") == 42) && monitored_at(x, A)"#;

        let mut row = BTreeMap::<VarName, Value>::new();
        row.insert(
            VarName::new("m"),
            Value::Map(BTreeMap::from([("k".into(), Value::Int(42))])),
        );
        let mut snapshot = BTreeMap::<usize, BTreeMap<VarName, Value>>::new();
        snapshot.insert(0usize, row);
        let replay_history = ReplayHistory::StoreAll(Rc::new(RefCell::new(
            StoreAllReplayHistory::from_snapshot(snapshot),
        )));

        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["c".into()],
            vec!["x".into(), "c".into()],
            spec.to_string(),
            Some(replay_history),
        ));

        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment with replay-bound map variable");

        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        assert!(labelled.node_labels[&a_idx].contains(&VarName::new("x")));
    }

    #[apply(crate::async_test)]
    async fn sat_solver_supports_replay_bound_list_variables_in_constraints(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

        use crate::io::replay_history::{ReplayHistory, StoreAllReplayHistory};

        let spec = r#"in xs
out x
out c
c = (List.get(xs, 1) == 42) && (List.len(List.append(xs, 0)) == 3) && monitored_at(x, A)"#;

        let mut row = BTreeMap::<VarName, Value>::new();
        row.insert(
            VarName::new("xs"),
            Value::List(vec![Value::Int(1), Value::Int(42)].into()),
        );
        let mut snapshot = BTreeMap::<usize, BTreeMap<VarName, Value>>::new();
        snapshot.insert(0usize, row);
        let replay_history = ReplayHistory::StoreAll(Rc::new(RefCell::new(
            StoreAllReplayHistory::from_snapshot(snapshot),
        )));

        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["c".into()],
            vec!["x".into(), "c".into()],
            spec.to_string(),
            Some(replay_history),
        ));

        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment with replay-bound list variable");

        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        assert!(labelled.node_labels[&a_idx].contains(&VarName::new("x")));
    }

    #[test]
    fn sat_compiler_emits_exactly_one_constraints_for_constrained_streams() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let lit_a = st.atom_for_monitored_at(VarName::new("x"), "A".into());
        let lit_b = st.atom_for_monitored_at(VarName::new("x"), "B".into());

        st.emit_exactly_one_constraints();

        assert!(st.cnf.iter().any(|c| c == &vec![lit_a, lit_b]));
        assert!(st.cnf.iter().any(|c| c == &vec![-lit_a, -lit_b]));
    }

    #[test]
    fn sat_compiler_tseitin_and_emits_expected_clauses() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let a_lit = 1_i32;
        let b_lit = 2_i32;
        st.next_var_id = 3;
        let x = st.tseitin_and(a_lit, b_lit);

        assert!(st.cnf.iter().any(|c| c == &vec![-x, a_lit]));
        assert!(st.cnf.iter().any(|c| c == &vec![-x, b_lit]));
        assert!(st.cnf.iter().any(|c| c == &vec![x, -a_lit, -b_lit]));
    }

    #[test]
    fn sat_compiler_rejects_non_boolean_constant_constraint_expr() {
        let spec_src = "out c\nc = (1 + 2)";
        let mut s = spec_src;
        let spec = dsrv_specification(&mut s).expect("spec should parse");

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let expr = spec.var_expr(&VarName::new("c")).expect("expr exists");
        let res = compile_expr_to_lit(expr, &spec, &mut st);

        assert!(
            res.is_err(),
            "expected non-boolean const expr to be rejected"
        );
    }

    #[test]
    fn sat_compiler_inlines_constraint_var_reference() {
        let spec_src = "in x\nout c1\nout c2\nc1 = monitored_at(x, A)\nc2 = c1";
        let mut s = spec_src;
        let spec = dsrv_specification(&mut s).expect("spec should parse");

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let expr = spec.var_expr(&VarName::new("c2")).expect("expr exists");
        let lit = compile_expr_to_lit(expr, &spec, &mut st).expect("compile succeeds");

        let a_lit = st.atom_for_monitored_at(VarName::new("x"), "A".into());
        assert_eq!(lit, a_lit);
    }

    #[test]
    fn sat_compiler_uses_replay_bound_bool_for_var_to_lit() {
        let spec_src = "in b\nout c\nc = b";
        let mut s = spec_src;
        let spec = dsrv_specification(&mut s).expect("spec should parse");

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        st.value_bindings
            .insert(VarName::new("b"), Value::Bool(true));

        let lit =
            compile_expr_to_lit(SExpr::Var(VarName::new("b")), &spec, &mut st).expect("compiles");

        assert!(st.cnf.iter().any(|c| c == &vec![lit]));
    }

    #[test]
    fn sat_compiler_unresolved_vars_compile_to_deterministic_false_literals() {
        let spec_src = "in b\nin c\nout d\nd = (b && c)";
        let mut s = spec_src;
        let spec = dsrv_specification(&mut s).expect("spec should parse");

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);

        let lit1 =
            compile_expr_to_lit(SExpr::Var(VarName::new("b")), &spec, &mut st).expect("compiles");
        let lit2 =
            compile_expr_to_lit(SExpr::Var(VarName::new("b")), &spec, &mut st).expect("compiles");
        let lit3 =
            compile_expr_to_lit(SExpr::Var(VarName::new("c")), &spec, &mut st).expect("compiles");

        assert_ne!(lit1, lit2, "fresh literal expected for unresolved var");
        assert_ne!(
            lit1, lit3,
            "different unresolved vars should produce independent fresh literals"
        );
        assert!(
            st.cnf.iter().any(|c| c == &vec![-lit1]),
            "first unresolved var literal should be constrained to false"
        );
        assert!(
            st.cnf.iter().any(|c| c == &vec![-lit2]),
            "second unresolved var literal should be constrained to false"
        );
        assert!(
            st.cnf.iter().any(|c| c == &vec![-lit3]),
            "third unresolved var literal should be constrained to false"
        );
    }

    #[test]
    fn sat_compiler_tseitin_or_emits_expected_clauses() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let a_lit = 1_i32;
        let b_lit = 2_i32;
        st.next_var_id = 3;

        let x = st.tseitin_or(a_lit, b_lit);

        assert!(st.cnf.iter().any(|c| c == &vec![-a_lit, x]));
        assert!(st.cnf.iter().any(|c| c == &vec![-b_lit, x]));
        assert!(st.cnf.iter().any(|c| c == &vec![-x, a_lit, b_lit]));
    }

    #[test]
    fn sat_compiler_tseitin_impl_emits_or_equivalent_clauses() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let a_lit = 1_i32;
        let b_lit = 2_i32;
        st.next_var_id = 3;

        let x = st.tseitin_impl(a_lit, b_lit);

        assert!(st.cnf.iter().any(|c| c == &vec![a_lit, x]));
        assert!(st.cnf.iter().any(|c| c == &vec![-b_lit, x]));
        assert!(st.cnf.iter().any(|c| c == &vec![-x, -a_lit, b_lit]));
    }

    #[test]
    fn sat_compiler_tseitin_eq_contains_bidirectional_implication_structure() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let a_lit = 1_i32;
        let b_lit = 2_i32;
        st.next_var_id = 3;

        let x = st.tseitin_eq(a_lit, b_lit);

        assert!(x > 0);
        assert!(
            st.cnf.iter().any(|c| c.len() == 3),
            "eq encoding should include ternary clauses through Tseitin composition"
        );
        assert!(
            st.cnf.len() >= 9,
            "eq encoding should emit a non-trivial number of clauses"
        );
    }

    #[test]
    fn sat_compiler_tseitin_ite_emits_expected_clauses() {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let c_lit = 1_i32;
        let t_lit = 2_i32;
        let e_lit = 3_i32;
        st.next_var_id = 4;

        let x = st.tseitin_ite(c_lit, t_lit, e_lit);

        assert!(st.cnf.iter().any(|cl| cl == &vec![-c_lit, -x, t_lit]));
        assert!(st.cnf.iter().any(|cl| cl == &vec![-c_lit, x, -t_lit]));
        assert!(st.cnf.iter().any(|cl| cl == &vec![c_lit, -x, e_lit]));
        assert!(st.cnf.iter().any(|cl| cl == &vec![c_lit, x, -e_lit]));
    }

    #[test]
    fn sat_compiler_rejects_time_indexed_expressions() {
        let spec_src = "in x\nout c\nc = x[1]";
        let mut s = spec_src;
        let spec = dsrv_specification(&mut s).expect("spec should parse");

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let expr = spec.var_expr(&VarName::new("c")).expect("expr exists");

        let res = compile_expr_to_lit(expr, &spec, &mut st);
        assert!(res.is_err(), "time-indexed expression should be rejected");
    }

    #[test]
    fn sat_compiler_rejects_non_constant_non_boolean_arithmetic_constraint() {
        let spec_src = "in x\nout c\nc = (x + 1)";
        let mut s = spec_src;
        let spec = dsrv_specification(&mut s).expect("spec should parse");

        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        graph.add_edge(a, b, 1);
        graph.add_edge(b, a, 1);
        let graph = Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        });

        let mut st = CnfCompilerState::new(&graph);
        let expr = spec.var_expr(&VarName::new("c")).expect("expr exists");

        let res = compile_expr_to_lit(expr, &spec, &mut st);
        assert!(
            res.is_err(),
            "non-boolean arithmetic expression should be rejected"
        );
    }

    #[apply(crate::async_test)]
    async fn sat_solver_merges_replay_values_across_rows_for_const_eval(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

        use crate::io::replay_history::{ReplayHistory, StoreAllReplayHistory};

        let spec = r#"in m
in xs
in n
out x
out c
c = (Map.get(m, "k") == 7)
    && (List.get(xs, 1) == 42)
    && (n == 5)
    && monitored_at(x, A)"#;

        let mut row0 = BTreeMap::<VarName, Value>::new();
        row0.insert(
            VarName::new("m"),
            Value::Map(BTreeMap::from([("k".into(), Value::Int(7))])),
        );
        row0.insert(VarName::new("xs"), Value::NoVal);

        let mut row1 = BTreeMap::<VarName, Value>::new();
        row1.insert(
            VarName::new("xs"),
            Value::List(vec![Value::Int(1), Value::Int(42)].into()),
        );
        row1.insert(VarName::new("m"), Value::NoVal);

        let mut row2 = BTreeMap::<VarName, Value>::new();
        row2.insert(VarName::new("n"), Value::Int(5));

        let mut snapshot = BTreeMap::<usize, BTreeMap<VarName, Value>>::new();
        snapshot.insert(0usize, row0);
        snapshot.insert(1usize, row1);
        snapshot.insert(2usize, row2);

        let replay_history = ReplayHistory::StoreAll(Rc::new(RefCell::new(
            StoreAllReplayHistory::from_snapshot(snapshot),
        )));

        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["c".into()],
            vec!["x".into(), "c".into()],
            spec.to_string(),
            Some(replay_history),
        ));

        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment using merged replay-bound values");

        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        assert!(labelled.node_labels[&a_idx].contains(&VarName::new("x")));
    }

    #[apply(crate::async_test)]
    async fn sat_solver_supports_const_non_eq_comparison_operations_in_constraints(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec = r#"out x
out c1
out c2
out c3
out c4
out c5
out c6
out c7
out c8
c1 = (1 <= 2)
c2 = (2 < 3)
c3 = (3 >= 3)
c4 = (4 > 1)
c5 = (1.0 <= 1)
c6 = (2.0 >= 2)
c7 = ("a" < "b")
c8 = (true >= false) && monitored_at(x, A)"#;

        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec![
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
                "c6".into(),
                "c7".into(),
                "c8".into(),
            ],
            vec![
                "x".into(),
                "c1".into(),
                "c2".into(),
                "c3".into(),
                "c4".into(),
                "c5".into(),
                "c6".into(),
                "c7".into(),
                "c8".into(),
            ],
            spec.to_string(),
            None,
        ));

        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment with non-equality comparison const folding");

        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        assert!(labelled.node_labels[&a_idx].contains(&VarName::new("x")));
    }

    #[apply(crate::async_test)]
    async fn sat_solver_excludes_dist_constraint_vars_from_assignments(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let spec =
            "in c\nout w\nout distW\ndistW = if c then monitored_at(w, A) else monitored_at(w, B)";
        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            vec!["distW".into()],
            vec!["w".into(), "distW".into()],
            spec.to_string(),
            None,
        ));

        let graph = simple_dist_graph();
        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment");

        assert_eq!(labelled.var_names, vec![VarName::new("w")]);

        let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
        let b_idx = graph.get_node_index_by_name(&"B".into()).unwrap();

        let w_on_a = labelled.node_labels[&a_idx].contains(&VarName::new("w"));
        let w_on_b = labelled.node_labels[&b_idx].contains(&VarName::new("w"));

        assert!(w_on_a ^ w_on_b, "w should be assigned to exactly one node");

        assert!(!labelled.node_labels[&a_idx].contains(&VarName::new("distW")));
        assert!(!labelled.node_labels[&b_idx].contains(&VarName::new("distW")));
    }

    fn clique_graph_with_nodes(node_count: usize) -> Rc<DistributionGraph> {
        let mut graph = DiGraph::new();
        let mut nodes = Vec::with_capacity(node_count);
        for i in 0..node_count {
            let name = format!("N{}", i + 1);
            nodes.push(graph.add_node(name.into()));
        }
        for i in 0..node_count {
            for j in 0..node_count {
                if i != j {
                    graph.add_edge(nodes[i], nodes[j], 1);
                }
            }
        }

        Rc::new(DistributionGraph {
            central_monitor: nodes[0],
            graph,
        })
    }

    fn make_large_sat_spec(
        node_count: usize,
        stream_count: usize,
        constrain_count: usize,
    ) -> (String, Vec<VarName>, Vec<VarName>) {
        let mut lines = Vec::<String>::new();

        // Boolean control inputs used by if-then-else constraints
        for i in 0..constrain_count {
            lines.push(format!("in c{}", i + 1));
        }

        // Output streams to be assigned
        for i in 0..stream_count {
            lines.push(format!("out s{}", i + 1));
        }

        // Declare all distribution constraint outputs before any assignments
        for i in 0..constrain_count {
            lines.push(format!("out dist{}", i + 1));
        }

        // Distribution constraint assignments
        for i in 0..constrain_count {
            let s_idx = i + 1;
            let c_idx = i + 1;
            let n_then = (i % node_count) + 1;
            let n_else = ((i + 1) % node_count) + 1;
            lines.push(format!(
                "dist{} = if c{} then monitored_at(s{}, N{}) else monitored_at(s{}, N{})",
                s_idx, c_idx, s_idx, n_then, s_idx, n_else
            ));
        }

        let spec = lines.join("\n");
        let mut output_vars = Vec::<VarName>::new();
        let mut dist_constraints = Vec::<VarName>::new();

        for i in 0..stream_count {
            output_vars.push(VarName::new(&format!("s{}", i + 1)));
        }
        for i in 0..constrain_count {
            let v = VarName::new(&format!("dist{}", i + 1));
            output_vars.push(v.clone());
            dist_constraints.push(v);
        }

        (spec, output_vars, dist_constraints)
    }

    #[apply(crate::async_test)]
    async fn sat_solver_scales_to_5_nodes_20_streams_without_replay(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let node_count = 5usize;
        let stream_count = 20usize;
        let constrain_count = 20usize;

        let graph = clique_graph_with_nodes(node_count);
        let (spec, output_vars, dist_constraints) =
            make_large_sat_spec(node_count, stream_count, constrain_count);

        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(dist_constraints, output_vars, spec, None));

        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment for large 5-node/20-stream case");

        assert_eq!(labelled.var_names.len(), stream_count);

        for i in 0..stream_count {
            let s = VarName::new(&format!("s{}", i + 1));
            let mut count = 0usize;
            for vars in labelled.node_labels.values() {
                if vars.contains(&s) {
                    count = count.saturating_add(1);
                }
            }
            assert_eq!(
                count,
                1,
                "stream {} should be assigned to exactly one node",
                i + 1
            );
        }

        for i in 0..constrain_count {
            let d = VarName::new(&format!("dist{}", i + 1));
            assert!(
                !labelled.var_names.contains(&d),
                "distribution constraint var should not be in assignment var_names"
            );
            for vars in labelled.node_labels.values() {
                assert!(
                    !vars.contains(&d),
                    "distribution constraint var should not be assigned to any node"
                );
            }
        }
    }

    #[apply(crate::async_test)]
    async fn sat_solver_scales_to_5_nodes_20_streams_with_replay_substitution(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

        use crate::io::replay_history::{ReplayHistory, StoreAllReplayHistory};

        let node_count = 5usize;
        let stream_count = 20usize;
        let constrain_count = 20usize;

        let graph = clique_graph_with_nodes(node_count);
        let (spec, output_vars, dist_constraints) =
            make_large_sat_spec(node_count, stream_count, constrain_count);

        let mut latest_row = BTreeMap::<VarName, Value>::new();
        for i in 0..constrain_count {
            // Alternate true/false so both branches are exercised
            latest_row.insert(
                VarName::new(&format!("c{}", i + 1)),
                Value::Bool(i % 2 == 0),
            );
        }

        let mut snapshot = BTreeMap::<usize, BTreeMap<VarName, Value>>::new();
        snapshot.insert(0usize, latest_row);

        let replay_history = ReplayHistory::StoreAll(Rc::new(RefCell::new(
            StoreAllReplayHistory::from_snapshot(snapshot),
        )));

        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            dist_constraints, output_vars, spec, Some(replay_history)
        ));

        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment for large replay-substituted case");

        assert_eq!(labelled.var_names.len(), stream_count);

        for i in 0..stream_count {
            let s = VarName::new(&format!("s{}", i + 1));
            let mut count = 0usize;
            for vars in labelled.node_labels.values() {
                if vars.contains(&s) {
                    count = count.saturating_add(1);
                }
            }
            assert_eq!(
                count,
                1,
                "stream {} should be assigned to exactly one node",
                i + 1
            );
        }

        // Check branch-driven placement for each constrained stream:
        // if c_i is true => s_i at N{i mod 5 + 1}
        // else          => s_i at N{(i+1) mod 5 + 1}
        for i in 0..constrain_count {
            let s = VarName::new(&format!("s{}", i + 1));
            let c_true = i % 2 == 0;
            let target_node = if c_true {
                format!("N{}", (i % node_count) + 1)
            } else {
                format!("N{}", ((i + 1) % node_count) + 1)
            };

            let target_idx = graph
                .get_node_index_by_name(&target_node.clone().into())
                .expect("target node missing in graph");

            assert!(
                labelled.node_labels[&target_idx].contains(&s),
                "expected stream {:?} at node {} from replay-substituted branch",
                s,
                target_node
            );
        }
    }

    #[apply(crate::async_test)]
    async fn sat_solver_scales_to_5_nodes_20_streams_mixed_constraints(
        _executor: Rc<LocalExecutor<'static>>,
    ) {
        let node_count = 5usize;
        let stream_count = 20usize;
        let constrain_count = 8usize;

        let graph = clique_graph_with_nodes(node_count);
        let (spec, output_vars, dist_constraints) =
            make_large_sat_spec(node_count, stream_count, constrain_count);

        let solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            dist_constraints.clone(), output_vars.clone(), spec, None
        ));

        let mut stream = solver.possible_labelled_dist_graph_stream(graph.clone());
        let labelled = futures::StreamExt::next(&mut stream)
            .await
            .expect("expected SAT assignment for mixed constrained/unconstrained case");

        // Only the real output streams should be assignment vars (dist vars excluded).
        assert_eq!(labelled.var_names.len(), stream_count);
        for i in 0..stream_count {
            assert!(
                labelled
                    .var_names
                    .contains(&VarName::new(&format!("s{}", i + 1))),
                "expected assignment var s{}",
                i + 1
            );
        }
        for i in 0..constrain_count {
            let d = VarName::new(&format!("dist{}", i + 1));
            assert!(
                !labelled.var_names.contains(&d),
                "distribution constraint var should not be in assignment var_names"
            );
            for vars in labelled.node_labels.values() {
                assert!(
                    !vars.contains(&d),
                    "distribution constraint var should not be assigned to any node"
                );
            }
        }

        // Every stream must be assigned exactly once.
        for i in 0..stream_count {
            let s = VarName::new(&format!("s{}", i + 1));
            let mut count = 0usize;
            for vars in labelled.node_labels.values() {
                if vars.contains(&s) {
                    count = count.saturating_add(1);
                }
            }
            assert_eq!(
                count,
                1,
                "stream {} should be assigned to exactly one node",
                i + 1
            );
        }

        // Constrained streams (s1..s8) should be placed according to monitored_at choices.
        for i in 0..constrain_count {
            let s = VarName::new(&format!("s{}", i + 1));
            let n1 = format!("N{}", (i % node_count) + 1);
            let n2 = format!("N{}", ((i + 1) % node_count) + 1);

            let idx1 = graph
                .get_node_index_by_name(&n1.clone().into())
                .expect("expected node for first branch");
            let idx2 = graph
                .get_node_index_by_name(&n2.clone().into())
                .expect("expected node for second branch");

            let on_n1 = labelled.node_labels[&idx1].contains(&s);
            let on_n2 = labelled.node_labels[&idx2].contains(&s);

            assert!(
                on_n1 ^ on_n2,
                "constrained stream should be on exactly one branch node"
            );
        }

        // Unconstrained streams (s9..s20) should be at central monitor by default.
        let central_idx = graph.central_monitor;
        for i in constrain_count..stream_count {
            let s = VarName::new(&format!("s{}", i + 1));
            assert!(
                labelled.node_labels[&central_idx].contains(&s),
                "unconstrained stream {:?} should be assigned to central monitor",
                s
            );
        }
    }

    fn fixed_nontrivial_spec_for_3_nodes() -> (String, Vec<VarName>, Vec<VarName>) {
        let spec = r#"
in c1
in c2
in c3
out s1
out s2
out s3
out dist1
out dist2
out dist3
dist1 = if c1 then monitored_at(s1, A) else monitored_at(s1, B)
dist2 = if c2 then monitored_at(s2, B) else monitored_at(s2, C)
dist3 = if c3 then monitored_at(s3, C) else monitored_at(s3, A)
"#
        .trim()
        .to_string();

        let output_vars = vec![
            VarName::new("s1"),
            VarName::new("s2"),
            VarName::new("s3"),
            VarName::new("dist1"),
            VarName::new("dist2"),
            VarName::new("dist3"),
        ];

        let dist_constraints = vec![
            VarName::new("dist1"),
            VarName::new("dist2"),
            VarName::new("dist3"),
        ];

        (spec, output_vars, dist_constraints)
    }

    fn spec_3_nodes_nested_bool_constraints() -> (String, Vec<VarName>, Vec<VarName>) {
        let spec = r#"
in c1
in c2
in c3
out s1
out s2
out s3
out d1
out d2
out d3
d1 = if (c1 && !c2) then monitored_at(s1, A) else monitored_at(s1, C)
d2 = if (c2 || c3) then monitored_at(s2, B) else monitored_at(s2, A)
d3 = if (c3 => c1) then monitored_at(s3, C) else monitored_at(s3, B)
"#
        .trim()
        .to_string();

        let output_vars = vec![
            VarName::new("s1"),
            VarName::new("s2"),
            VarName::new("s3"),
            VarName::new("d1"),
            VarName::new("d2"),
            VarName::new("d3"),
        ];

        let dist_constraints = vec![VarName::new("d1"), VarName::new("d2"), VarName::new("d3")];

        (spec, output_vars, dist_constraints)
    }

    fn spec_3_nodes_interdependent_constraint_vars() -> (String, Vec<VarName>, Vec<VarName>) {
        let spec = r#"
in c1
in c2
in c3
out s1
out s2
out s3
out d1
out d2
out d3
d1 = if c1 then monitored_at(s1, A) else monitored_at(s1, B)
d2 = if c2 then monitored_at(s2, B) else monitored_at(s2, C)
d3 = if ((c1 && c2) || c3) then monitored_at(s3, C) else monitored_at(s3, A)
"#
        .trim()
        .to_string();

        let output_vars = vec![
            VarName::new("s1"),
            VarName::new("s2"),
            VarName::new("s3"),
            VarName::new("d1"),
            VarName::new("d2"),
            VarName::new("d3"),
        ];

        let dist_constraints = vec![VarName::new("d1"), VarName::new("d2"), VarName::new("d3")];

        (spec, output_vars, dist_constraints)
    }

    fn spec_3_nodes_with_const_and_replay_mixed() -> (String, Vec<VarName>, Vec<VarName>) {
        let spec = r#"
in c1
in c2
in c3
out s1
out s2
out s3
out dist1
out dist2
out dist3
dist1 = ((true && c1) == c1) && monitored_at(s1, A)
dist2 = if ((false || c2) && (1 < 2)) then monitored_at(s2, B) else monitored_at(s2, C)
dist3 = if ((!!c3) == c3) then monitored_at(s3, C) else monitored_at(s3, A)
"#
        .trim()
        .to_string();

        let output_vars = vec![
            VarName::new("s1"),
            VarName::new("s2"),
            VarName::new("s3"),
            VarName::new("dist1"),
            VarName::new("dist2"),
            VarName::new("dist3"),
        ];

        let dist_constraints = vec![
            VarName::new("dist1"),
            VarName::new("dist2"),
            VarName::new("dist3"),
        ];

        (spec, output_vars, dist_constraints)
    }

    fn graph_3_nodes_with_weights(w_ab: u64, w_bc: u64, w_ca: u64) -> Rc<DistributionGraph> {
        let mut graph = DiGraph::new();
        let a = graph.add_node("A".into());
        let b = graph.add_node("B".into());
        let c = graph.add_node("C".into());

        graph.add_edge(a, b, w_ab);
        graph.add_edge(b, a, w_ab);

        graph.add_edge(b, c, w_bc);
        graph.add_edge(c, b, w_bc);

        graph.add_edge(c, a, w_ca);
        graph.add_edge(a, c, w_ca);

        Rc::new(DistributionGraph {
            central_monitor: a,
            graph,
        })
    }

    fn normalized_assignment_projection(
        labelled: &LabelledDistributionGraph,
    ) -> BTreeMap<VarName, NodeName> {
        let mut projection = BTreeMap::new();
        for (node_idx, vars) in &labelled.node_labels {
            let node_name = labelled.dist_graph.graph[*node_idx].clone();
            for v in vars {
                projection.insert(v.clone(), node_name.clone());
            }
        }
        projection
    }

    fn solve_sat_and_bruteforce_once(
        graph: Rc<DistributionGraph>,
        spec: String,
        output_vars: Vec<VarName>,
        dist_constraints: Vec<VarName>,
        replay_snapshot: std::collections::BTreeMap<
            usize,
            std::collections::BTreeMap<VarName, Value>,
        >,
    ) -> (Rc<LabelledDistributionGraph>, Rc<LabelledDistributionGraph>) {
        let (sat_opt, brute_opt) = solve_sat_and_bruteforce_once_optional(
            graph,
            spec,
            output_vars,
            dist_constraints,
            replay_snapshot,
        );

        let sat_labelled = sat_opt.expect("SAT solver should produce at least one solution");
        let brute_labelled =
            brute_opt.expect("Brute-force solver should produce at least one solution");

        (sat_labelled, brute_labelled)
    }

    fn solve_sat_and_bruteforce_once_optional(
        graph: Rc<DistributionGraph>,
        spec: String,
        output_vars: Vec<VarName>,
        dist_constraints: Vec<VarName>,
        replay_snapshot: std::collections::BTreeMap<
            usize,
            std::collections::BTreeMap<VarName, Value>,
        >,
    ) -> (
        Option<Rc<LabelledDistributionGraph>>,
        Option<Rc<LabelledDistributionGraph>>,
    ) {
        let mut inferred_input_vars = std::collections::BTreeSet::<VarName>::new();
        for row in replay_snapshot.values() {
            for var in row.keys() {
                inferred_input_vars.insert(var.clone());
            }
        }
        let input_vars = inferred_input_vars.into_iter().collect::<Vec<_>>();

        let shared_replay = ReplayHistory::store_all_with_snapshot(replay_snapshot);

        let sat_solver = Rc::new(SatMonitoredAtDistConstraintSolver::<
            DistributedSemantics<LALRParser>,
            TestDistConfig,
        >::new(
            dist_constraints.clone(),
            output_vars.clone(),
            spec.clone(),
            Some(shared_replay.clone()),
        ));

        let sat_labelled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut sat_stream = sat_solver.possible_labelled_dist_graph_stream(graph.clone());
            smol::block_on(async { futures::StreamExt::next(&mut sat_stream).await })
        }))
        .ok()
        .flatten();

        let mut spec_src = spec.as_str();
        let parsed_spec =
            dsrv_specification(&mut spec_src).expect("fixed property-test spec should parse");

        let executor = Rc::new(LocalExecutor::new());
        let monitor_builder =
            DistAsyncMonitorBuilder::<TestDistConfig, DistributedSemantics<LALRParser>>::new()
                .executor(executor.clone())
                .model(parsed_spec);

        let brute_solver = Rc::new(
            crate::distributed::solvers::brute_solver::BruteForceDistConstraintSolver::<
                DistributedSemantics<LALRParser>,
                TestDistConfig,
            > {
                executor: executor.clone(),
                monitor_builder,
                context_builder: None,
                dist_constraints: dist_constraints.clone(),
                input_vars,
                output_vars,
                replay_history: Some(shared_replay),
            },
        );

        let mut brute_stream = brute_solver.possible_labelled_dist_graph_stream(graph);
        let brute_labelled = smol::block_on(
            executor.run(async { futures::StreamExt::next(&mut brute_stream).await }),
        );

        (sat_labelled, brute_labelled)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn prop_sat_matches_bruteforce_on_3_node_graphs(
            w_ab in 1u64..=4,
            w_bc in 1u64..=4,
            w_ca in 1u64..=4,
            c1 in any::<bool>(),
            c2 in any::<bool>(),
            c3 in any::<bool>(),
        ) {
            let graph = graph_3_nodes_with_weights(w_ab, w_bc, w_ca);
            let (spec, output_vars, dist_constraints) = fixed_nontrivial_spec_for_3_nodes();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("c1"), Value::Bool(c1)),
                    (VarName::new("c2"), Value::Bool(c2)),
                    (VarName::new("c3"), Value::Bool(c3)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph,
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(&sat_labelled.var_names, &brute_labelled.var_names);
            prop_assert_eq!(sat_projection, brute_projection);
        }

        #[test]
        fn prop_sat_matches_bruteforce_on_3_node_graphs_nested_bool_constraints(
            w_ab in 1u64..=4,
            w_bc in 1u64..=4,
            w_ca in 1u64..=4,
            c1 in any::<bool>(),
            c2 in any::<bool>(),
            c3 in any::<bool>(),
        ) {
            let graph = graph_3_nodes_with_weights(w_ab, w_bc, w_ca);
            let (spec, output_vars, dist_constraints) = spec_3_nodes_nested_bool_constraints();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("c1"), Value::Bool(c1)),
                    (VarName::new("c2"), Value::Bool(c2)),
                    (VarName::new("c3"), Value::Bool(c3)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph,
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(&sat_labelled.var_names, &brute_labelled.var_names);
            prop_assert_eq!(sat_projection, brute_projection);
        }

        #[test]
        fn prop_sat_matches_bruteforce_on_3_node_graphs_interdependent_constraint_vars(
            w_ab in 1u64..=4,
            w_bc in 1u64..=4,
            w_ca in 1u64..=4,
            c1 in any::<bool>(),
            c2 in any::<bool>(),
            c3 in any::<bool>(),
        ) {
            let graph = graph_3_nodes_with_weights(w_ab, w_bc, w_ca);
            let (spec, output_vars, dist_constraints) = spec_3_nodes_interdependent_constraint_vars();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("c1"), Value::Bool(c1)),
                    (VarName::new("c2"), Value::Bool(c2)),
                    (VarName::new("c3"), Value::Bool(c3)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph,
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(&sat_labelled.var_names, &brute_labelled.var_names);
            prop_assert_eq!(sat_projection, brute_projection);
        }

        #[test]
        fn prop_sat_matches_bruteforce_on_3_node_graphs_const_and_replay_mixed(
            w_ab in 1u64..=4,
            w_bc in 1u64..=4,
            w_ca in 1u64..=4,
            c1 in any::<bool>(),
            c2 in any::<bool>(),
            c3 in any::<bool>(),
        ) {
            let graph = graph_3_nodes_with_weights(w_ab, w_bc, w_ca);
            let (spec, output_vars, dist_constraints) = spec_3_nodes_with_const_and_replay_mixed();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("c1"), Value::Bool(c1)),
                    (VarName::new("c2"), Value::Bool(c2)),
                    (VarName::new("c3"), Value::Bool(c3)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph,
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(&sat_labelled.var_names, &brute_labelled.var_names);
            prop_assert_eq!(sat_projection, brute_projection);
        }
    }

    #[test]
    fn localise_can_leave_unresolved_helper_inputs_regression() {
        let spec_src = r#"
in c1
in c2
in c3
out s1
out s2
out s3
out d1
out d2
out d3
aux h1
aux h2
h1 = if c1 then monitored_at(s1, A) else monitored_at(s1, B)
h2 = if c2 then monitored_at(s2, B) else monitored_at(s2, C)
d1 = h1
d2 = h2
d3 = if ((h1 && h2) || c3) then monitored_at(s3, C) else monitored_at(s3, A)
"#
        .trim();

        let mut src = spec_src;
        let parsed = dsrv_specification(&mut src).expect("spec should parse");
        let localised = parsed.localise(&vec![
            VarName::new("d1"),
            VarName::new("d2"),
            VarName::new("d3"),
        ]);

        assert_eq!(
            localised.input_vars,
            vec!["c1".into(), "c2".into(), "c3".into()]
        );
        assert!(localised.var_expr(&VarName::new("h1")).is_none());
        assert!(localised.var_expr(&VarName::new("h2")).is_none());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn prop_sat_law_de_morgan_and(
            a in any::<bool>(),
            b in any::<bool>(),
        ) {
            let spec = r#"
in a
in b
out x
out c
c = ((!a || !b) == !(a && b)) && monitored_at(x, A)
"#
            .trim()
            .to_string();

            let output_vars = vec![
                VarName::new("x"),
                VarName::new("c"),
            ];
            let dist_constraints = vec![VarName::new("c")];

            let graph = simple_dist_graph();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("a"), Value::Bool(a)),
                    (VarName::new("b"), Value::Bool(b)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph,
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(sat_projection, brute_projection);
        }

        #[test]
        fn prop_sat_law_de_morgan_or(
            a in any::<bool>(),
            b in any::<bool>(),
        ) {
            let spec = r#"
in a
in b
out x
out c
c = ((!a && !b) == !(a || b)) && monitored_at(x, A)
"#
            .trim()
            .to_string();

            let output_vars = vec![
                VarName::new("x"),
                VarName::new("c"),
            ];
            let dist_constraints = vec![VarName::new("c")];

            let graph = simple_dist_graph();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("a"), Value::Bool(a)),
                    (VarName::new("b"), Value::Bool(b)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph,
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(sat_projection, brute_projection);
        }

        #[test]
        fn prop_sat_law_implication_equivalence(
            a in any::<bool>(),
            b in any::<bool>(),
        ) {
            let spec = r#"
in a
in b
out x
out c
c = ((a => b) == (!a || b)) && monitored_at(x, A)
"#
            .trim()
            .to_string();

            let output_vars = vec![
                VarName::new("x"),
                VarName::new("c"),
            ];
            let dist_constraints = vec![VarName::new("c")];

            let graph = simple_dist_graph();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("a"), Value::Bool(a)),
                    (VarName::new("b"), Value::Bool(b)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph,
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(sat_projection, brute_projection);
        }

        #[test]
        fn prop_sat_law_double_negation(
            a in any::<bool>(),
        ) {
            let spec = r#"
in a
out x
out c
c = (!!a == a) && monitored_at(x, A)
"#
            .trim()
            .to_string();

            let output_vars = vec![
                VarName::new("x"),
                VarName::new("c"),
            ];
            let dist_constraints = vec![VarName::new("c")];

            let graph = simple_dist_graph();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("a"), Value::Bool(a)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph,
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(sat_projection, brute_projection);
        }

        #[test]
        fn prop_sat_law_comparison_duality_int(
            x in -20i64..=20,
            y in -20i64..=20,
        ) {
            let spec = r#"
in x
in y
out w
out c
c = ((x < y) == !(x >= y)) && ((x > y) == !(x <= y)) && monitored_at(w, A)
"#
            .trim()
            .to_string();

            let output_vars = vec![
                VarName::new("w"),
                VarName::new("c"),
            ];
            let dist_constraints = vec![VarName::new("c")];

            let graph = simple_dist_graph();

            let replay_snapshot = std::collections::BTreeMap::from([(
                0usize,
                std::collections::BTreeMap::from([
                    (VarName::new("x"), Value::Int(x)),
                    (VarName::new("y"), Value::Int(y)),
                ]),
            )]);

            let (sat_labelled, brute_labelled) = solve_sat_and_bruteforce_once(
                graph.clone(),
                spec,
                output_vars,
                dist_constraints,
                replay_snapshot,
            );

            let sat_projection = normalized_assignment_projection(&sat_labelled);
            let brute_projection = normalized_assignment_projection(&brute_labelled);

            prop_assert_eq!(sat_projection, brute_projection);

            let a_idx = graph.get_node_index_by_name(&"A".into()).unwrap();
            prop_assert!(
                sat_labelled.node_labels[&a_idx].contains(&VarName::new("w")),
                "comparison duality should hold and place w at A"
            );
        }
    }
}

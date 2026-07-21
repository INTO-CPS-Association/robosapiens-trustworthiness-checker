use std::fmt::{Debug, Display, Error};

use crate::core::{StreamTypeAscription, VarName};

use super::{
    CheckedDsrvSpecification, CheckedExpr, DsrvSpecification, DynamicExprScope, Expr, ExprArena,
    ExprId, ExprKind, ExprRef,
};

impl Debug for CheckedExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.expr, f)
    }
}

impl Display for CheckedExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.expr, f)
    }
}

impl serde::Serialize for CheckedExpr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.expr.serialize(serializer)
    }
}

impl Debug for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&crate::lang::dsrv::span::strip_span(self))
    }
}

impl serde::Serialize for Expr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.display().to_string())
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.display().fmt(f)
    }
}

impl Debug for ExprRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.display().fmt(f)
    }
}

impl Display for ExprRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.display().fmt(f)
    }
}

impl Display for CheckedDsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.spec, f)
    }
}

/// A borrowed expression formatting adapter.
pub struct ExprDisplay<'arena> {
    pub(super) arena: &'arena ExprArena,
    pub(super) id: ExprId,
}

impl Display for ExprDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ExprKind::*;
        let show = |id| ExprDisplay {
            arena: self.arena,
            id,
        };
        match &self.arena.get(self.id).node {
            Val(value) => write!(f, "{value}"),
            Var(var) => write!(f, "{var}"),
            BinOp(lhs, rhs, op) => write!(f, "({} {op} {})", show(*lhs), show(*rhs)),
            If(cond, yes, no) => write!(
                f,
                "(if {} then {} else {})",
                show(*cond),
                show(*yes),
                show(*no)
            ),
            SIndex(expr, index) => write!(f, "{}[{index}]", show(*expr)),
            Not(expr) => write!(f, "!{}", show(*expr)),
            Neg(expr) => write!(f, "-{}", show(*expr)),
            Dynamic(source, result_type, scope) | Defer(source, result_type, scope) => {
                let name = if matches!(&self.arena.get(self.id).node, Dynamic(..)) {
                    "dynamic"
                } else {
                    "defer"
                };
                write!(f, "{name}({}", show(*source))?;
                if let StreamTypeAscription::Ascribed(typ) = result_type {
                    write!(f, ": {typ}")?;
                }
                if let DynamicExprScope::Explicit(vars) = scope {
                    let vars = vars
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, ", {{{vars}}}")?;
                }
                write!(f, ")")
            }
            Update(a, b) => write!(f, "update({}, {})", show(*a), show(*b)),
            Default(a, b) => write!(f, "default({}, {})", show(*a), show(*b)),
            IsDefined(expr) => write!(f, "is_defined({})", show(*expr)),
            When(expr) => write!(f, "when({})", show(*expr)),
            Latch(a, b) => write!(f, "latch({}, {})", show(*a), show(*b)),
            Init(a, b) => write!(f, "init({}, {})", show(*a), show(*b)),
            Lambda(params, body) => {
                let params = params
                    .iter()
                    .map(|(name, typ)| format!("{name}: {typ}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "\\{params} -> {}", show(*body))
            }
            Apply(function, args) | Partial(function, args) => {
                let partial = matches!(&self.arena.get(self.id).node, Partial(_, _));
                if partial {
                    write!(f, "partial({}", show(*function))?;
                } else {
                    write!(f, "{}(", show(*function))?;
                }
                for (index, arg) in args.iter().enumerate() {
                    if partial || index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", show(*arg))?;
                }
                write!(f, ")")
            }
            Fix(function) => write!(f, "fix({})", show(*function)),
            List(items) | Tuple(items) => {
                let (open, close) = if matches!(&self.arena.get(self.id).node, List(_)) {
                    ("[", "]")
                } else {
                    ("Tuple(", ")")
                };
                let items = items
                    .iter()
                    .map(|id| show(*id).to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{open}{items}{close}")
            }
            LIndex(a, b) => write!(f, "List.get({}, {})", show(*a), show(*b)),
            LAppend(a, b) => write!(f, "List.append({}, {})", show(*a), show(*b)),
            LConcat(a, b) => write!(f, "List.concat({}, {})", show(*a), show(*b)),
            LHead(expr) => write!(f, "List.head({})", show(*expr)),
            LTail(expr) => write!(f, "List.tail({})", show(*expr)),
            LLen(expr) => write!(f, "List.len({})", show(*expr)),
            LMap(a, b) => write!(f, "List.map({}, {})", show(*a), show(*b)),
            LFilter(a, b) => write!(f, "List.filter({}, {})", show(*a), show(*b)),
            LFold(a, b, c) => write!(f, "List.fold({}, {}, {})", show(*a), show(*b), show(*c)),
            Map(fields) | Struct(fields) | ObjectLiteral(fields) => {
                let fields = fields
                    .iter()
                    .map(|(key, value)| format!("{key:?}: {}", show(*value)))
                    .collect::<Vec<_>>()
                    .join(", ");
                match &self.arena.get(self.id).node {
                    Map(_) => write!(f, "Map({fields})"),
                    Struct(_) => write!(f, "Struct({fields})"),
                    ObjectLiteral(_) => write!(f, "{{{fields}}}"),
                    _ => unreachable!(),
                }
            }
            MGet(expr, key) => write!(f, "Map.get({}, {key:?})", show(*expr)),
            SGet(expr, key) => write!(f, "{}.{key}", show(*expr)),
            MInsert(expr, key, value) => {
                write!(f, "Map.insert({}, {key:?}, {})", show(*expr), show(*value))
            }
            MRemove(expr, key) => write!(f, "Map.remove({}, {key:?})", show(*expr)),
            MHasKey(expr, key) => write!(f, "Map.has_key({}, {key:?})", show(*expr)),
            Sin(expr) => write!(f, "sin({})", show(*expr)),
            Cos(expr) => write!(f, "cos({})", show(*expr)),
            Tan(expr) => write!(f, "tan({})", show(*expr)),
            Abs(expr) => write!(f, "abs({})", show(*expr)),
            MonitoredAt(var, node) => write!(f, "monitored_at({var}, {node})"),
            Dist(a, b) => write!(f, "dist({a}, {b})"),
        }
    }
}

impl Display for DsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let aux_vars = self.aux_vars();
        let out_vars = self.output_vars().iter().cloned().collect::<Vec<VarName>>();
        if self.type_annotations.is_empty() {
            for v in &self.input_vars {
                writeln!(f, "in {v}")?;
            }
            for v in &out_vars {
                writeln!(f, "out {v}")?;
            }
            for v in aux_vars {
                writeln!(f, "aux {v}")?;
            }
        } else {
            for v in &self.input_vars {
                let typ = self.type_annotations.get(&v).ok_or(Error)?;
                writeln!(f, "in {v}: {typ}")?;
            }
            for v in &out_vars {
                let typ = self.type_annotations.get(v).ok_or(Error)?;
                writeln!(f, "out {v}: {typ}")?;
            }
            for v in aux_vars {
                let typ = self.type_annotations.get(&v).ok_or(Error)?;
                writeln!(f, "aux {v}: {typ}")?;
            }
        }
        for (v, expression) in &self.exprs {
            writeln!(f, "{v} = {}", expression.display())?;
        }
        Ok(())
    }
}

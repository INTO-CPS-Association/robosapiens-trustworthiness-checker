use std::fmt::{Debug, Display, Error};

use crate::core::{BinaryOperator, StreamType, StreamTypeAscription};
use crate::lang::dsrv::path::TypePath;
use crate::lang::dsrv::source::SourceTypeDisplay;

use super::{
    CheckedDsrvSpecification, CheckedExpr, Declaration, DsrvSpecification, Expr, ExprRef,
    ReconfigurableExprScope, SyntaxLiteral,
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

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.as_ref(), f)
    }
}

impl Display for CheckedDsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt_specification(&self.spec, f, |name, _| self.type_annotation(name))
    }
}

impl Display for ExprRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use super::ExprView::*;

        match self.view() {
            Val(value) => write!(f, "{value}"),
            Var(var) => write!(f, "{var}"),
            Match(scrutinee, arms, shape) => {
                write!(f, "match({scrutinee}) {{ ")?;
                let mut children = arms.into_iter();
                for arm in shape.iter() {
                    write!(f, "{}", arm.pattern)?;
                    if arm.guarded {
                        let guard = children.next().expect("a guarded arm has its guard");
                        write!(f, " if {guard}")?;
                    }
                    let body = children.next().expect("an arm has a body");
                    write!(f, " -> {body}, ")?;
                }
                f.write_str("}")
            }
            Matches(scrutinee, guard, pattern) => {
                write!(f, "matches({scrutinee}, {pattern}")?;
                if let Some(guard) = guard.into_iter().next() {
                    write!(f, " if {guard}")?;
                }
                f.write_str(")")
            }
            // Printed as written: a qualifier the writer gave is kept, and a
            // nullary alternative takes no parentheses.
            Constructor(payload, tag, qualifier) => {
                if let Some(qualifier) = qualifier {
                    write!(f, "{qualifier}::")?;
                }
                write!(f, "{tag}")?;
                match payload.into_iter().next() {
                    Some(payload) => write!(f, "({payload})"),
                    None => Ok(()),
                }
            }
            BinOp(lhs, rhs, operator) => {
                let negative_base = match lhs.view() {
                    super::ExprView::Neg(_) => true,
                    super::ExprView::Val(SyntaxLiteral::Int(value)) => *value < 0,
                    super::ExprView::Val(SyntaxLiteral::Float(value)) => value.is_sign_negative(),
                    _ => false,
                };
                if operator == BinaryOperator::Power && negative_base {
                    write!(f, "(({}) {} {})", lhs, operator.symbol(), rhs)
                } else {
                    write!(f, "({} {} {})", lhs, operator.symbol(), rhs)
                }
            }
            Cast(value, target) => write!(f, "({value} as {target})"),
            Ascribe(value, target) => write!(f, "({value}: {target})"),
            If(cond, yes, no) => write!(f, "(if {} then {} else {})", cond, yes, no),
            SIndex(expr, index) => write!(f, "{}[{index}]", expr),
            Not(expr) => write!(f, "!{expr}"),
            Neg(expr) => write!(f, "-{expr}"),
            Dynamic(source, result_type, scope) => {
                write!(f, "dynamic({}", source)?;
                if let StreamTypeAscription::Ascribed(typ) = result_type {
                    write!(f, ": {}", SourceTypeDisplay(typ))?;
                }
                if let ReconfigurableExprScope::Explicit(vars) = scope {
                    let vars = vars
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, ", {{{vars}}}")?;
                }
                write!(f, ")")
            }
            Defer(source, result_type, scope) => {
                write!(f, "defer({}", source)?;
                if let StreamTypeAscription::Ascribed(typ) = result_type {
                    write!(f, ": {}", SourceTypeDisplay(typ))?;
                }
                if let ReconfigurableExprScope::Explicit(vars) = scope {
                    let vars = vars
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, ", {{{vars}}}")?;
                }
                write!(f, ")")
            }
            Update(a, b) => write!(f, "update({a}, {b})"),
            Default(a, b) => write!(f, "default({a}, {b})"),
            IsDefined(expr) => write!(f, "is_defined({expr})"),
            When(expr) => write!(f, "when({expr})"),
            Latch(a, b) => write!(f, "latch({a}, {b})"),
            Init(a, b) => write!(f, "init({a}, {b})"),
            Lambda(params, body) => {
                let params = params
                    .iter()
                    .map(|(name, ascription)| match ascription {
                        crate::core::StreamTypeAscription::Ascribed(typ) => {
                            format!("{name}: {}", SourceTypeDisplay(typ))
                        }
                        crate::core::StreamTypeAscription::Unascribed => name.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "\\{params} -> {body}")
            }
            Apply(function, args) => {
                write!(f, "{function}(")?;
                for (index, arg) in args.into_iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
            Partial(function, args) => {
                write!(f, "partial({function}")?;
                for arg in args.into_iter() {
                    write!(f, ", ")?;
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
            Fix(function) => write!(f, "fix({function})"),
            List(items) => {
                let items = items
                    .into_iter()
                    .map(|item| format!("{item}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{items}]")
            }
            Tuple(items) => {
                let items = items
                    .into_iter()
                    .map(|item| format!("{item}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Tuple({items})")
            }
            LIndex(a, b) => write!(f, "List.get({a}, {b})"),
            LAppend(a, b) => write!(f, "List.append({a}, {b})"),
            LConcat(a, b) => write!(f, "List.concat({a}, {b})"),
            LHead(expr) => write!(f, "List.head({expr})"),
            LTail(expr) => write!(f, "List.tail({expr})"),
            LLen(expr) => write!(f, "List.len({expr})"),
            LMap(a, b) => write!(f, "List.map({a}, {b})"),
            LFilter(a, b) => write!(f, "List.filter({a}, {b})"),
            LFold(a, b, c) => write!(f, "List.fold({a}, {b}, {c})"),
            Map(fields) => {
                let fields = fields
                    .iter()
                    .map(|(key, value)| format!("{key:?}: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Map({fields})")
            }
            Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|(key, value)| format!("{key:?}: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Struct({fields})")
            }
            ObjectLiteral(fields) => {
                let fields = fields
                    .iter()
                    .map(|(key, value)| format!("{key:?}: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{{{fields}}}")
            }
            MGet(expr, key) => write!(f, "Map.get({expr}, {key:?})"),
            SGet(expr, key) => write!(f, "{expr}.{key}"),
            MInsert(expr, key, value) => {
                write!(f, "Map.insert({expr}, {key:?}, {value})")
            }
            MRemove(expr, key) => write!(f, "Map.remove({expr}, {key:?})"),
            MHasKey(expr, key) => write!(f, "Map.has_key({expr}, {key:?})"),
            Sin(expr) => write!(f, "sin({expr})"),
            Cos(expr) => write!(f, "cos({expr})"),
            Tan(expr) => write!(f, "tan({expr})"),
            Abs(expr) => write!(f, "abs({expr})"),
            Trunc(expr) => write!(f, "trunc({expr})"),
            Floor(expr) => write!(f, "floor({expr})"),
            Ceil(expr) => write!(f, "ceil({expr})"),
            Round(expr) => write!(f, "round({expr})"),
            MonitoredAt(var, node) => write!(f, "monitored_at({var}, {node})"),
            Dist(a, b) => write!(f, "dist({a}, {b})"),
        }
    }
}

impl Display for DsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source_context.language().header())?;
        fmt_specification(self, f, |_, annotation| annotation)
    }
}

fn fmt_specification<'a>(
    spec: &'a DsrvSpecification,
    f: &mut std::fmt::Formatter<'_>,
    annotation_for: impl Fn(&'a crate::VarName, Option<&'a StreamType>) -> Option<&'a StreamType>,
) -> std::fmt::Result {
    for entry in &spec.declarations {
        match entry {
            Declaration::Input {
                name, annotation, ..
            }
            | Declaration::Output {
                name, annotation, ..
            }
            | Declaration::Aux {
                name, annotation, ..
            } => {
                let keyword = match entry {
                    Declaration::Input { .. } => "in",
                    Declaration::Output { .. } => "out",
                    Declaration::Aux { .. } => "aux",
                    Declaration::Equation { .. } | Declaration::TypeAlias { .. } => unreachable!(),
                };
                write!(f, "{keyword} {name}")?;
                if let Some(typ) = annotation_for(name, annotation.as_ref()) {
                    write!(f, ": {}", SourceTypeDisplay(typ))?;
                }
                writeln!(f)?;
            }
            Declaration::Equation { name, .. } => {
                let expression = spec.exprs.get(name).ok_or(Error)?;
                writeln!(f, "{name} = {expression}")?;
            }
            Declaration::TypeAlias { name, .. } => {
                let ty = spec
                    .source_context
                    .get(&TypePath::local(name.clone()))
                    .ok_or(Error)?;
                writeln!(f, "type {name} = {}", SourceTypeDisplay(ty))?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::parser::parse_expr;
    use crate::{TypeCheckOptions, VarName, core::StreamType};

    #[test]
    fn power_display_parenthesizes_direct_negative_value_bases() {
        for value in [
            SyntaxLiteral::Int(-2),
            SyntaxLiteral::Float(-2.0),
            SyntaxLiteral::Float(f64::NEG_INFINITY),
            SyntaxLiteral::Float(-0.0),
        ] {
            let expression = Expr::BinOp(
                Box::new(Expr::Val(value)),
                Box::new(Expr::Val(SyntaxLiteral::Int(2))),
                BinaryOperator::Power,
            );
            let displayed = expression.to_string();
            assert!(
                displayed.starts_with("(("),
                "negative base was not parenthesized: {displayed}"
            );
            let reparsed = parse_expr(&displayed).unwrap();
            assert!(matches!(
                reparsed.as_ref().view(),
                super::super::ExprView::BinOp(lhs, _, BinaryOperator::Power)
                    if matches!(lhs.view(), super::super::ExprView::Neg(_))
            ));
        }
    }

    #[test]
    fn checked_display_uses_inferred_annotation_projection() {
        let checked = crate::dsrv_fixtures::checked_with("out y\ny = 1", TypeCheckOptions::GRADUAL);

        assert_eq!(
            checked.type_annotation(&VarName::new("y")),
            Some(&StreamType::Int)
        );
        let displayed = checked.to_string();
        assert_eq!(displayed, "out y: Int\ny = 1\n");
        crate::dsrv_fixtures::checked(&displayed);
    }

    #[test]
    fn raw_display_preserves_annotations_for_duplicate_declaration_occurrences() {
        let raw = "in x: Int\nin x: Bool\nout y\ny = x"
            .parse::<DsrvSpecification>()
            .expect("raw parsing retains declaration occurrences");

        assert_eq!(raw.to_string(), "in x: Int\nin x: Bool\nout y\ny = x\n");
    }
}

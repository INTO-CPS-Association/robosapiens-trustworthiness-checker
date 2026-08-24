use std::fmt::{Debug, Display, Error};

use crate::core::{BinaryOperator, StreamTypeAscription};

use super::{
    CheckedDsrvSpecification, CheckedExpr, DsrvSpecification, Expr, ExprRef,
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
        Display::fmt(&self.spec, f)
    }
}

impl Display for ExprRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use super::ExprView::*;

        match self.view() {
            Val(value) => write!(f, "{value}"),
            Var(var) => write!(f, "{var}"),
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
            If(cond, yes, no) => write!(f, "(if {} then {} else {})", cond, yes, no),
            SIndex(expr, index) => write!(f, "{}[{index}]", expr),
            Not(expr) => write!(f, "!{expr}"),
            Neg(expr) => write!(f, "-{expr}"),
            Dynamic(source, result_type, scope) => {
                write!(f, "dynamic({}", source)?;
                if let StreamTypeAscription::Ascribed(typ) = result_type {
                    write!(f, ": {typ}")?;
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
                    write!(f, ": {typ}")?;
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
                    .map(|(name, typ)| format!("{name}: {typ}"))
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
            MonitoredAt(var, node) => write!(f, "monitored_at({var}, {node})"),
            Dist(a, b) => write!(f, "dist({a}, {b})"),
        }
    }
}

impl Display for DsrvSpecification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.type_annotations.is_empty() {
            for v in &self.input_order {
                writeln!(f, "in {v}")?;
            }
            for v in &self.output_order {
                writeln!(f, "out {v}")?;
            }
            for v in &self.aux_order {
                writeln!(f, "aux {v}")?;
            }
        } else {
            for v in &self.input_order {
                let typ = self.type_annotations.get(v).ok_or(Error)?;
                writeln!(f, "in {v}: {typ}")?;
            }
            for v in &self.output_order {
                let typ = self.type_annotations.get(v).ok_or(Error)?;
                writeln!(f, "out {v}: {typ}")?;
            }
            for v in &self.aux_order {
                let typ = self.type_annotations.get(v).ok_or(Error)?;
                writeln!(f, "aux {v}: {typ}")?;
            }
        }
        for v in &self.assignment_order {
            let expression = self.exprs.get(v).ok_or(Error)?;
            writeln!(f, "{v} = {expression}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::dsrv::parser::parse_expr;

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
}

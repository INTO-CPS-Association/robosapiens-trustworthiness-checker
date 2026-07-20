// From: https://stackoverflow.com/questions/51121446/how-do-i-assert-an-enum-is-a-specific-variant-if-i-dont-care-about-its-fields
#[macro_export]
macro_rules! is_enum_variant {
    ($v:expr, $p:pat) => {
        if let $p = $v { true } else { false }
    };
}

// Creates a struct name with the given name, and implements AsyncConfig for it with the specified
// associated types
// E.g.: define_config!(ValueConfig, Val = Value, Expr = ExprKind, Ctx = Context, Spec = DsrvSpecification);
// Creates the struct ValueConfig with AsyncConfig implementation where Val = Value, Expr = ExprKind,
// Ctx = Context<ValueConfig>, and Spec = DsrvSpecification
#[macro_export]
macro_rules! define_config {
    ($name:ident, Val=$val:ty, Expr=$expr:ty, Ctx=$ctx:ident, Spec=$spec:ty) => {
        #[derive(Clone)]
        pub struct $name;

        impl AsyncConfig for $name {
            type Val = $val;
            type Expr = $expr;
            type Ctx = $ctx<Self>;
            type Spec = $spec;
        }
    };
}

/// Construct a DSRV expression for tests.
///
/// The syntax intentionally mirrors `ExprKind` while omitting boxes and operator enum wrappers.
#[doc(hidden)]
#[macro_export]
macro_rules! __dsrv_construct_node {
    ($builder:ident; $variant:ident($($field:expr),* $(,)?)) => {
        $builder.$variant($($field),*)
    };
}

#[macro_export]
macro_rules! sexpr {
    ($variant:ident $args:tt) => {
        {
            let mut builder = $crate::lang::dsrv::ast::ExprBuilder::with_capacity(0);
            let root = $crate::__sexpr_node!(builder; $variant $args);
            builder.finish(root)
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __sexpr_op {
    (Add) => {
        $crate::lang::dsrv::ast::SBinOp::NOp($crate::lang::dsrv::ast::NumericalBinOp::Add)
    };
    (Sub) => {
        $crate::lang::dsrv::ast::SBinOp::NOp($crate::lang::dsrv::ast::NumericalBinOp::Sub)
    };
    (Mul) => {
        $crate::lang::dsrv::ast::SBinOp::NOp($crate::lang::dsrv::ast::NumericalBinOp::Mul)
    };
    (Div) => {
        $crate::lang::dsrv::ast::SBinOp::NOp($crate::lang::dsrv::ast::NumericalBinOp::Div)
    };
    (Mod) => {
        $crate::lang::dsrv::ast::SBinOp::NOp($crate::lang::dsrv::ast::NumericalBinOp::Mod)
    };
    (Or) => {
        $crate::lang::dsrv::ast::SBinOp::BOp($crate::lang::dsrv::ast::BoolBinOp::Or)
    };
    (And) => {
        $crate::lang::dsrv::ast::SBinOp::BOp($crate::lang::dsrv::ast::BoolBinOp::And)
    };
    (Impl) => {
        $crate::lang::dsrv::ast::SBinOp::BOp($crate::lang::dsrv::ast::BoolBinOp::Impl)
    };
    (Concat) => {
        $crate::lang::dsrv::ast::SBinOp::SOp($crate::lang::dsrv::ast::StrBinOp::Concat)
    };
    (Eq) => {
        $crate::lang::dsrv::ast::SBinOp::COp($crate::lang::dsrv::ast::CompBinOp::Eq)
    };
    (Le) => {
        $crate::lang::dsrv::ast::SBinOp::COp($crate::lang::dsrv::ast::CompBinOp::Le)
    };
    (Ge) => {
        $crate::lang::dsrv::ast::SBinOp::COp($crate::lang::dsrv::ast::CompBinOp::Ge)
    };
    (Lt) => {
        $crate::lang::dsrv::ast::SBinOp::COp($crate::lang::dsrv::ast::CompBinOp::Lt)
    };
    (Gt) => {
        $crate::lang::dsrv::ast::SBinOp::COp($crate::lang::dsrv::ast::CompBinOp::Gt)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __sexpr_node {
    ($builder:ident; Val($value:expr)) => {
        $crate::__dsrv_construct_node!($builder; Val(($value).into()))
    };
    ($builder:ident; Var($name:expr)) => {
        $crate::__dsrv_construct_node!($builder; Var(($name).into()))
    };
    ($builder:ident; MonitoredAt($var:expr, $node:expr)) => {
        $crate::__dsrv_construct_node!($builder; MonitoredAt(($var).into(), ($node).into()))
    };
    ($builder:ident; BinOp($lv:ident $la:tt, $op:ident, $rv:ident $ra:tt)) => {{
        let left = $crate::__sexpr_node!($builder; $lv $la);
        let right = $crate::__sexpr_node!($builder; $rv $ra);
        $crate::__dsrv_construct_node!($builder; BinOp(left, right, $crate::__sexpr_op!($op)))
    }};
    ($builder:ident; If($cv:ident $ca:tt, $tv:ident $ta:tt, $ev:ident $ea:tt)) => {{
        let condition = $crate::__sexpr_node!($builder; $cv $ca);
        let then_expr = $crate::__sexpr_node!($builder; $tv $ta);
        let else_expr = $crate::__sexpr_node!($builder; $ev $ea);
        $crate::__dsrv_construct_node!($builder; If(condition, then_expr, else_expr))
    }};
    ($builder:ident; Dynamic($cv:ident $ca:tt, $result_type:expr $(,)?)) => {{
        let source = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; Dynamic(
            source,
            $result_type,
            $crate::lang::dsrv::ast::DynamicExprScope::Automatic
        ))
    }};
    ($builder:ident; RestrictedDynamic($cv:ident $ca:tt, $result_type:expr, [$($var:expr),* $(,)?])) => {{
        let source = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; Dynamic(
            source,
            $result_type,
            $crate::lang::dsrv::ast::DynamicExprScope::Explicit(vec![$(($var).into()),*].into())
        ))
    }};
    ($builder:ident; Defer($cv:ident $ca:tt, $result_type:expr, [])) => {{
        let source = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; Defer(
            source,
            $result_type,
            $crate::lang::dsrv::ast::DynamicExprScope::Automatic
        ))
    }};
    ($builder:ident; Defer($cv:ident $ca:tt, $result_type:expr, [$($var:expr),+ $(,)?])) => {{
        let source = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; Defer(
            source,
            $result_type,
            $crate::lang::dsrv::ast::DynamicExprScope::Explicit(vec![$(($var).into()),+].into())
        ))
    }};
    ($builder:ident; SIndex($cv:ident $ca:tt, $index:expr)) => {{
        let child = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; SIndex(child, $index))
    }};
    ($builder:ident; MGet($cv:ident $ca:tt, $key:expr)) => {{
        let child = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; MGet(child, ($key).into()))
    }};
    ($builder:ident; SGet($cv:ident $ca:tt, $key:expr)) => {{
        let child = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; SGet(child, ($key).into()))
    }};
    ($builder:ident; MRemove($cv:ident $ca:tt, $key:expr)) => {{
        let child = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; MRemove(child, ($key).into()))
    }};
    ($builder:ident; MHasKey($cv:ident $ca:tt, $key:expr)) => {{
        let child = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; MHasKey(child, ($key).into()))
    }};
    ($builder:ident; MInsert($mv:ident $ma:tt, $key:expr, $vv:ident $va:tt)) => {{
        let map = $crate::__sexpr_node!($builder; $mv $ma);
        let value = $crate::__sexpr_node!($builder; $vv $va);
        $crate::__dsrv_construct_node!($builder; MInsert(map, ($key).into(), value))
    }};
    ($builder:ident; List[$($variant:ident $args:tt),* $(,)?]) => {{
        let items = vec![$($crate::__sexpr_node!($builder; $variant $args)),*].into();
        $crate::__dsrv_construct_node!($builder; List(items))
    }};
    ($builder:ident; Tuple[$($variant:ident $args:tt),* $(,)?]) => {{
        let items = vec![$($crate::__sexpr_node!($builder; $variant $args)),*].into();
        $crate::__dsrv_construct_node!($builder; Tuple(items))
    }};
    ($builder:ident; Map{$($key:expr => $variant:ident $args:tt),* $(,)?}) => {{
        let fields = [$(($key.into(), $crate::__sexpr_node!($builder; $variant $args))),*]
            .into_iter()
            .collect();
        $crate::__dsrv_construct_node!($builder; Map(fields))
    }};
    ($builder:ident; Struct{$($key:expr => $variant:ident $args:tt),* $(,)?}) => {{
        let fields = [$(($key.into(), $crate::__sexpr_node!($builder; $variant $args))),*]
            .into_iter()
            .collect();
        $crate::__dsrv_construct_node!($builder; Struct(fields))
    }};
    ($builder:ident; ObjectLiteral{$($key:expr => $variant:ident $args:tt),* $(,)?}) => {{
        let fields = [$(($key.into(), $crate::__sexpr_node!($builder; $variant $args))),*]
            .into_iter()
            .collect();
        $crate::__dsrv_construct_node!($builder; ObjectLiteral(fields))
    }};
    ($builder:ident; Existing($expr:expr)) => {
        $builder.clone_tree(&$expr)
    };
    ($builder:ident; $name:ident($cv:ident $ca:tt)) => {{
        let child = $crate::__sexpr_node!($builder; $cv $ca);
        $crate::__dsrv_construct_node!($builder; $name(child))
    }};
    ($builder:ident; $name:ident($lv:ident $la:tt, $rv:ident $ra:tt)) => {{
        let left = $crate::__sexpr_node!($builder; $lv $la);
        let right = $crate::__sexpr_node!($builder; $rv $ra);
        $crate::__dsrv_construct_node!($builder; $name(left, right))
    }};
}

/// Parse a complete DSRV test specification with the production LALR parser.
///
/// `dsrv_spec!(strict "...")` and `dsrv_spec!(gradual "...")` additionally
/// run the corresponding type checker and return a `CheckedDsrvSpecification`.
#[macro_export]
macro_rules! dsrv_spec {
    (strict $source:literal) => {{
        $crate::lang::dsrv::type_checker::type_check($crate::dsrv_spec!($source))
            .expect("test DSRV specification should type check")
    }};
    (gradual $source:literal) => {{
        $crate::lang::dsrv::type_checker::type_check_gradual($crate::dsrv_spec!($source))
            .expect("test DSRV specification should type check gradually")
    }};
    ($source:literal) => {{
        $crate::lang::dsrv::lalr_parser::parse_str($source)
            .expect("test DSRV specification should parse")
    }};
}

/// A shorthand attribute for `#[test(apply(smol_test))]` used to create async
/// tests
///
/// This sets up logging and creates an executor for use in the test
///
/// ## Usage
/// ```rust
/// use macro_rules_attribute::apply;
/// use trustworthiness_checker::async_test;
///
/// #[apply(async_test)]
/// async fn my_async_test(executor: Rc<LocalExecutor<'static>>) {
///     // test body
/// }
/// ```
///
/// or with a Result:
///
/// ```rust
/// use macro_rules_attribute::apply;
/// use trustworthiness_checker::async_test;
///
/// #[apply(async_test)]
/// async fn my_async_test(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
///     // test body
/// }
/// ```
#[macro_export]
macro_rules! async_test {
    (
        $(#[$attr:meta])*
        async fn $name:ident $($rest:tt)*
    ) => {
        $(#[$attr])*
        #[test_log::test(macro_rules_attribute::apply(smol_macros::test))]
        async fn $name $($rest)*
    };
}

#[cfg(test)]
mod tests {
    use crate::lang::dsrv::ast::{Expr, NumericalBinOp, SBinOp};
    use crate::lang::dsrv::span::strip_span;
    use crate::lang::dsrv::type_checker::TCType;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;
    use test_log::test;

    #[test]
    fn example() {
        assert!(is_enum_variant!(Some(42), Some(_)));
    }

    #[test]
    fn sexpr_builds_nested_expression() {
        let expression = sexpr!(If(Var("enabled"), BinOp(Val(40), Add, Val(2)), Val(0)));

        let expected = Expr::If(
            Box::new(Expr::Var("enabled".into())),
            Box::new(Expr::BinOp(
                Box::new(Expr::Val(40)),
                Box::new(Expr::Val(2)),
                SBinOp::NOp(NumericalBinOp::Add),
            )),
            Box::new(Expr::Val(0)),
        );
        assert_eq!(strip_span(&expression), strip_span(&expected));
    }

    #[test]
    fn dsrv_spec_parses_complete_specification() {
        let specification = dsrv_spec!("in x: Int\nout y: Int\ny = x + 1");

        assert!(specification.input_vars.contains(&"x".into()));
        assert!(specification.output_vars.contains(&"y".into()));
        assert!(specification.exprs.contains_key(&"y".into()));
    }

    #[test]
    fn dsrv_spec_can_strictly_type_check_specification() {
        let specification = dsrv_spec!(strict "in x: Int\nout y: Int\ny = x + 1");

        assert_eq!(
            specification
                .var_expr(&"y".into())
                .unwrap()
                .typ()
                .to_string(),
            "Int"
        );
    }

    #[test]
    fn dsrv_spec_can_gradually_type_check_specification() {
        let specification = dsrv_spec!(gradual "in x\nout y\ny = x + 1");

        assert_eq!(
            specification.var_expr(&"y".into()).unwrap().typ(),
            &TCType::Int
        );
    }

    // Example usage of the new #[apply(async_test)] attribute
    #[apply(async_test)]
    async fn test_async_test_with_executor(executor: Rc<LocalExecutor<'static>>) {
        // Test that the executor parameter is properly passed through
        let result = executor.spawn(async { 42 }).await;
        assert_eq!(result, 42);
    }

    #[apply(async_test)]
    async fn test_async_test_without_executor() {
        // Test without executor parameter
        let value = async { "hello" }.await;
        assert_eq!(value, "hello");
    }

    #[ignore]
    #[apply(async_test)]
    async fn test_async_test_with_attributes(_executor: Rc<LocalExecutor<'static>>) {
        // Test that additional attributes like #[ignore] work correctly
        panic!("This test is ignored so it won't fail the build");
    }

    #[apply(async_test)]
    async fn test_async_test_with_return_type() -> Result<(), &'static str> {
        // Test that the macro works with functions that have return types
        let result = async { 42 }.await;
        assert_eq!(result, 42);
        Ok(())
    }

    //     #[apply(async_test)]
    //     async fn test_async_test_multiline_signature(executor: Rc<LocalExecutor<'static>>) {
    //         // Test that the macro works with multiline function signatures and return types
    //         let result = executor
    //             .spawn(async {
    //                 smol::Timer::after(std::time::Duration::from_millis(1)).await;
    //                 "multiline test completed"
    //             })
    //             .await;

    //         assert_eq!(result, "multiline test completed");
    //         Ok(())
    //     }
}

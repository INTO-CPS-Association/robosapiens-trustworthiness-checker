// From: https://stackoverflow.com/questions/51121446/how-do-i-assert-an-enum-is-a-specific-variant-if-i-dont-care-about-its-fields
#[macro_export]
macro_rules! is_enum_variant {
    ($v:expr, $p:pat) => {
        if let $p = $v { true } else { false }
    };
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
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;
    use test_log::test;

    #[test]
    fn example() {
        assert!(is_enum_variant!(Some(42), Some(_)));
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

    #[apply(async_test)]
    async fn test_async_test_multiline_signature(
        executor: Rc<LocalExecutor<'static>>,
    ) -> anyhow::Result<()> {
        // Test that the macro works with multiline function signatures and return types
        let result = executor
            .spawn(async {
                smol::Timer::after(std::time::Duration::from_millis(1)).await;
                "multiline test completed"
            })
            .await;

        assert_eq!(result, "multiline test completed");
        Ok(())
    }
}

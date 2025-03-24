pub mod untimed_typed_lola;
pub use untimed_typed_lola::TypedUntimedLolaSemantics;
pub mod untimed_untyped_lola;
pub use untimed_untyped_lola::UntimedLolaSemantics;
pub mod distributed;

#[cfg(test)]
mod tests {
    use crate::Value;

    use super::*;
    use approx::relative_eq;
    use proptest::prelude::*;
    use smol::stream::{self, StreamExt};
    use test_log::test;
    use untimed_typed_lola::combinators as tc;
    use untimed_untyped_lola::combinators as uc;

    // Property-based testing that the typed and untyped combinators
    // are equivalent on well-typed inputs
    proptest! {
        #[test]
        fn test_typed_untyped_not(xs: Vec<bool>) {
            let xs_typed_stream = Box::pin(stream::iter(xs.clone().into_iter()));
            let xs_untyped_stream = Box::pin(stream::iter(xs.clone().into_iter()).map(Value::Bool));

            let ys_typed = tc::not(xs_typed_stream).map(Value::Bool);
            let ys_typed: Vec<_> = smol::block_on(ys_typed.collect());
            let ys_untyped = uc::not(xs_untyped_stream);
            let ys_untyped: Vec<_> = smol::block_on(ys_untyped.collect());

            prop_assert_eq!(ys_typed, ys_untyped);
        }

        #[test]
        fn test_typed_untyped_int_int(xs: Vec<i32>, ys: Vec<i32>) {
            // Construct function pointers to the typed and untyped combinators
            let mut tc_plus = tc::plus;
            let mut tc_minus = tc::minus;
            let mut tc_mult = tc::mult;
            let mut tc_div = tc::div;
            let mut uc_plus = uc::plus;
            let mut uc_minus = uc::minus;
            let mut uc_mult = uc::mult;
            let mut uc_div = uc::div;

            let ops: Vec<(&mut dyn FnMut(_, _) -> _, &mut dyn FnMut(_, _) -> _)> = vec![(&mut tc_plus, &mut uc_plus), (&mut tc_minus, &mut uc_minus), (&mut tc_mult, &mut uc_mult), (&mut tc_div, &mut uc_div)];

            // Convert to i64 to avoid overflow
            let xs: Vec<i64> = xs.iter().map(|&x| x as i64).collect();
            let ys: Vec<i64> = ys.iter().map(|&y| y as i64).collect();

            for (typed_op, untyped_op) in ops {
                // Create distinct typed and untyped input and output streams
                let xs_typed_stream = Box::pin(stream::iter(xs.clone().into_iter()));
                let xs_untyped_stream = Box::pin(stream::iter(xs.clone().into_iter()).map(Value::Int));
                let ys_typed_stream = Box::pin(stream::iter(ys.clone().into_iter()));
                let ys_untyped_stream = Box::pin(stream::iter(ys.clone().into_iter()).map(Value::Int));

                // Apply the typed and untyped operators to the input streams
                let zs_typed = typed_op(xs_typed_stream, ys_typed_stream).map(Value::Int);
                let zs_typed: Vec<_> = smol::block_on(zs_typed.collect());
                let zs_untyped = untyped_op(xs_untyped_stream, ys_untyped_stream);
                let zs_untyped: Vec<_> = smol::block_on(zs_untyped.collect());

                // Assert that the typed and untyped outputs are equal
                prop_assert_eq!(zs_typed, zs_untyped);
            }
        }

        #[test]
        fn test_typed_untyped_float_float(xs: Vec<f32>, ys: Vec<f32>) {
            // Construct function pointers to the typed and untyped combinators
            let mut tc_plus = tc::plus;
            let mut tc_minus = tc::minus;
            let mut tc_mult = tc::mult;
            let mut tc_div = tc::div;
            let mut tc_modulo = tc::modulo;
            let mut uc_plus = uc::plus;
            let mut uc_minus = uc::minus;
            let mut uc_mult = uc::mult;
            let mut uc_div = uc::div;
            let mut uc_modulo = uc::modulo;

            let ops: Vec<(&str, &mut dyn FnMut(_, _) -> _, &mut dyn FnMut(_, _) -> _)> = vec![("plus", &mut tc_plus, &mut uc_plus), ("minus", &mut tc_minus, &mut uc_minus), ("mult", &mut tc_mult, &mut uc_mult), ("div", &mut tc_div, &mut uc_div), ("modulo", &mut tc_modulo, &mut uc_modulo)];

            for (op_name, typed_op, untyped_op) in ops {
                // Create distinct typed and untyped input and output streams
                let xs_typed_stream = Box::pin(stream::iter(xs.clone().into_iter()));
                let xs_untyped_stream = Box::pin(stream::iter(xs.clone().into_iter()).map(Value::Float));
                let ys_typed_stream = Box::pin(stream::iter(ys.clone().into_iter()));
                let ys_untyped_stream = Box::pin(stream::iter(ys.clone().into_iter()).map(Value::Float));

                // Apply the typed and untyped operators to the input streams
                let zs_typed = typed_op(xs_typed_stream, ys_typed_stream).map(Value::Float);
                let zs_typed: Vec<_> = smol::block_on(zs_typed.collect());
                let zs_untyped = untyped_op(xs_untyped_stream, ys_untyped_stream);
                let zs_untyped: Vec<_> = smol::block_on(zs_untyped.collect());

                // Assert that the typed and untyped outputs are equal
                prop_assert_eq!(
                    zs_typed.len(), zs_untyped.len()
                );
                for (t, u) in zs_typed.iter().zip(zs_untyped.iter()) {
                    match (t, u) {
                        (Value::Float(t), Value::Float(u)) => {
                            prop_assert!(t.is_nan() && u.is_nan() ||relative_eq!(t, u, epsilon = 1e-4, max_relative = 1e-2), "Expected {:?} to be approximately equal to {:?}after op {}", t, u, op_name);
                        }
                        _ => panic!("Expected floats"),
                    }
                }
            }
        }
    }
}

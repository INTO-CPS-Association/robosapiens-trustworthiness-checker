use super::*;
#[cfg(feature = "jit")]
use crate::dataflow::{JitConfig, JitPlan};
use crate::lang::dsrv::ast::CheckedDsrvSpecification;

#[test]
fn typed_monitor_mixed_rows_avoid_the_value_interface() {
    let specification = "in x: Int\nin scale: Float\nout result: Float\nresult = x * scale"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor =
        TypedDataflowMonitor::<(i64, f64), (f64,)>::compile_checked(specification).unwrap();
    assert_eq!(monitor.evaluate(&(4, 1.5)), (6.0,));
    assert_eq!(monitor.evaluate(&(4, 0.5)), (2.0,));
}

#[test]
fn typed_monitor_reports_a_non_concrete_output_rather_than_guessing() {
    // Sparse ticks are not representable in a typed row. The first tick of a delayed stream has
    // no previous value, so the monitor must surface that instead of inventing a scalar.
    let specification = "in x: Int\nout result: Int\nresult = x[1]"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor =
        TypedDataflowMonitor::<(i64,), (i64,)>::compile_checked(specification).unwrap();
    assert!(matches!(
        monitor.try_evaluate(&(4,)),
        Err(TypedEvaluationError::NonConcreteOutput { index: 0 })
    ));
    assert_eq!(monitor.try_evaluate(&(7,)).unwrap(), (4,));
}

#[cfg(feature = "jit")]
#[test]
fn jit_direct_uses_native_mixed_tuple_layout() {
    let specification = "in x: Int\nin scale: Float\nout result: Float\nout alert: Bool\nresult = x * scale\nalert = result > 3.5"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
    let mut monitor =
        TypedJitMonitor::<(i64, f64), (f64, bool)>::compile_checked(specification).unwrap();
    assert_eq!(monitor.evaluate(&(2, 2.0)), (4.0, true));
    assert_eq!(monitor.evaluate(&(1, 2.0)), (2.0, false));
}

#[cfg(feature = "jit")]
#[test]
fn jit_direct_supports_integer_division_and_remainder() {
    let specification = "in x: Int\nin divisor: Int\nout quotient: Int\nout remainder: Int\nquotient = x / divisor\nremainder = x % divisor"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
    let mut monitor =
        TypedJitMonitor::<(i64, i64), (i64, i64)>::compile_checked(specification).unwrap();

    assert_eq!(monitor.evaluate(&(17, 5)), (3, 2));
    assert_eq!(monitor.evaluate(&(-17, 5)), (-3, -2));
    assert_eq!(monitor.evaluate(&(i64::MIN, -1)), (i64::MIN, 0));
}

#[cfg(feature = "jit")]
#[test]
#[should_panic(expected = "integer division by zero in direct JIT monitor")]
fn jit_direct_rejects_zero_integer_divisors_without_a_hardware_trap() {
    let specification = "in x: Int\nin divisor: Int\nout result: Int\nresult = x / divisor"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor =
        TypedJitMonitor::<(i64, i64), (i64,)>::compile_checked(specification).unwrap();

    monitor.evaluate(&(17, 0));
}

#[cfg(feature = "jit")]
#[test]
fn jit_direct_preserves_temporal_window_state() {
    let specification = "in x: Int\nout result: Bool\nresult = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
    let mut monitor = TypedJitMonitor::<(i64,), (bool,)>::compile_checked(specification).unwrap();

    let outputs = [1, 4, 5, 6, 2].map(|input| monitor.evaluate(&(input,)).0);
    assert_eq!(outputs, [false, false, false, true, false]);
}

#[cfg(feature = "jit")]
#[test]
fn jit_direct_preserves_recursive_accumulator_state() {
    let specification = "in x: Int\nout result: Int\nresult = default(result[1], 0) + x"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = TypedJitMonitor::<(i64,), (i64,)>::compile_checked(specification).unwrap();

    let outputs = [1, 2, 3, 4].map(|input| monitor.evaluate(&(input,)).0);
    assert_eq!(outputs, [1, 3, 6, 10]);
}

#[cfg(feature = "jit")]
#[test]
fn jit_direct_temporal_supports_dynamic_integer_remainder() {
    let specification =
        "in x: Int\nin divisor: Int\nout result: Int\nresult = default(x[1], 0) % divisor"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
    let mut monitor =
        TypedJitMonitor::<(i64, i64), (i64,)>::compile_checked(specification).unwrap();

    assert_eq!(monitor.evaluate(&(17, 5)), (0,));
    assert_eq!(monitor.evaluate(&(23, 5)), (2,));
    assert_eq!(monitor.evaluate(&(9, 4)), (3,));
}

#[cfg(feature = "jit")]
#[test]
fn jit_direct_temporal_uses_native_mixed_layout_and_stream_ssa() {
    let specification = "in x: Int\nin scale: Float\nout result: Float\nout alert: Bool\nresult = default(x[1], 0) * scale\nalert = result > 3.5"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
    let mut monitor =
        TypedJitMonitor::<(i64, f64), (f64, bool)>::compile_checked(specification).unwrap();

    assert_eq!(monitor.evaluate(&(2, 2.0)), (0.0, false));
    assert_eq!(monitor.evaluate(&(3, 2.0)), (4.0, true));
    assert_eq!(monitor.evaluate(&(1, 0.5)), (1.5, false));
}

#[cfg(feature = "jit")]
#[test]
fn typed_monitor_activates_direct_jit_after_normal_warmup() {
    crate::dataflow::execution::jit::reset_compile_count();
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = TypedDataflowMonitor::<(i64,), (i64,)>::compile_checked_with_jit(
        specification,
        JitConfig::after_events(2),
    )
    .unwrap();

    assert!(!monitor.is_direct_jit_active());
    assert_eq!(monitor.evaluate(&(1,)), (2,));
    assert!(!monitor.is_direct_jit_active());
    assert_eq!(monitor.evaluate(&(2,)), (3,));
    assert!(!monitor.is_direct_jit_active());
    assert_eq!(monitor.evaluate(&(3,)), (4,));
    assert!(monitor.is_direct_jit_active());
    assert_eq!(crate::dataflow::execution::jit::compile_count(), 1);
    assert_eq!(monitor.jit_report().unwrap().plan(), JitPlan::WholeSchedule);
    assert_eq!(monitor.evaluate(&(4,)), (5,));
}

#[cfg(feature = "jit")]
#[test]
fn typed_monitor_preserves_window_state_across_direct_activation() {
    let specification = "in x: Int\nout result: Bool\nresult = x > 3 && default(x[1], 4) > 3 && default(x[2], 4) > 3"
            .parse::<CheckedDsrvSpecification>()
            .unwrap();
    let mut monitor = TypedDataflowMonitor::<(i64,), (bool,)>::compile_checked_with_jit(
        specification,
        JitConfig::after_events(2),
    )
    .unwrap();

    let outputs = [1, 4, 5, 6, 2].map(|input| monitor.evaluate(&(input,)).0);
    assert_eq!(outputs, [false, false, false, true, false]);
    assert!(monitor.is_direct_jit_active());
}

#[cfg(feature = "jit")]
#[test]
fn typed_monitor_keeps_warm_executor_when_direct_extraction_fails() {
    let specification = "in x: Int\nout result: Int\nresult = x + 1"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = TypedDataflowMonitor::<(i64,), (i64,)>::compile_checked_with_jit(
        specification,
        JitConfig::after_events(1),
    )
    .unwrap();

    assert_eq!(monitor.evaluate(&(1,)), (2,));
    monitor.fail_next_direct_extraction();
    assert!(matches!(
        monitor.try_evaluate(&(2,)),
        Err(TypedEvaluationError::DirectActivation)
    ));
    assert!(!monitor.is_direct_jit_active());
    assert_eq!(monitor.evaluate(&(3,)), (4,));
    assert!(monitor.is_direct_jit_active());
}

#[cfg(feature = "jit")]
#[test]
fn typed_monitor_preserves_temporal_state_when_direct_extraction_fails() {
    let specification = "in x: Int\nout result: Int\nresult = default(result[1], 0) + x"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = TypedDataflowMonitor::<(i64,), (i64,)>::compile_checked_with_jit(
        specification,
        JitConfig::after_events(1),
    )
    .unwrap();

    assert_eq!(monitor.evaluate(&(1,)), (1,));
    monitor.fail_next_direct_extraction();
    assert!(matches!(
        monitor.try_evaluate(&(2,)),
        Err(TypedEvaluationError::DirectActivation)
    ));
    assert!(!monitor.is_direct_jit_active());
    assert_eq!(monitor.evaluate(&(3,)), (6,));
    assert!(monitor.is_direct_jit_active());
    assert_eq!(monitor.evaluate(&(4,)), (10,));
}

#[cfg(feature = "jit")]
#[test]
fn typed_monitor_preserves_recursive_state_across_direct_activation() {
    let specification = "in x: Int\nout result: Int\nresult = default(result[1], 0) + x"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    let mut monitor = TypedDataflowMonitor::<(i64,), (i64,)>::compile_checked_with_jit(
        specification,
        JitConfig::after_events(2),
    )
    .unwrap();

    let outputs = [1, 2, 3, 4].map(|input| monitor.evaluate(&(input,)).0);
    assert_eq!(outputs, [1, 3, 6, 10]);
    assert!(monitor.is_direct_jit_active());
}

#[test]
fn binding_rejects_wrong_tuple_kind() {
    let specification = "in x: Int\nout result: Bool\nresult = x > 3"
        .parse::<CheckedDsrvSpecification>()
        .unwrap();
    assert!(matches!(
        TypedDataflowMonitor::<(f64,), (bool,)>::compile_checked(specification),
        Err(TypedBindingError::TypeMismatch { side: "input", .. })
    ));
}

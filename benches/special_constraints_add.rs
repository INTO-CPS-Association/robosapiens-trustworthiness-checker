use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::{criterion_group, criterion_main};
use std::collections::BTreeMap;
use trustworthiness_checker::Value;
use trustworthiness_checker::dep_manage::interface::DependencyKind;
use trustworthiness_checker::dep_manage::interface::create_dependency_manager;
use trustworthiness_checker::runtime::constraints::runtime::ConstraintBasedRuntime;

pub fn spec_simple_add_monitor() -> &'static str {
    "in x\n\
     in y\n\
     out z\n\
     z = x + y"
}

async fn monitor_outputs_untyped_constraints(num_outputs: usize, dependency_kind: DependencyKind) {
    let size = num_outputs as i64;
    let mut xs = (0..size).map(|i| Value::Int(i * 2));
    let mut ys = (0..size).map(|i| Value::Int(i * 2 + 1));
    let spec = trustworthiness_checker::lola_specification(&mut spec_simple_add_monitor()).unwrap();
    let mut runtime =
        ConstraintBasedRuntime::new(create_dependency_manager(dependency_kind, spec.clone()));
    runtime.store_from_spec(spec);

    for _ in 0..size {
        let inputs = BTreeMap::from([
            ("x".into(), xs.next().unwrap()),
            ("y".into(), ys.next().unwrap()),
        ]);
        runtime.step(inputs.iter());
        runtime.cleanup();
    }
}

fn from_elem(c: &mut Criterion) {
    if cfg!(feature = "bench-full") {
        let sizes = vec![
            1, 10, 100, 500, 1000, 2000, 5000, 10000,
            25000, // 100000,
                  // 1000000,
        ];

        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let mut group = c.benchmark_group("special_constraints_add");
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.measurement_time(std::time::Duration::from_secs(5));

        for size in sizes {
            group.bench_with_input(
                BenchmarkId::new("special_constraints_add_dep_empty", size),
                &size,
                |b, &size| {
                    b.to_async(&tokio_rt)
                        .iter(|| monitor_outputs_untyped_constraints(size, DependencyKind::Empty))
                },
            );
            group.bench_with_input(
                BenchmarkId::new("special_constraints_add_dep_graph", size),
                &size,
                |b, &size| {
                    b.to_async(&tokio_rt).iter(|| {
                        monitor_outputs_untyped_constraints(size, DependencyKind::DepGraph)
                    })
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, from_elem);
criterion_main!(benches);

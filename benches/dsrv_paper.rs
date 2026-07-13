use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;
use tc_testutils::streams::with_timeout;
use trustworthiness_checker::Specification;
use trustworthiness_checker::UntypedDsrvSpecification;
use trustworthiness_checker::Value;
use trustworthiness_checker::VarName;
use trustworthiness_checker::benches_common::RECONF_TOPIC;
use trustworthiness_checker::benches_common::input_factory_dsrv_paper_bench;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_reconf_limited;
use trustworthiness_checker::benches_common::output_builder_dsrv_paper_bench;
use trustworthiness_checker::stream_utils::FanoutSender;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use itertools::Itertools;
use smol::LocalExecutor;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Clone)]
struct LocalSmolExecutor {
    pub executor: Rc<LocalExecutor<'static>>,
}

impl LocalSmolExecutor {
    fn new() -> Self {
        Self {
            executor: Rc::new(LocalExecutor::new()),
        }
    }
}

impl AsyncExecutor for LocalSmolExecutor {
    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        smol::block_on(self.executor.run(future))
    }
}

/// Wait for all fanout senders to have at least one new subscriber registered.
async fn wait_for_input_stream_subscription<T: Clone + 'static>(
    tx_fans: &BTreeMap<VarName, FanoutSender<T>>,
) {
    let sub_event_futs: Vec<_> = tx_fans
        .iter()
        .map(|(_, fan_tx)| {
            let fan_rc = fan_tx.fanout();
            let seen = fan_rc.sub_events();
            async move {
                let fan = fan_rc.as_ref();
                fan.wait_for_sub_event(seen).await
            }
        })
        .collect();
    futures::future::join_all(sub_event_futs).await;
}

/// Run a reconfiguration benchmark with a single input function for all iterations.
/// The input function is called per iteration and must produce values for ALL input
/// variables present in the union of both specs.
async fn run_reconf_bench(
    executor: Rc<LocalExecutor<'static>>,
    spec_1: &UntypedDsrvSpecification,
    spec_2: &UntypedDsrvSpecification,
    size: usize,
    ct: bool,
    percent: usize,
    input_fn: impl Fn(usize) -> BTreeMap<VarName, Value>,
) {
    let input_vars = spec_1
        .input_vars()
        .union(&spec_2.input_vars())
        .cloned()
        .collect();
    let (input_factory, tx_fans) = input_factory_dsrv_paper_bench(input_vars);
    let (output_builder, mut rx) =
        output_builder_dsrv_paper_bench(spec_1.output_vars().into(), executor.clone());
    let mut is_spec_1 = true;

    let _handle = executor.spawn(monitor_outputs_untyped_reconf_limited(
        executor.clone(),
        spec_1.clone(),
        input_factory,
        output_builder,
        ct,
    ));

    wait_for_input_stream_subscription(&tx_fans).await;

    for i in 0..size {
        let inputs = input_fn(i);

        if percent > 0 && (i + 1) % (100 / percent) == 0 {
            for name in inputs.keys() {
                tx_fans[name].send(Value::NoVal).await;
            }
            let spec = if is_spec_1 {
                is_spec_1 = false;
                spec_2.to_string()
            } else {
                is_spec_1 = true;
                spec_1.to_string()
            };
            let reconf_json = serde_json::json!({
                "spec": spec,
                "type_info": {},
                "topic_mapping": {}
            })
            .to_string()
            .into();
            tx_fans[&VarName::new(RECONF_TOPIC)]
                .send(Value::Str(reconf_json))
                .await;
            wait_for_input_stream_subscription(&tx_fans).await;
        }

        for (name, val) in &inputs {
            with_timeout(
                tx_fans[name].send(val.clone()),
                1,
                &format!("{}_send", name),
            )
            .await
            .expect(&format!("Failed to send {name} on iteration {i}"));
        }
        with_timeout(
            tx_fans[&VarName::new(RECONF_TOPIC)].send(Value::NoVal),
            1,
            "r_send",
        )
        .await
        .expect("Failed to send reconf trigger");
        let _: BTreeMap<VarName, Value> = with_timeout(rx.recv(), 1, "recv")
            .await
            .expect("Failed to receive output")
            .expect("Output channel closed");
    }
}

/// Instantiate one benchmark group per [BenchConfig] entry.
fn run_reconf_bench_group(c: &mut Criterion, configs: &[BenchConfig]) {
    let sizes = vec![
        1, 100,
        // 1000,
        //2500, 5000, 7500, 10000
    ];
    let percents = [0, 1, 5, 10, 20, 50, 100];
    let cts = [true, false];

    for config in configs {
        let mut group = c.benchmark_group(config.group_name);
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(2));

        let input_fn = &config.input_fn;

        for ((size, percent), ct) in sizes
            .iter()
            .copied()
            .cartesian_product(percents)
            .cartesian_product(cts)
        {
            let ct_str = if ct { "on" } else { "off" };
            group.bench_with_input(
                BenchmarkId::new(format!("reconf_ct_{}_percent_{}", ct_str, percent), size),
                &(size, percent),
                |b, param| {
                    let ex = LocalSmolExecutor::new();
                    b.to_async(ex.clone()).iter(move || {
                        let ex = ex.executor.clone();
                        let spec1 = config.spec1.clone();
                        let spec2 = config.spec2.clone();
                        let input_fn = input_fn.as_ref();
                        let size = param.0;
                        let percent = param.1;
                        async move {
                            run_reconf_bench(ex, &spec1, &spec2, size, ct, percent, |i| input_fn(i))
                                .await
                        }
                    });
                },
            );
        }

        group.finish();
    }
}

/// Configuration for a single reconfiguration benchmark group.
/// Encapsulates all parameters so that run_reconf_bench_generic can execute it.
struct BenchConfig<'a> {
    /// Name of the Criterion benchmark group.
    group_name: &'a str,
    /// The "old" specification.
    spec1: UntypedDsrvSpecification,
    /// The "new" specification (reconfigured to at runtime).
    spec2: UntypedDsrvSpecification,
    /// Input values produced per iteration. Must cover all input vars in spec1 ∪ spec2.
    input_fn: Box<dyn Fn(usize) -> BTreeMap<VarName, Value> + 'a>,
}

fn simple_and(c: &mut Criterion) {
    let spec_1 = trustworthiness_checker::dsrv_specification(
        &mut "in x: Bool
              in y: Bool
              out z: Bool
              z = x && y",
    )
    .unwrap();
    let spec_2 = trustworthiness_checker::dsrv_specification(
        &mut "in x: Bool
              in y: Bool
              out z: Bool
              z = x && y && true",
    )
    .unwrap();

    let configs = [BenchConfig {
        group_name: "simple_and",
        spec1: spec_1,
        spec2: spec_2,
        input_fn: Box::new(|_| {
            BTreeMap::from([
                (VarName::new("x"), Value::Bool(true)),
                (VarName::new("y"), Value::Bool(false)),
            ])
        }),
    }];

    run_reconf_bench_group(c, &configs);
}

fn rec_moving_average(c: &mut Criterion) {
    let spec_n3 = trustworthiness_checker::dsrv_specification(
        &mut "in x
              out y
              y = default(y[1], 0) + (x - default(x[3], 0)) / 3",
    )
    .unwrap();
    let spec_n5 = trustworthiness_checker::dsrv_specification(
        &mut "in x
              out y
              y = default(y[1], 0) + (x - default(x[5], 0)) / 5",
    )
    .unwrap();

    let configs = [BenchConfig {
        group_name: "rec_moving_average",
        spec1: spec_n3,
        spec2: spec_n5,
        input_fn: Box::new(|i| BTreeMap::from([(VarName::new("x"), Value::Int(i as i64))])),
    }];

    run_reconf_bench_group(c, &configs);
}

fn sindex(c: &mut Criterion) {
    for i in [1, 10, 100] {
        let spec1 = trustworthiness_checker::dsrv_specification(
            &mut format!(
                "in x
              out y
              y = x[{}]",
                i
            )
            .as_str(),
        )
        .unwrap();
        let spec2 = trustworthiness_checker::dsrv_specification(
            &mut format!(
                "in x
              out y
              y = x[{}] + 0",
                i
            )
            .as_str(),
        )
        .unwrap();

        let name = format!("sindex{}", i);
        let configs = [BenchConfig {
            group_name: name.as_str(),
            spec1: spec1,
            spec2: spec2,
            input_fn: Box::new(|_| BTreeMap::from([(VarName::new("x"), Value::Int(42))])),
        }];

        run_reconf_bench_group(c, &configs);
    }
}

fn maple_index(c: &mut Criterion) {
    let spec1 = trustworthiness_checker::dsrv_specification(
        &mut "in stage : Str\n
     out m: Bool\n
     out a: Bool\n
     out p: Bool\n
     out l: Bool\n
     out e: Bool\n
     out maple : Bool\n
     m = (stage == \"m\") && default(e[1], true)\n
     a = (stage == \"a\") && default(m[1], false)\n
     p = (stage == \"p\") && default(a[1], false)\n
     l = (stage == \"l\") && default(p[1], false)\n
     e = (stage == \"e\") && default(l[1], false)\n
     maple = m || a || p || l || e",
    )
    .unwrap();
    let spec2 = trustworthiness_checker::dsrv_specification(
        &mut "in stage : Str\n
     out m: Bool\n
     out a: Bool\n
     out p: Bool\n
     out l: Bool\n
     out e: Bool\n
     out maple : Bool\n
     m = (stage == \"m\") && default(e[1], true)\n
     a = (stage == \"a\") && default(m[1], false)\n
     p = (stage == \"p\") && default(a[1], false)\n
     l = (stage == \"l\") && default(p[1], false)\n
     e = (stage == \"e\") && default(l[1], false)\n
     maple = m || a || p || l || e || false",
    )
    .unwrap();

    let configs = [BenchConfig {
        group_name: "maple_index",
        spec1: spec1,
        spec2: spec2,
        input_fn: Box::new(|_| BTreeMap::from([(VarName::new("stage"), Value::Str("m".into()))])),
    }];

    run_reconf_bench_group(c, &configs);
}

criterion_group!(benches, simple_and, rec_moving_average, sindex, maple_index);
criterion_main!(benches);

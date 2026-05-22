use std::collections::BTreeMap;
use std::iter;
use std::rc::Rc;
use std::time::Duration;
use tc_testutils::streams::with_timeout;
use trustworthiness_checker::Value;
use trustworthiness_checker::benches_common::RECONF_TOPIC;
use trustworthiness_checker::benches_common::input_builder_dsrv_paper_bench;
use trustworthiness_checker::benches_common::output_builder_dsrv_paper_bench;
use trustworthiness_checker::stream_utils::FanoutSender;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::async_executor::AsyncExecutor;
use criterion::{criterion_group, criterion_main};
use itertools::Itertools;
use smol::LocalExecutor;
use trustworthiness_checker::benches_common::monitor_outputs_untyped_reconf_limited;

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
async fn wait_for_input_provider_subscription<T: Clone + 'static>(
    tx_fans: &BTreeMap<&str, FanoutSender<T>>,
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

fn from_elem(c: &mut Criterion) {
    let sizes = vec![1, 100, 1000, 2500, 5000, 7500, 10000];
    let percents = [0, 1, 5, 10, 20, 50, 100];

    let local_smol_executor = LocalSmolExecutor::new();

    let mut group = c.benchmark_group("dsrv_paper");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(2));

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

    for (size, percent) in sizes.into_iter().cartesian_product(percents) {
        group.bench_with_input(
            BenchmarkId::new(format!("dsrv_paper_reconf_percent_{}", percent), size),
            &(&spec_1, &spec_2),
            |b, &(spec_1, spec_2)| {
                b.to_async(local_smol_executor.clone()).iter(|| {
                    let executor = local_smol_executor.executor.clone();
                    let spec_1 = spec_1.clone();
                    let spec_2 = spec_2.clone();
                    async move {
                        let (input_builder, tx_fans) =
                            input_builder_dsrv_paper_bench(spec_1.clone(), executor.clone());
                        let (output_builder, mut rx) =
                            output_builder_dsrv_paper_bench(executor.clone());
                        let x = iter::repeat(Value::Bool(true)).take(size);
                        let y = iter::repeat(Value::Bool(false)).take(size);
                        let mut is_spec_1 = true;

                        let _handle = executor.spawn(monitor_outputs_untyped_reconf_limited(
                            executor.clone(),
                            spec_1.clone(),
                            input_builder,
                            output_builder,
                        ));

                        wait_for_input_provider_subscription(&tx_fans).await;

                        let mut outputs = Vec::new();
                        for (i, (x_val, y_val)) in x.zip(y).enumerate() {
                            if percent > 0 && (i + 1) % (100 / percent) == 0 {
                                tx_fans["x"].send(Value::NoVal).await;
                                tx_fans["y"].send(Value::NoVal).await;
                                let spec = if is_spec_1 {
                                    is_spec_1 = false;
                                    spec_2.clone().to_string()
                                } else {
                                    is_spec_1 = true;
                                    spec_1.clone().to_string()
                                };
                                let reconf_json: ecow::EcoString = serde_json::json!({
                                    "spec": spec,
                                    "type_info": {},
                                    "topic_mapping": {}
                                })
                                .to_string()
                                .into();
                                tx_fans[RECONF_TOPIC].send(Value::Str(reconf_json)).await;
                                wait_for_input_provider_subscription(&tx_fans).await;
                            }
                            with_timeout(tx_fans["x"].send(x_val), 1, "x_send")
                                .await
                                .expect(format!("Failed to send x on iteration {}", i).as_str());
                            with_timeout(tx_fans["y"].send(y_val), 1, "y_send")
                                .await
                                .expect("Failed to send y");
                            with_timeout(tx_fans[RECONF_TOPIC].send(Value::NoVal), 1, "r_send")
                                .await
                                .expect("Failed to send reconf trigger");
                            let output = with_timeout(rx.recv(), 1, "recv")
                                .await
                                .expect("Failed to receive output")
                                .expect("Output channel closed");
                            outputs.push(output);
                        }

                        let outputs = outputs
                            .into_iter()
                            .map(|m| m[&("z".into())].clone())
                            .collect::<Vec<_>>();
                        assert_eq!(outputs, vec![Value::Bool(false); size]);
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, from_elem);
criterion_main!(benches);

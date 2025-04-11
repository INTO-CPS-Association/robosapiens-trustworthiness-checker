use std::{collections::BTreeMap, rc::Rc};

use macro_rules_attribute::apply;
use petgraph::graph::DiGraph;
use smol::{stream::{self, StreamExt}, LocalExecutor};
use smol_macros::test as smol_test;
use test_log::test;
use trustworthiness_checker::{
    core::AbstractMonitorBuilder, distributed::distribution_graphs::{DistributionGraph, LabelledDistributionGraph}, io::{mqtt::output_handler, testing::ManualOutputHandler}, lola_specification, runtime::distributed::{DistAsyncMonitorBuilder, DistributedMonitorRunner}, semantics::{distributed::semantics::DistributedSemantics, UntimedLolaSemantics}, Monitor, MonitoringSemantics, OutputStream, Value
};
use winnow::Parser;

#[test(apply(smol_test))]
async fn test_distributed_at_stream(executor: Rc<LocalExecutor<'static>>) {
    // Just a little test to check that we can do our tests... :-)
    let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
    let input_handler = BTreeMap::from([
        ("x".into(), x),
    ]);

    let mut graph = DiGraph::new();
    let a = graph.add_node("A".into());
    let b = graph.add_node("B".into());
    let c = graph.add_node("C".into());
    graph.add_edge(a, b, 0);
    graph.add_edge(b, c, 0);
    let dist_graph = DistributionGraph {
        central_monitor: a,
        graph,
    };
    let labelled_graph = LabelledDistributionGraph {
        dist_graph,
        var_names: vec!["x".into(), "y".into(), "z".into()],
        node_labels: BTreeMap::from([
            (a, vec![]),
            (b, vec!["x".into()]),
            (c, vec!["y".into(), "z".into()]),
        ]),
    };

    let spec = "in x\n
    out w\n
    out y\n
    out z\n
    y = x + 1\n
    z = x + 2\n
    w = monitored_at(x, B)";
    let var_names = vec!["w".into(), "y".into(), "z".into()];
    let spec = lola_specification.parse(spec).unwrap();

    let mut output_handler = ManualOutputHandler::new(executor.clone(), var_names);

    let output_stream: OutputStream<Vec<Value>> = output_handler.get_output();

    let monitor = DistAsyncMonitorBuilder::<_, _, _, _, DistributedSemantics>::new()
        .executor(executor.clone())
        .input(Box::new(input_handler))
        .model(spec)
        .static_dist_graph(labelled_graph)
        .output(Box::new(output_handler))
        .build();

    executor.spawn(monitor.run()).detach();

    let output: Vec<_> = output_stream.collect().await;

    assert_eq!(output.len(), 3);
    assert_eq!(output[0], vec![true.into(), 2.into(), 3.into()]);
    assert_eq!(output[1], vec![true.into(), 3.into(), 4.into()]);
    assert_eq!(output[2], vec![true.into(), 4.into(), 5.into()]);

    // let monitor

    // let res_x = monitored_at("x".into(), "B".into(), &ctx);

    // let spec = LolaS

    // let res_x: Vec<_> = res_x.take(3).collect().await;

    // assert_eq!(res_x, vec![true.into(), true.into(), true.into()]);
}

use std::{collections::BTreeMap, rc::Rc};

use macro_rules_attribute::apply;
use petgraph::graph::DiGraph;
use smol::{
    LocalExecutor,
    stream::{self, StreamExt},
};
use trustworthiness_checker::async_test;
use trustworthiness_checker::{
    OutputStream, Value,
    core::{AbstractMonitorBuilder, Runnable},
    distributed::distribution_graphs::{DistributionGraph, LabelledDistributionGraph},
    io::testing::ManualOutputHandler,
    lola_specification,
    runtime::distributed::DistAsyncMonitorBuilder,
    semantics::distributed::semantics::DistributedSemantics,
};
use winnow::Parser;

#[apply(async_test)]
async fn test_distributed_at_stream(executor: Rc<LocalExecutor<'static>>) {
    let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
    let input_handler = BTreeMap::from([("x".into(), x)]);

    let mut graph = DiGraph::new();
    let a = graph.add_node("A".into());
    let b = graph.add_node("B".into());
    let c = graph.add_node("C".into());
    graph.add_edge(a, b, 0);
    graph.add_edge(b, c, 0);
    let dist_graph = Rc::new(DistributionGraph {
        central_monitor: a,
        graph,
    });
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
}

#[apply(async_test)]
async fn test_distributed_dist_spec_1(executor: Rc<LocalExecutor<'static>>) {
    let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
    let input_handler = BTreeMap::from([("x".into(), x)]);

    let mut graph = DiGraph::new();
    let a = graph.add_node("A".into());
    let b = graph.add_node("B".into());
    let c = graph.add_node("C".into());
    graph.add_edge(a, b, 0);
    graph.add_edge(b, c, 0);
    let dist_graph = Rc::new(DistributionGraph {
        central_monitor: a,
        graph,
    });
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
    w = dist(x, y)";
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
    assert_eq!(output[0], vec![0.into(), 2.into(), 3.into()]);
    assert_eq!(output[1], vec![0.into(), 3.into(), 4.into()]);
    assert_eq!(output[2], vec![0.into(), 4.into(), 5.into()]);
}

#[apply(async_test)]
async fn test_distributed_dist_spec_2(executor: Rc<LocalExecutor<'static>>) {
    let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
    let input_handler = BTreeMap::from([("x".into(), x)]);

    let mut graph = DiGraph::new();
    let a = graph.add_node("A".into());
    let b = graph.add_node("B".into());
    let c = graph.add_node("C".into());
    graph.add_edge(a, b, 1);
    graph.add_edge(b, c, 1);
    let dist_graph = Rc::new(DistributionGraph {
        central_monitor: a,
        graph,
    });
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
    w = dist(A, C)";
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
    assert_eq!(output[0], vec![2.into(), 2.into(), 3.into()]);
    assert_eq!(output[1], vec![2.into(), 3.into(), 4.into()]);
    assert_eq!(output[2], vec![2.into(), 4.into(), 5.into()]);
}

#[apply(async_test)]
async fn test_distributed_dist_spec_3(executor: Rc<LocalExecutor<'static>>) {
    let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
    let input_handler = BTreeMap::from([("x".into(), x)]);

    let mut graph = DiGraph::new();
    let a = graph.add_node("A".into());
    let b = graph.add_node("B".into());
    let c = graph.add_node("C".into());
    graph.add_edge(a, b, 1);
    graph.add_edge(b, c, 1);
    let dist_graph = Rc::new(DistributionGraph {
        central_monitor: a,
        graph,
    });
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
    w = dist(x, C)";
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
    assert_eq!(output[0], vec![1.into(), 2.into(), 3.into()]);
    assert_eq!(output[1], vec![1.into(), 3.into(), 4.into()]);
    assert_eq!(output[2], vec![1.into(), 4.into(), 5.into()]);
}

#[apply(async_test)]
async fn test_distributed_dist_spec_4(executor: Rc<LocalExecutor<'static>>) {
    let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
    let input_handler = BTreeMap::from([("x".into(), x)]);

    let mut graph = DiGraph::new();
    let a = graph.add_node("A".into());
    let b = graph.add_node("B".into());
    let c = graph.add_node("C".into());
    graph.add_edge(a, b, 1);
    graph.add_edge(b, c, 1);
    let dist_graph = Rc::new(DistributionGraph {
        central_monitor: a,
        graph,
    });
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
    w = dist(x, z)";
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
    assert_eq!(output[0], vec![1.into(), 2.into(), 3.into()]);
    assert_eq!(output[1], vec![1.into(), 3.into(), 4.into()]);
    assert_eq!(output[2], vec![1.into(), 4.into(), 5.into()]);
}

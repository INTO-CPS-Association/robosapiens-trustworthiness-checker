use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    rc::Rc,
};

use macro_rules_attribute::apply;
use petgraph::graph::DiGraph;
use smol::{LocalExecutor, stream::StreamExt};
use trustworthiness_checker::VarName;
use trustworthiness_checker::async_test;
use trustworthiness_checker::io::map;
use trustworthiness_checker::{
    DsrvSpecification, OutputStream, Value,
    core::{OutputBackend, OutputInterface, OutputWriter, Runtime},
    distributed::distribution_graphs::{DistributionGraph, LabelledDistributionGraph},
    dsrv_fixtures::TestDistConfig,
    io::output::ManualOutputBackend,
    runtime::RuntimeBuilder,
    runtime::distributed::DistAsyncRuntimeBuilder,
    semantics::distributed::semantics::DistributedSemantics,
};
type TestDistSemantics = DistributedSemantics;
type TestDistRuntimeBuilder = DistAsyncRuntimeBuilder<TestDistConfig, TestDistSemantics>;

async fn manual_output(
    variables: BTreeSet<VarName>,
) -> (OutputWriter<Value>, OutputStream<BTreeMap<VarName, Value>>) {
    let (backend, receiver) = ManualOutputBackend::<Value>::channel(1024);
    let variables = variables;
    let writer = backend
        .open(OutputInterface::outputs(variables.iter().cloned()).unwrap())
        .await
        .unwrap();
    let queues = variables
        .iter()
        .cloned()
        .map(|variable| (variable, VecDeque::<Value>::new()))
        .collect::<BTreeMap<_, _>>();
    let stream = Box::pin(futures::stream::unfold(
        (receiver, queues, 0_usize),
        |(mut receiver, mut queues, emitted)| async move {
            if emitted >= 3 {
                return None;
            }
            loop {
                if queues.values().all(|queue| !queue.is_empty()) {
                    let row = queues
                        .iter_mut()
                        .map(|(variable, queue)| {
                            (
                                variable.clone(),
                                queue.pop_front().expect("queue was checked above"),
                            )
                        })
                        .collect();
                    return Some((row, (receiver, queues, emitted + 1)));
                }
                let map = receiver.recv().await?;
                for (variable, value) in map {
                    if let Some(queue) = queues.get_mut(&variable) {
                        queue.push_back(value);
                    }
                }
            }
        },
    ));
    (writer, stream)
}

fn parse_spec(source: &str) -> trustworthiness_checker::DsrvSpecification {
    (source)
        .parse::<DsrvSpecification>()
        .expect("test DSRV specification should parse")
}

#[apply(async_test)]
async fn test_distributed_at_stream(executor: Rc<LocalExecutor<'static>>) {
    let x = vec![1.into(), 2.into(), 3.into()];
    let input_handler = map::input_stream(BTreeMap::from([("x".into(), x)]));

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
    out tuple_element\n
    out y\n
    out z\n
    y = x + 1\n
    z = x + 2\n
    tuple_element = Tuple(x, y).1\n
    w = monitored_at(x, B)";
    let var_names = BTreeSet::from(["tuple_element".into(), "w".into(), "y".into(), "z".into()]);
    let spec = parse_spec(spec);

    let (output_writer, output_stream) = manual_output(var_names).await;

    let var_msg_types = BTreeMap::from([
        ("x".into(), "Int32".to_string()),
        ("y".into(), "Int32".to_string()),
        ("z".into(), "Int32".to_string()),
        ("w".into(), "Int32".to_string()),
        ("tuple_element".into(), "Int32".to_string()),
    ]);

    let monitor = TestDistRuntimeBuilder::new()
        .executor(executor.clone())
        .input(input_handler)
        .model(spec)
        .var_msg_types(var_msg_types)
        .static_dist_graph(labelled_graph)
        .output_writer(output_writer)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();

    let output: Vec<_> = output_stream.take(3).collect().await;

    assert_eq!(output.len(), 3);
    assert_eq!(
        output[0],
        BTreeMap::from([
            ("tuple_element".into(), 2.into()),
            ("w".into(), true.into()),
            ("y".into(), 2.into()),
            ("z".into(), 3.into())
        ])
    );
    assert_eq!(
        output[1],
        BTreeMap::from([
            ("tuple_element".into(), 3.into()),
            ("w".into(), true.into()),
            ("y".into(), 3.into()),
            ("z".into(), 4.into())
        ])
    );
    assert_eq!(
        output[2],
        BTreeMap::from([
            ("tuple_element".into(), 4.into()),
            ("w".into(), true.into()),
            ("y".into(), 4.into()),
            ("z".into(), 5.into())
        ])
    );
}

#[apply(async_test)]
async fn test_distributed_dist_spec_1(executor: Rc<LocalExecutor<'static>>) {
    let x = vec![1.into(), 2.into(), 3.into()];
    let input_handler = map::input_stream(BTreeMap::from([("x".into(), x)]));

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
    let var_names = BTreeSet::from(["w".into(), "y".into(), "z".into()]);
    let spec = parse_spec(spec);

    let (output_writer, output_stream) = manual_output(var_names).await;

    let var_msg_types = BTreeMap::from([
        ("x".into(), "Int32".to_string()),
        ("y".into(), "Int32".to_string()),
        ("z".into(), "Int32".to_string()),
        ("w".into(), "Int32".to_string()),
    ]);

    let monitor = TestDistRuntimeBuilder::new()
        .executor(executor.clone())
        .input(input_handler)
        .model(spec)
        .var_msg_types(var_msg_types)
        .static_dist_graph(labelled_graph)
        .output_writer(output_writer)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();

    let output: Vec<_> = output_stream.collect().await;

    assert_eq!(output.len(), 3);
    assert_eq!(
        output[0],
        BTreeMap::from([
            ("w".into(), 0.into()),
            ("y".into(), 2.into()),
            ("z".into(), 3.into())
        ])
    );
    assert_eq!(
        output[1],
        BTreeMap::from([
            ("w".into(), 0.into()),
            ("y".into(), 3.into()),
            ("z".into(), 4.into())
        ])
    );
    assert_eq!(
        output[2],
        BTreeMap::from([
            ("w".into(), 0.into()),
            ("y".into(), 4.into()),
            ("z".into(), 5.into())
        ])
    );
}

#[apply(async_test)]
async fn test_distributed_dist_spec_2(executor: Rc<LocalExecutor<'static>>) {
    let x = vec![1.into(), 2.into(), 3.into()];
    let input_handler = map::input_stream(BTreeMap::from([("x".into(), x)]));

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
    let var_names = BTreeSet::from(["w".into(), "y".into(), "z".into()]);
    let spec = parse_spec(spec);

    let (output_writer, output_stream) = manual_output(var_names).await;

    let var_msg_types = BTreeMap::from([
        ("x".into(), "Int32".to_string()),
        ("y".into(), "Int32".to_string()),
        ("z".into(), "Int32".to_string()),
        ("w".into(), "Int32".to_string()),
    ]);

    let monitor = TestDistRuntimeBuilder::new()
        .executor(executor.clone())
        .input(input_handler)
        .model(spec)
        .var_msg_types(var_msg_types)
        .static_dist_graph(labelled_graph)
        .output_writer(output_writer)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();

    let output: Vec<_> = output_stream.collect().await;

    assert_eq!(output.len(), 3);
    assert_eq!(
        output[0],
        BTreeMap::from([
            ("w".into(), 2.into()),
            ("y".into(), 2.into()),
            ("z".into(), 3.into())
        ])
    );
    assert_eq!(
        output[1],
        BTreeMap::from([
            ("w".into(), 2.into()),
            ("y".into(), 3.into()),
            ("z".into(), 4.into())
        ])
    );
    assert_eq!(
        output[2],
        BTreeMap::from([
            ("w".into(), 2.into()),
            ("y".into(), 4.into()),
            ("z".into(), 5.into())
        ])
    );
}

#[apply(async_test)]
async fn test_distributed_dist_spec_3(executor: Rc<LocalExecutor<'static>>) {
    let x = vec![1.into(), 2.into(), 3.into()];
    let input_handler = map::input_stream(BTreeMap::from([("x".into(), x)]));

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
    let var_names = BTreeSet::from(["w".into(), "y".into(), "z".into()]);
    let spec = parse_spec(spec);

    let (output_writer, output_stream) = manual_output(var_names).await;

    let var_msg_types = BTreeMap::from([
        ("x".into(), "Int32".to_string()),
        ("y".into(), "Int32".to_string()),
        ("z".into(), "Int32".to_string()),
        ("w".into(), "Int32".to_string()),
    ]);

    let monitor = TestDistRuntimeBuilder::new()
        .executor(executor.clone())
        .input(input_handler)
        .model(spec)
        .var_msg_types(var_msg_types)
        .static_dist_graph(labelled_graph)
        .output_writer(output_writer)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();

    let output: Vec<_> = output_stream.collect().await;

    assert_eq!(output.len(), 3);
    assert_eq!(
        output[0],
        BTreeMap::from([
            ("w".into(), 1.into()),
            ("y".into(), 2.into()),
            ("z".into(), 3.into())
        ])
    );
    assert_eq!(
        output[1],
        BTreeMap::from([
            ("w".into(), 1.into()),
            ("y".into(), 3.into()),
            ("z".into(), 4.into())
        ])
    );
    assert_eq!(
        output[2],
        BTreeMap::from([
            ("w".into(), 1.into()),
            ("y".into(), 4.into()),
            ("z".into(), 5.into())
        ])
    );
}

#[apply(async_test)]
async fn test_distributed_dist_spec_4(executor: Rc<LocalExecutor<'static>>) {
    let x = vec![1.into(), 2.into(), 3.into()];
    let input_handler = map::input_stream(BTreeMap::from([("x".into(), x)]));

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
    let var_names = BTreeSet::from(["w".into(), "y".into(), "z".into()]);
    let spec = parse_spec(spec);

    let (output_writer, output_stream) = manual_output(var_names).await;

    let var_msg_types = BTreeMap::from([
        ("x".into(), "Int32".to_string()),
        ("y".into(), "Int32".to_string()),
        ("z".into(), "Int32".to_string()),
        ("w".into(), "Int32".to_string()),
    ]);

    let monitor = TestDistRuntimeBuilder::new()
        .executor(executor.clone())
        .input(input_handler)
        .model(spec)
        .var_msg_types(var_msg_types)
        .static_dist_graph(labelled_graph)
        .output_writer(output_writer)
        .build()
        .await;

    executor.spawn(monitor.run()).detach();

    let output: Vec<_> = output_stream.collect().await;

    assert_eq!(output.len(), 3);
    assert_eq!(
        output[0],
        BTreeMap::from([
            ("w".into(), 1.into()),
            ("y".into(), 2.into()),
            ("z".into(), 3.into())
        ])
    );
    assert_eq!(
        output[1],
        BTreeMap::from([
            ("w".into(), 1.into()),
            ("y".into(), 3.into()),
            ("z".into(), 4.into())
        ])
    );
    assert_eq!(
        output[2],
        BTreeMap::from([
            ("w".into(), 1.into()),
            ("y".into(), 4.into()),
            ("z".into(), 5.into())
        ])
    );
}

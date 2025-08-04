use crate::VarName;
use crate::core::OutputStream;
use crate::core::{StreamData, Value};
use crate::distributed::distribution_graphs::{Distance, NodeName};
use crate::lang::dynamic_lola::ast::VarOrNodeName;
use async_stream::stream;
use futures::StreamExt;

use super::contexts::DistributedContext;

pub fn monitored_at<Val: StreamData>(
    var_name: VarName,
    node_name: NodeName,
    ctx: &DistributedContext<Val>,
) -> OutputStream<Value> {
    let mut graph_stream = ctx.graph().unwrap();

    Box::pin(stream! {
        while let Some(graph) = graph_stream.next().await {
            let idx = graph.get_node_index_by_name(&node_name).expect("Label not inside graph");
            let res = graph.node_labels
                .get(&idx)
                .is_some_and(|vec| vec.iter().any(|name| *name == var_name));
            yield Value::Bool(res);
        }
    })
}

pub fn dist<Val: StreamData>(
    u: VarOrNodeName,
    v: VarOrNodeName,
    ctx: &DistributedContext<Val>,
) -> OutputStream<Value> {
    let u = ctx
        .disambiguate_name(u)
        .expect("Could not find node or variable in the graph");
    let v = ctx
        .disambiguate_name(v)
        .expect("Could not find node or variable in the graph");
    let mut graph_stream = ctx.graph().unwrap();

    // TODO: Think about a better way to handle disconnected nodes
    // This hack should work for now as we actually do expect the nodes to be
    // connected
    Box::pin(stream! {
        while let Some(graph) = graph_stream.next().await {
            let res = graph.dist(u.clone(), v.clone());
            yield Value::Int(res.map(|x| x.try_into().unwrap()).unwrap_or(i64::MAX));
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, rc::Rc};

    use super::*;
    use crate::async_test;
    use crate::{
        core::Value,
        distributed::distribution_graphs::{
            DistributionGraph, LabelledDistributionGraph, TaggedVarOrNodeName,
        },
        semantics::{
            AbstractContextBuilder, StreamContext, distributed::contexts::DistributedContextBuilder,
        },
    };
    use futures::stream;
    use macro_rules_attribute::apply;
    use petgraph::graph::DiGraph;
    use smol::LocalExecutor;

    #[apply(async_test)]
    async fn test_that_test_can_test(executor: Rc<LocalExecutor<'static>>) {
        // Just a little test to check that we can do our tests... :-)
        let e: OutputStream<Value> = Box::pin(stream::iter(vec!["x + 1".into(), "x + 2".into()]));
        let x = Box::pin(stream::iter(vec![1.into(), 2.into()]));
        let graph_stream = Box::pin(stream::iter(vec![]));
        let mut ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into()])
            .input_streams(vec![x])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();
        let exp = vec![Value::Int(2), Value::Int(4)];
        let res_stream =
            crate::semantics::untimed_untyped_lola::combinators::dynamic(&ctx, e, None, 10);
        ctx.run().await;
        let res: Vec<Value> = res_stream.collect().await;
        assert_eq!(res, exp);
    }

    #[apply(async_test)]
    async fn test_monitor_at_stream(executor: Rc<LocalExecutor<'static>>) {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let z = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));

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
        let labelled_graph = Rc::new(LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([
                (a, vec![]),
                (b, vec!["x".into()]),
                (c, vec!["y".into(), "z".into()]),
            ]),
        });

        let graph_stream = Box::pin(stream::repeat(labelled_graph));

        let mut ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into(), "y".into(), "z".into()])
            .input_streams(vec![x, y, z])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();

        let res_x = monitored_at("x".into(), "B".into(), &ctx);
        ctx.run().await;
        let res_x: Vec<_> = res_x.take(3).collect().await;

        assert_eq!(res_x, vec![true.into(), true.into(), true.into()]);
    }

    #[apply(async_test)]
    async fn test_dist_stream_nodes(executor: Rc<LocalExecutor<'static>>) {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let z = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));

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
        let labelled_graph = Rc::new(LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([
                (a, vec![]),
                (b, vec!["x".into()]),
                (c, vec!["y".into(), "z".into()]),
            ]),
        });

        let graph_stream = Box::pin(stream::repeat(labelled_graph));

        let mut ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into(), "y".into(), "z".into()])
            .input_streams(vec![x, y, z])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();

        let res = dist(VarOrNodeName("A".into()), VarOrNodeName("C".into()), &ctx);
        ctx.run().await;
        let res: Vec<_> = res.take(3).collect().await;

        assert_eq!(res, vec![2.into(), 2.into(), 2.into()]);
    }

    #[apply(async_test)]
    async fn test_dist_stream_var_node(executor: Rc<LocalExecutor<'static>>) {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let z = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));

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
        let labelled_graph = Rc::new(LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([
                (a, vec![]),
                (b, vec!["x".into()]),
                (c, vec!["y".into(), "z".into()]),
            ]),
        });

        let graph_stream = Box::pin(stream::repeat(labelled_graph));

        let mut ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into(), "y".into(), "z".into()])
            .input_streams(vec![x, y, z])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();

        let res = dist(VarOrNodeName("x".into()), VarOrNodeName("C".into()), &ctx);
        ctx.run().await;
        let res: Vec<_> = res.take(3).collect().await;

        assert_eq!(res, vec![1.into(), 1.into(), 1.into()]);
    }

    #[apply(async_test)]
    async fn test_disambiguate_name(executor: Rc<LocalExecutor<'static>>) {
        let x: OutputStream<Value> = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let y = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));
        let z = Box::pin(stream::iter(vec![1.into(), 2.into(), 3.into()]));

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
        let labelled_graph = Rc::new(LabelledDistributionGraph {
            dist_graph,
            var_names: vec!["x".into(), "y".into(), "z".into()],
            node_labels: BTreeMap::from([
                (a, vec![]),
                (b, vec!["x".into()]),
                (c, vec!["y".into(), "z".into()]),
            ]),
        });

        let graph_stream = Box::pin(stream::repeat(labelled_graph));

        let ctx = DistributedContextBuilder::new()
            .executor(executor.clone())
            .var_names(vec!["x".into(), "y".into(), "z".into()])
            .input_streams(vec![x, y, z])
            .history_length(10)
            .graph_stream(graph_stream)
            .node_names(vec!["A".into(), "B".into(), "C".into()])
            .build();

        let name = ctx.disambiguate_name(VarOrNodeName("x".into()));
        assert_eq!(name, Some(TaggedVarOrNodeName::VarName("x".into())));
        let name = ctx.disambiguate_name(VarOrNodeName("A".into()));
        assert_eq!(
            name,
            Some(TaggedVarOrNodeName::NodeName(NodeName::new("A")))
        );
        let name = ctx.disambiguate_name(VarOrNodeName("D".into()));
        assert_eq!(name, None);
    }
}

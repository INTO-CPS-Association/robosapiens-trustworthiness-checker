use super::super::diagnostics::*;
use crate::lang::dsrv::ast::ExprId;
use crate::lang::dsrv::diagnostics::{SemanticWarning, SemanticWarningKind};
use crate::lang::dsrv::parser::parse_expr;
use crate::lang::dsrv::span::Span;
use contiguous_tree::TreeCursorExt;

fn warning(message: &str, span: Option<Span>) -> SemanticWarning {
    SemanticWarning::new(SemanticWarningKind::RedundantCast, message, span)
}

fn reported(collector: WarningCollector) -> Vec<SemanticWarning> {
    collector.finish_report(Ok(()), None).into_parts().1
}

#[test]
fn two_nodes_sharing_a_span_report_separately() {
    let expr = parse_expr("[1,2]").unwrap();
    let nodes: Vec<ExprId> = expr.as_ref().postorder().map(|node| node.id()).collect();
    let mut collector = WarningCollector::default();
    collector.record(nodes[0], warning("first node", Some(Span::new(0, 1))));
    collector.record(nodes[1], warning("second node", Some(Span::new(0, 1))));
    assert_eq!(reported(collector).len(), 2);
}

#[test]
fn findings_are_ordered_by_position_with_none_last_then_by_node() {
    let expr = parse_expr("[1,2,3,4]").unwrap();
    let mut nodes: Vec<ExprId> = expr.as_ref().postorder().map(|node| node.id()).collect();
    nodes.sort();
    let [first, second, later, first_unplaced, second_unplaced] = nodes.as_slice() else {
        panic!("five distinct nodes");
    };
    let early_first = warning("early first node", Some(Span::new(0, 1)));
    let early_second = warning("early second node", Some(Span::new(0, 1)));
    let late = warning("later span", Some(Span::new(5, 6)));
    let unplaced_first = warning("unplaced first node", None);
    let unplaced_second = warning("unplaced second node", None);
    let mut collector = WarningCollector::default();
    collector.record(*second_unplaced, unplaced_second.clone());
    collector.record(*second, early_second.clone());
    collector.record(*first_unplaced, unplaced_first.clone());
    collector.record(*later, late.clone());
    collector.record(*first, early_first.clone());
    assert_eq!(
        reported(collector),
        [
            early_first,
            early_second,
            late,
            unplaced_first,
            unplaced_second
        ]
    );
}

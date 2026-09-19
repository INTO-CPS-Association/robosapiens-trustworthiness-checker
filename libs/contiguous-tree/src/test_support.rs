#![allow(
    dead_code,
    reason = "associated-child generator is reusable test infrastructure"
)]

use proptest::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AssociatedNodeModel {
    pub label: u16,
    pub annotation: Option<u16>,
    pub ordinary: Vec<AssociatedNodeModel>,
    pub associated: Vec<(u16, AssociatedNodeModel)>,
}

#[derive(Clone, Debug)]
pub(crate) struct AssociatedForestCase {
    pub roots: Vec<AssociatedNodeModel>,
    pub injected_label: Option<u16>,
    pub distinct_destination_layout: bool,
}

fn arb_shape() -> BoxedStrategy<AssociatedNodeModel> {
    let leaf =
        (prop_oneof![4 => Just(None), 1 => (0_u16..32).prop_map(Some)]).prop_map(|annotation| {
            AssociatedNodeModel {
                label: 0,
                annotation,
                ordinary: Vec::new(),
                associated: Vec::new(),
            }
        });
    leaf.prop_recursive(6, 31, 7, |child| {
        (
            prop_oneof![4 => Just(None), 1 => (0_u16..32).prop_map(Some)],
            proptest::collection::vec(child.clone(), 0..=3),
            proptest::collection::vec((0_u16..64, child), 0..=4),
        )
            .prop_map(|(annotation, ordinary, associated)| AssociatedNodeModel {
                label: 0,
                annotation,
                ordinary,
                associated,
            })
    })
    .boxed()
}

fn number(mut roots: Vec<AssociatedNodeModel>) -> Vec<AssociatedNodeModel> {
    fn visit(node: &mut AssociatedNodeModel, next: &mut u16) {
        node.label = *next;
        *next += 1;
        for child in &mut node.ordinary {
            visit(child, next);
        }
        for (_, child) in &mut node.associated {
            visit(child, next);
        }
    }
    let mut next = 0;
    for root in &mut roots {
        visit(root, &mut next);
    }
    roots
}

/// Bounded recursive model; labels are assigned after shrinking and are unique.
pub(crate) fn arb_associated_forest() -> BoxedStrategy<AssociatedForestCase> {
    let empty = Just(Vec::new());
    let one = arb_shape().prop_map(|root| number(vec![root]));
    let many = proptest::collection::vec(arb_shape(), 2..=3).prop_map(number);
    (
        prop_oneof![1 => empty, 7 => one, 2 => many],
        prop_oneof![3 => any::<u16>().prop_map(Some), 1 => Just(None)],
        any::<bool>(),
    )
        .prop_map(|(roots, requested, distinct_destination_layout)| {
            let count = fn_count(&roots) as u16;
            let injected_label = requested.and_then(|n| (count > 0).then(|| n % count));
            AssociatedForestCase {
                roots,
                injected_label,
                distinct_destination_layout,
            }
        })
        .boxed()
}

fn fn_count(nodes: &[AssociatedNodeModel]) -> usize {
    nodes
        .iter()
        .map(|n| {
            1 + fn_count(&n.ordinary)
                + n.associated
                    .iter()
                    .map(|x| fn_count(std::slice::from_ref(&x.1)))
                    .sum::<usize>()
        })
        .sum()
}

#[test]
fn generator_contract_smoke() {
    use proptest::test_runner::{Config, TestRunner};

    let mut runner = TestRunner::new(Config {
        // Sample empty, multi-root, nested, and injected cases.
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(&arb_associated_forest(), |case| {
            let count = fn_count(&case.roots);
            proptest::prop_assert!(
                case.injected_label
                    .is_none_or(|label| usize::from(label) < count)
            );
            Ok(())
        })
        .unwrap();
}

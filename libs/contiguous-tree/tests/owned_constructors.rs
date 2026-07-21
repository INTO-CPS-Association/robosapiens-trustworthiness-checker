use contiguous_tree::{TreeCursorExt, tree_schema};
use ecow::{EcoString, EcoVec, eco_vec};

tree_schema! {
    pub tree PublicTree {
        internals: pub(crate),
        owned_constructors: pub,
        metadata: metadata: () = (),
        id: u32,
        key: EcoString,
        children: EcoVec,
        keyed_children: EcoVec,

        Leaf(value: data(EcoString)),
        Number(value: copy(u32)),
        Branch(label: data(EcoString), first: child, rest: children),
        Keyed(entries: keyed_children),
    }
}

#[test]
fn public_owned_constructors_work_from_an_integration_crate() {
    let branch = PublicTree::Branch(
        "branch".into(),
        Box::new(PublicTree::Leaf("first".into())),
        eco_vec![PublicTree::Number(2), PublicTree::Number(3)],
    );
    let tree = PublicTree::Keyed(eco_vec![("root".into(), branch)]);

    assert_eq!(tree.as_ref().postorder().count(), 5);
    let PublicTreeView::Keyed(fields) = tree.as_ref().view() else {
        panic!("expected keyed root");
    };
    let PublicTreeView::Branch(label, first, mut rest) = fields.get("root").unwrap().view() else {
        panic!("expected branch");
    };
    assert_eq!(label, "branch");
    assert!(matches!(first.view(), PublicTreeView::Leaf(value) if value == "first"));
    assert!(matches!(
        rest.next().unwrap().view(),
        PublicTreeView::Number(2)
    ));
    assert!(matches!(
        rest.next().unwrap().view(),
        PublicTreeView::Number(3)
    ));
}

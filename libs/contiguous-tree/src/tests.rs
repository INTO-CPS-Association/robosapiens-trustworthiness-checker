use super::{
    __private::ChildIds, AnnotationError, Arena, ArenaId, CloneTreeError, ContextCursor, Forest,
    ForestBuilder, ForestError, ForestMap, ForestMapError, IdRange, PostorderStorage,
    ResolvedFields, ResolvedIds, StorageIdentity, TreeCursor, TreeCursorExt, TreeHandle, TreeNode,
    TreeNodeMut, TreeStorage,
};
use ecow::{EcoString, EcoVec, eco_vec};

crate::tree_schema! {
    pub tree Fixture {
        schema: pub(crate),
        serialize: display,
        owned_constructors: #[cfg(test)] pub(crate),
        metadata: offset: u32 = 0,
        id: u32,
        key: EcoString,
        children: EcoVec,
        keyed_children: EcoVec,

        Leaf(value: data(EcoString)),
        Offset(value: copy(u64)),
        Unary(value: child),
        Sequence(values: children),
        Record(fields: keyed_children),
        Mixed(label: data(EcoString), first: child, rest: children, flag: copy(bool)),
    }
}

#[cfg(feature = "serde")]
impl std::fmt::Display for FixtureRef<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, formatter)
    }
}

crate::tree_schema! {
    pub tree Minimal {
        schema: pub(crate),
        metadata: metadata: () = (),
        id: u32,

        Leaf(value: data(EcoString)),
        Unary(child: child),
    }
}

// This name remains available because schemas without keyed children do not
// generate keyed-field support.
struct MinimalFields;

crate::tree_schema! {
    pub tree SequenceOnly {
        schema: pub(crate),
        metadata: metadata: () = (),
        id: u32,
        children: EcoVec,

        Leaf(value: data(EcoString)),
        Sequence(children: children),
    }
}

#[cfg(feature = "serde")]
#[test]
fn generated_serialization_uses_display_and_preserves_forest_map_entries() {
    let tree = Fixture::Leaf("leaf".into());
    assert_eq!(
        serde_json::to_value(&tree).unwrap(),
        serde_json::Value::String(tree.as_ref().to_string())
    );
    assert_eq!(
        serde_json::to_value(tree.as_ref()).unwrap(),
        serde_json::Value::String(tree.as_ref().to_string())
    );

    let mut builder = FixtureBuilder::with_capacity(1);
    let root = builder.alloc_default(FixtureKind::Leaf("leaf".into()));
    let forest = builder.finish_forest([root]).unwrap();
    let map = FixtureForestMap::new([String::from("root")], forest).unwrap();
    assert_eq!(
        serde_json::to_value(map).unwrap(),
        serde_json::json!({"root": "Leaf(\"leaf\")"})
    );
}

#[test]
fn generated_support_is_demand_driven_by_field_shapes() {
    let _available_name = MinimalFields;

    let mut minimal = MinimalBuilder::with_capacity(2);
    let leaf = minimal.alloc_default(MinimalKind::Leaf("leaf".into()));
    let root = minimal.alloc_default(MinimalKind::Unary(leaf));
    assert_eq!(minimal.finish(root).unwrap().as_ref().postorder().len(), 2);

    let mut sequence = SequenceOnlyBuilder::with_capacity(2);
    let leaf = sequence.alloc_default(SequenceOnlyKind::Leaf("leaf".into()));
    let root = sequence.alloc_default(SequenceOnlyKind::Sequence(eco_vec![leaf]));
    assert_eq!(sequence.finish(root).unwrap().as_ref().postorder().len(), 2);
}

#[test]
fn owned_constructors_cover_fields_and_preserve_schema_child_order() {
    let leaf = Fixture::Leaf("leaf".into());
    let offset = Fixture::Offset(2);
    let unary = Fixture::Unary(Box::new(leaf));
    let sequence = Fixture::Sequence(eco_vec![unary, offset]);
    let fixture = Fixture::Record(eco_vec![("value".into(), sequence)]);

    assert_eq!(fixture.as_ref().postorder().count(), 5);
    let FixtureView::Record(fields) = fixture.as_ref().view() else {
        panic!("expected record root");
    };
    let FixtureView::Sequence(mut values) = fields.get("value").unwrap().view() else {
        panic!("expected sequence child");
    };
    let FixtureView::Unary(leaf) = values.next().unwrap().view() else {
        panic!("expected unary child first");
    };
    assert!(matches!(leaf.view(), FixtureView::Leaf(value) if value == "leaf"));
    assert!(matches!(
        values.next().unwrap().view(),
        FixtureView::Offset(2)
    ));

    let mixed = Fixture::Mixed(
        "ordered".into(),
        Box::new(Fixture::Offset(1)),
        eco_vec![Fixture::Offset(2), Fixture::Offset(3)],
        true,
    );
    let FixtureView::Mixed(label, first, rest, flag) = mixed.as_ref().view() else {
        panic!("expected mixed root");
    };
    assert_eq!(label, "ordered");
    assert!(matches!(first.view(), FixtureView::Offset(1)));
    assert_eq!(
        rest.map(|child| match child.view() {
            FixtureView::Offset(value) => value,
            _ => panic!("expected offset"),
        })
        .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(flag);
}

#[test]
fn generated_schema_covers_every_field_shape() {
    let empty_fields = FixtureFields::default();
    assert!(empty_fields.iter().next().is_none());

    let mut builder = FixtureBuilder::with_capacity(6);
    let leaf = builder.alloc_default(FixtureKind::Leaf("leaf".into()));
    let unary = builder.alloc_default(FixtureKind::Unary(leaf));
    let offset = builder.alloc_default(FixtureKind::Offset(2));
    let sequence = builder.alloc_default(FixtureKind::Sequence(eco_vec![unary, offset]));
    let record = builder.alloc_default(FixtureKind::Record(
        [(EcoString::from("value"), sequence)].into_iter().collect(),
    ));
    let fixture = builder.finish(record).unwrap();

    let FixtureView::Record(fields) = fixture.as_ref().view() else {
        panic!("expected record root");
    };
    let sequence = fields.get("value").unwrap();
    let FixtureView::Sequence(mut values) = sequence.view() else {
        panic!("expected sequence child");
    };
    assert!(matches!(
        values.next().unwrap().view(),
        FixtureView::Unary(_)
    ));
    assert!(matches!(
        values.next().unwrap().view(),
        FixtureView::Offset(2)
    ));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TestId(usize);

impl ArenaId for TestId {
    fn from_index(index: usize) -> Self {
        Self(index)
    }

    fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestNode(Vec<TestId>);

impl TreeNode<TestId> for TestNode {
    type ChildIds<'node> = std::iter::Copied<std::slice::Iter<'node, TestId>>;

    fn child_ids(&self) -> Self::ChildIds<'_> {
        self.0.iter().copied()
    }
}

impl TreeNodeMut<TestId> for TestNode {
    fn for_each_child_id_mut(&mut self, mut visit: impl FnMut(&mut TestId)) {
        self.0.iter_mut().for_each(&mut visit);
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TestCursor<'arena> {
    arena: &'arena Arena<TestId, TestNode>,
    id: TestId,
}

impl<'arena> TreeCursor for TestCursor<'arena> {
    type Id = TestId;
    type ChildIds = std::iter::Copied<std::slice::Iter<'arena, TestId>>;

    fn id(self) -> Self::Id {
        self.id
    }

    fn storage_identity(self) -> StorageIdentity {
        StorageIdentity::for_ref(self.arena)
    }

    fn same_node(self, other: Self) -> bool {
        std::ptr::eq(self.arena, other.arena) && self.id == other.id
    }

    fn child_ids(self) -> Self::ChildIds {
        self.arena.get(self.id).0.iter().copied()
    }

    fn child(self, id: Self::Id) -> Self {
        Self {
            arena: self.arena,
            id,
        }
    }

    fn subtree_ids(self) -> IdRange<Self::Id> {
        self.arena.subtree_ids(self.id)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct TestStorage(Arena<TestId, TestNode>);

impl TreeStorage for TestStorage {
    type Id = TestId;
    type Cursor<'arena> = TestCursor<'arena>;

    fn cursor(&self, id: Self::Id) -> Self::Cursor<'_> {
        TestCursor { arena: &self.0, id }
    }

    fn owns(&self, cursor: Self::Cursor<'_>) -> bool {
        std::ptr::eq(&self.0, cursor.arena)
    }

    fn validate_forest(&self, roots: &[Self::Id]) -> Result<(), ForestError> {
        self.0.validate_forest(roots)
    }

    fn node_count(&self) -> usize {
        self.0.len()
    }
}

impl PostorderStorage<TestNode> for TestStorage {
    fn push_node(&mut self, node: TestNode) -> Self::Id {
        self.0.push_tree(node)
    }

    fn reserve_nodes(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    fn truncate_nodes(&mut self, len: usize) {
        self.0.truncate(len);
    }
}

#[test]
fn forest_builder_enforces_the_root_frontier() {
    let mut builder = ForestBuilder::new(TestStorage::default());
    let first = builder.try_alloc(TestNode(vec![])).unwrap();
    let second = builder.try_alloc(TestNode(vec![])).unwrap();
    assert_eq!(
        builder.try_alloc(TestNode(vec![first])),
        Err(super::BuildError::ChildNotOnFrontier {
            position: 0,
            expected: second,
            actual: first,
        })
    );

    let root = builder.try_alloc(TestNode(vec![first, second])).unwrap();
    let forest = builder.finish([root]).unwrap();
    assert_eq!(forest.nodes().count(), 3);
}

#[test]
fn arena_assigns_dense_ids_and_iterates_in_allocation_order() {
    let mut arena: Arena<TestId, _> = Arena::with_capacity(2);
    let first = arena.push_raw("first");
    let second = arena.push_raw("second");

    assert_eq!(first, TestId(0));
    assert_eq!(second, TestId(1));
    assert_eq!(arena.get(second), &"second");
    assert_eq!(
        arena.iter().collect::<Vec<_>>(),
        vec![(first, &"first"), (second, &"second")]
    );
}

#[test]
fn tree_arena_derives_contiguous_subtree_ranges() {
    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    let root = arena.push_tree(TestNode(vec![first, second]));

    assert_eq!(arena.subtree_len(root), 3);
    assert_eq!(
        arena.subtree_ids(root).collect::<Vec<_>>(),
        vec![first, second, root]
    );
    arena.validate_forest(&[root]).unwrap();
    arena.assert_forest([root]);

    let cursor = TestCursor {
        arena: &arena,
        id: root,
    };
    assert_eq!(
        cursor.children().map(|child| child.id).collect::<Vec<_>>(),
        vec![first, second]
    );
    assert_eq!(cursor.subtree_len(), 3);
    assert_eq!(
        cursor.postorder().map(|node| node.id).collect::<Vec<_>>(),
        vec![first, second, root]
    );

    let resolved = ResolvedIds::new(cursor, [first, second].into_iter())
        .map(|child| child.id)
        .collect::<Vec<_>>();
    assert_eq!(resolved, vec![first, second]);

    let fields = [("first", first), ("second", second)];
    let resolved_fields = ResolvedFields::new(cursor, &fields);
    assert_eq!(resolved_fields.get("second").unwrap().id, second);
    assert_eq!(
        resolved_fields.keys().copied().collect::<Vec<_>>(),
        vec!["first", "second"]
    );

    let node_count = cursor.fold(|node| 1 + node.children().copied().sum::<usize>());
    assert_eq!(node_count, 3);

    let contextual = ContextCursor::new(cursor, "checked");
    assert!(
        contextual
            .children()
            .all(|child| child.context() == "checked")
    );

    let mut copied = Arena::default();
    let copied_root = copied.clone_tree_from(cursor, |cursor| cursor.arena.get(cursor.id).clone());
    assert_eq!(copied_root, root);
    assert_eq!(copied, arena);
}

#[test]
fn forests_validate_empty_single_and_multiple_roots() {
    let empty = Forest::new(TestStorage::default(), Vec::new()).unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert!(empty.root_ids().is_empty());
    assert_eq!(empty.cursors().count(), 0);

    let mut arena = Arena::default();
    let only = arena.push_tree(TestNode(vec![]));
    let single = Forest::new(TestStorage(arena), [only]).unwrap();
    assert_eq!(single.root_ids(), &[only]);
    assert_eq!(single.cursor(0).id(), only);

    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    let multiple = Forest::new(TestStorage(arena), [second, first]).unwrap();
    assert_eq!(
        multiple.cursors().map(TreeCursor::id).collect::<Vec<_>>(),
        vec![second, first]
    );
    assert_eq!(multiple.storage().0.len(), 2);
}

#[test]
fn forest_validation_reports_duplicate_missing_nested_and_shared_roots() {
    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    assert_eq!(
        arena.validate_forest(&[first, first, second]),
        Err(ForestError::DuplicateRoot {
            root: first.index()
        })
    );
    assert_eq!(
        arena.validate_forest(&[first]),
        Err(ForestError::MissingRoot {
            index: second.index()
        })
    );

    let mut nested = Arena::default();
    let child = nested.push_tree(TestNode(vec![]));
    let root = nested.push_tree(TestNode(vec![child]));
    assert_eq!(
        nested.validate_forest(&[child, root]),
        Err(ForestError::RootHasParent {
            root: child.index(),
            parent_count: 1
        })
    );

    let mut shared = Arena::default();
    let child = shared.push_raw(TestNode(vec![]));
    let first_parent = shared.push_raw(TestNode(vec![child]));
    let second_parent = shared.push_raw(TestNode(vec![child]));
    assert_eq!(
        shared.validate_forest(&[first_parent, second_parent]),
        Err(ForestError::SharedOccurrence {
            index: child.index(),
            parent_count: 2
        })
    );
}

#[test]
fn forest_validation_reports_malformed_edges() {
    let mut out_of_bounds = Arena::default();
    let root = out_of_bounds.push_raw(TestNode(vec![TestId(9)]));
    assert_eq!(
        out_of_bounds.validate_forest(&[root]),
        Err(ForestError::ChildOutOfBounds {
            parent: 0,
            child: 9,
            len: 1
        })
    );

    let mut not_postorder = Arena::default();
    let root = not_postorder.push_raw(TestNode(vec![TestId(0)]));
    assert_eq!(
        not_postorder.validate_forest(&[root]),
        Err(ForestError::ChildNotPostorder {
            parent: 0,
            child: 0
        })
    );

    let mut gapped_children = Arena::default();
    let first = gapped_children.push_raw(TestNode(vec![]));
    let gap = gapped_children.push_raw(TestNode(vec![]));
    let second = gapped_children.push_raw(TestNode(vec![]));
    let root = gapped_children.push_raw(TestNode(vec![first, second]));
    assert_eq!(
        gapped_children.validate_forest(&[gap, root]),
        Err(ForestError::ChildSubtreeGap {
            parent: root.index(),
            child: second.index(),
            expected_start: first.index() + 1,
            actual_start: second.index(),
        })
    );

    let mut detached_parent = Arena::default();
    let child = detached_parent.push_raw(TestNode(vec![]));
    let gap = detached_parent.push_raw(TestNode(vec![]));
    let root = detached_parent.push_raw(TestNode(vec![child]));
    assert_eq!(
        detached_parent.validate_forest(&[gap, root]),
        Err(ForestError::ChildrenNotAdjacentToParent {
            parent: root.index(),
            last_child: child.index(),
            child_end: child.index() + 1,
        })
    );
}

#[test]
fn forest_handles_share_storage_and_preserve_root_order() {
    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    let forest = Forest::new(TestStorage(arena), [second, first]).unwrap();

    let handles = forest.handles().collect::<Vec<_>>();
    assert_eq!(
        handles.iter().map(TreeHandle::root_id).collect::<Vec<_>>(),
        vec![second, first]
    );
    assert!(handles[0].shares_storage_with(&handles[1]));
    assert!(forest.handle(0).shares_storage_with(&handles[0]));
    drop(handles);

    let handles = forest.into_handles().collect::<Vec<_>>();
    assert_eq!(
        handles.iter().map(TreeHandle::root_id).collect::<Vec<_>>(),
        vec![second, first]
    );
    assert!(handles[0].shares_storage_with(&handles[1]));
}

#[test]
fn forest_maps_require_sorted_unique_keys_and_preserve_associations() {
    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    let forest = Forest::new(TestStorage(arena), [second, first]).unwrap();
    let map = ForestMap::new(["a", "b"], forest).unwrap();

    assert_eq!(map.keys().copied().collect::<Vec<_>>(), ["a", "b"]);
    assert_eq!(map.get(&"a").unwrap().id(), second);
    assert_eq!(map.get(&"b").unwrap().id(), first);

    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    let forest = Forest::new(TestStorage(arena), [first, second]).unwrap();
    assert!(matches!(
        ForestMap::new(["b", "a"], forest),
        Err(ForestMapError::KeysNotStrictlyOrdered { index: 1 })
    ));

    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    let forest = Forest::new(TestStorage(arena), [first, second]).unwrap();
    let map = ForestMap::from_unsorted(["b", "a"], forest).unwrap();
    assert_eq!(map.keys().copied().collect::<Vec<_>>(), ["a", "b"]);
    assert_eq!(map.get(&"a").unwrap().id(), second);
    assert_eq!(map.get(&"b").unwrap().id(), first);

    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    let forest = Forest::new(TestStorage(arena), [first, second]).unwrap();
    assert_eq!(
        ForestMap::from_unsorted(["a", "a"], forest).err().unwrap(),
        ForestMapError::DuplicateKey {
            first_index: 0,
            duplicate_index: 1,
        }
    );
}

#[test]
fn generated_forest_map_rewrite_rebuilds_all_roots_in_compact_shared_storage() {
    let mut builder = FixtureBuilder::with_capacity(3);
    let child = builder.alloc_default(FixtureKind::Leaf("replace".into()));
    let first = builder.alloc_default(FixtureKind::Unary(child));
    let second = builder.alloc_default(FixtureKind::Leaf("replacement".into()));
    let forest = builder.finish_forest([first, second]).unwrap();
    let map = FixtureForestMap::new(["a", "b"], forest).unwrap();
    let replacement = map.get(&"b").unwrap();
    let original = map.get_owned(&"a").unwrap();

    let rewritten = map
        .try_rewrite_with(|cursor| {
            Ok::<_, std::convert::Infallible>(match cursor.view() {
                FixtureView::Leaf(value) if value == "replace" => Some(replacement),
                _ => None,
            })
        })
        .unwrap();

    let FixtureView::Unary(child) = rewritten.get(&"a").unwrap().view() else {
        panic!("expected rewritten unary root");
    };
    assert!(matches!(
        child.view(),
        FixtureView::Leaf(value) if value == "replacement"
    ));
    assert!(matches!(
        rewritten.get(&"b").unwrap().view(),
        FixtureView::Leaf(value) if value == "replacement"
    ));
    let first = rewritten.get_owned(&"a").unwrap();
    let second = rewritten.get_owned(&"b").unwrap();
    assert!(first.shares_storage_with(&second));
    assert!(!original.shares_storage_with(&first));
    assert_eq!(rewritten.nodes().len(), 3);
}

#[test]
fn generated_forest_map_rewrite_selected_omits_unselected_storage() {
    let mut builder = FixtureBuilder::with_capacity(3);
    let child = builder.alloc_default(FixtureKind::Leaf("child".into()));
    let retained = builder.alloc_default(FixtureKind::Unary(child));
    let removed = builder.alloc_default(FixtureKind::Leaf("removed".into()));
    let forest = builder.finish_forest([retained, removed]).unwrap();
    let map = FixtureForestMap::new(["a", "b"], forest).unwrap();

    let rewritten = map
        .try_rewrite_selected_with(
            |key| *key == "a",
            |_| Ok::<_, std::convert::Infallible>(None),
        )
        .unwrap();

    assert_eq!(rewritten.keys().copied().collect::<Vec<_>>(), ["a"]);
    assert_eq!(rewritten.nodes().len(), 2);
    let FixtureView::Unary(child) = rewritten.get(&"a").unwrap().view() else {
        panic!("expected retained unary root");
    };
    assert!(matches!(
        child.view(),
        FixtureView::Leaf(value) if value == "child"
    ));

    let empty = map
        .try_rewrite_selected_with(|_| false, |_| Ok::<_, std::convert::Infallible>(None))
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.nodes().len(), 0);
}

#[test]
fn generated_forest_map_rewrite_reports_transitive_replacement_cycles() {
    let mut builder = FixtureBuilder::with_capacity(2);
    let first = builder.alloc_default(FixtureKind::Leaf("first".into()));
    let second = builder.alloc_default(FixtureKind::Leaf("second".into()));
    let forest = builder.finish_forest([first, second]).unwrap();
    let map = FixtureForestMap::new(["a", "b"], forest).unwrap();
    let first = map.get(&"a").unwrap();
    let second = map.get(&"b").unwrap();

    let error = map
        .try_rewrite_with(|cursor| {
            Ok::<_, std::convert::Infallible>(if cursor.id() == first.id() {
                Some(second)
            } else if cursor.id() == second.id() {
                Some(first)
            } else {
                None
            })
        })
        .unwrap_err();

    assert!(matches!(
        error,
        CloneTreeError::ReplacementCycle { cursors } if cursors.len() == 3
    ));
}

#[test]
fn generated_forest_map_retain_preserves_storage_when_every_root_is_kept() {
    let mut builder = FixtureBuilder::with_capacity(2);
    let first = builder.alloc_default(FixtureKind::Leaf("first".into()));
    let second = builder.alloc_default(FixtureKind::Leaf("second".into()));
    let forest = builder.finish_forest([first, second]).unwrap();
    let mut map = FixtureForestMap::new(["a", "b"], forest).unwrap();
    let before = map.get_owned(&"a").unwrap();

    map.retain(|_| true);

    let after = map.get_owned(&"a").unwrap();
    assert!(before.shares_storage_with(&after));
    assert_eq!(map.nodes().len(), 2);
}

#[test]
fn generated_forest_map_retain_rebuilds_compact_storage_after_removal() {
    let mut builder = FixtureBuilder::with_capacity(3);
    let child = builder.alloc_default(FixtureKind::Leaf("child".into()));
    let retained = builder.alloc_default(FixtureKind::Unary(child));
    let removed = builder.alloc_default(FixtureKind::Leaf("removed".into()));
    let forest = builder.finish_forest([retained, removed]).unwrap();
    let mut map = FixtureForestMap::new(["a", "b"], forest).unwrap();
    let before = map.get_owned(&"a").unwrap();

    map.retain(|key| *key == "a");

    let after = map.get_owned(&"a").unwrap();
    assert_eq!(map.len(), 1);
    assert_eq!(map.nodes().len(), before.as_ref().subtree_len());
    assert!(!before.shares_storage_with(&after));
}

#[test]
fn annotations_are_bound_to_storage_and_scope() {
    let mut arena = Arena::default();
    let first_root = arena.push_tree(TestNode(vec![]));
    let second_child = arena.push_tree(TestNode(vec![]));
    let second_root = arena.push_tree(TestNode(vec![second_child]));
    let forest = Forest::new(TestStorage(arena), [first_root, second_root]).unwrap();
    let nodes = forest.nodes().collect::<Vec<_>>();

    let mut subtree = forest.handle(1).annotations_builder();
    assert_eq!(subtree.len(), 2);
    assert_eq!(
        subtree.insert(nodes[0], "outside"),
        Err(AnnotationError::CursorOutsideScope { index: 0 })
    );
    subtree.insert(nodes[1], "child").unwrap();
    assert_eq!(
        subtree.finish().unwrap_err(),
        AnnotationError::Incomplete { index: 2 }
    );

    let mut subtree = forest.handle(1).annotations_builder();
    subtree.insert(nodes[1], "child").unwrap();
    subtree.insert(nodes[2], "root").unwrap();
    let subtree = subtree.finish().unwrap();
    assert_eq!(subtree.get(nodes[1]), Some(&"child"));
    assert_eq!(subtree.get(nodes[2]), Some(&"root"));
    assert_eq!(subtree.get(nodes[0]), None);

    let mut other_arena = Arena::default();
    let other_root = other_arena.push_tree(TestNode(vec![]));
    let other = Forest::new(TestStorage(other_arena), [other_root]).unwrap();
    let mut whole_forest = forest.annotations_builder();
    assert_eq!(
        whole_forest.insert(other.cursor(0), 99),
        Err(AnnotationError::CursorOutsideScope { index: 0 })
    );
    for (index, node) in forest.nodes().enumerate() {
        whole_forest.insert(node, index).unwrap();
    }
    let whole_forest = whole_forest.finish().unwrap();
    assert_eq!(whole_forest.get(nodes[0]), Some(&0));
    assert_eq!(whole_forest.get(nodes[1]), Some(&1));
    assert_eq!(whole_forest.get(nodes[2]), Some(&2));
}

#[test]
#[should_panic(expected = "subtree root is not a descendant of the owning root")]
fn tree_handles_reject_sibling_roots_as_subtrees() {
    let mut arena = Arena::default();
    let first = arena.push_tree(TestNode(vec![]));
    let second = arena.push_tree(TestNode(vec![]));
    let forest = Forest::new(TestStorage(arena), [first, second]).unwrap();
    let first = forest.handle(0);

    first.subtree(forest.cursor(1));
}

#[test]
fn clone_tree_with_rewrites_replacements_and_reports_cycles() {
    let mut source = Arena::default();
    let first = source.push_tree(TestNode(vec![]));
    let root = source.push_tree(TestNode(vec![first]));
    let second = source.push_tree(TestNode(vec![]));
    let cursor = |id| TestCursor { arena: &source, id };

    let mut target = Arena::default();
    let rewritten = target
        .try_clone_tree_with(
            cursor(root),
            |node| Ok::<_, std::convert::Infallible>((node.id == first).then(|| cursor(second))),
            |node| node.arena.get(node.id).clone(),
        )
        .unwrap();
    assert_eq!(target.subtree_len(rewritten), 2);

    let mut target = Arena::default();
    let cycle = target
        .try_clone_tree_with(
            cursor(root),
            |node| {
                Ok::<_, std::convert::Infallible>(if node.id == first {
                    Some(cursor(second))
                } else if node.id == second {
                    Some(cursor(first))
                } else {
                    None
                })
            },
            |node| node.arena.get(node.id).clone(),
        )
        .unwrap_err();
    assert!(matches!(
        cycle,
        CloneTreeError::ReplacementCycle { cursors } if cursors.len() == 3
    ));
}

#[test]
fn replacement_cloning_handles_transitive_chains_recursively() {
    let mut source = Arena::default();
    let mut root = source.push_raw(TestNode(vec![]));
    for _ in 0..256 {
        root = source.push_raw(TestNode(vec![root]));
    }
    let root = TestCursor {
        arena: &source,
        id: root,
    };
    let mut target = Arena::default();

    let cloned = target
        .try_clone_tree_with(
            root,
            |node| {
                Ok::<_, std::convert::Infallible>(
                    node.child_ids().next().map(|child| node.child(child)),
                )
            },
            |node| node.arena.get(node.id).clone(),
        )
        .unwrap();

    assert_eq!(target.len(), 1);
    assert_eq!(target.get(cloned), &TestNode(vec![]));
}

#[test]
fn clone_tree_with_policy_errors_stop_sibling_traversal() {
    let mut source = Arena::default();
    let first = source.push_tree(TestNode(vec![]));
    let second = source.push_tree(TestNode(vec![]));
    let root = source.push_tree(TestNode(vec![first, second]));
    let root = TestCursor {
        arena: &source,
        id: root,
    };
    let mut visited_second = false;
    let mut target = Arena::default();

    let error = target
        .try_clone_tree_with(
            root,
            |node| {
                if node.id == first {
                    Err("stopped")
                } else {
                    visited_second |= node.id == second;
                    Ok(None)
                }
            },
            |node| node.arena.get(node.id).clone(),
        )
        .unwrap_err();

    assert_eq!(error, CloneTreeError::Policy("stopped"));
    assert!(!visited_second);
}

#[test]
fn fallible_transforms_roll_back_partial_output() {
    let mut source = Arena::default();
    let first = source.push_tree(TestNode(vec![]));
    let second = source.push_tree(TestNode(vec![]));
    let root = source.push_tree(TestNode(vec![first, second]));
    let root = TestCursor {
        arena: &source,
        id: root,
    };

    let mut mapped = Arena::default();
    mapped.push_tree(TestNode(vec![]));
    let map_error = mapped
        .try_map_tree_from(root, |node| {
            if node.cursor().id == second {
                Err("stopped")
            } else {
                Ok(node.rebuild(source.get(node.cursor().id).clone()))
            }
        })
        .unwrap_err();
    assert_eq!(map_error, "stopped");
    assert_eq!(mapped.len(), 1);

    let mut cloned = Arena::default();
    cloned.push_tree(TestNode(vec![]));
    let clone_error = cloned
        .try_clone_tree_with(
            root,
            |node| {
                if node.id == second {
                    Err("stopped")
                } else {
                    Ok(None)
                }
            },
            |node| node.arena.get(node.id).clone(),
        )
        .unwrap_err();
    assert_eq!(clone_error, CloneTreeError::Policy("stopped"));
    assert_eq!(cloned.len(), 1);
}

#[test]
fn generated_clone_transactions_restore_storage_and_frontier() {
    let retained_source = Fixture::Leaf("retained".into());
    let source = Fixture::Sequence(eco_vec![
        Fixture::Leaf("first".into()),
        Fixture::Leaf("second".into()),
    ]);
    let mut builder = FixtureBuilder::with_capacity(6);
    let retained = builder.clone_subtree(retained_source.as_ref());

    let error = builder
        .try_clone_subtree_with(source.as_ref(), |node| match node.view() {
            FixtureView::Leaf(value) if value == "second" => Err("stopped"),
            _ => Ok(None),
        })
        .unwrap_err();
    assert!(matches!(error, CloneTreeError::Policy("stopped")));

    let root = builder.clone_subtree(source.as_ref());
    let forest = builder.finish_forest([retained, root]).unwrap();
    assert_eq!(forest.nodes().len(), 4);
    assert_eq!(forest.len(), 2);
}

#[test]
fn fixed_children_precede_a_lazy_tail_in_both_directions() {
    let tail = [2, 3, 4];
    let mut children = ChildIds::<_, ()>::empty();
    children.push(1);
    children.extend_slice(&tail);

    assert_eq!(children.len(), 4);
    assert_eq!(children.next(), Some(1));
    assert_eq!(children.next_back(), Some(4));
    assert_eq!(children.collect::<Vec<_>>(), vec![2, 3]);
}

#[test]
fn keyed_children_yield_values_in_source_order() {
    let fields = [("first", 1), ("second", 2)];
    let mut children = ChildIds::empty();
    children.extend_keyed(&fields);

    assert_eq!(children.collect::<Vec<_>>(), vec![1, 2]);
}

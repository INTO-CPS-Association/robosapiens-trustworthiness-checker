//! Contiguous postorder storage for occurrence trees.
//!
//! Nodes are allocated after their children, so every subtree occupies one
//! contiguous range of IDs. This makes [`TreeCursorExt::postorder`] and
//! [`TreeCursorExt::fold`] linear scans over storage. Use
//! [`TreeCursorExt::children`] for direct children and
//! [`TreeCursorExt::try_zip_with`] for structural comparison.
//!
//! [`Arena`] and [`TreeNode`] are the low-level storage interface. Most typed
//! trees are declared with [`tree_schema!`] and inspected through a copyable
//! [`TreeCursor`].
//!
//! # Representation invariants
//!
//! Child subtrees must be allocated before their parent, adjacent to one
//! another, and in source order. A node ID represents one occurrence and cannot
//! be shared by multiple parents; multiple independent roots may share storage.
//!
//! # Example
//!
//! ```
//! use contiguous_tree::{TreeCursorExt, tree_schema};
//! use ecow::EcoVec;
//!
//! tree_schema! {
//!     pub tree Term {
//!         internals: pub(crate),
//!         metadata: offset: u32 = 0,
//!         id: u32,
//!         key: String,
//!         children: EcoVec,
//!         keyed_children: EcoVec,
//!
//!         Number(value: data(i64)),
//!         Add(left: child, right: child),
//!     }
//! }
//!
//! let mut builder = TermBuilder::with_capacity(3);
//! let left = builder.alloc_default(TermKind::Number(1));
//! let right = builder.alloc_default(TermKind::Number(2));
//! let root = builder.alloc_default(TermKind::Add(left, right));
//! let term = Term::new(TermHandle::new(builder.finish_arena(), root));
//!
//! assert_eq!(term.as_ref().postorder().count(), 3);
//! ```

extern crate self as contiguous_tree;

mod arena;
mod cursor;
mod fields;
mod fold;
mod handle;
mod node;
mod transform;

pub use arena::{Arena, ArenaId, IdRange};
pub use cursor::{Children, ContextCursor, Postorder, TreeCursor, TreeCursorExt, try_zip_with};
pub use fields::{KeyedFields, ResolvedFields, ResolvedIds};
pub use fold::{FoldNode, fold, try_fold};
pub use handle::{NodeAnnotations, TreeHandle, TreeStorage};
pub use node::{ChildIds, TreeNode, TreeNodeMut, map_child_ids};
pub use transform::CloneTreeError;

pub use contiguous_tree_macros::{TreeCursor, tree_schema};

#[cfg(test)]
mod tests {
    use super::{
        Arena, ArenaId, ChildIds, CloneTreeError, ContextCursor, IdRange, ResolvedFields,
        ResolvedIds, TreeCursor, TreeCursorExt, TreeNode, TreeNodeMut,
    };
    use ecow::{EcoString, EcoVec, eco_vec};

    crate::tree_schema! {
        pub tree Fixture {
            internals: pub(crate),
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
        let fixture = Fixture::new(FixtureHandle::new(builder.finish_arena(), record));

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
        arena.assert_forest([root]);

        let cursor = TestCursor {
            arena: &arena,
            id: root,
        };
        assert_eq!(
            cursor.children().map(|child| child.id).collect::<Vec<_>>(),
            vec![first, second]
        );
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
        let copied_root =
            copied.clone_tree_from(cursor, |cursor| cursor.arena.get(cursor.id).clone());
        assert_eq!(copied_root, root);
        assert_eq!(copied, arena);
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
                |node| {
                    Ok::<_, std::convert::Infallible>((node.id == first).then(|| cursor(second)))
                },
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
}

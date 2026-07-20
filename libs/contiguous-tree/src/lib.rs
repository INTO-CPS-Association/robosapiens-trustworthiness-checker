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
//! let left = builder.Number(1);
//! let right = builder.Number(2);
//! let root = builder.Add(left, right);
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
pub use fields::{ResolvedFields, ResolvedIds};
pub use fold::{FoldNode, fold, try_fold};
pub use handle::{NodeAnnotations, TreeHandle, TreeStorage};
pub use node::{ChildIds, TreeNode, TreeNodeMut, map_child_ids};

/// Dependencies used by generated code. Not part of the application-facing API.
#[doc(hidden)]
pub mod __private {
    pub use serde;
}

pub use contiguous_tree_macros::{TreeCursor, tree_schema};

#[cfg(test)]
mod tests {
    use super::{
        Arena, ArenaId, ChildIds, ContextCursor, IdRange, ResolvedFields, ResolvedIds, TreeCursor,
        TreeCursorExt, TreeNode, TreeNodeMut,
    };
    use ecow::{EcoString, EcoVec, eco_vec};

    crate::tree_schema! {
        pub tree Fixture {
            internals: pub(crate),
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
        }
    }

    #[test]
    fn generated_schema_covers_every_field_shape() {
        let mut builder = FixtureBuilder::with_capacity(6);
        let leaf = builder.Leaf("leaf".into());
        let offset = builder.Offset(2);
        let unary = builder.Unary(leaf);
        let sequence = builder.Sequence(eco_vec![unary, offset]);
        let record = builder.Record([(EcoString::from("value"), sequence)].into_iter().collect());
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

    #[derive(Clone, Copy)]
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

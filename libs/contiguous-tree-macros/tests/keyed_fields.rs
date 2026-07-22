use std::{fmt, iter::FromIterator, ops::Deref};

use renamed_contiguous_tree::tree_schema;

#[derive(Clone, Debug, PartialEq)]
pub struct Storage<T>(Vec<T>);

impl<T> Default for Storage<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T> Storage<T> {
    fn make_mut(&mut self) -> &mut [T] {
        &mut self.0
    }
}

impl<T> AsRef<[T]> for Storage<T> {
    fn as_ref(&self) -> &[T] {
        &self.0
    }
}

impl<T> Deref for Storage<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> FromIterator<T> for Storage<T> {
    fn from_iter<Items: IntoIterator<Item = T>>(items: Items) -> Self {
        Self(items.into_iter().collect())
    }
}

impl<T> IntoIterator for Storage<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

tree_schema! {
    pub tree Fixture {
        schema: pub(crate),
        metadata: metadata: () = (),
        id: u32,
        key: String,
        children: Storage,
        keyed_children: Storage,

        Leaf(),
        Record(fields: keyed_children),
    }
}

mod default_visibility {
    use renamed_contiguous_tree::tree_schema;

    tree_schema! {
        pub tree DefaultFixture {
            metadata: metadata: () = (),
            id: u32,
            key: String,
            children: Storage,
            keyed_children: Storage,

            Leaf(value: data(String)),
            Parent(child: child),
        }
    }
}

impl fmt::Display for FixtureFields {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} fields", self.iter().len())
    }
}

#[test]
fn schema_visibility_defaults_to_crate() {
    let mut builder = default_visibility::DefaultFixtureBuilder::with_capacity(2);
    let leaf = builder.alloc_default(default_visibility::DefaultFixtureKind::Leaf(String::from(
        "leaf",
    )));
    let root = builder.alloc_default(default_visibility::DefaultFixtureKind::Parent(leaf));
    let tree = builder.finish(root).unwrap();

    assert_eq!(tree.id(), root);
}

#[test]
fn generated_keyed_fields_preserve_the_delegated_api() {
    let mut builder = FixtureBuilder::with_capacity(3);
    let first = builder.alloc_default(FixtureKind::Leaf());
    let second = builder.alloc_default(FixtureKind::Leaf());
    let fields: FixtureFields = [
        (String::from("name"), first),
        (String::from("name"), second),
    ]
    .into_iter()
    .collect();

    assert_eq!(fields.get("name"), Some(second));
    assert!(fields.contains_key("name"));
    assert_eq!(fields.duplicate_key(), Some(&String::from("name")));
    assert_eq!(fields.keys().collect::<Vec<_>>(), ["name", "name"]);
    assert_eq!(
        fields.values().copied().collect::<Vec<_>>(),
        [first, second]
    );
    assert_eq!((&fields).into_iter().count(), 2);
    assert_eq!(fields.as_slice().len(), 2);
    assert_eq!(fields.to_string(), "2 fields");
    assert_eq!(fields.clone(), fields);

    let from_storage = FixtureFields::from(Storage::from_iter([(String::from("other"), first)]));
    assert_eq!(
        from_storage.into_iter().collect::<Vec<_>>(),
        [(String::from("other"), first)]
    );

    let empty = FixtureFields::default();
    assert!(empty.iter().next().is_none());
    assert!(format!("{empty:?}").contains("FixtureFields"));
}

#[test]
fn generated_builder_finishes_validated_roots_and_forests() {
    let mut single = FixtureBuilder::with_capacity(1);
    let root = single.alloc_default(FixtureKind::Leaf());
    assert_eq!(single.metadata(root), &());
    let tree = single.finish(root).unwrap();
    assert_eq!(tree.id(), root);

    let mut multiple = FixtureBuilder::with_capacity(2);
    let first = multiple.alloc_default(FixtureKind::Leaf());
    let second = multiple.alloc_default(FixtureKind::Leaf());
    let forest = multiple.finish_forest([first, second]).unwrap();
    assert_eq!(forest.len(), 2);
    assert!(!forest.is_empty());
    assert_eq!(forest.root_ids(), &[first, second]);
    assert_eq!(
        forest.roots().map(FixtureRef::id).collect::<Vec<_>>(),
        [first, second]
    );
    assert_eq!(
        forest
            .into_roots()
            .map(|root| root.id())
            .collect::<Vec<_>>(),
        [first, second]
    );
}

#[test]
fn generated_builder_reports_incomplete_forests() {
    let mut builder = FixtureBuilder::with_capacity(2);
    let first = builder.alloc_default(FixtureKind::Leaf());
    let _second = builder.alloc_default(FixtureKind::Leaf());

    assert!(matches!(
        builder.finish(first),
        Err(renamed_contiguous_tree::ForestError::MissingRoot { index: 1 })
    ));
}

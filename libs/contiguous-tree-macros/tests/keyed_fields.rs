use std::{fmt, iter::FromIterator, ops::Deref};

use contiguous_tree::tree_schema;

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
        internals: pub(crate),
        metadata: metadata: () = (),
        id: u32,
        key: String,
        children: Storage,
        keyed_children: Storage,

        Leaf(),
        Record(fields: keyed_children),
    }
}

impl fmt::Display for FixtureFields {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} fields", self.iter().len())
    }
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

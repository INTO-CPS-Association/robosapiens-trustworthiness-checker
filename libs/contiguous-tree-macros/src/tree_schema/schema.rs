use std::collections::HashSet;

use syn::{
    Attribute, Expr, Ident, Token, Type, Visibility, braced, parenthesized,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

mod keyword {
    syn::custom_keyword!(tree);
    syn::custom_keyword!(child);
    syn::custom_keyword!(children);

    syn::custom_keyword!(keyed_children);
    syn::custom_keyword!(data);
    syn::custom_keyword!(into_data);
    syn::custom_keyword!(copy);
    syn::custom_keyword!(metadata);
    syn::custom_keyword!(schema);
    syn::custom_keyword!(owned_constructors);
    syn::custom_keyword!(id);
    syn::custom_keyword!(key);
}

pub(super) enum FieldKind {
    Child,
    Children,

    KeyedChildren,
    Borrowed(Type),
    IntoOwned(Type),
    Copied(Type),
}

impl FieldKind {
    fn is_fixed_child(&self) -> bool {
        matches!(self, Self::Child)
    }

    fn is_collection(&self) -> bool {
        matches!(self, Self::Children | Self::KeyedChildren)
    }
}

pub(super) struct Field {
    pub(super) name: Ident,
    pub(super) kind: FieldKind,
    pub(super) owned_default: Option<Expr>,
}

impl Parse for Field {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let name = input.parse()?;
        input.parse::<Token![:]>()?;
        let kind = if input.peek(keyword::child) {
            input.parse::<keyword::child>()?;
            FieldKind::Child
        } else if input.peek(keyword::children) {
            input.parse::<keyword::children>()?;
            FieldKind::Children
        } else if input.peek(keyword::keyed_children) {
            input.parse::<keyword::keyed_children>()?;
            FieldKind::KeyedChildren
        } else if input.peek(keyword::data) {
            input.parse::<keyword::data>()?;
            FieldKind::Borrowed(parenthesized_type(input)?)
        } else if input.peek(keyword::into_data) {
            input.parse::<keyword::into_data>()?;
            FieldKind::IntoOwned(parenthesized_type(input)?)
        } else if input.peek(keyword::copy) {
            input.parse::<keyword::copy>()?;
            FieldKind::Copied(parenthesized_type(input)?)
        } else {
            return Err(input.error(
                "expected child, children, keyed_children, data(T), into_data(T), or copy(T)",
            ));
        };
        let owned_default = if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            if !matches!(
                kind,
                FieldKind::Borrowed(_) | FieldKind::IntoOwned(_) | FieldKind::Copied(_)
            ) {
                return Err(
                    input.error("owned constructor defaults are only supported for data fields")
                );
            }
            Some(input.parse()?)
        } else {
            None
        };
        Ok(Self {
            name,
            kind,
            owned_default,
        })
    }
}

fn parenthesized_type(input: ParseStream<'_>) -> syn::Result<Type> {
    let content;
    parenthesized!(content in input);
    content.parse()
}

pub(super) struct Variant {
    pub(super) name: Ident,
    pub(super) fields: Punctuated<Field, Token![,]>,
}

impl Variant {
    fn validate(&self) -> syn::Result<()> {
        let mut field_names = HashSet::new();
        for field in &self.fields {
            if !field_names.insert(field.name.to_string()) {
                return Err(syn::Error::new(
                    field.name.span(),
                    "duplicate field in tree variant",
                ));
            }
        }
        if self
            .fields
            .iter()
            .filter(|field| field.kind.is_fixed_child())
            .count()
            > 3
        {
            return Err(syn::Error::new(
                self.name.span(),
                "a tree variant supports at most three fixed children",
            ));
        }
        if self
            .fields
            .iter()
            .filter(|field| field.kind.is_collection())
            .count()
            > 1
        {
            return Err(syn::Error::new(
                self.name.span(),
                "a tree variant supports at most one child collection",
            ));
        }
        let mut saw_collection = false;
        for field in &self.fields {
            if field.kind.is_collection() {
                saw_collection = true;
            } else if field.kind.is_fixed_child() && saw_collection {
                return Err(syn::Error::new(
                    field.name.span(),
                    "fixed children must precede a child collection",
                ));
            }
        }
        Ok(())
    }
}

pub(super) struct OwnedConstructors {
    pub(super) attributes: Vec<Attribute>,
    pub(super) visibility: Visibility,
}

pub(crate) struct TreeSchema {
    pub(super) visibility: Visibility,
    pub(super) schema_visibility: Visibility,
    pub(super) owned_constructors: Option<OwnedConstructors>,
    pub(super) root: Ident,
    pub(super) metadata_name: Ident,
    pub(super) metadata: Type,
    pub(super) metadata_default: Expr,
    pub(super) id: Type,
    pub(super) key: Type,
    pub(super) children: Type,
    pub(super) keyed_children: Type,
    pub(super) uses_keyed_children: bool,
    pub(super) variants: Vec<Variant>,
}

impl Parse for TreeSchema {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let visibility = input.parse()?;
        input.parse::<keyword::tree>()?;
        let root = input.parse()?;
        let content;
        braced!(content in input);

        let mut metadata = None;
        let mut schema_visibility = None;
        let mut owned_constructors = None;
        let mut id = None;
        let mut key = None;
        let mut children = None;
        let mut keyed_children = None;
        let mut variants = Vec::new();
        while !content.is_empty() {
            if content.peek(keyword::metadata) {
                let name: Ident = content.parse()?;
                content.parse::<Token![:]>()?;
                let field_name: Ident = content.parse()?;
                content.parse::<Token![:]>()?;
                let typ: Type = content.parse()?;
                content.parse::<Token![=]>()?;
                let default: Expr = content.parse()?;
                set_once(&mut metadata, (field_name, typ, default), &name)?;
            } else if content.peek(keyword::schema) {
                let name: Ident = content.parse()?;
                content.parse::<Token![:]>()?;
                let visibility: Visibility = content.parse()?;
                if matches!(visibility, Visibility::Inherited) {
                    return Err(syn::Error::new(
                        name.span(),
                        "schema requires an explicit visibility",
                    ));
                }
                set_once(&mut schema_visibility, visibility, &name)?;
            } else if content.peek(keyword::owned_constructors) {
                let name: Ident = content.parse()?;
                content.parse::<Token![:]>()?;
                let attributes = content.call(Attribute::parse_outer)?;
                let visibility: Visibility = content.parse()?;
                if matches!(visibility, Visibility::Inherited) {
                    return Err(syn::Error::new(
                        name.span(),
                        "owned_constructors requires an explicit visibility",
                    ));
                }
                set_once(
                    &mut owned_constructors,
                    OwnedConstructors {
                        attributes,
                        visibility,
                    },
                    &name,
                )?;
            } else if content.peek(keyword::id) {
                parse_type_setting(&content, &mut id)?;
            } else if content.peek(keyword::key) {
                parse_type_setting(&content, &mut key)?;
            } else if content.peek(keyword::children) {
                parse_type_setting(&content, &mut children)?;
            } else if content.peek(keyword::keyed_children) {
                parse_type_setting(&content, &mut keyed_children)?;
            } else {
                let name: Ident = content.parse()?;
                if content.peek(Token![:]) {
                    return Err(syn::Error::new(name.span(), "unknown tree setting"));
                }
                let fields;
                parenthesized!(fields in content);
                variants.push(Variant {
                    name,
                    fields: fields.parse_terminated(Field::parse, Token![,])?,
                });
            }
            content.parse::<Token![,]>()?;
        }

        validate_variants(&variants)?;
        let uses_children = variants.iter().any(|variant| {
            variant
                .fields
                .iter()
                .any(|field| matches!(field.kind, FieldKind::Children))
        });
        let uses_keyed_children = variants.iter().any(|variant| {
            variant
                .fields
                .iter()
                .any(|field| matches!(field.kind, FieldKind::KeyedChildren))
        });
        let children = if uses_children {
            children.ok_or_else(|| content.error("missing children setting"))?
        } else {
            children.unwrap_or_else(|| syn::parse_quote!(()))
        };
        let (key, keyed_children) = if uses_keyed_children {
            (
                key.ok_or_else(|| content.error("missing key setting"))?,
                keyed_children.ok_or_else(|| content.error("missing keyed_children setting"))?,
            )
        } else {
            (syn::parse_quote!(()), syn::parse_quote!(()))
        };

        Ok(Self {
            visibility,
            schema_visibility: schema_visibility.unwrap_or_else(|| syn::parse_quote!(pub(crate))),
            owned_constructors,
            root,
            metadata_name: metadata
                .as_ref()
                .map(|(name, _, _)| name.clone())
                .ok_or_else(|| content.error("missing metadata setting"))?,
            metadata: metadata
                .as_ref()
                .map(|(_, typ, _)| typ.clone())
                .expect("checked above"),
            metadata_default: metadata
                .map(|(_, _, default)| default)
                .expect("checked above"),
            id: id.ok_or_else(|| content.error("missing id setting"))?,
            key,
            children,
            keyed_children,
            uses_keyed_children,
            variants,
        })
    }
}

fn parse_type_setting(content: ParseStream<'_>, slot: &mut Option<Type>) -> syn::Result<()> {
    let name: Ident = content.parse()?;
    content.parse::<Token![:]>()?;
    set_once(slot, content.parse()?, &name)
}

fn set_once<T>(slot: &mut Option<T>, value: T, name: &Ident) -> syn::Result<()> {
    if slot.replace(value).is_some() {
        Err(syn::Error::new(name.span(), "duplicate tree setting"))
    } else {
        Ok(())
    }
}

fn validate_variants(variants: &[Variant]) -> syn::Result<()> {
    let mut errors = None;
    let mut names = HashSet::new();
    for variant in variants {
        if !names.insert(variant.name.to_string()) {
            combine_error(
                &mut errors,
                syn::Error::new(variant.name.span(), "duplicate tree variant"),
            );
        }
        if let Err(error) = variant.validate() {
            combine_error(&mut errors, error);
        }
    }
    errors.map_or(Ok(()), Err)
}

fn combine_error(errors: &mut Option<syn::Error>, error: syn::Error) {
    match errors {
        Some(errors) => errors.combine(error),
        None => *errors = Some(error),
    }
}

#[cfg(test)]
mod tests {
    use super::TreeSchema;

    fn error(schema: &str) -> String {
        match syn::parse_str::<TreeSchema>(schema) {
            Ok(_) => panic!("schema unexpectedly parsed"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn accepts_outer_attributes_on_owned_constructors() {
        let schema = syn::parse_str::<TreeSchema>(
            "pub tree Expr {
                schema: pub(crate),
                owned_constructors: #[cfg(test)] #[allow(dead_code)] pub(crate),
                metadata: m: () = (),
                id: u32,
                key: String,
                children: Vec,
                keyed_children: Vec,
            }",
        )
        .unwrap();
        let owned_constructors = schema.owned_constructors.unwrap();

        assert!(matches!(
            owned_constructors.visibility,
            syn::Visibility::Restricted(_)
        ));
        assert_eq!(owned_constructors.attributes.len(), 2);
        assert!(owned_constructors.attributes[0].path().is_ident("cfg"));
        assert!(owned_constructors.attributes[1].path().is_ident("allow"));
    }

    #[test]
    fn rejects_duplicate_settings() {
        assert!(
            error("pub tree Expr { schema: pub(crate), id: u32, id: u64, }")
                .contains("duplicate tree setting")
        );
    }

    #[test]
    fn rejects_the_removed_internals_setting() {
        assert!(error("pub tree Expr { internals: pub(crate), }").contains("unknown tree setting"));
    }

    #[test]
    fn defaults_schema_visibility_and_requires_an_explicit_override() {
        let schema = syn::parse_str::<TreeSchema>(
            "pub tree Expr {
                metadata: m: () = (),
                id: u32,
                key: String,
                children: Vec,
                keyed_children: Vec,
            }",
        )
        .unwrap();
        assert!(matches!(
            schema.schema_visibility,
            syn::Visibility::Restricted(_)
        ));
        assert!(
            error("pub tree Expr { schema: , }").contains("schema requires an explicit visibility")
        );
        assert!(
            error("pub tree Expr { owned_constructors: , }")
                .contains("owned_constructors requires an explicit visibility")
        );
    }

    #[test]
    fn collection_settings_are_required_only_when_used() {
        syn::parse_str::<TreeSchema>(
            "pub tree LeafOnly {
                metadata: m: () = (),
                id: u32,
                Leaf(),
            }",
        )
        .unwrap();
        syn::parse_str::<TreeSchema>(
            "pub tree WithUnusedSettings {
                metadata: m: () = (),
                id: u32,
                key: String,
                children: Vec,
                keyed_children: Vec,
                Leaf(),
            }",
        )
        .unwrap();

        assert!(
            error(
                "pub tree Sequence {
                    metadata: m: () = (),
                    id: u32,
                    Sequence(items: children),
                }"
            )
            .contains("missing children setting")
        );
        assert!(
            error(
                "pub tree Record {
                    metadata: m: () = (),
                    id: u32,
                    keyed_children: Vec,
                    Record(fields: keyed_children),
                }"
            )
            .contains("missing key setting")
        );
        assert!(
            error(
                "pub tree Record {
                    metadata: m: () = (),
                    id: u32,
                    key: String,
                    Record(fields: keyed_children),
                }"
            )
            .contains("missing keyed_children setting")
        );
    }

    #[test]
    fn rejects_duplicate_variants_and_fields() {
        assert!(error("pub tree Expr { Leaf(), Leaf(), }").contains("duplicate tree variant"));
        assert!(
            error("pub tree Expr { Leaf(value: data(u8), value: data(u8)), }")
                .contains("duplicate field")
        );
    }

    #[test]
    fn rejects_unsupported_child_shapes() {
        assert!(
            error("pub tree Expr { Node(a: child, b: child, c: child, d: child), }")
                .contains("at most three fixed children")
        );
        assert!(
            error("pub tree Expr { Node(a: children, b: children), }")
                .contains("at most one child collection")
        );
        assert!(
            error("pub tree Expr { Node(rest: children, first: child), }")
                .contains("fixed children must precede")
        );
    }
}

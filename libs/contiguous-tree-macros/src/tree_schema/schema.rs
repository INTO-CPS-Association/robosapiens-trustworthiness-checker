use std::collections::HashSet;

use syn::{
    Expr, Ident, Token, Type, Visibility, braced, parenthesized,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

mod keyword {
    syn::custom_keyword!(tree);
    syn::custom_keyword!(child);
    syn::custom_keyword!(children);
    syn::custom_keyword!(keyed_children);
    syn::custom_keyword!(data);
    syn::custom_keyword!(copy);
    syn::custom_keyword!(metadata);
    syn::custom_keyword!(internals);
    syn::custom_keyword!(id);
    syn::custom_keyword!(key);
}

pub(super) enum FieldKind {
    Child,
    Children,
    KeyedChildren,
    Borrowed(Type),
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
        } else if input.peek(keyword::copy) {
            input.parse::<keyword::copy>()?;
            FieldKind::Copied(parenthesized_type(input)?)
        } else {
            return Err(
                input.error("expected child, children, keyed_children, data(T), or copy(T)")
            );
        };
        Ok(Self { name, kind })
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

pub(crate) struct TreeSchema {
    pub(super) visibility: Visibility,
    pub(super) internals: Visibility,
    pub(super) root: Ident,
    pub(super) metadata_name: Ident,
    pub(super) metadata: Type,
    pub(super) metadata_default: Expr,
    pub(super) id: Type,
    pub(super) key: Type,
    pub(super) children: Type,
    pub(super) keyed_children: Type,
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
        let mut internals = None;
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
            } else if content.peek(keyword::internals) {
                let name: Ident = content.parse()?;
                content.parse::<Token![:]>()?;
                let visibility: Visibility = content.parse()?;
                if matches!(visibility, Visibility::Inherited) {
                    return Err(syn::Error::new(
                        name.span(),
                        "internals requires an explicit visibility",
                    ));
                }
                set_once(&mut internals, visibility, &name)?;
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
        Ok(Self {
            visibility,
            internals: internals.ok_or_else(|| content.error("missing internals setting"))?,
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
            key: key.ok_or_else(|| content.error("missing key setting"))?,
            children: children.ok_or_else(|| content.error("missing children setting"))?,
            keyed_children: keyed_children
                .ok_or_else(|| content.error("missing keyed_children setting"))?,
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
    fn rejects_duplicate_settings() {
        assert!(
            error("pub tree Expr { internals: pub(crate), id: u32, id: u64, }")
                .contains("duplicate tree setting")
        );
    }

    #[test]
    fn requires_explicit_internal_visibility() {
        assert!(
            error("pub tree Expr { internals: , }")
                .contains("internals requires an explicit visibility")
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

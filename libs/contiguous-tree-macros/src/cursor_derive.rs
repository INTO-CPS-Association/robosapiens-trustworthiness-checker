use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, Type};

#[derive(Default)]
struct CursorOptions {
    delegate: Option<Ident>,
    target: Option<Type>,
}

impl CursorOptions {
    fn parse(input: &DeriveInput) -> syn::Result<Self> {
        let mut options = Self::default();
        for attribute in input
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("tree_cursor"))
        {
            attribute.parse_nested_meta(|meta| {
                let value = meta.value()?;
                if meta.path.is_ident("delegate") {
                    if options.delegate.is_some() {
                        return Err(meta.error("duplicate delegate option"));
                    }
                    options.delegate = Some(value.parse()?);
                } else if meta.path.is_ident("target") {
                    if options.target.is_some() {
                        return Err(meta.error("duplicate target option"));
                    }
                    options.target = Some(value.parse()?);
                } else {
                    return Err(meta.error("expected delegate or target"));
                }
                Ok(())
            })?;
        }
        Ok(options)
    }
}

pub(crate) fn expand(
    input: DeriveInput,
    runtime: &proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let options = CursorOptions::parse(&input)?;
    expand_delegate(&input, options, runtime)
}

fn expand_delegate(
    input: &DeriveInput,
    options: CursorOptions,
    runtime: &proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let CursorOptions {
        delegate: Some(field),
        target,
    } = options
    else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "TreeCursor derive requires a delegate field",
        ));
    };

    let Data::Struct(structure) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "TreeCursor can only be derived for structs",
        ));
    };
    let Fields::Named(fields) = &structure.fields else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "delegate requires a named field",
        ));
    };
    if fields.named.len() != 1 {
        return Err(syn::Error::new_spanned(
            fields,
            "a delegated TreeCursor must contain only the delegate field",
        ));
    }
    let field_type = &fields
        .named
        .first()
        .filter(|candidate| candidate.ident.as_ref() == Some(&field))
        .ok_or_else(|| syn::Error::new_spanned(&field, "delegate field not found"))?
        .ty;
    let target = target.as_ref().unwrap_or(field_type);
    let name = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #runtime::TreeCursor for #name #type_generics #where_clause {
            type Id = <#target as #runtime::TreeCursor>::Id;
            type ChildIds = <#target as #runtime::TreeCursor>::ChildIds;

            fn id(self) -> Self::Id { #runtime::TreeCursor::id(self.#field) }
            fn storage_identity(self) -> #runtime::StorageIdentity {
                #runtime::TreeCursor::storage_identity(self.#field)
            }
            fn same_node(self, other: Self) -> bool {
                #runtime::TreeCursor::same_node(self.#field, other.#field)
            }
            fn child_ids(self) -> Self::ChildIds {
                #runtime::TreeCursor::child_ids(self.#field)
            }
            fn child(self, id: Self::Id) -> Self {
                Self { #field: #runtime::TreeCursor::child(self.#field, id) }
            }
            fn subtree_ids(self) -> #runtime::IdRange<Self::Id> {
                #runtime::TreeCursor::subtree_ids(self.#field)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::expand;

    #[test]
    fn derive_requires_a_delegate() {
        let input = syn::parse_quote! {
            #[derive(TreeCursor)]
            struct Cursor<'a> { arena: &'a (), id: usize }
        };
        assert!(
            expand(input, &quote!(::renamed_contiguous_tree))
                .unwrap_err()
                .to_string()
                .contains("requires a delegate field")
        );
    }

    #[test]
    fn delegated_cursor_must_be_transparent() {
        let input = syn::parse_quote! {
            #[tree_cursor(delegate = cursor)]
            struct Cursor<C> { cursor: C, extra: usize }
        };
        assert!(
            expand(input, &quote!(::renamed_contiguous_tree))
                .unwrap_err()
                .to_string()
                .contains("only the delegate field")
        );
    }
}

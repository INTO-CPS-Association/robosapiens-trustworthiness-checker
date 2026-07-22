use quote::quote;
use syn::{Ident, Type, Visibility};

pub(super) fn expand(
    visibility: &Visibility,
    key: &Type,
    id: &Ident,
    fields: &Ident,
    storage: &Type,
) -> proc_macro2::TokenStream {
    quote! {
        /// Keyed child IDs in source order.
        #[derive(Clone, Debug, Default, PartialEq)]
        #[repr(transparent)]
        #visibility struct #fields(
            contiguous_tree::KeyedFields<#key, #id, #storage<(#key, #id)>>,
        );

        impl std::ops::Deref for #fields {
            type Target = contiguous_tree::KeyedFields<#key, #id, #storage<(#key, #id)>>;

            fn deref(&self) -> &Self::Target { &self.0 }
        }

        impl std::ops::DerefMut for #fields {
            fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
        }

        impl FromIterator<(#key, #id)> for #fields {
            fn from_iter<Items: IntoIterator<Item = (#key, #id)>>(items: Items) -> Self {
                Self(items.into_iter().collect())
            }
        }

        impl From<#storage<(#key, #id)>> for #fields {
            fn from(storage: #storage<(#key, #id)>) -> Self { Self(storage.into()) }
        }

        impl IntoIterator for #fields {
            type Item = (#key, #id);
            type IntoIter = <#storage<(#key, #id)> as IntoIterator>::IntoIter;

            fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
        }

        impl<'fields> IntoIterator for &'fields #fields {
            type Item = (&'fields #key, &'fields #id);
            type IntoIter = <&'fields contiguous_tree::KeyedFields<
                #key,
                #id,
                #storage<(#key, #id)>,
            > as IntoIterator>::IntoIter;

            fn into_iter(self) -> Self::IntoIter { (&self.0).into_iter() }
        }
    }
}

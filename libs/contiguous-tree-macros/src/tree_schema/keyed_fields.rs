use quote::quote;
use syn::{Ident, Type, Visibility};

pub(super) fn expand(
    runtime: &proc_macro2::TokenStream,
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
            #runtime::KeyedFields<#key, #id, #storage<(#key, #id)>>,
        );

        impl #fields {
            #visibility fn iter(
                &self,
            ) -> impl DoubleEndedIterator<Item = (&#key, &#id)> + ExactSizeIterator {
                self.0.iter()
            }

            #visibility fn keys(
                &self,
            ) -> impl DoubleEndedIterator<Item = &#key> + ExactSizeIterator {
                self.0.keys()
            }

            #visibility fn values(
                &self,
            ) -> impl DoubleEndedIterator<Item = &#id> + ExactSizeIterator {
                self.0.values()
            }

            #visibility fn get<Q>(&self, key: &Q) -> Option<#id>
            where
                #key: std::borrow::Borrow<Q>,
                Q: Eq + ?Sized,
            {
                self.0.get(key)
            }

            #visibility fn contains_key<Q>(&self, key: &Q) -> bool
            where
                #key: std::borrow::Borrow<Q>,
                Q: Eq + ?Sized,
            {
                self.0.contains_key(key)
            }

            #visibility fn duplicate_key(&self) -> Option<&#key> {
                self.0.duplicate_key()
            }

            #visibility fn as_slice(&self) -> &[(#key, #id)] {
                self.0.as_slice()
            }

            fn child_ids_mut(&mut self) -> &mut [(#key, #id)] {
                self.0.make_mut_with(|storage| storage.make_mut())
            }
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
            type IntoIter = <&'fields #runtime::KeyedFields<
                #key,
                #id,
                #storage<(#key, #id)>,
            > as IntoIterator>::IntoIter;

            fn into_iter(self) -> Self::IntoIter { (&self.0).into_iter() }
        }
    }
}

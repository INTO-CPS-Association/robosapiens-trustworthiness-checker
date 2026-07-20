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
        #[derive(Clone, Debug, Default, PartialEq, contiguous_tree::__private::serde::Serialize)]
        #visibility struct #fields(#storage<(#key, #id)>);

        impl #fields {
            pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&#key, &#id)> + ExactSizeIterator {
                self.0.iter().map(|(key, value)| (key, value))
            }
            pub fn keys(&self) -> impl DoubleEndedIterator<Item = &#key> + ExactSizeIterator {
                self.0.iter().map(|(key, _)| key)
            }
            pub fn values(&self) -> impl DoubleEndedIterator<Item = &#id> + ExactSizeIterator {
                self.0.iter().map(|(_, value)| value)
            }
            pub fn get<Q>(&self, key: &Q) -> Option<#id>
            where
                #key: std::borrow::Borrow<Q>,
                Q: Eq + ?Sized,
            {
                self.0.iter().rev()
                    .find(|(found, _)| std::borrow::Borrow::borrow(found) == key)
                    .map(|(_, value)| *value)
            }
            pub fn contains_key<Q>(&self, key: &Q) -> bool
            where
                #key: std::borrow::Borrow<Q>,
                Q: Eq + ?Sized,
            {
                self.get(key).is_some()
            }
            pub fn duplicate_key(&self) -> Option<&#key> {
                self.0.iter().enumerate().find_map(|(index, (key, _))| {
                    self.0[..index].iter().any(|(previous, _)| previous == key).then_some(key)
                })
            }
            pub(crate) fn make_mut(&mut self) -> &mut [(#key, #id)] {
                self.0.make_mut()
            }
            pub(crate) fn raw_slice(&self) -> &[(#key, #id)] {
                &self.0
            }
        }

        impl FromIterator<(#key, #id)> for #fields {
            fn from_iter<Items: IntoIterator<Item = (#key, #id)>>(items: Items) -> Self {
                Self(items.into_iter().collect())
            }
        }
        impl From<#storage<(#key, #id)>> for #fields {
            fn from(fields: #storage<(#key, #id)>) -> Self { Self(fields) }
        }
        impl IntoIterator for #fields {
            type Item = (#key, #id);
            type IntoIter = <#storage<(#key, #id)> as IntoIterator>::IntoIter;
            fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
        }
        impl<'fields> IntoIterator for &'fields #fields {
            type Item = (&'fields #key, &'fields #id);
            type IntoIter = std::iter::Map<
                std::slice::Iter<'fields, (#key, #id)>,
                fn(&(#key, #id)) -> (&#key, &#id),
            >;
            fn into_iter(self) -> Self::IntoIter {
                fn split(pair: &(#key, #id)) -> (&#key, &#id) { (&pair.0, &pair.1) }
                self.0.iter().map(split)
            }
        }
    }
}

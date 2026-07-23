//! Generation of the Rust types and implementations described by a tree schema.
//!

use quote::{format_ident, quote};
use syn::{Ident, Type};

use super::keyed_fields;
use super::schema::{FieldKind, TreeSchema, Variant};

impl Variant {
    fn stored_definition(
        &self,
        id: &Ident,
        children: &Type,
        keyed_fields: &Ident,
    ) -> proc_macro2::TokenStream {
        let name = &self.name;
        let fields = self
            .fields
            .iter()
            .map(|field| stored_type(&field.kind, id, children, keyed_fields));
        quote!(#name( #( #fields ),* ))
    }

    fn view_definition(
        &self,
        runtime: &proc_macro2::TokenStream,
        cursor: &Ident,
        id: &Ident,
        key: &Type,
    ) -> proc_macro2::TokenStream {
        let name = &self.name;
        let fields = self
            .fields
            .iter()
            .map(|field| view_type(runtime, &field.kind, cursor, id, key));
        quote!(#name( #( #fields ),* ))
    }

    fn resolve_arm(
        &self,
        runtime: &proc_macro2::TokenStream,
        kind: &Ident,
        view: &Ident,
        cursor: &Ident,
    ) -> proc_macro2::TokenStream {
        let name = &self.name;
        let fields = self.fields.iter().map(|field| &field.name);
        let resolved = self
            .fields
            .iter()
            .map(|field| resolve(runtime, &field.kind, &field.name, cursor));
        quote!(#kind::#name( #( #fields ),* ) => #view::#name( #( #resolved ),* ))
    }

    fn child_ids_arm(&self, kind: &Ident) -> proc_macro2::TokenStream {
        let name = &self.name;
        let fields = self.fields.iter().map(|field| &field.name);
        let recorded = self
            .fields
            .iter()
            .map(|field| record(&field.kind, &field.name));
        quote!(#kind::#name( #( #fields ),* ) => { #( #recorded )* })
    }

    fn visit_mut_arm(&self, kind: &Ident) -> proc_macro2::TokenStream {
        let name = &self.name;
        let fields = self.fields.iter().map(|field| &field.name);
        let visited = self
            .fields
            .iter()
            .map(|field| visit(&field.kind, &field.name));
        quote!(#kind::#name( #( #fields ),* ) => { #( #visited )* })
    }

    fn duplicate_key_arm(&self, kind: &Ident) -> proc_macro2::TokenStream {
        let name = &self.name;
        let keyed_field = self
            .fields
            .iter()
            .find(|field| matches!(field.kind, FieldKind::KeyedChildren));
        let fields = self.fields.iter().map(|field| {
            if matches!(field.kind, FieldKind::KeyedChildren) {
                let name = &field.name;
                quote!(#name)
            } else {
                quote!(_)
            }
        });
        let duplicate = keyed_field
            .map(|field| {
                let name = &field.name;
                quote!(#name.duplicate_key())
            })
            .unwrap_or_else(|| quote!(None));
        quote!(#kind::#name( #( #fields ),* ) => #duplicate)
    }

    fn owned_constructor(
        &self,
        visibility: &syn::Visibility,
        kind: &Ident,
        root: &Ident,
        key: &Type,
        children: &Type,
        keyed_children: &Type,
    ) -> proc_macro2::TokenStream {
        let name = &self.name;
        let constructor_fields = self
            .fields
            .iter()
            .filter(|field| field.owned_default.is_none())
            .collect::<Vec<_>>();
        let field_names = constructor_fields.iter().map(|field| &field.name);
        let parameter_types = constructor_fields.iter().map(|field| match &field.kind {
            FieldKind::Child => quote!(Box<impl Into<#root>>),
            FieldKind::Children => quote!(#children<#root>),
            FieldKind::KeyedChildren => quote!(impl IntoIterator<Item = (#key, #root)>),
            FieldKind::Borrowed(typ) => quote!(#typ),
            FieldKind::IntoOwned(typ) => quote!(impl Into<#typ>),
            FieldKind::Copied(typ) => quote!(#typ),
        });
        let prepared = self.fields.iter().map(|field| {
            let field_name = &field.name;
            if let Some(default) = &field.owned_default {
                return quote!(let #field_name = #default;);
            }
            match &field.kind {
                FieldKind::Child => quote!(let #field_name: #root = (*#field_name).into();),
                FieldKind::Children => quote!(
                    let #field_name: Vec<#root> = #field_name.into_iter().collect();
                ),
                FieldKind::KeyedChildren => quote!(
                    let (#field_name, children): (Vec<#key>, Vec<#root>) =
                        #field_name.into_iter().unzip();
                    let #field_name = (#field_name, children);
                ),
                FieldKind::Borrowed(_) => quote!(),
                FieldKind::IntoOwned(typ) => quote!(let #field_name: #typ = #field_name.into();),
                FieldKind::Copied(_) => quote!(),
            }
        });
        let appended = self.fields.iter().map(|field| {
            let field_name = &field.name;
            match field.kind {
                FieldKind::Child => quote!(owned_children.push(#field_name);),
                FieldKind::Children => quote!(owned_children.extend(#field_name);),
                FieldKind::KeyedChildren => quote!(owned_children.extend(#field_name.1);),
                FieldKind::Borrowed(_) | FieldKind::IntoOwned(_) | FieldKind::Copied(_) => quote!(),
            }
        });
        let stored = self.fields.iter().map(|field| {
            let field_name = &field.name;
            match &field.kind {
                FieldKind::Child => quote!(ids.next().expect("missing fixed child ID")),
                FieldKind::Children => quote!(ids.by_ref().collect::<#children<_>>()),
                FieldKind::KeyedChildren => quote!(#field_name.0.into_iter()
                    .zip(ids.by_ref())
                    .collect::<#keyed_children<_>>()
                    .into()),
                FieldKind::Borrowed(_) | FieldKind::IntoOwned(_) | FieldKind::Copied(_) => {
                    quote!(#field_name)
                }
            }
        });
        quote! {
            #visibility fn #name(#( #field_names: #parameter_types ),*) -> Self {
                #( #prepared )*
                let mut owned_children = Vec::new();
                #( #appended )*
                Self::__merge_owned_children(owned_children, |ids| {
                    let mut ids = ids.iter().copied();
                    #kind::#name( #( #stored ),* )
                })
            }
        }
    }

    fn payload_equality_arm(&self, kind: &Ident) -> proc_macro2::TokenStream {
        let name = &self.name;
        let left = self
            .fields
            .iter()
            .map(|field| {
                if matches!(field.kind, FieldKind::Child | FieldKind::Children) {
                    format_ident!("_left_{}", field.name)
                } else {
                    format_ident!("left_{}", field.name)
                }
            })
            .collect::<Vec<_>>();
        let right = self
            .fields
            .iter()
            .map(|field| {
                if matches!(field.kind, FieldKind::Child | FieldKind::Children) {
                    format_ident!("_right_{}", field.name)
                } else {
                    format_ident!("right_{}", field.name)
                }
            })
            .collect::<Vec<_>>();
        let comparisons = self
            .fields
            .iter()
            .zip(left.iter().zip(right.iter()))
            .filter_map(|(field, (left, right))| match &field.kind {
                FieldKind::Child | FieldKind::Children => None,
                FieldKind::KeyedChildren => Some(quote!(
                    #left.iter().map(|(key, _)| key).eq(#right.iter().map(|(key, _)| key))
                )),
                FieldKind::Borrowed(_) | FieldKind::IntoOwned(_) | FieldKind::Copied(_) => {
                    Some(quote!(#left == #right))
                }
            });
        quote! {
            (
                #kind::#name( #( #left ),* ),
                #kind::#name( #( #right ),* )
            ) => true #( && #comparisons )*
        }
    }
}

struct GeneratedNames {
    kind: Ident,
    id: Ident,
    node: Ident,
    arena: Ident,
    reference: Ident,
    view: Ident,
    builder: Ident,
    forest: Ident,
    forest_map: Ident,
    handle: Ident,
    refs: Ident,
    field_refs: Ident,
    keyed_fields: Ident,
}

impl GeneratedNames {
    fn new(root: &Ident) -> Self {
        Self {
            kind: format_ident!("{root}Kind"),
            id: format_ident!("{root}Id"),
            node: format_ident!("{root}Node"),
            arena: format_ident!("{root}Arena"),
            reference: format_ident!("{root}Ref"),
            view: format_ident!("{root}View"),
            builder: format_ident!("{root}Builder"),
            forest: format_ident!("{root}Forest"),
            forest_map: format_ident!("{root}ForestMap"),
            handle: format_ident!("{root}Handle"),
            refs: format_ident!("{root}Refs"),
            field_refs: format_ident!("{root}FieldRefs"),
            keyed_fields: format_ident!("{root}Fields"),
        }
    }
}

pub(super) fn expand(
    schema: TreeSchema,
    runtime: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let TreeSchema {
        visibility,
        schema_visibility,
        owned_constructors,
        serialize_display,
        root,
        metadata_name,
        metadata,
        metadata_default,
        id: id_type,
        key,
        children,
        keyed_children,
        uses_keyed_children,
        variants,
    } = schema;
    let GeneratedNames {
        kind,
        id,
        node,
        arena,
        reference,
        view,
        builder,
        forest,
        forest_map,
        handle,
        refs,
        field_refs,
        keyed_fields,
    } = GeneratedNames::new(&root);
    let cursor = format_ident!("Cursor");
    let cursor_value = format_ident!("cursor");
    let root_name = root.to_string();
    let id_doc = format!("Typed node ID for a `{root_name}` tree.");
    let kind_doc = format!("Stored `{root_name}` node data with children represented by `{id}`.");
    let reference_doc = format!("Borrowed cursor into a `{root_name}` tree.");
    let view_doc = format!("Borrowed `{root_name}` node view with child IDs resolved to cursors.");
    let node_doc = format!("Stored `{root_name}` node and its metadata.");
    let arena_doc = format!("Contiguous storage for `{root_name}` nodes.");
    let handle_doc = format!("Shared owning handle for a `{root_name}` root.");
    let root_doc = format!("Owning root of a `{root_name}` tree.");
    let builder_doc = format!("Bottom-up builder for `{root_name}` trees and forests.");
    let forest_doc = format!("Ordered owning forest of `{root_name}` roots in shared storage.");
    let forest_map_doc = format!("Sorted unique keys associated with `{root_name}` forest roots.");

    let stored_variants = variants
        .iter()
        .map(|variant| variant.stored_definition(&id, &children, &keyed_fields))
        .collect::<Vec<_>>();
    let view_variants = variants
        .iter()
        .map(|variant| variant.view_definition(runtime, &cursor, &id, &key))
        .collect::<Vec<_>>();
    let resolve_arms = variants
        .iter()
        .map(|variant| variant.resolve_arm(runtime, &kind, &view, &cursor_value))
        .collect::<Vec<_>>();
    let child_ids_arms = variants
        .iter()
        .map(|variant| variant.child_ids_arm(&kind))
        .collect::<Vec<_>>();
    let visit_mut_arms = variants
        .iter()
        .map(|variant| variant.visit_mut_arm(&kind))
        .collect::<Vec<_>>();
    let payload_arms = variants
        .iter()
        .map(|variant| variant.payload_equality_arm(&kind))
        .collect::<Vec<_>>();
    let duplicate_key_arms = variants
        .iter()
        .map(|variant| variant.duplicate_key_arm(&kind))
        .collect::<Vec<_>>();
    let keyed_support = uses_keyed_children.then(|| {
        let keyed_fields_definition = keyed_fields::expand(
            runtime,
            &schema_visibility,
            &key,
            &id,
            &keyed_fields,
            &keyed_children,
        );
        quote! {
            #schema_visibility type #field_refs<'arena> =
                #runtime::ResolvedFields<'arena, #reference<'arena>, #key>;

            #keyed_fields_definition

            impl #kind {
                #schema_visibility fn duplicate_key(&self) -> Option<&#key>
                where
                    #key: Eq,
                {
                    match self {
                        #( #duplicate_key_arms, )*
                    }
                }
            }
        }
    });
    #[cfg(feature = "serde")]
    let serialization = {
        let display_serialization = serialize_display.then(|| {
            quote! {
                impl #runtime::__private::serde::Serialize for #reference<'_>
                where
                    Self: std::fmt::Display,
                {
                    fn serialize<Serializer>(
                        &self,
                        serializer: Serializer,
                    ) -> Result<Serializer::Ok, Serializer::Error>
                    where
                        Serializer: #runtime::__private::serde::Serializer,
                    {
                        #runtime::__private::serde::Serializer::serialize_str(
                            serializer,
                            &std::string::ToString::to_string(self),
                        )
                    }
                }

                impl #runtime::__private::serde::Serialize for #root {
                    fn serialize<Serializer>(
                        &self,
                        serializer: Serializer,
                    ) -> Result<Serializer::Ok, Serializer::Error>
                    where
                        Serializer: #runtime::__private::serde::Serializer,
                    {
                        #runtime::__private::serde::Serialize::serialize(&self.as_ref(), serializer)
                    }
                }
            }
        });
        quote! {
            #display_serialization

            impl<Key> #runtime::__private::serde::Serialize for #forest_map<Key>
            where
                Key: Ord + #runtime::__private::serde::Serialize,
                for<'arena> #reference<'arena>: #runtime::__private::serde::Serialize,
            {
                fn serialize<Serializer>(
                    &self,
                    serializer: Serializer,
                ) -> Result<Serializer::Ok, Serializer::Error>
                where
                    Serializer: #runtime::__private::serde::Serializer,
                {
                    let mut map = #runtime::__private::serde::Serializer::serialize_map(
                        serializer,
                        Some(self.len()),
                    )?;
                    for (key, expression) in self.iter() {
                        #runtime::__private::serde::ser::SerializeMap::serialize_entry(
                            &mut map,
                            key,
                            &expression,
                        )?;
                    }
                    #runtime::__private::serde::ser::SerializeMap::end(map)
                }
            }
        }
    };
    #[cfg(not(feature = "serde"))]
    let serialization = {
        let _ = serialize_display;
        quote!()
    };

    let owned_constructor_impl = owned_constructors.map(|owned_constructors| {
        let constructor_attributes = owned_constructors.attributes;
        let constructor_visibility = owned_constructors.visibility;
        let methods = variants.iter().map(|variant| {
            variant.owned_constructor(
                &constructor_visibility,
                &kind,
                &root,
                &key,
                &children,
                &keyed_children,
            )
        });
        quote! {
            #( #constructor_attributes )*
            #[doc(hidden)]
            #[allow(non_snake_case, clippy::boxed_local)]
            impl #root {
                fn __merge_owned_children(
                    children: impl IntoIterator<Item = Self>,
                    make: impl FnOnce(&[#id]) -> #kind,
                ) -> Self {
                    let children: Vec<_> = children.into_iter().collect();
                    let capacity = children.iter()
                        .map(|child| #runtime::TreeCursorExt::postorder(child.as_ref()).len())
                        .sum::<usize>() + 1;
                    let mut builder = #builder::with_capacity(capacity);
                    let ids = children
                        .iter()
                        .map(|child| builder.clone_subtree(child.as_ref()))
                        .collect::<Vec<_>>();
                    let root = builder.alloc(make(&ids), #metadata_default);
                    builder
                        .finish(root)
                        .expect("owned children form one complete tree")
                }

                #( #methods )*
            }
        }
    });

    quote! {
        #[doc = #id_doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #visibility struct #id(#id_type);

        impl #id {
            fn new(index: usize) -> Self {
                Self(<#id_type>::try_from(index).expect("tree contains more nodes than its ID type can represent"))
            }

            #visibility fn index(self) -> usize {
                self.0 as usize
            }
        }

        impl #runtime::ArenaId for #id {
            fn from_index(index: usize) -> Self {
                Self::new(index)
            }

            fn index(self) -> usize {
                self.index()
            }
        }

        #schema_visibility type #refs<'arena> = #runtime::ResolvedIds<
            #reference<'arena>,
            std::iter::Copied<std::slice::Iter<'arena, #id>>,
        >;

        #keyed_support

        #[doc = #kind_doc]
        #[derive(Clone, PartialEq, Debug)]
        #schema_visibility enum #kind {
            #( #stored_variants, )*
        }

        #[doc = #reference_doc]
        #[derive(Clone, Copy)]
        #visibility struct #reference<'arena> {
            arena: &'arena #arena,
            id: #id,
        }

        #[doc = #view_doc]
        #[derive(Debug)]
        #visibility enum #view<
            'arena,
            #cursor: #runtime::TreeCursor<Id = #id> + 'arena = #reference<'arena>,
        > {
            #( #view_variants, )*
        }

        #[doc = #node_doc]
        #[derive(Clone, Debug, PartialEq)]
        struct #node {
            node: #kind,
            #metadata_name: #metadata,
        }

        #[doc = #arena_doc]
        #[derive(Clone, Debug, Default, PartialEq)]
        #schema_visibility struct #arena {
            nodes: #runtime::Arena<#id, #node>,
        }

        #[doc = #handle_doc]
        type #handle = #runtime::TreeHandle<#arena>;

        #[doc = #root_doc]
        #[derive(Clone)]
        #visibility struct #root {
            tree: #handle,
        }

        impl std::fmt::Debug for #root {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&self.as_ref(), formatter)
            }
        }

        impl #runtime::TreeStorage for #arena {
            type Id = #id;
            type Cursor<'arena> = #reference<'arena>;

            fn cursor(&self, id: Self::Id) -> Self::Cursor<'_> {
                #reference::from_arena(self, id)
            }

            fn owns(&self, cursor: Self::Cursor<'_>) -> bool {
                cursor.belongs_to(self)
            }

            fn validate_forest(
                &self,
                roots: &[Self::Id],
            ) -> Result<(), #runtime::ForestError> {
                self.nodes.validate_forest(roots)
            }

            fn node_count(&self) -> usize {
                self.nodes.len()
            }
        }

        impl #runtime::PostorderStorage<#node> for #arena {
            fn push_node(&mut self, node: #node) -> Self::Id {
                #runtime::__private::arena_push_node(&mut self.nodes, node)
            }

            fn reserve_nodes(&mut self, additional: usize) {
                self.nodes.reserve(additional);
            }

            fn truncate_nodes(&mut self, len: usize) {
                #runtime::__private::arena_truncate(&mut self.nodes, len);
            }
        }

        impl #root {
            fn new(tree: #handle) -> Self {
                Self { tree }
            }

            #visibility fn id(&self) -> #id {
                self.tree.root_id()
            }



            #schema_visibility fn shares_storage_with(&self, other: &Self) -> bool {
                self.tree.shares_storage_with(&other.tree)
            }

            #schema_visibility fn same_root(&self, other: &Self) -> bool {
                self.tree.same_root(&other.tree)
            }

            #[cfg(test)]
            #schema_visibility fn storage_strong_count(&self) -> usize {
                self.tree.strong_count()
            }



            #schema_visibility fn subtree(&self, root: #reference<'_>) -> Self {
                Self::new(self.tree.subtree(root))
            }

            #schema_visibility fn annotations_builder<T>(
                &self,
            ) -> #runtime::NodeAnnotationsBuilder<#arena, T> {
                self.tree.annotations_builder()
            }



            #visibility fn as_ref(&self) -> #reference<'_> {
                self.tree.cursor()
            }


        }

        #[doc = #forest_doc]
        #[derive(Clone)]
        #schema_visibility struct #forest {
            forest: #runtime::Forest<#arena>,
        }

        impl #forest {
            /// Validate and create a forest whose roots cover the complete arena.
            #schema_visibility fn new(
                arena: #arena,
                roots: impl IntoIterator<Item = #id>,
            ) -> Result<Self, #runtime::ForestError> {
                #runtime::Forest::new(arena, roots).map(|forest| Self { forest })
            }

            #schema_visibility fn len(&self) -> usize {
                self.forest.len()
            }

            #schema_visibility fn is_empty(&self) -> bool {
                self.forest.is_empty()
            }

            #schema_visibility fn root_ids(&self) -> &[#id] {
                self.forest.root_ids()
            }



            #schema_visibility fn root(&self, index: usize) -> #root {
                #root::new(self.forest.handle(index))
            }

            /// Iterate over borrowed root cursors in caller-provided order.
            #schema_visibility fn roots(
                &self,
            ) -> impl DoubleEndedIterator<Item = #reference<'_>> + ExactSizeIterator + '_ {
                self.forest.cursors()
            }

            /// Iterate over every node in contiguous allocation order.
            #schema_visibility fn nodes(
                &self,
            ) -> impl DoubleEndedIterator<Item = #reference<'_>> + ExactSizeIterator + '_ {
                self.forest.nodes()
            }

            #schema_visibility fn annotations_builder<T>(
                &self,
            ) -> #runtime::NodeAnnotationsBuilder<#arena, T> {
                self.forest.annotations_builder()
            }



            /// Consume the forest and iterate over owning roots without cloning storage.
            #schema_visibility fn into_roots(
                self,
            ) -> impl DoubleEndedIterator<Item = #root> + ExactSizeIterator {
                self.forest.into_handles().map(#root::new)
            }
        }

        #[doc = #forest_map_doc]
        #[derive(Clone)]
        #schema_visibility struct #forest_map<Key: Ord> {
            map: #runtime::ForestMap<Key, #arena>,
        }

        impl<Key: Ord + std::fmt::Debug> std::fmt::Debug for #forest_map<Key> {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.debug_map().entries(self.iter()).finish()
            }
        }

        impl<Key: Ord + PartialEq> PartialEq for #forest_map<Key>
        where
            for<'arena> #reference<'arena>: PartialEq,
        {
            fn eq(&self, other: &Self) -> bool {
                self.iter().eq(other.iter())
            }
        }

        #serialization

        impl<Key: Ord> #forest_map<Key> {
            #schema_visibility fn new(
                keys: impl IntoIterator<Item = Key>,
                forest: #forest,
            ) -> Result<Self, #runtime::ForestMapError> {
                #runtime::ForestMap::new(keys, forest.forest).map(|map| Self { map })
            }

            #schema_visibility fn from_unsorted(
                keys: impl IntoIterator<Item = Key>,
                forest: #forest,
            ) -> Result<Self, #runtime::ForestMapError> {
                #runtime::ForestMap::from_unsorted(keys, forest.forest).map(|map| Self { map })
            }

            #schema_visibility fn len(&self) -> usize {
                self.map.len()
            }

            #schema_visibility fn is_empty(&self) -> bool {
                self.map.is_empty()
            }

            #schema_visibility fn keys(
                &self,
            ) -> impl DoubleEndedIterator<Item = &Key> + ExactSizeIterator {
                self.map.keys()
            }

            #schema_visibility fn values(
                &self,
            ) -> impl DoubleEndedIterator<Item = #reference<'_>> + ExactSizeIterator + '_ {
                self.map.values()
            }

            #schema_visibility fn iter(
                &self,
            ) -> impl DoubleEndedIterator<Item = (&Key, #reference<'_>)> + ExactSizeIterator + '_ {
                self.map.iter()
            }

            /// Iterate over every node in contiguous allocation order.
            #schema_visibility fn nodes(
                &self,
            ) -> impl DoubleEndedIterator<Item = #reference<'_>> + ExactSizeIterator + '_ {
                self.map.nodes()
            }

            #schema_visibility fn contains_key(&self, key: &Key) -> bool {
                self.map.contains_key(key)
            }

            #schema_visibility fn get(&self, key: &Key) -> Option<#reference<'_>> {
                self.map.get(key)
            }

            #schema_visibility fn get_owned(&self, key: &Key) -> Option<#root> {
                self.map.handle(key).map(#root::new)
            }



            #schema_visibility fn annotations_builder<T>(
                &self,
            ) -> #runtime::NodeAnnotationsBuilder<#arena, T> {
                self.map.annotations_builder()
            }



            /// Clone and recursively rewrite every root into fresh compact storage.
            #schema_visibility fn try_rewrite_with<'source, Error>(
                &'source self,
                replace: impl FnMut(
                    #reference<'source>,
                ) -> Result<Option<#reference<'source>>, Error>,
            ) -> Result<
                Self,
                #runtime::CloneTreeError<#reference<'source>, Error>,
            >
            where
                Key: Clone,
            {
                self.try_rewrite_selected_with(|_| true, replace)
            }

            /// Clone selected roots and recursively rewrite them into fresh compact storage.
            ///
            /// Root keys retain their existing order. Unselected roots and nodes that become
            /// unreachable through replacement are omitted from the resulting storage.
            #schema_visibility fn try_rewrite_selected_with<'source, Error>(
                &'source self,
                mut keep: impl FnMut(&Key) -> bool,
                mut replace: impl FnMut(
                    #reference<'source>,
                ) -> Result<Option<#reference<'source>>, Error>,
            ) -> Result<
                Self,
                #runtime::CloneTreeError<#reference<'source>, Error>,
            >
            where
                Key: Clone,
            {
                let selected = self.iter().filter(|(key, _)| keep(key)).collect::<Vec<_>>();
                let capacity = selected
                    .iter()
                    .map(|(_, root)| #runtime::TreeCursorExt::subtree_len(*root))
                    .sum();
                let mut builder = #builder::with_capacity(capacity);
                let mut keys = Vec::with_capacity(selected.len());
                let mut roots = Vec::with_capacity(selected.len());
                for (key, root) in selected {
                    keys.push(key.clone());
                    roots.push(builder.try_clone_subtree_with(root, &mut replace)?);
                }
                let forest = builder
                    .finish_forest(roots)
                    .expect("rewritten roots form a complete forest");
                Ok(Self::new(keys, forest)
                    .expect("selecting entries preserves sorted unique keys"))
            }

            /// Retain entries whose keys satisfy `keep`.
            ///
            /// If every entry is retained, storage is preserved unchanged. Removing
            /// any root rebuilds the retained trees into complete shared storage.
            #schema_visibility fn retain(&mut self, mut keep: impl FnMut(&Key) -> bool)
            where
                Key: Clone,
            {
                let selected = self.iter().filter(|(key, _)| keep(key)).collect::<Vec<_>>();
                if selected.len() == self.len() {
                    return;
                }

                let capacity = selected
                    .iter()
                    .map(|(_, root)| #runtime::TreeCursorExt::subtree_len(*root))
                    .sum();
                let mut builder = #builder::with_capacity(capacity);
                let mut keys = Vec::with_capacity(selected.len());
                let mut roots = Vec::with_capacity(selected.len());
                for (key, root) in selected {
                    keys.push(key.clone());
                    roots.push(builder.clone_subtree(root));
                }
                let forest = builder
                    .finish_forest(roots)
                    .expect("retained roots form a complete forest");
                *self = Self::new(keys, forest)
                    .expect("retaining entries preserves sorted unique keys");
            }

            #schema_visibility fn into_entries(
                self,
            ) -> impl DoubleEndedIterator<Item = (Key, #root)> + ExactSizeIterator {
                self.map.into_entries().map(|(key, root)| (key, #root::new(root)))
            }

            #schema_visibility fn into_forest(self) -> #forest {
                #forest {
                    forest: self.map.into_forest(),
                }
            }
        }



        impl #arena {
            #schema_visibility fn with_capacity(capacity: usize) -> Self {
                Self {
                    nodes: #runtime::Arena::with_capacity(capacity),
                }
            }

            #schema_visibility fn len(&self) -> usize {
                self.nodes.len()
            }

            #schema_visibility fn is_empty(&self) -> bool {
                self.nodes.is_empty()
            }

            #schema_visibility fn ids(
                &self,
            ) -> impl DoubleEndedIterator<Item = #id> + ExactSizeIterator + '_ {
                self.nodes.iter().map(|(id, _)| id)
            }

            #schema_visibility fn kind(&self, id: #id) -> &#kind {
                &self.nodes.get(id).node
            }

            #schema_visibility fn metadata(&self, id: #id) -> &#metadata {
                &self.nodes.get(id).#metadata_name
            }

            #schema_visibility fn subtree_ids(&self, root: #id) -> #runtime::IdRange<#id> {
                self.nodes.subtree_ids(root)
            }



            fn node(&self, id: #id) -> &#node {
                self.nodes.get(id)
            }



        }

        #[doc = #builder_doc]
        #schema_visibility struct #builder {
            builder: #runtime::ForestBuilder<#arena, #node>,
        }

        impl #builder {
            #schema_visibility fn with_capacity(capacity: usize) -> Self {
                Self {
                    builder: #runtime::ForestBuilder::new(
                        #arena::with_capacity(capacity),
                    ),
                }
            }

            #schema_visibility fn try_alloc(
                &mut self,
                node: #kind,
                metadata: #metadata,
            ) -> Result<#id, #runtime::BuildError<#id>> {
                self.builder.try_alloc(#node {
                    node,
                    #metadata_name: metadata,
                })
            }

            #schema_visibility fn alloc(&mut self, node: #kind, metadata: #metadata) -> #id {
                self.try_alloc(node, metadata)
                    .unwrap_or_else(|error| panic!("invalid postorder allocation: {error}"))
            }

            /// Allocate a node using the schema's default metadata value.
            #schema_visibility fn alloc_default(&mut self, node: #kind) -> #id {
                self.alloc(node, #metadata_default)
            }

            #schema_visibility fn metadata(&self, id: #id) -> &#metadata {
                self.builder.storage().metadata(id)
            }

            /// Finish one owning root, validating if it differs from the constructed frontier.
            #schema_visibility fn finish(self, root: #id) -> Result<#root, #runtime::ForestError> {
                let forest = #forest {
                    forest: self.builder.finish([root])?,
                };
                Ok(forest
                    .into_roots()
                    .next()
                    .expect("a single-root forest contains one root"))
            }

            /// Finish ordered owning roots, validating if they differ from the constructed frontier.
            #schema_visibility fn finish_forest(
                self,
                roots: impl IntoIterator<Item = #id>,
            ) -> Result<#forest, #runtime::ForestError> {
                self.builder.finish(roots).map(|forest| #forest { forest })
            }

            /// Clone one complete occurrence subtree into this builder.
            #schema_visibility fn clone_subtree(&mut self, root: #reference<'_>) -> #id {
                self.builder
                    .clone_tree_from(root, |cursor| cursor.node().clone())
            }

            /// Clone a subtree while recursively replacing selected source subtrees.
            #schema_visibility fn try_clone_subtree_with<'source, Error>(
                &mut self,
                root: #reference<'source>,
                replace: impl FnMut(#reference<'source>) -> Result<Option<#reference<'source>>, Error>,
            ) -> Result<#id, #runtime::CloneTreeError<#reference<'source>, Error>> {
                self.builder.try_clone_tree_with(
                    root,
                    replace,
                    |cursor| cursor.node().clone(),
                )
            }
        }

        impl std::fmt::Debug for #reference<'_> {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(&self.view(), formatter)
            }
        }

        impl<'arena> #reference<'arena> {
            #schema_visibility fn from_arena(arena: &'arena #arena, id: #id) -> Self {
                Self { arena, id }
            }

            fn belongs_to(self, arena: &#arena) -> bool {
                std::ptr::eq(self.arena, arena)
            }

            #schema_visibility fn shares_storage_with(self, other: Self) -> bool {
                std::ptr::eq(self.arena, other.arena)
            }



            #schema_visibility fn kind(self) -> &'arena #kind {
                &self.node().node
            }

            #schema_visibility fn metadata(self) -> &'arena #metadata {
                &self.node().#metadata_name
            }

            #visibility fn id(self) -> #id {
                self.id
            }

            fn child(self, id: #id) -> Self {
                Self {
                    arena: self.arena,
                    id,
                }
            }

            fn node(self) -> &'arena #node {
                self.arena.node(self.id)
            }

            #schema_visibility fn view_with<#cursor>(self, cursor: #cursor) -> #view<'arena, #cursor>
            where
                #cursor: #runtime::TreeCursor<Id = #id>,
            {
                match self.kind() {
                    #( #resolve_arms, )*
                }
            }

            #visibility fn view(self) -> #view<'arena> {
                self.view_with(self)
            }

        }

        impl #kind {
            #schema_visibility fn child_ids(
                &self,
            ) -> #runtime::__private::ChildIds<'_, #id, #key> {
                let mut ids = #runtime::__private::ChildIds::empty();
                match self {
                    #( #child_ids_arms, )*
                }
                ids
            }

            fn for_each_child_id_mut(&mut self, mut visit: impl FnMut(&mut #id)) {
                match self {
                    #( #visit_mut_arms, )*
                }
            }

            #schema_visibility fn same_payload(&self, other: &Self) -> bool {
                match (self, other) {
                    #( #payload_arms, )*
                    _ => false,
                }
            }

        }

        impl #runtime::TreeNode<#id> for #kind {
            type ChildIds<'node> = #runtime::__private::ChildIds<'node, #id, #key>;

            fn child_ids(&self) -> Self::ChildIds<'_> {
                self.child_ids()
            }
        }

        impl #runtime::TreeNodeMut<#id> for #kind {
            fn for_each_child_id_mut(&mut self, visit: impl FnMut(&mut #id)) {
                self.for_each_child_id_mut(visit)
            }
        }

        impl #runtime::TreeNode<#id> for #node {
            type ChildIds<'node> = #runtime::__private::ChildIds<'node, #id, #key>;

            fn child_ids(&self) -> Self::ChildIds<'_> {
                self.node.child_ids()
            }
        }

        impl #runtime::TreeNodeMut<#id> for #node {
            fn for_each_child_id_mut(&mut self, visit: impl FnMut(&mut #id)) {
                self.node.for_each_child_id_mut(visit)
            }
        }

        impl<'arena> #runtime::TreeCursor for #reference<'arena> {
            type Id = #id;
            type ChildIds = #runtime::__private::ChildIds<'arena, #id, #key>;

            fn id(self) -> Self::Id {
                self.id()
            }

            fn storage_identity(self) -> #runtime::StorageIdentity {
                #runtime::StorageIdentity::for_ref(self.arena)
            }

            fn same_node(self, other: Self) -> bool {
                std::ptr::eq(self.arena, other.arena) && self.id == other.id
            }

            fn child_ids(self) -> Self::ChildIds {
                self.kind().child_ids()
            }

            fn child(self, id: Self::Id) -> Self {
                self.child(id)
            }

            fn subtree_ids(self) -> #runtime::IdRange<Self::Id> {
                self.arena.subtree_ids(self.id())
            }
        }

        #owned_constructor_impl
    }
}

fn stored_type(
    kind: &FieldKind,
    id: &Ident,
    children: &Type,
    keyed: &Ident,
) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(#id),
        FieldKind::Children => quote!(#children<#id>),

        FieldKind::KeyedChildren => quote!(#keyed),
        FieldKind::Borrowed(typ) | FieldKind::IntoOwned(typ) | FieldKind::Copied(typ) => {
            quote!(#typ)
        }
    }
}

fn view_type(
    runtime: &proc_macro2::TokenStream,
    kind: &FieldKind,
    cursor: &Ident,
    id: &Ident,
    key: &Type,
) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(#cursor),
        FieldKind::Children => quote!(#runtime::ResolvedIds<
            #cursor,
            std::iter::Copied<std::slice::Iter<'arena, #id>>
        >),

        FieldKind::KeyedChildren => {
            quote!(#runtime::ResolvedFields<'arena, #cursor, #key>)
        }
        FieldKind::Borrowed(typ) | FieldKind::IntoOwned(typ) => quote!(&'arena #typ),
        FieldKind::Copied(typ) => quote!(#typ),
    }
}

fn resolve(
    runtime: &proc_macro2::TokenStream,
    kind: &FieldKind,
    field: &Ident,
    cursor: &Ident,
) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(#runtime::TreeCursor::child(#cursor, *#field)),
        FieldKind::Children => {
            quote!(#runtime::ResolvedIds::new(#cursor, #field.iter().copied()))
        }

        FieldKind::KeyedChildren => {
            quote!(#runtime::ResolvedFields::new(#cursor, #field.as_slice()))
        }
        FieldKind::Borrowed(_) | FieldKind::IntoOwned(_) => quote!(#field),
        FieldKind::Copied(_) => quote!(*#field),
    }
}

fn record(kind: &FieldKind, field: &Ident) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(ids.push(*#field);),
        FieldKind::Children => quote!(ids.extend_slice(#field);),
        FieldKind::KeyedChildren => quote!(ids.extend_keyed(#field.as_slice());),
        FieldKind::Borrowed(_) | FieldKind::IntoOwned(_) | FieldKind::Copied(_) => {
            quote!(let _ = #field;)
        }
    }
}

fn visit(kind: &FieldKind, field: &Ident) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(visit(#field);),
        FieldKind::Children => quote!(#field.make_mut().iter_mut().for_each(&mut visit);),
        FieldKind::KeyedChildren => quote!(
            for (_, child) in #field.child_ids_mut() {
                visit(child);
            }
        ),
        FieldKind::Borrowed(_) | FieldKind::IntoOwned(_) | FieldKind::Copied(_) => {
            quote!(let _ = #field;)
        }
    }
}

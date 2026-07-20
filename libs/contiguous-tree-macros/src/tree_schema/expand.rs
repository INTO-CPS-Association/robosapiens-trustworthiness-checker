//! Generation of the Rust types and implementations described by a tree schema.
//!

use quote::{format_ident, quote};
use syn::{Expr, Ident, Type};

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

    fn view_definition(&self, cursor: &Ident, id: &Ident, key: &Type) -> proc_macro2::TokenStream {
        let name = &self.name;
        let fields = self
            .fields
            .iter()
            .map(|field| view_type(&field.kind, cursor, id, key));
        quote!(#name( #( #fields ),* ))
    }

    fn resolve_arm(&self, kind: &Ident, view: &Ident, cursor: &Ident) -> proc_macro2::TokenStream {
        let name = &self.name;
        let fields = self.fields.iter().map(|field| &field.name);
        let resolved = self
            .fields
            .iter()
            .map(|field| resolve(&field.kind, &field.name, cursor));
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

    fn builder_method(
        &self,
        kind: &Ident,
        id: &Ident,
        children: &Type,
        keyed_fields: &Ident,
        metadata_default: &Expr,
    ) -> proc_macro2::TokenStream {
        let name = &self.name;
        let field_names = self
            .fields
            .iter()
            .map(|field| &field.name)
            .collect::<Vec<_>>();
        let field_types = self
            .fields
            .iter()
            .map(|field| stored_type(&field.kind, id, children, keyed_fields));
        quote! {
            pub fn #name(&mut self, #( #field_names: #field_types ),*) -> #id {
                self.alloc(#kind::#name( #( #field_names ),*), #metadata_default)
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
                FieldKind::Borrowed(_) | FieldKind::Copied(_) => Some(quote!(#left == #right)),
            });
        quote! {
            (
                #kind::#name( #( #left ),* ),
                #kind::#name( #( #right ),* )
            ) => true #( && #comparisons )*
        }
    }

    fn duplicate_key_arm(&self, kind: &Ident) -> Option<proc_macro2::TokenStream> {
        let keyed_index = self
            .fields
            .iter()
            .position(|field| matches!(field.kind, FieldKind::KeyedChildren))?;
        let name = &self.name;
        let patterns = (0..self.fields.len()).map(|index| {
            if index == keyed_index {
                quote!(fields)
            } else {
                quote!(_)
            }
        });
        Some(quote!(#kind::#name( #( #patterns ),* ) => fields.duplicate_key()))
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
    handle: Ident,
    child_ids: Ident,
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
            handle: format_ident!("{root}Handle"),
            child_ids: format_ident!("{root}ChildIds"),
            refs: format_ident!("{root}Refs"),
            field_refs: format_ident!("{root}FieldRefs"),
            keyed_fields: format_ident!("{root}Fields"),
        }
    }
}

pub(super) fn expand(schema: TreeSchema) -> proc_macro2::TokenStream {
    let TreeSchema {
        visibility,
        internals,
        root,
        metadata_name,
        metadata,
        metadata_default,
        id: id_type,
        key,
        children,
        keyed_children,
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
        handle,
        child_ids,
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
    let builder_doc = format!("Bottom-up builder for `{root_name}` trees.");

    let stored_variants = variants
        .iter()
        .map(|variant| variant.stored_definition(&id, &children, &keyed_fields))
        .collect::<Vec<_>>();
    let view_variants = variants
        .iter()
        .map(|variant| variant.view_definition(&cursor, &id, &key))
        .collect::<Vec<_>>();
    let resolve_arms = variants
        .iter()
        .map(|variant| variant.resolve_arm(&kind, &view, &cursor_value))
        .collect::<Vec<_>>();
    let child_ids_arms = variants
        .iter()
        .map(|variant| variant.child_ids_arm(&kind))
        .collect::<Vec<_>>();
    let visit_mut_arms = variants
        .iter()
        .map(|variant| variant.visit_mut_arm(&kind))
        .collect::<Vec<_>>();
    let builder_methods = variants
        .iter()
        .map(|variant| {
            variant.builder_method(&kind, &id, &children, &keyed_fields, &metadata_default)
        })
        .collect::<Vec<_>>();
    let payload_arms = variants
        .iter()
        .map(|variant| variant.payload_equality_arm(&kind))
        .collect::<Vec<_>>();
    let duplicate_key_arms = variants
        .iter()
        .filter_map(|variant| variant.duplicate_key_arm(&kind))
        .collect::<Vec<_>>();
    let keyed_fields_definition =
        keyed_fields::expand(&visibility, &key, &id, &keyed_fields, &keyed_children);

    quote! {
        #[doc = #id_doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, contiguous_tree::__private::serde::Serialize)]
        #visibility struct #id(#id_type);

        impl #id {
            #internals fn new(index: usize) -> Self {
                Self(<#id_type>::try_from(index).expect("tree contains more nodes than its ID type can represent"))
            }

            pub fn index(self) -> usize {
                self.0 as usize
            }
        }

        impl contiguous_tree::ArenaId for #id {
            fn from_index(index: usize) -> Self {
                Self::new(index)
            }

            fn index(self) -> usize {
                self.index()
            }
        }

        pub(crate) type #child_ids<'node> = contiguous_tree::ChildIds<'node, #id, #key>;
        pub(crate) type #refs<'arena> = contiguous_tree::ResolvedIds<
            #reference<'arena>,
            std::iter::Copied<std::slice::Iter<'arena, #id>>,
        >;
        pub(crate) type #field_refs<'arena> =
            contiguous_tree::ResolvedFields<'arena, #reference<'arena>, #key>;

        #keyed_fields_definition

        #[doc = #kind_doc]
        #[derive(Clone, PartialEq, Debug, contiguous_tree::__private::serde::Serialize)]
        #visibility enum #kind {
            #( #stored_variants, )*
        }

        #[doc = #reference_doc]
        #[derive(Clone, Copy)]
        #visibility struct #reference<'arena> {
            arena: &'arena #arena,
            id: #id,
        }

        #[doc = #view_doc]
        #visibility enum #view<
            'arena,
            #cursor: contiguous_tree::TreeCursor<Id = #id> + 'arena = #reference<'arena>,
        > {
            #( #view_variants, )*
        }

        #[doc = #node_doc]
        #[derive(Clone, Debug, PartialEq, contiguous_tree::__private::serde::Serialize)]
        #internals struct #node {
            pub(crate) node: #kind,
            pub(crate) #metadata_name: #metadata,
        }

        #[doc = #arena_doc]
        #[derive(Clone, Debug, Default, PartialEq, contiguous_tree::__private::serde::Serialize)]
        #internals struct #arena {
            #internals nodes: contiguous_tree::Arena<#id, #node>,
        }

        #[doc = #handle_doc]
        #internals type #handle = contiguous_tree::TreeHandle<#arena>;

        #[doc = #root_doc]
        #[derive(Clone)]
        #visibility struct #root {
            tree: #handle,
        }

        impl contiguous_tree::TreeStorage for #arena {
            type Id = #id;
            type Cursor<'arena> = #reference<'arena>;

            fn cursor(&self, id: Self::Id) -> Self::Cursor<'_> {
                #reference::from_arena(self, id)
            }

            fn owns(&self, cursor: Self::Cursor<'_>) -> bool {
                cursor.belongs_to(self)
            }
        }

        impl #root {
            pub(crate) fn new(tree: #handle) -> Self {
                Self { tree }
            }

            pub fn id(&self) -> #id {
                self.tree.root_id()
            }

            #internals fn arena(&self) -> &#arena {
                self.tree.storage()
            }

            pub(crate) fn shares_storage_with(&self, other: &Self) -> bool {
                self.tree.shares_storage_with(&other.tree)
            }

            pub(crate) fn same_root(&self, other: &Self) -> bool {
                self.tree.same_root(&other.tree)
            }

            #[cfg(test)]
            pub(crate) fn storage_strong_count(&self) -> usize {
                self.tree.strong_count()
            }

            pub(crate) fn child(&self, id: #id) -> Self {
                self.subtree(self.as_ref().child(id))
            }

            pub(crate) fn subtree(&self, root: #reference<'_>) -> Self {
                Self::new(self.tree.subtree(root))
            }

            pub fn as_ref(&self) -> #reference<'_> {
                self.tree.cursor()
            }

            #internals fn into_owned_arena(self) -> (#arena, #id) {
                self.tree.into_storage_and_root()
            }
        }

        impl #arena {
            pub fn with_capacity(capacity: usize) -> Self {
                Self {
                    nodes: contiguous_tree::Arena::with_capacity(capacity),
                }
            }

            #internals fn alloc(&mut self, node: #kind, metadata: #metadata) -> #id {
                self.nodes.push_tree(#node {
                    node,
                    #metadata_name: metadata,
                })
            }

            pub(crate) fn get_mut(&mut self, id: #id) -> &mut #node {
                self.nodes.get_mut(id)
            }

            /// Clone an owning tree into this arena.
            pub fn clone_tree(&mut self, tree: &#root) -> #id {
                self.clone_subtree(tree.as_ref())
            }

            /// Clone one complete occurrence subtree into this arena.
            pub(crate) fn clone_subtree(&mut self, root: #reference<'_>) -> #id {
                self.nodes.clone_tree_from(root, |cursor| cursor.node().clone())
            }

            pub(crate) fn duplicate_key(&self) -> Option<&#key> {
                self.nodes.iter().find_map(|(_, node)| match &node.node {
                    #( #duplicate_key_arms, )*
                    _ => None,
                })
            }
        }

        impl std::ops::Deref for #arena {
            type Target = contiguous_tree::Arena<#id, #node>;

            fn deref(&self) -> &Self::Target {
                &self.nodes
            }
        }

        #[doc = #builder_doc]
        #visibility struct #builder {
            #internals arena: #arena,
        }

        impl #builder {
            pub fn with_capacity(capacity: usize) -> Self {
                Self {
                    arena: #arena::with_capacity(capacity),
                }
            }

            pub fn alloc(&mut self, node: #kind, metadata: #metadata) -> #id {
                self.arena.alloc(node, metadata)
            }

            pub(crate) fn get(&self, id: #id) -> &#node {
                self.arena.get(id)
            }

            pub(crate) fn finish_arena(self) -> #arena {
                self.arena
            }

            /// Clone one complete occurrence subtree into this builder.
            pub(crate) fn clone_subtree(&mut self, root: #reference<'_>) -> #id {
                self.arena.clone_subtree(root)
            }

            #[doc(hidden)]
            pub fn clone_tree(&mut self, tree: &#root) -> #id {
                self.arena.clone_tree(tree)
            }
        }

        impl<'arena> #reference<'arena> {
            pub(crate) fn from_arena(arena: &'arena #arena, id: #id) -> Self {
                Self { arena, id }
            }

            pub(crate) fn belongs_to(self, arena: &#arena) -> bool {
                std::ptr::eq(self.arena, arena)
            }

            pub(crate) fn arena(self) -> &'arena #arena {
                self.arena
            }

            pub(crate) fn kind(self) -> &'arena #kind {
                &self.node().node
            }

            pub fn id(self) -> #id {
                self.id
            }

            pub(crate) fn child(self, id: #id) -> Self {
                Self {
                    arena: self.arena,
                    id,
                }
            }

            pub(crate) fn node(self) -> &'arena #node {
                self.arena.get(self.id)
            }

            pub(crate) fn view_with<#cursor>(self, cursor: #cursor) -> #view<'arena, #cursor>
            where
                #cursor: contiguous_tree::TreeCursor<Id = #id>,
            {
                match self.kind() {
                    #( #resolve_arms, )*
                }
            }

            pub fn view(self) -> #view<'arena> {
                self.view_with(self)
            }

            pub(crate) fn duplicate_key(self) -> Option<&'arena #key> {
                contiguous_tree::Postorder::new(self).find_map(|node| match node.kind() {
                    #( #duplicate_key_arms, )*
                    _ => None,
                })
            }
        }

        impl #kind {
            pub fn child_ids(&self) -> #child_ids<'_> {
                let mut ids = #child_ids::empty();
                match self {
                    #( #child_ids_arms, )*
                }
                ids
            }

            #internals fn for_each_child_id_mut(&mut self, mut visit: impl FnMut(&mut #id)) {
                match self {
                    #( #visit_mut_arms, )*
                }
            }

            pub(crate) fn same_payload(&self, other: &Self) -> bool {
                match (self, other) {
                    #( #payload_arms, )*
                    _ => false,
                }
            }
        }

        impl contiguous_tree::TreeNode<#id> for #kind {
            type ChildIds<'node> = #child_ids<'node>;

            fn child_ids(&self) -> Self::ChildIds<'_> {
                self.child_ids()
            }
        }

        impl contiguous_tree::TreeNodeMut<#id> for #kind {
            fn for_each_child_id_mut(&mut self, visit: impl FnMut(&mut #id)) {
                self.for_each_child_id_mut(visit)
            }
        }

        impl contiguous_tree::TreeNode<#id> for #node {
            type ChildIds<'node> = #child_ids<'node>;

            fn child_ids(&self) -> Self::ChildIds<'_> {
                self.node.child_ids()
            }
        }

        impl contiguous_tree::TreeNodeMut<#id> for #node {
            fn for_each_child_id_mut(&mut self, visit: impl FnMut(&mut #id)) {
                self.node.for_each_child_id_mut(visit)
            }
        }

        impl<'arena> contiguous_tree::TreeCursor for #reference<'arena> {
            type Id = #id;
            type ChildIds = #child_ids<'arena>;

            fn id(self) -> Self::Id {
                self.id()
            }

            fn child_ids(self) -> Self::ChildIds {
                self.kind().child_ids()
            }

            fn child(self, id: Self::Id) -> Self {
                self.child(id)
            }

            fn subtree_ids(self) -> contiguous_tree::IdRange<Self::Id> {
                self.arena.subtree_ids(self.id())
            }
        }

        #[doc(hidden)]
        #[allow(non_snake_case)]
        impl #builder {
            #( #builder_methods )*
        }
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
        FieldKind::Borrowed(typ) | FieldKind::Copied(typ) => quote!(#typ),
    }
}

fn view_type(kind: &FieldKind, cursor: &Ident, id: &Ident, key: &Type) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(#cursor),
        FieldKind::Children => quote!(contiguous_tree::ResolvedIds<
            #cursor,
            std::iter::Copied<std::slice::Iter<'arena, #id>>
        >),
        FieldKind::KeyedChildren => {
            quote!(contiguous_tree::ResolvedFields<'arena, #cursor, #key>)
        }
        FieldKind::Borrowed(typ) => quote!(&'arena #typ),
        FieldKind::Copied(typ) => quote!(#typ),
    }
}

fn resolve(kind: &FieldKind, field: &Ident, cursor: &Ident) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(contiguous_tree::TreeCursor::child(#cursor, *#field)),
        FieldKind::Children => {
            quote!(contiguous_tree::ResolvedIds::new(#cursor, #field.iter().copied()))
        }
        FieldKind::KeyedChildren => {
            quote!(contiguous_tree::ResolvedFields::new(#cursor, #field.raw_slice()))
        }
        FieldKind::Borrowed(_) => quote!(#field),
        FieldKind::Copied(_) => quote!(*#field),
    }
}

fn record(kind: &FieldKind, field: &Ident) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(ids.push(*#field);),
        FieldKind::Children => quote!(ids.extend_slice(#field);),
        FieldKind::KeyedChildren => quote!(ids.extend_keyed(#field.raw_slice());),
        FieldKind::Borrowed(_) | FieldKind::Copied(_) => quote!(let _ = #field;),
    }
}

fn visit(kind: &FieldKind, field: &Ident) -> proc_macro2::TokenStream {
    match kind {
        FieldKind::Child => quote!(visit(#field);),
        FieldKind::Children => quote!(#field.make_mut().iter_mut().for_each(&mut visit);),
        FieldKind::KeyedChildren => quote!(for (_, child) in #field.make_mut() { visit(child); }),
        FieldKind::Borrowed(_) | FieldKind::Copied(_) => quote!(let _ = #field;),
    }
}

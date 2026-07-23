//! Procedural macros for contiguous postorder trees.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{DeriveInput, Ident, parse_macro_input};

mod cursor_derive;
mod tree_schema;

/// Generate a contiguous tree family from a node schema.
///
/// ```text
/// pub tree Expr {
///     schema: pub(crate),
///     owned_constructors: #[cfg(test)] pub(crate),
///     metadata: span: Span = Span::default(),
///     id: u32,
///     key: String,
///     children: EcoVec,
///     keyed_children: EcoVec,
///
///     Literal(value: data(Value)),
///     Offset(value: child, amount: copy(u64)),
///     Sequence(items: children),
///     Record(fields: keyed_children),
/// }
/// ```
///
/// Field forms have the following meaning:
///
/// | Form | Stored value | Borrowed view |
/// |------|--------------|---------------|
/// | `child` | typed node ID | child cursor |
/// | `children` | collection of IDs | iterator of child cursors |
/// | `keyed_children` | ordered key/ID collection | ordered key/cursor view |
/// | `data(T)` | owned `T` | `&T` |
/// | `into_data(T)` | owned `T` (constructor accepts `Into<T>`) | `&T` |
/// | `copy(T)` | owned `T` | copied `T` |
///
/// For a root named `Expr`, the generated types are:
///
/// | Generated type | Role |
/// |----------------|------|
/// | `Expr` | language-facing owning tree root |
/// | `ExprRef<'a>` | language-facing borrowed traversal cursor |
/// | `ExprView<'a, C>` | language-facing node view with child IDs resolved to cursors |
/// | `ExprId` | language-facing typed node ID |
/// | `ExprKind` | schema-author node payload using `ExprId` for children |
/// | `ExprBuilder` | schema-author bottom-up allocator with validated completion methods |
/// | `ExprForest` | schema-author ordered owning forest backed by one shared arena |
/// | `ExprForestMap<Key>` | schema-author sorted keys associated with shared forest roots |
/// | `ExprFields` | schema-author keyed children in source order |
/// | `ExprArena` | schema-author storage with safe ID, kind, and metadata accessors |
///
/// The declared tree visibility is used for `Expr`, `ExprRef`, `ExprView`, and
/// `ExprId`. The optional `schema` setting controls `ExprKind`, `ExprBuilder`,
/// `ExprForest`, `ExprArena`, keyed fields, and schema-facing aliases and
/// methods; it defaults to `pub(crate)`. Raw node storage and handle aliases are
/// implementation-private.
///
/// `ExprBuilder::finish(root)` returns `Result<Expr, ForestError>`, while
/// `ExprBuilder::finish_forest(roots)` returns `Result<ExprForest, ForestError>`.
/// `ExprForest` provides `new`, `len`, `is_empty`, `root_ids`, `roots` (borrowed
/// cursors), and `into_roots` (owning `Expr` values), all at schema visibility.
/// `ExprForestMap<Key>` additionally provides keyed lookup, retention, and fallible
/// rewriting of all or selected roots into fresh compact storage.
/// The optional `owned_constructors` setting generates
/// variant-named constructors with the requested visibility. Outer attributes
/// between `:` and the visibility are applied to the constructor `impl`, so
/// `owned_constructors: #[cfg(test)] pub(crate),` makes them test-only.
///
/// `children` and `keyed_children` name generic collection constructors, not
/// fully instantiated types. For example, use `EcoVec`, not `EcoVec<ExprId>`.
/// For an element type `T`, both collections must support cloning, equality,
/// debug formatting, slice access, and an inherent `make_mut(&mut self) ->
/// &mut [T]` operation. The keyed collection is
/// instantiated with `(Key, Id)` and must additionally support `Default`,
/// `FromIterator<T>`, and owned `IntoIterator<Item = T>`. `EcoVec` satisfies
/// this contract.
#[proc_macro]
pub fn tree_schema(input: TokenStream) -> TokenStream {
    match runtime_crate_path() {
        Ok(runtime) => tree_schema::expand(input, &runtime),
        Err(error) => error.into_compile_error().into(),
    }
}

/// Delegate `TreeCursor` through a transparent cursor wrapper.
#[proc_macro_derive(TreeCursor, attributes(tree_cursor))]
pub fn derive_tree_cursor(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    runtime_crate_path()
        .and_then(|runtime| cursor_derive::expand(input, &runtime))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn runtime_crate_path() -> syn::Result<TokenStream2> {
    match crate_name("contiguous-tree")
        .map_err(|error| syn::Error::new(Span::call_site(), error.to_string()))?
    {
        FoundCrate::Itself => Ok(quote!(::contiguous_tree)),
        FoundCrate::Name(name) => {
            let name = Ident::new(&name, Span::call_site());
            Ok(quote!(::#name))
        }
    }
}

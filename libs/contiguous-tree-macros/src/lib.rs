//! Procedural macros for contiguous postorder trees.

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod cursor_derive;
mod tree_schema;

/// Generate a contiguous tree family from a node schema.
///
/// ```text
/// pub tree Expr {
///     internals: pub(crate),
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
/// | `copy(T)` | owned `T` | copied `T` |
///
/// For a root named `Expr`, the public interface consists of:
///
/// | Generated type | Role |
/// |----------------|------|
/// | `Expr` | owning tree root |
/// | `ExprRef<'a>` | borrowed traversal cursor |
/// | `ExprView<'a, C>` | node view with child IDs resolved to cursors |
/// | `ExprKind` | stored node payload using `ExprId` for children |
/// | `ExprId` | typed node ID |
/// | `ExprBuilder` | bottom-up construction |
/// | `ExprFields` | keyed children in source order |
/// | `ExprArena`, `ExprNode`, `ExprHandle` | storage implementation |
///
/// `internals` controls the visibility of generated storage types such as
/// `ExprArena`, `ExprNode`, and `ExprHandle`; the declared tree visibility is
/// used for the language-facing root, ID, kind, reference, view, builder, and
/// keyed-field types. The optional `owned_constructors` setting generates
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
    tree_schema::expand(input)
}

/// Delegate `TreeCursor` through a transparent cursor wrapper.
#[proc_macro_derive(TreeCursor, attributes(tree_cursor))]
pub fn derive_tree_cursor(input: TokenStream) -> TokenStream {
    cursor_derive::expand(parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

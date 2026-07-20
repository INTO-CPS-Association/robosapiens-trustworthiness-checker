//! Schema generation for contiguous postorder trees.

mod expand;
mod keyed_fields;
mod schema;

pub(crate) fn expand(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    match syn::parse::<schema::TreeSchema>(input) {
        Ok(schema) => expand::expand(schema).into(),
        Err(error) => error.into_compile_error().into(),
    }
}

use contiguous_tree_macros::tree_schema;

tree_schema! {
    pub tree Expr {
        internals: pub(crate),
        metadata: span: () = (),
        id: u32,
        key: String,
        children: Vec,
        keyed_children: Vec,
        Leaf(),
        Leaf(),
    }
}

fn main() {}

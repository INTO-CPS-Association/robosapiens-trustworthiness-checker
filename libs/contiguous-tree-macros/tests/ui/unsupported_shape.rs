use contiguous_tree_macros::tree_schema;

tree_schema! {
    pub tree Expr {
        schema: pub(crate),
        metadata: span: () = (),
        id: u32,
        key: String,
        children: Vec,
        keyed_children: Vec,
        Node(a: child, b: child, c: child, d: child),
    }
}

fn main() {}

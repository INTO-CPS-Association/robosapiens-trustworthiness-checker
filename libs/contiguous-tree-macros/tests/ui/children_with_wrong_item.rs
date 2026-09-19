use contiguous_tree_macros::tree_schema;

tree_schema! {
    pub tree Associated {
        schema: pub,
        owned_constructors: pub,
        metadata: metadata: () = (),
        id: u32,
        children: Vec,

        Leaf(),
        Arms(children: children_with(u16)),
    }
}

fn main() {
    let _ = Associated::Arms([Associated::Leaf()]);
}

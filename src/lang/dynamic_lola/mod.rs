use lalrpop_util::lalrpop_mod;
lalrpop_mod!(pub lalr, "/lang/dynamic_lola/lalr.rs");

pub mod ast;
pub mod parser;
#[cfg(test)]
pub mod test_generation;
pub mod type_checker;

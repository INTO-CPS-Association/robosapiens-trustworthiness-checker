use lalrpop_util::lalrpop_mod;
lalrpop_mod!(pub lalr, "/lang/dsrv/lalr.rs");

pub mod ast;
pub mod lalr_parser;
pub mod parser;
#[cfg(test)]
pub mod test_generation;
pub mod type_checker;

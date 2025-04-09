use lalrpop_util::lalrpop_mod;
lalrpop_mod!(pub lalr_parser, "/lang/dynamic_lola/lalr_parser.rs");

pub mod ast;
pub mod parser;
#[cfg(test)]
pub mod test_generation;
pub mod type_checker;

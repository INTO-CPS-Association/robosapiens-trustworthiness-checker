fn main() {
    lalrpop::Configuration::new()
        .process_dir("src")
        .expect("failed to generate LALRPOP parsers");
}

//! Translate an application's observation schema into the DSRV declarations
//! its specifications import.
//!
//! ```sh
//! generate_rpio_declarations observation_schema.json --output declarations.dsrv
//! ```

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use trustworthiness_checker::io::rpio::{ObservationSchema, declarations};

#[derive(Parser)]
#[command(about = "Generate DSRV declarations from an RPIO observation schema")]
struct Args {
    /// The application's `observation_schema.json`.
    schema: PathBuf,
    /// Where to write the module; standard output if omitted.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Exit with failure if the output file differs from what would be
    /// generated, instead of writing it.
    #[arg(long, requires = "output")]
    check: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let path = args.schema.display().to_string();
    let schema = ObservationSchema::read(&path)?;
    let source = args
        .schema
        .file_name()
        .map_or(path.clone(), |name| name.to_string_lossy().into_owned());
    let text = declarations::generate(&schema, &source);
    match args.output {
        None => print!("{text}"),
        Some(output) if args.check => {
            let current = std::fs::read_to_string(&output)
                .with_context(|| format!("{} could not be read", output.display()))?;
            anyhow::ensure!(
                current == text,
                "{} is not the declarations generated from {path}; regenerate it",
                output.display()
            );
        }
        Some(output) => std::fs::write(&output, text)
            .with_context(|| format!("{} could not be written", output.display()))?,
    }
    Ok(())
}

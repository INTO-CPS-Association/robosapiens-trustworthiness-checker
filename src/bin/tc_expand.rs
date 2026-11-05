use std::{io, io::Write as _, process::ExitCode};

use clap::Parser;
use trustworthiness_checker::cli::expand;

#[derive(Parser)]
#[command(
    name = "tc-expand",
    version,
    about = "Inspect an expanded DSRV program"
)]
struct Args {
    /// Root DSRV model file.
    model: String,

    /// Optionally type-check the expanded program before inspecting it.
    #[arg(long)]
    check: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match expand::run_inspection(
        &args.model,
        args.check,
        &mut io::stdout().lock(),
        &mut io::stderr().lock(),
    ) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            if matches!(error, expand::InspectionIoError::Stdout(_)) {
                let _ = writeln!(io::stderr().lock(), "error: {error}");
            }
            ExitCode::FAILURE
        }
    }
}

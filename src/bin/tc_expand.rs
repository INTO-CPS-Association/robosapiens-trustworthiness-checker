use std::{io, io::Write as _, process::ExitCode};

use clap::{Parser, ValueEnum};
use trustworthiness_checker::cli::expand::{self, CheckMode};

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
    #[arg(long, value_enum)]
    check_mode: Option<Mode>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Strict,
    Gradual,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let mode = args.check_mode.map(|mode| match mode {
        Mode::Strict => CheckMode::Strict,
        Mode::Gradual => CheckMode::Gradual,
    });
    match expand::run(
        &args.model,
        mode,
        &mut io::stdout().lock(),
        &mut io::stderr().lock(),
    ) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            if matches!(error, expand::ExpandIoError::Stdout(_)) {
                let _ = writeln!(io::stderr().lock(), "error: {error}");
            }
            ExitCode::FAILURE
        }
    }
}

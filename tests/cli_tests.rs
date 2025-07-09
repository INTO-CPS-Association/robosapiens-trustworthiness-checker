//! # CLI Integration Tests
//!
//! This module contains integration tests for the trustworthiness checker's command-line interface.
//! These tests run the actual compiled binary and verify its behavior with various input files
//! and command-line options using custom fixture files.
//!
//! ## Test Organization
//!
//! The tests are organized into several categories:
//! - **Basic functionality tests**: Simple addition, counter, string concatenation
//! - **Data type tests**: Integer, Float, String, Boolean operations
//! - **Language feature tests**: If-else conditions, past indexing, arithmetic operations
//! - **Error handling tests**: Invalid files, malformed input, missing arguments
//! - **CLI option tests**: Different parser modes, language modes, distribution modes
//! - **Edge case tests**: Empty input, single timestep, help output
//!
//! ## Test Fixtures
//!
//! All test data is stored in the `tests/fixtures/` directory:
//!
//! ### Model Files (`.lola`)
//! - `simple_add_typed.lola` - Basic addition with typed inputs
//! - `counter.lola` - Counter with past indexing
//! - `string_concat.lola` - String concatenation
//! - `float_arithmetic.lola` - Float arithmetic operations
//! - `if_else.lola` - Conditional logic
//! - `past_indexing.lola` - Past value access
//! - `debug_simple.lola` - Minimal test case
//!
//! ### Input Files (`.input`)
//! - `simple_add_typed.input` - Integer inputs for addition
//! - `counter.input` - Sequential integer inputs
//! - `string_concat.input` - String inputs
//! - `float_arithmetic.input` - Float inputs
//! - `if_else.input` - Mixed positive/negative integers
//! - `past_indexing.input` - Sequential inputs for past indexing
//! - `debug_simple.input` - Minimal test inputs
//! - `malformed.input` - Malformed input for error testing
//! - `single_timestep.input` - Single timestep test data
//! - `empty.input` - Empty input file for edge case testing
//!
//! ## Input File Format
//!
//! Input files use this format:
//! ```text
//! 0: var1 = value1
//!    var2 = value2
//! 1: var1 = value3
//!    var2 = value4
//! ```
//!
//! Where:
//! - `0:`, `1:`, etc. are timestep markers
//! - `var1`, `var2` are input variable names
//! - `value1`, `value2` are the values (Int, Float, String, Bool)
//!
//! ## Running Tests
//!
//! ### Prerequisites
//! 1. Build the project: `cargo build`
//! 2. Ensure the binary exists at `target/debug/trustworthiness_checker`
//!
//! ### Commands
//! ```bash
//! # Run all CLI integration tests
//! cargo test cli_integration_tests
//!
//! # Run specific test
//! cargo test test_simple_add_typed
//!
//! # Run with verbose output
//! cargo test test_simple_add_typed -- --nocapture
//! ```
//!
//! ## Test Coverage
//!
//! The tests cover:
//! - Basic arithmetic operations (add, subtract, multiply, divide)
//! - String operations (concatenation)
//! - Boolean operations and conditionals
//! - Past indexing and temporal operations
//! - Type system (Int, Float, String, Bool)
//! - Error handling (file not found, parse errors)
//! - CLI argument parsing
//! - Different parser modes
//! - Different language modes
//! - Output formatting
//! - Edge cases (empty input, single timestep)
//!
//! ## Debugging Failed Tests
//!
//! ### Common Issues
//! 1. **Binary not found**: Run `cargo build` first
//! 2. **File not found**: Check that fixture files exist
//! 3. **Parse errors**: Verify model syntax is correct
//! 4. **Output format changes**: Update expected output strings
//!
//! ### Debug Commands
//! ```bash
//! # Run binary manually to see output
//! ./target/debug/trustworthiness_checker tests/fixtures/simple_add_typed.lola \
//!   --input-file tests/fixtures/simple_add_typed.input --output-stdout
//!
//! # Run single test in isolation
//! cargo test test_simple_add_typed --exact
//! ```
//!
//! ## Dependencies
//!
//! The integration tests require:
//! - Access to the compiled binary
//! - Test fixture files in `tests/fixtures/` directory

use std::path::Path;
use std::process::Command;

/// Helper function to get the path to the binary
fn get_binary_path() -> String {
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!("{}/{}/trustworthiness_checker", target_dir, profile)
}

/// Helper function to run the CLI with given arguments and return output
fn run_cli(args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    let binary_path = get_binary_path();
    Command::new(binary_path).args(args).output()
}

/// Helper function to get fixture file path
fn fixture_path(filename: &str) -> String {
    format!("tests/fixtures/{}", filename)
}

/// Test simple addition with typed inputs
#[test]
fn test_simple_add_typed() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Expected output: z = 3, 7 (from adding x + y for each timestep)
    assert!(
        stdout.contains("3"),
        "Expected output '3' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("7"),
        "Expected output '7' not found in: {}",
        stdout
    );
}

/// Test simple addition with typed inputs: check default stdout usage
#[test]
fn test_simple_add_typed_no_stdout() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Expected output: z = 3, 7 (from adding x + y for each timestep)
    assert!(
        stdout.contains("3"),
        "Expected output '3' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("7"),
        "Expected output '7' not found in: {}",
        stdout
    );
}

/// Test counter with past indexing
#[test]
fn test_counter() {
    let output = run_cli(&[
        &fixture_path("counter.lola"),
        "--input-file",
        &fixture_path("counter.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Expected output: z = 1, 2, 3, 4 (cumulative sum)
    assert!(
        stdout.contains("1"),
        "Expected output '1' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("2"),
        "Expected output '2' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("3"),
        "Expected output '3' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("4"),
        "Expected output '4' not found in: {}",
        stdout
    );
}

/// Test string concatenation
#[test]
fn test_string_concat() {
    let output = run_cli(&[
        &fixture_path("string_concat.lola"),
        "--input-file",
        &fixture_path("string_concat.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Expected output: concatenated strings
    assert!(
        stdout.contains("HelloWorld"),
        "Expected 'HelloWorld' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("Test123"),
        "Expected 'Test123' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("AB"),
        "Expected 'AB' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("Empty"),
        "Expected 'Empty' not found in: {}",
        stdout
    );
}

/// Test float arithmetic operations
#[test]
fn test_float_arithmetic() {
    let output = run_cli(&[
        &fixture_path("float_arithmetic.lola"),
        "--input-file",
        &fixture_path("float_arithmetic.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Expected output: sum=13.0, diff=8.0, product=26.25, quotient=4.2 for first timestep
    assert!(
        stdout.contains("13"),
        "Expected sum '13' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("8"),
        "Expected diff '8' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("26.25"),
        "Expected product '26.25' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("4.2"),
        "Expected quotient '4.2' not found in: {}",
        stdout
    );
}

/// Test if-else conditional logic
#[test]
fn test_if_else() {
    let output = run_cli(&[
        &fixture_path("if_else.lola"),
        "--input-file",
        &fixture_path("if_else.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Check that the output contains expected values
    // First timestep: x=5, y=3, so result=5 (max), positive=true
    // Second timestep: x=2, y=8, so result=8 (max), positive=true
    // Fourth timestep: x=-5, y=2, so result=2 (max), positive=false
    assert!(
        stdout.contains("5"),
        "Expected result '5' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("8"),
        "Expected result '8' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("true"),
        "Expected 'true' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("false"),
        "Expected 'false' not found in: {}",
        stdout
    );
}

/// Test past indexing functionality
#[test]
fn test_past_indexing() {
    let output = run_cli(&[
        &fixture_path("past_indexing.lola"),
        "--input-file",
        &fixture_path("past_indexing.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify that past indexing works correctly
    // At timestep 0: current=1, prev1=0, prev2=0, diffprev=1, sumlast3=1
    // At timestep 1: current=2, prev1=1, prev2=0, diffprev=1, sumlast3=3
    // At timestep 2: current=3, prev1=2, prev2=1, diffprev=1, sumlast3=6
    assert!(
        stdout.contains("1"),
        "Expected current '1' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("2"),
        "Expected current '2' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("3"),
        "Expected current '3' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("6"),
        "Expected sumlast3 '6' not found in: {}",
        stdout
    );
}

/// Test error handling for invalid model file
#[test]
fn test_invalid_model_file() {
    let output = run_cli(&[
        "nonexistent_model.lola",
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        !output.status.success(),
        "CLI should have failed with invalid model file"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("could not be parsed") || stderr.contains("No such file"),
        "Expected error message not found in: {}",
        stderr
    );
}

/// Test error handling for invalid input file
#[test]
fn test_invalid_input_file() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        "nonexistent_input.input",
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        !output.status.success(),
        "CLI should have failed with invalid input file"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No such file") || stderr.contains("could not be"),
        "Expected error message not found in: {}",
        stderr
    );
}

/// Test error handling for malformed input
#[test]
fn test_malformed_input() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("malformed.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        !output.status.success(),
        "CLI should have failed with malformed input"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("parse") || stderr.contains("invalid") || stderr.contains("error"),
        "Expected error message not found in: {}",
        stderr
    );
}

/// Test CLI with different parser modes
#[test]
fn test_combinator_parser() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--parser-mode",
        "combinator",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("3"),
        "Expected output '3' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("7"),
        "Expected output '7' not found in: {}",
        stdout
    );
}

/// Test CLI with different language modes
#[test]
fn test_lola_language() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--language",
        "lola",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("3"),
        "Expected output '3' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("7"),
        "Expected output '7' not found in: {}",
        stdout
    );
}

/// Test CLI with centralised distribution mode (default)
#[test]
fn test_centralised_mode() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--centralised",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("3"),
        "Expected output '3' not found in: {}",
        stdout
    );
    assert!(
        stdout.contains("7"),
        "Expected output '7' not found in: {}",
        stdout
    );
}

/// Test CLI with empty input file
#[test]
fn test_empty_input() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("empty.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    // This should fail because empty input is not valid
    assert!(
        !output.status.success(),
        "CLI should have failed with empty input"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid addition") || stderr.contains("parse") || stderr.contains("error"),
        "Expected error message not found in: {}",
        stderr
    );
}

/// Test CLI with single timestep input
#[test]
fn test_single_timestep() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("single_timestep.input"),
        "--output-stdout",
    ])
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("100"),
        "Expected output '100' (42+58) not found in: {}",
        stdout
    );
}

/// Test that CLI produces help output
#[test]
fn test_help_output() {
    let output = run_cli(&["--help"]).expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI help command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Usage:"),
        "Expected 'Usage:' in help output: {}",
        stdout
    );
    assert!(
        stdout.contains("input-file"),
        "Expected 'input-file' in help output: {}",
        stdout
    );
    assert!(
        stdout.contains("output-stdout"),
        "Expected 'output-stdout' in help output: {}",
        stdout
    );
}

/// Test CLI with missing required arguments
#[test]
fn test_missing_required_args() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"), // Missing input mode
    ])
    .expect("Failed to run CLI");

    assert!(
        !output.status.success(),
        "CLI should have failed with missing required args"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("required") || stderr.contains("error"),
        "Expected error message about missing args not found in: {}",
        stderr
    );
}

/// Integration test to ensure binary is built before running tests
#[test]
fn test_binary_exists() {
    let binary_path = get_binary_path();
    assert!(
        Path::new(&binary_path).exists(),
        "Binary not found at {}. Please run 'cargo build' first",
        binary_path
    );
}

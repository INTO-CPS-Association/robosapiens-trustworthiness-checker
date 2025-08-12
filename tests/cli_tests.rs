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

use async_compat::Compat as TokioCompat;
use async_unsync::oneshot;
use futures::{FutureExt, StreamExt};
use macro_rules_attribute::apply;
use paho_mqtt as mqtt;
use smol::LocalExecutor;
use smol::process::Command;
use std::rc::Rc;
use std::time::Duration;
use std::{os::unix::process::ExitStatusExt, path::Path};
use tc_testutils::{
    mqtt::{dummy_mqtt_publisher, start_mqtt},
    redis::{dummy_redis_receiver, dummy_redis_sender, start_redis},
};
use trustworthiness_checker::Value;
use trustworthiness_checker::async_test;
use trustworthiness_checker::core::{MQTT_HOSTNAME, REDIS_HOSTNAME};

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
async fn run_cli(args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    let binary_path = get_binary_path();

    // Add timeout to prevent hanging
    let command_future = Command::new(binary_path).args(args).output();
    let timeout_future = smol::Timer::after(Duration::from_secs(5));

    futures::select! {
        result = command_future.fuse() => result,
        _ = futures::FutureExt::fuse(timeout_future) => {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "CLI command timed out after 5 seconds"
            ))
        }
    }
}

/// Helper function to run CLI with streaming output capture for infinite processes
async fn run_cli_streaming(
    args: &[&str],
    timeout: Duration,
) -> Result<(String, String, Option<std::process::ExitStatus>), std::io::Error> {
    use smol::io::{AsyncBufReadExt, BufReader};
    use std::process::Stdio;

    let binary_path = get_binary_path();

    let mut child = Command::new(binary_path)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut stdout_reader = BufReader::new(stdout);
    let mut stderr_reader = BufReader::new(stderr);
    let mut stdout_output = String::new();
    let mut stderr_output = String::new();
    let mut stdout_line = String::new();
    let mut stderr_line = String::new();

    // Read output with timeout
    let mut expected_lines = 0;
    let start_time = std::time::Instant::now();
    let timeout_duration = Duration::from_secs(10);
    let mut process_exited = false;
    let mut exit_status = None;

    loop {
        // Check timeout
        if start_time.elapsed() > timeout_duration {
            break; // Timeout reached
        }

        let timeout_future = smol::Timer::after(timeout);
        futures::select! {
            result = stdout_reader.read_line(&mut stdout_line).fuse() => {
                match result {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        stdout_output.push_str(&stdout_line);
                        // Check if we got expected outputs (z = 4 and z = 6)
                        if stdout_line.contains("4") || stdout_line.contains("6") {
                            expected_lines += 1;
                            if expected_lines >= 2 {
                                break; // Got both expected outputs
                            }
                        }
                        stdout_line.clear();
                    }
                    Err(e) => return Err(e),
                }
            }
            result = stderr_reader.read_line(&mut stderr_line).fuse() => {
                match result {
                    Ok(0) => {}, // EOF on stderr
                    Ok(_) => {
                        stderr_output.push_str(&stderr_line);
                        stderr_line.clear();
                    }
                    Err(_) => {}, // Ignore stderr read errors
                }
            }
            _ = futures::FutureExt::fuse(timeout_future) => {
                // Check if process has exited
                if let Ok(status) = child.try_status() {
                    if let Some(exit_status_val) = status {
                        exit_status = Some(exit_status_val);
                        process_exited = true;
                        break;
                    }
                }
                // Short timeout to avoid blocking, continue loop
            }
        }

        if process_exited {
            break;
        }
    }

    // Terminate the child process if it hasn't exited naturally
    if !process_exited {
        if let Err(_) = child.kill() {
            // If kill fails, try to wait for natural termination
        }
        if let Ok(status) = child.status().await {
            exit_status = Some(status);
        }
    }

    Ok((stdout_output, stderr_output, exit_status))
}

/// Helper function to get fixture file path
fn fixture_path(filename: &str) -> String {
    format!("tests/fixtures/{}", filename)
}

/// Test simple addition with typed inputs
#[apply(async_test)]
async fn test_simple_add_typed() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_simple_add_typed_no_stdout() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
    ])
    .await
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
#[apply(async_test)]
async fn test_counter() {
    let output = run_cli(&[
        &fixture_path("counter.lola"),
        "--input-file",
        &fixture_path("counter.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_string_concat() {
    let output = run_cli(&[
        &fixture_path("string_concat.lola"),
        "--input-file",
        &fixture_path("string_concat.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_float_arithmetic() {
    let output = run_cli(&[
        &fixture_path("float_arithmetic.lola"),
        "--input-file",
        &fixture_path("float_arithmetic.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_if_else() {
    let output = run_cli(&[
        &fixture_path("if_else.lola"),
        "--input-file",
        &fixture_path("if_else.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_past_indexing() {
    let output = run_cli(&[
        &fixture_path("past_indexing.lola"),
        "--input-file",
        &fixture_path("past_indexing.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_invalid_model_file() {
    let output = run_cli(&[
        "nonexistent_model.lola",
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_invalid_input_file() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        "nonexistent_input.input",
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_malformed_input() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("malformed.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_combinator_parser() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--parser-mode",
        "combinator",
    ])
    .await
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
#[apply(async_test)]
async fn test_lola_language() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--language",
        "lola",
    ])
    .await
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
#[apply(async_test)]
async fn test_dynsrv_language() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--language",
        "dynsrv",
    ])
    .await
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
#[apply(async_test)]
async fn test_centralised_mode() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--centralised",
    ])
    .await
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
#[apply(async_test)]
async fn test_empty_input(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("empty.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_single_timestep(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("single_timestep.input"),
        "--output-stdout",
    ])
    .await
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
#[apply(async_test)]
async fn test_help_output(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&["--help"]).await.expect("Failed to run CLI");

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
#[apply(async_test)]
async fn test_missing_required_args() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"), // Missing input mode
    ])
    .await
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

/// Test MQTT connection on the cli
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
async fn test_add_monitor_mqtt_input_cli(executor: Rc<LocalExecutor>) {
    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];

    let mqtt_server = start_mqtt().await;
    let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
        .await
        .expect("Failed to get host port for MQTT server");
    let cli_timeout = Duration::from_secs(3);

    // Start CLI process with streaming output capture
    let args = vec![
        fixture_path("simple_add_typed.lola"),
        "--mqtt-input".to_string(),
        "--mqtt-port".to_string(),
        format!("{}", mqtt_port),
        "--output-stdout".to_string(),
    ];
    let cli_task = executor.spawn(async move {
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_cli_streaming(&args_refs, cli_timeout).await
    });

    // Wait for CLI to start and subscribe to MQTT topics
    smol::Timer::after(Duration::from_millis(150)).await;

    // Now start publishers to send data to the waiting CLI
    let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "x_publisher".to_string(),
        "x".to_string(),
        xs,
        mqtt_port,
    ));

    let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "y_publisher".to_string(),
        "y".to_string(),
        ys,
        mqtt_port,
    ));

    // Wait for publishers to complete
    x_publisher_task.await;
    y_publisher_task.await;

    // Give CLI additional time to process the messages
    smol::Timer::after(Duration::from_millis(150)).await;

    // Wait for CLI to capture output or timeout
    let (stdout, stderr, exit_status) = cli_task.await.expect("Failed to run CLI streaming");

    if let Some(status) = exit_status {
        if !(status.success() || status.signal().is_some()) {
            panic!(
                "CLI failed with exit status: {:?}, stderr: '{}'",
                status, stderr
            );
        }
    }

    // Expected output: z = 4, 6 (from adding x + y for each timestep)
    assert!(
        stdout.contains("4"),
        "Expected output '4' not found in: '{}'",
        stdout
    );
    assert!(
        stdout.contains("6"),
        "Expected output '6' not found in: '{}'",
        stdout
    );
}

/// Test Redis connection on the cli
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
#[ignore = "Test failing since at least commit c4b7721fafeb853b7ada6ff16729e932b8c400e9 when running with --all-features"]
async fn test_add_monitor_redis_input_cli(executor: Rc<LocalExecutor>) {
    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];

    let redis_server = start_redis().await;
    let redis_port = TokioCompat::new(redis_server.get_host_port_ipv4(6379))
        .await
        .expect("Failed to get host port for Redis server");
    let cli_timeout = Duration::from_secs(3);

    // Start CLI process with streaming output capture
    let args = vec![
        fixture_path("simple_add_typed.lola"),
        "--redis-input".to_string(),
        "--redis-port".to_string(),
        format!("{}", redis_port),
        "--output-stdout".to_string(),
    ];
    let cli_task = executor.spawn(async move {
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_cli_streaming(&args_refs, cli_timeout).await
    });

    // Wait for CLI to start and subscribe to Redis channels
    // smol::Timer::after(Duration::from_millis(5)).await;

    // Now start publishers to send data to the waiting CLI
    let ready_rx1 = Box::pin(futures::FutureExt::map(
        smol::Timer::after(Duration::from_millis(50)),
        |_| (),
    ));
    let ready_rx2 = Box::pin(futures::FutureExt::map(
        smol::Timer::after(Duration::from_millis(50)),
        |_| (),
    ));
    let x_publisher_task = executor.spawn(dummy_redis_sender(
        REDIS_HOSTNAME,
        Some(redis_port),
        // "x_publisher".to_string(),
        "x".to_string(),
        xs,
        ready_rx1,
    ));

    let y_publisher_task = executor.spawn(dummy_redis_sender(
        REDIS_HOSTNAME,
        Some(redis_port),
        // "y_publisher".to_string(),
        "y".to_string(),
        ys,
        ready_rx2,
    ));

    // Wait for publishers to complete
    x_publisher_task.await.unwrap();
    y_publisher_task.await.unwrap();

    // Wait for CLI to capture output or timeout
    let (stdout, stderr, exit_status) = cli_task.await.expect("Failed to run CLI streaming");

    if let Some(status) = exit_status {
        if !(status.success() || status.signal().is_some()) {
            panic!(
                "CLI failed with exit status: {:?}, stderr: '{}'",
                status, stderr
            );
        }
    }

    // Expected output: z = 4, 6 (from adding x + y for each timestep)
    assert!(
        stdout.contains("4"),
        "Expected output '4' not found in: '{}'",
        stdout
    );
    assert!(
        stdout.contains("6"),
        "Expected output '6' not found in: '{}'",
        stdout
    );
}

/// Test file input with Redis output
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
async fn test_file_input_redis_output(executor: Rc<LocalExecutor>) {
    let redis_server = start_redis().await;
    let redis_port = TokioCompat::new(redis_server.get_host_port_ipv4(6379))
        .await
        .expect("Failed to get host port for Redis server");
    let cli_timeout = Duration::from_secs(5);

    // Set up Redis receiver to capture output
    let ready_channel = oneshot::channel();
    let (ready_tx, ready_rx) = ready_channel.into_split();

    let mut receiver_outputs = dummy_redis_receiver(
        executor.clone(),
        REDIS_HOSTNAME,
        Some(redis_port),
        vec!["z".to_string()], // The output variable from simple_add_typed.lola
        ready_tx,
    )
    .await
    .expect("Failed to create Redis receiver");

    // Wait for receiver to be ready
    ready_rx.await.unwrap();

    // Start CLI process with file input and Redis output
    let args = vec![
        fixture_path("simple_add_typed.lola"),
        "--input-file".to_string(),
        fixture_path("simple_add_typed.input"),
        "--redis-output".to_string(),
        "--redis-port".to_string(),
        format!("{}", redis_port),
    ];

    let cli_task = executor.spawn(async move {
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_cli_streaming(&args_refs, cli_timeout).await
    });

    // Wait for CLI to process the file and output to Redis
    let (_stdout, stderr, exit_status) = cli_task.await.expect("Failed to run CLI streaming");

    // Check that CLI completed successfully
    if let Some(status) = exit_status {
        if !(status.success() || status.signal().is_some()) {
            panic!(
                "CLI failed with exit status: {:?}, stderr: '{}'",
                status, stderr
            );
        }
    }

    // Capture output from Redis
    if let Some(mut stream) = receiver_outputs.pop() {
        let mut results = Vec::new();

        // Collect messages with timeout
        for _ in 0..3 {
            // Expect 3 timesteps from simple_add_typed.input
            let timeout = smol::Timer::after(Duration::from_millis(1000));
            futures::select! {
                value = stream.next().fuse() => {
                    if let Some(val) = value {
                        results.push(val);
                    } else {
                        break;
                    }
                }
                _ = futures::FutureExt::fuse(timeout) => {
                    break; // Timeout reached
                }
            }
        }

        // Verify the expected results: x + y for each timestep
        // simple_add_typed.input has: (1,2), (3,4), (5,6) -> expected z: 3, 7, 11
        assert_eq!(
            results.len(),
            3,
            "Expected 3 output values, got {}",
            results.len()
        );
        assert_eq!(results[0], Value::Int(3), "First result should be 3");
        assert_eq!(results[1], Value::Int(7), "Second result should be 7");
        assert_eq!(results[2], Value::Int(11), "Third result should be 11");
    } else {
        panic!("No Redis output stream found");
    }
}

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
async fn test_redis_input_output_specific_topics(executor: Rc<LocalExecutor>) {
    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];

    let redis_server = start_redis().await;
    let redis_port = TokioCompat::new(redis_server.get_host_port_ipv4(6379))
        .await
        .expect("Failed to get host port for Redis server");
    let cli_timeout = Duration::from_secs(3);

    // Start CLI process with Redis input and output
    let args = vec![
        fixture_path("simple_add_typed.lola"),
        "--input-redis-topics".to_string(),
        "x".to_string(),
        "y".to_string(),
        "--output-redis-topics".to_string(),
        "z".to_string(),
        "--redis-port".to_string(),
        format!("{}", redis_port),
    ];

    let cli_task = executor.spawn(async move {
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_cli_streaming(&args_refs, cli_timeout).await
    });

    // Wait for CLI to start and subscribe to Redis channels
    smol::Timer::after(Duration::from_millis(150)).await;

    // Now start publishers to send data to the waiting CLI
    let ready_rx1 = Box::pin(futures::FutureExt::map(
        smol::Timer::after(Duration::from_millis(50)),
        |_| (),
    ));
    let ready_rx2 = Box::pin(futures::FutureExt::map(
        smol::Timer::after(Duration::from_millis(50)),
        |_| (),
    ));

    let x_publisher_task = executor.spawn(dummy_redis_sender(
        REDIS_HOSTNAME,
        Some(redis_port),
        "x".to_string(),
        xs,
        ready_rx1,
    ));

    let y_publisher_task = executor.spawn(dummy_redis_sender(
        REDIS_HOSTNAME,
        Some(redis_port),
        "y".to_string(),
        ys,
        ready_rx2,
    ));

    // Wait for publishers to complete
    x_publisher_task.await.unwrap();
    y_publisher_task.await.unwrap();

    // Give CLI additional time to process the messages
    smol::Timer::after(Duration::from_millis(150)).await;

    // Wait for CLI to capture output or timeout
    let (_stdout, stderr, exit_status) = cli_task.await.expect("Failed to run CLI streaming");

    // Check that CLI completed successfully
    if let Some(status) = exit_status {
        if !(status.success() || status.signal().is_some()) {
            panic!(
                "CLI failed with exit status: {:?}, stderr: '{}'",
                status, stderr
            );
        }
    }
}

#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
async fn test_mqtt_input_output_specific_topics(executor: Rc<LocalExecutor>) {
    let xs = vec![Value::Int(1), Value::Int(2)];
    let ys = vec![Value::Int(3), Value::Int(4)];

    let mqtt_server = start_mqtt().await;
    let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
        .await
        .expect("Failed to get host port for MQTT server");
    let cli_timeout = Duration::from_secs(3);

    // Start CLI process with MQTT input and output
    let args = vec![
        fixture_path("simple_add_typed.lola"),
        "--input-mqtt-topics".to_string(),
        "x".to_string(),
        "y".to_string(),
        "--output-mqtt-topics".to_string(),
        "z".to_string(),
        "--mqtt-port".to_string(),
        format!("{}", mqtt_port),
    ];

    let cli_task = executor.spawn(async move {
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_cli_streaming(&args_refs, cli_timeout).await
    });

    // Wait for CLI to start and subscribe to MQTT topics
    smol::Timer::after(Duration::from_millis(150)).await;

    // Now start publishers to send data to the waiting CLI
    let x_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "x_publisher".to_string(),
        "x".to_string(),
        xs,
        mqtt_port,
    ));

    let y_publisher_task = executor.spawn(dummy_mqtt_publisher(
        "y_publisher".to_string(),
        "y".to_string(),
        ys,
        mqtt_port,
    ));

    // Wait for publishers to complete
    x_publisher_task.await;
    y_publisher_task.await;

    // Give CLI additional time to process the messages
    smol::Timer::after(Duration::from_millis(150)).await;

    // Wait for CLI to capture output or timeout
    let (_stdout, stderr, exit_status) = cli_task.await.expect("Failed to run CLI streaming");

    // Check that CLI completed successfully
    if let Some(status) = exit_status {
        if !(status.success() || status.signal().is_some()) {
            panic!(
                "CLI failed with exit status: {:?}, stderr: '{}'",
                status, stderr
            );
        }
    }
}

/// Test file input with MQTT output
#[cfg_attr(not(feature = "testcontainers"), ignore)]
#[apply(async_test)]
async fn test_file_input_mqtt_output(executor: Rc<LocalExecutor>) {
    let mqtt_server = start_mqtt().await;
    let mqtt_port = TokioCompat::new(mqtt_server.get_host_port_ipv4(1883))
        .await
        .expect("Failed to get host port for MQTT server");
    let cli_timeout = Duration::from_secs(5);

    // Create MQTT client for receiving
    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(format!("tcp://{}:{}", MQTT_HOSTNAME, mqtt_port))
        .client_id("test_receiver".to_string())
        .finalize();

    let connect_opts = mqtt::ConnectOptionsBuilder::new_v3()
        .keep_alive_interval(Duration::from_secs(30))
        .clean_session(true)
        .finalize();

    let mut client = mqtt::AsyncClient::new(create_opts).unwrap();
    let mut stream = client.get_stream(10);

    client.connect(connect_opts).await.unwrap();
    client.subscribe("z", 1).await.unwrap();

    // Give MQTT subscriber time to fully connect and subscribe
    smol::Timer::after(Duration::from_millis(1000)).await;

    // Start CLI process with file input and MQTT output
    let args = vec![
        fixture_path("simple_add_typed.lola"),
        "--input-file".to_string(),
        fixture_path("simple_add_typed.input"),
        "--mqtt-output".to_string(),
        "--mqtt-port".to_string(),
        format!("{}", mqtt_port),
    ];

    let cli_task = executor.spawn(async move {
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        run_cli_streaming(&args_refs, cli_timeout).await
    });

    // Wait for CLI to complete
    let (_stdout, stderr, exit_status) = cli_task.await.expect("Failed to run CLI streaming");

    // Check that CLI completed successfully
    if let Some(status) = exit_status {
        if !(status.success() || status.signal().is_some()) {
            panic!(
                "CLI failed with exit status: {:?}, stderr: '{}'",
                status, stderr
            );
        }
    }

    // Capture output from MQTT with timeout
    let mut results = Vec::new();

    // Collect messages with individual timeouts
    for i in 0..3 {
        let timeout = smol::Timer::after(Duration::from_millis(5000)); // Longer timeout
        futures::select! {
            msg_opt = stream.next().fuse() => {
                if let Some(msg_result) = msg_opt {
                    if let Some(msg) = msg_result {
                        let payload = msg.payload_str();

                        // Try to deserialize the JSON
                        match serde_json::from_str::<Value>(&payload) {
                            Ok(val) => {
                                results.push(val);
                            }
                            Err(_) => {
                                // Skip malformed messages
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            _ = futures::FutureExt::fuse(timeout) => {
                println!("Timeout waiting for MQTT message {}", i + 1);
                break;
            }
        }
    }

    // Verify the expected results: x + y for each timestep
    // simple_add_typed.input has: (1,2), (3,4), (5,6) -> expected z: 3, 7, 11
    assert_eq!(
        results.len(),
        3,
        "Expected 3 output values, got {}",
        results.len()
    );
    assert_eq!(results[0], Value::Int(3), "First result should be 3");
    assert_eq!(results[1], Value::Int(7), "Second result should be 7");
    assert_eq!(results[2], Value::Int(11), "Third result should be 11");
}

#[apply(async_test)]
async fn test_distribution_graph_with_local_node(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--distribution-graph",
        &fixture_path("simple_add_distribution_graph.json"),
        "--local-node",
        "A",
    ])
    .await
    .expect("Failed to run CLI");

    // Should succeed with proper arguments
    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_distribution_graph_missing_local_node(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--distribution-graph",
        &fixture_path("simple_add_distribution_graph.json"),
    ])
    .await
    .expect("Failed to run CLI");

    // Should fail because --distribution-graph requires --local-node
    assert!(
        !output.status.success(),
        "CLI command should have failed due to missing --local-node"
    );
}

#[apply(async_test)]
async fn test_local_topics_mode(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--local-topics",
        "topic1",
        "--local-topics",
        "topic2",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_mqtt_centralised_distributed_mode(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-centralised-distributed",
        "node1",
        "node2",
        "node3",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_mqtt_randomized_distributed_mode(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-randomized-distributed",
        "node1",
        "node2",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_mqtt_static_optimized_with_constraints(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-static-optimized",
        "node1",
        "node2",
        "--distribution-constraints",
        "constraint1",
        "constraint2",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_mqtt_static_optimized_missing_constraints(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-static-optimized",
        "node1",
        "node2",
    ])
    .await
    .expect("Failed to run CLI");

    // Should fail because --mqtt-static-optimized requires --distribution-constraints
    assert!(
        !output.status.success(),
        "CLI command should have failed due to missing --distribution-constraints"
    );
}

#[apply(async_test)]
async fn test_mqtt_dynamic_optimized_with_constraints(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-dynamic-optimized",
        "node1",
        "node2",
        "--distribution-constraints",
        "constraint1",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_mqtt_dynamic_optimized_missing_constraints(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-dynamic-optimized",
        "node1",
    ])
    .await
    .expect("Failed to run CLI");

    // Should fail because --mqtt-dynamic-optimized requires --distribution-constraints
    assert!(
        !output.status.success(),
        "CLI command should have failed due to missing --distribution-constraints"
    );
}

#[apply(async_test)]
async fn test_distributed_work_with_local_node(_executor: Rc<LocalExecutor>) {
    // Use streaming version since --distributed-work waits indefinitely for work assignment
    let (_stdout, stderr, exit_status) = run_cli_streaming(
        &[
            &fixture_path("simple_add_typed.lola"),
            "--input-file",
            &fixture_path("simple_add_typed.input"),
            "--output-stdout",
            "--distributed-work",
            "--local-node",
            "worker1",
        ],
        Duration::from_secs(2),
    )
    .await
    .expect("Failed to run CLI streaming");

    // Process should start successfully (no immediate error)
    // It will be terminated by timeout since it waits for work assignment
    if let Some(status) = exit_status {
        // If process exited naturally, it should not be due to argument parsing errors
        if !status.success() {
            // Check if it's not an argument parsing error
            assert!(
                !stderr.contains("error:")
                    || !stderr.contains("required")
                    || !stderr.contains("argument"),
                "CLI argument parsing failed: {}",
                stderr
            );
        }
    }
    // If no exit status, the process was terminated due to timeout (expected behavior)
}

#[apply(async_test)]
async fn test_distributed_work_missing_local_node(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--distributed-work",
    ])
    .await
    .expect("Failed to run CLI");

    // Should fail because --distributed-work requires --local-node
    assert!(
        !output.status.success(),
        "CLI command should have failed due to missing --local-node"
    );
}

#[apply(async_test)]
async fn test_scheduling_mode_mock(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--scheduling-mode",
        "mock",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_scheduling_mode_mqtt(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--scheduling-mode",
        "mqtt",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_scheduling_mode_invalid(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--scheduling-mode",
        "invalid",
    ])
    .await
    .expect("Failed to run CLI");

    // Should fail due to invalid scheduling mode
    assert!(
        !output.status.success(),
        "CLI command should have failed due to invalid scheduling mode"
    );
}

#[apply(async_test)]
async fn test_distribution_constraints_standalone(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--distribution-constraints",
        "memory<1GB",
        "cpu<50%",
        "network<10Mbps",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_mqtt_port_configuration(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-port",
        "1884",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_redis_port_configuration(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--redis-port",
        "6380",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_multiple_distribution_modes_conflict(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-centralised-distributed",
        "node1",
        "--mqtt-randomized-distributed",
        "node2",
    ])
    .await
    .expect("Failed to run CLI");

    // Should fail because multiple distribution modes are conflicting
    assert!(
        !output.status.success(),
        "CLI command should have failed due to conflicting distribution modes"
    );
}

#[apply(async_test)]
async fn test_complex_distributed_configuration(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--distribution-graph",
        &fixture_path("simple_add_distribution_graph.json"),
        "--local-node",
        "A",
        "--scheduling-mode",
        "mqtt",
        "--mqtt-port",
        "1885",
        "--distribution-constraints",
        "x",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_default_centralised_mode(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        // No distribution mode specified - should default to centralised
    ])
    .await
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
}

#[apply(async_test)]
async fn test_explicit_centralised_mode(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--centralised",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("7"),
        "Expected output '7' not found in: {}",
        stdout
    );
}

#[apply(async_test)]
async fn test_async_runtime_with_distribution(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--runtime",
        "async",
        "--distribution-graph",
        &fixture_path("simple_add_distribution_graph.json"),
        "--local-node",
        "A",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_async_runtime_default_with_mqtt_distributed(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--mqtt-centralised-distributed",
        "node1",
        "node2",
        // No --runtime specified, should default to async
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[apply(async_test)]
async fn test_runtime_async(_executor: Rc<LocalExecutor>) {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--runtime",
        "async",
    ])
    .await
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
}

#[apply(async_test)]
async fn test_runtime_constraints() {
    let output = run_cli(&[
        &fixture_path("simple_add_typed.lola"),
        "--input-file",
        &fixture_path("simple_add_typed.input"),
        "--output-stdout",
        "--runtime",
        "constraints",
    ])
    .await
    .expect("Failed to run CLI");

    assert!(
        output.status.success(),
        "CLI command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

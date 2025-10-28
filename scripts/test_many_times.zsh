#!/usr/bin/env zsh
set -e  # Exit immediately on any command failure
set -u  # Treat unset variables as an error
set -o pipefail  # Propagate errors in pipes

# === Configuration ===
TEST_NAME=${1:-""} # Default to all tests if not specified
RUNS=${2:-10}  # Default to 10 runs if not specified

echo "Running test '$TEST_NAME' $RUNS times..."
echo "--------------------------------------------"

for i in $(seq 1 $RUNS); do
    echo "🔁 Run #$i of $RUNS"
    cargo test "$TEST_NAME" -- --nocapture
    echo "✅ Run #$i completed successfully."
    echo "--------------------------------------------"
done

echo "🎉 All $RUNS runs of '$TEST_NAME' passed successfully."


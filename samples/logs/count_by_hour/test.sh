#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$PROJECT_ROOT"

# Run the pipeline and capture output
echo "Running log count by hour..."
OUTPUT=$(go run "$SCRIPT_DIR"/*.go)

echo "$OUTPUT"
echo ""

# Verify counts
echo "Verifying output..."

# Check 08:00 hour has 3 logs
if ! echo "$OUTPUT" | grep -q "2024-01-15 08:00 |  3"; then
    echo "FAIL: Expected 3 logs at 08:00"
    exit 1
fi

# Check 09:00 hour has 2 logs
if ! echo "$OUTPUT" | grep -q "2024-01-15 09:00 |  2"; then
    echo "FAIL: Expected 2 logs at 09:00"
    exit 1
fi

# Check 10:00 hour has 4 logs
if ! echo "$OUTPUT" | grep -q "2024-01-15 10:00 |  4"; then
    echo "FAIL: Expected 4 logs at 10:00"
    exit 1
fi

# Check 11:00 hour has 1 log
if ! echo "$OUTPUT" | grep -q "2024-01-15 11:00 |  1"; then
    echo "FAIL: Expected 1 log at 11:00"
    exit 1
fi

# Check total
if ! echo "$OUTPUT" | grep -q "Total: 10 logs"; then
    echo "FAIL: Expected total of 10 logs"
    exit 1
fi

echo "PASS: All validations successful"

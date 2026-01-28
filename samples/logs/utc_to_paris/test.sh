#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT_FILE="$SCRIPT_DIR/logs_paris.json"

cd "$PROJECT_ROOT"

# Clean previous output
rm -f "$OUTPUT_FILE"

# Run the pipeline
echo "Running UTC to Paris timezone conversion..."
go run "$SCRIPT_DIR"/*.go

# Verify output file exists
if [[ ! -f "$OUTPUT_FILE" ]]; then
    echo "FAIL: Output file not created"
    exit 1
fi

# Verify content
echo "Verifying output..."

# Check log count
LOG_COUNT=$(jq 'length' "$OUTPUT_FILE")
if [[ "$LOG_COUNT" != "4" ]]; then
    echo "FAIL: Expected 4 logs, got $LOG_COUNT"
    exit 1
fi

# Check first log: 2024-01-15T08:30:00Z -> 2024-01-15T09:30:00+01:00 (winter time UTC+1)
TIMESTAMP_1=$(jq -r '.[0].timestamp' "$OUTPUT_FILE")
if [[ "$TIMESTAMP_1" != "2024-01-15T09:30:00+01:00" ]]; then
    echo "FAIL: Expected timestamp '2024-01-15T09:30:00+01:00', got '$TIMESTAMP_1'"
    exit 1
fi

# Check second log: 2024-01-15T12:45:00Z -> 2024-01-15T13:45:00+01:00
TIMESTAMP_2=$(jq -r '.[1].timestamp' "$OUTPUT_FILE")
if [[ "$TIMESTAMP_2" != "2024-01-15T13:45:00+01:00" ]]; then
    echo "FAIL: Expected timestamp '2024-01-15T13:45:00+01:00', got '$TIMESTAMP_2'"
    exit 1
fi

# Check third log: 2024-01-15T23:15:00Z -> 2024-01-16T00:15:00+01:00 (day change!)
TIMESTAMP_3=$(jq -r '.[2].timestamp' "$OUTPUT_FILE")
if [[ "$TIMESTAMP_3" != "2024-01-16T00:15:00+01:00" ]]; then
    echo "FAIL: Expected timestamp '2024-01-16T00:15:00+01:00', got '$TIMESTAMP_3'"
    exit 1
fi

# Check fourth log: 2024-07-20T14:00:00Z -> 2024-07-20T16:00:00+02:00 (summer time UTC+2)
TIMESTAMP_4=$(jq -r '.[3].timestamp' "$OUTPUT_FILE")
if [[ "$TIMESTAMP_4" != "2024-07-20T16:00:00+02:00" ]]; then
    echo "FAIL: Expected timestamp '2024-07-20T16:00:00+02:00', got '$TIMESTAMP_4'"
    exit 1
fi

# Check that other fields are preserved
LEVEL=$(jq -r '.[0].level' "$OUTPUT_FILE")
if [[ "$LEVEL" != "INFO" ]]; then
    echo "FAIL: Expected level='INFO', got '$LEVEL'"
    exit 1
fi

MESSAGE=$(jq -r '.[0].message' "$OUTPUT_FILE")
if [[ "$MESSAGE" != "Server started" ]]; then
    echo "FAIL: Expected message='Server started', got '$MESSAGE'"
    exit 1
fi

echo "PASS: All validations successful"
echo ""
echo "Conversions verified:"
echo "  - Winter time (Jan): UTC+1"
echo "  - Summer time (Jul): UTC+2"
echo "  - Day change handled correctly"

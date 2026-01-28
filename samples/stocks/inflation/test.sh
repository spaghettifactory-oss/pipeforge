#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT_FILE="$SCRIPT_DIR/products_inflated.json"

cd "$PROJECT_ROOT"

# Clean previous output
rm -f "$OUTPUT_FILE"

# Run the pipeline
echo "Running inflation pipeline..."
go run "$SCRIPT_DIR"/*.go

# Verify output file exists
if [[ ! -f "$OUTPUT_FILE" ]]; then
    echo "FAIL: Output file not created"
    exit 1
fi

# Verify content
echo "Verifying output..."

# Check Laptop price (999 * 3 = 2997)
LAPTOP_PRICE=$(jq '.[0].pricing' "$OUTPUT_FILE")
if [[ "$LAPTOP_PRICE" != "2997" ]]; then
    echo "FAIL: Expected Laptop pricing=2997, got $LAPTOP_PRICE"
    exit 1
fi

# Check Phone price (499 * 3 = 1497)
PHONE_PRICE=$(jq '.[1].pricing' "$OUTPUT_FILE")
if [[ "$PHONE_PRICE" != "1497" ]]; then
    echo "FAIL: Expected Phone pricing=1497, got $PHONE_PRICE"
    exit 1
fi

# Check record count
COUNT=$(jq 'length' "$OUTPUT_FILE")
if [[ "$COUNT" != "5" ]]; then
    echo "FAIL: Expected 5 records, got $COUNT"
    exit 1
fi

echo "PASS: All validations successful"

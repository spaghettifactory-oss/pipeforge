#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT_FILE="$SCRIPT_DIR/store_inflated.json"

cd "$PROJECT_ROOT"

# Clean previous output
rm -f "$OUTPUT_FILE"

# Run the pipeline
echo "Running inflation pipeline (complex object)..."
go run "$SCRIPT_DIR"/*.go

# Verify output file exists
if [[ ! -f "$OUTPUT_FILE" ]]; then
    echo "FAIL: Output file not created"
    exit 1
fi

# Verify content
echo "Verifying output..."

# Check store count
STORE_COUNT=$(jq 'length' "$OUTPUT_FILE")
if [[ "$STORE_COUNT" != "2" ]]; then
    echo "FAIL: Expected 2 stores, got $STORE_COUNT"
    exit 1
fi

# Check first store name
STORE_NAME=$(jq -r '.[0].store_name' "$OUTPUT_FILE")
if [[ "$STORE_NAME" != "Tech Shop Paris" ]]; then
    echo "FAIL: Expected store_name='Tech Shop Paris', got '$STORE_NAME'"
    exit 1
fi

# Check Laptop price in first store (999 * 3 = 2997)
LAPTOP_PRICE=$(jq '.[0].stock[0].pricing' "$OUTPUT_FILE")
if [[ "$LAPTOP_PRICE" != "2997" ]]; then
    echo "FAIL: Expected Laptop pricing=2997, got $LAPTOP_PRICE"
    exit 1
fi

# Check Phone price in first store (499 * 3 = 1497)
PHONE_PRICE=$(jq '.[0].stock[1].pricing' "$OUTPUT_FILE")
if [[ "$PHONE_PRICE" != "1497" ]]; then
    echo "FAIL: Expected Phone pricing=1497, got $PHONE_PRICE"
    exit 1
fi

# Check Monitor price in second store (299 * 3 = 897)
MONITOR_PRICE=$(jq '.[1].stock[0].pricing' "$OUTPUT_FILE")
if [[ "$MONITOR_PRICE" != "897" ]]; then
    echo "FAIL: Expected Monitor pricing=897, got $MONITOR_PRICE"
    exit 1
fi

# Check stock count in first store
STOCK_COUNT=$(jq '.[0].stock | length' "$OUTPUT_FILE")
if [[ "$STOCK_COUNT" != "3" ]]; then
    echo "FAIL: Expected 3 items in first store, got $STOCK_COUNT"
    exit 1
fi

echo "PASS: All validations successful"

#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT_FILE="$SCRIPT_DIR/products_fixed.json"

cd "$PROJECT_ROOT"

# Clean previous output
rm -f "$OUTPUT_FILE"

# Run the pipeline
echo "Running fix negative stock pipeline..."
go run "$SCRIPT_DIR"/*.go

# Verify output file exists
if [[ ! -f "$OUTPUT_FILE" ]]; then
    echo "FAIL: Output file not created"
    exit 1
fi

# Verify content
echo "Verifying output..."

# Check product count (all products kept)
COUNT=$(jq 'length' "$OUTPUT_FILE")
if [[ "$COUNT" != "5" ]]; then
    echo "FAIL: Expected 5 products, got $COUNT"
    exit 1
fi

# Check Laptop stock unchanged (15 -> 15)
LAPTOP_STOCK=$(jq '.[0].stock' "$OUTPUT_FILE")
if [[ "$LAPTOP_STOCK" != "15" ]]; then
    echo "FAIL: Expected Laptop stock=15, got $LAPTOP_STOCK"
    exit 1
fi

# Check Phone stock fixed (-3 -> 0)
PHONE_STOCK=$(jq '.[1].stock' "$OUTPUT_FILE")
if [[ "$PHONE_STOCK" != "0" ]]; then
    echo "FAIL: Expected Phone stock=0 (was -3), got $PHONE_STOCK"
    exit 1
fi

# Check Tablet stock unchanged (0 -> 0)
TABLET_STOCK=$(jq '.[2].stock' "$OUTPUT_FILE")
if [[ "$TABLET_STOCK" != "0" ]]; then
    echo "FAIL: Expected Tablet stock=0, got $TABLET_STOCK"
    exit 1
fi

# Check Monitor stock fixed (-10 -> 0)
MONITOR_STOCK=$(jq '.[3].stock' "$OUTPUT_FILE")
if [[ "$MONITOR_STOCK" != "0" ]]; then
    echo "FAIL: Expected Monitor stock=0 (was -10), got $MONITOR_STOCK"
    exit 1
fi

# Check Keyboard stock unchanged (50 -> 50)
KEYBOARD_STOCK=$(jq '.[4].stock' "$OUTPUT_FILE")
if [[ "$KEYBOARD_STOCK" != "50" ]]; then
    echo "FAIL: Expected Keyboard stock=50, got $KEYBOARD_STOCK"
    exit 1
fi

# Check prices unchanged
PHONE_PRICE=$(jq '.[1].price' "$OUTPUT_FILE")
if [[ "$PHONE_PRICE" != "499" ]]; then
    echo "FAIL: Expected Phone price=499, got $PHONE_PRICE"
    exit 1
fi

echo "PASS: All validations successful"
echo ""
echo "Fixed:"
echo "  - Phone: -3 -> 0"
echo "  - Monitor: -10 -> 0"

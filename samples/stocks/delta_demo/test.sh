#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$PROJECT_ROOT"

# Run the sample and capture output
echo "Running delta_demo..."
OUTPUT=$(go run "$SCRIPT_DIR"/*.go)

# Verify original data section
echo "Verifying original data..."

if ! echo "$OUTPUT" | grep -q "=== ORIGINAL DATA ==="; then
    echo "FAIL: Missing ORIGINAL DATA section"
    exit 1
fi

if ! echo "$OUTPUT" | grep -q "Tech Shop Paris"; then
    echo "FAIL: Missing Tech Shop Paris"
    exit 1
fi

if ! echo "$OUTPUT" | grep -q "Tech Shop Lyon"; then
    echo "FAIL: Missing Tech Shop Lyon"
    exit 1
fi

if ! echo "$OUTPUT" | grep -q "Tech Shop Bordeaux"; then
    echo "FAIL: Missing Tech Shop Bordeaux"
    exit 1
fi

if ! echo "$OUTPUT" | grep -q "Laptop: 999 EUR"; then
    echo "FAIL: Missing original Laptop pricing"
    exit 1
fi

# Verify transformed data section (filtered > 100, then x3)
echo "Verifying transformed data..."

if ! echo "$OUTPUT" | grep -q "=== TRANSFORMED DATA"; then
    echo "FAIL: Missing TRANSFORMED DATA section"
    exit 1
fi

# Laptop: 999 > 100, so 999 * 3 = 2997
if ! echo "$OUTPUT" | grep -q "Laptop: 2997 EUR"; then
    echo "FAIL: Expected transformed Laptop pricing=2997"
    exit 1
fi

# Phone was deleted from stock
# Verify SSD was added to Paris stock
if ! echo "$OUTPUT" | grep -q "SSD: 199 EUR"; then
    echo "FAIL: Expected SSD added to Paris stock"
    exit 1
fi

# Verify Marseille was added
if ! echo "$OUTPUT" | grep -q "Tech Shop Marseille"; then
    echo "FAIL: Missing added store Tech Shop Marseille"
    exit 1
fi

if ! echo "$OUTPUT" | grep -q "Camera: 599 EUR"; then
    echo "FAIL: Missing Camera in Marseille store"
    exit 1
fi

if ! echo "$OUTPUT" | grep -q "Drone: 899 EUR"; then
    echo "FAIL: Missing Drone in Marseille store"
    exit 1
fi

# Verify delta comparison section
echo "Verifying delta comparison..."

if ! echo "$OUTPUT" | grep -q "=== DELTA COMPARISON ==="; then
    echo "FAIL: Missing DELTA COMPARISON section"
    exit 1
fi

# Verify summary shows Modified and Deleted
if ! echo "$OUTPUT" | grep -q "Modified:2"; then
    echo "FAIL: Expected Modified:2 in delta summary"
    exit 1
fi

if ! echo "$OUTPUT" | grep -q "Deleted:1"; then
    echo "FAIL: Expected Deleted:1 in delta summary"
    exit 1
fi

# Verify Bordeaux was deleted
if ! echo "$OUTPUT" | grep -q "Tech Shop Bordeaux: deleted"; then
    echo "FAIL: Expected Bordeaux to be deleted"
    exit 1
fi

# Verify key-based matching section
if ! echo "$OUTPUT" | grep -q "=== DELTA WITH KEY-BASED MATCHING"; then
    echo "FAIL: Missing KEY-BASED MATCHING section"
    exit 1
fi

echo "PASS: All validations successful"

# Data Pipeline

A lightweight, type-safe data pipeline library for Go built on hexagonal architecture principles.

## Features

- **Type-safe records** with schema validation
- **Functional operations** (Map, Filter, Reduce) on record sets
- **Hexagonal architecture** for clean separation of concerns
- **Extensible** through ports and adapters pattern
- **Zero dependencies** on external frameworks

## Installation

```bash
go get github.com/spaghettifactory-oss/data-pipeline
```

## Quick Start

```go
package main

import (
    "project/internal/adapters/source"
    "project/internal/adapters/store"
    "project/internal/core/domain"
    "project/internal/core/services"
)

func main() {
    // Define schema
    schema := &domain.DataSchema{
        ID: "Product",
        Columns: []domain.SchemaColumn{
            domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
            domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeFloat},
        },
    }

    // Create and run pipeline
    pipeline := services.DataPipeline{
        Source:    source.NewJSONSource("products.json", schema),
        Transform: &MyTransform{},
        Store:     store.NewJSONStore("output.json"),
    }

    result, err := pipeline.RunWithResult()
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Processed %d records\n", result.Count())
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Application                          │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────┐    ┌─────────────┐    ┌─────────┐             │
│  │ Source  │───▶│  Transform  │───▶│  Store  │             │
│  └─────────┘    └─────────────┘    └─────────┘             │
│       │               │                 │                   │
├───────┼───────────────┼─────────────────┼───────────────────┤
│       ▼               ▼                 ▼                   │
│  ┌─────────┐    ┌─────────────┐    ┌─────────┐             │
│  │  Port   │    │    Port     │    │  Port   │   Core      │
│  └─────────┘    └─────────────┘    └─────────┘             │
│                       │                                     │
│              ┌────────┴────────┐                           │
│              │    Domain       │                           │
│              │ Record/Schema   │                           │
│              └─────────────────┘                           │
└─────────────────────────────────────────────────────────────┘
```

## Core Concepts

### Schema

Schemas define the structure of your data with typed columns.

```go
schema := &domain.DataSchema{
    ID: "Order",
    Columns: []domain.SchemaColumn{
        domain.SchemaColumnSingle{ID: "id", SchemaType: domain.NativeTypeString},
        domain.SchemaColumnSingle{ID: "total", SchemaType: domain.NativeTypeFloat},
        domain.SchemaColumnSingle{ID: "created_at", SchemaType: domain.NativeTypeDate},
        domain.SchemaColumnArray{ID: "items", RefSchema: itemSchema},
    },
}
```

#### Supported Types

| Type | Constant | Go Type |
|------|----------|---------|
| String | `NativeTypeString` | `string` |
| Integer | `NativeTypeInt` | `int64` |
| Float | `NativeTypeFloat` | `float64` |
| Date | `NativeTypeDate` | `time.Time` |
| Boolean | `NativeTypeBool` | `bool` |

### RecordSet Operations

RecordSet provides functional primitives for data manipulation.

```go
// Filter
activeUsers := users.Filter(func(r *domain.Record) bool {
    return r.GetBool("active")
})

// Map
enriched := records.Map(func(r *domain.Record) *domain.Record {
    result := domain.NewRecord(outputSchema)
    result.Set("name", r.Get("name"))
    result.Set("price_with_tax", domain.FloatValue(r.GetFloat("price") * 1.2))
    return result
})

// Reduce
total := records.Reduce(0.0, func(acc any, r *domain.Record) any {
    return acc.(float64) + r.GetFloat("amount")
}).(float64)

// Chaining
result := records.
    Filter(func(r *domain.Record) bool { return r.GetInt("stock") > 0 }).
    Take(10)
```

#### Available Operations

| Operation | Description |
|-----------|-------------|
| `Filter(predicate)` | Returns records matching the predicate |
| `Map(mapper)` | Transforms each record |
| `Reduce(initial, reducer)` | Aggregates records into a single value |
| `ForEach(action)` | Executes an action on each record |
| `Any(predicate)` | Returns true if any record matches |
| `All(predicate)` | Returns true if all records match |
| `Take(n)` | Returns the first n records |
| `Skip(n)` | Skips the first n records |
| `First()` | Returns the first record |
| `Last()` | Returns the last record |
| `Count()` | Returns the number of records |
| `IsEmpty()` | Returns true if no records |

### Custom Transforms

Implement the `TransformPort` interface to create custom transformations.

```go
type PriceInflationTransform struct {
    Multiplier float64
}

func (t *PriceInflationTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
    return input.Map(func(r *domain.Record) *domain.Record {
        result := domain.NewRecord(r.Schema)
        for col, val := range r.Values {
            if col == "price" {
                result.Set(col, domain.FloatValue(r.GetFloat(col) * t.Multiplier))
            } else {
                result.Set(col, val)
            }
        }
        return result
    }), nil
}
```

### Transform Chaining

Combine multiple transforms using the builder pattern.

```go
pipeline := transform.NewTransformBuilder().
    Add(&ValidateTransform{}).
    Add(&EnrichTransform{}).
    Add(&FilterTransform{}).
    Build()
```

## Project Structure

```
.
├── internal/
│   ├── core/
│   │   ├── domain/       # Record, RecordSet, Schema, Value types
│   │   ├── ports/        # SourcePort, TransformPort, StorePort interfaces
│   │   └── services/     # DataPipeline orchestration
│   └── adapters/
│       ├── source/       # JSONSource implementation
│       ├── store/        # JSONStore implementation
│       └── transform/    # TransformBuilder
└── samples/              # Usage examples
```

## Examples

The `samples/` directory contains working examples:

| Example | Description |
|---------|-------------|
| `stocks/inflation` | Price multiplication with Map |
| `stocks/inflation_complex_object` | Nested objects with arrays |
| `stocks/filter_products` | Data validation and correction |
| `logs/utc_to_paris` | Timezone conversion |
| `logs/count_by_hour` | Aggregation with Reduce |

```bash
# Run an example
go run ./samples/stocks/inflation/...

# Run example tests
bash samples/stocks/inflation/test.sh
```

## Testing

```bash
# Run all tests
go test ./...

# With coverage
go test ./... -cover

# Verbose output
go test ./... -v
```

## Design Philosophy

This library provides **primitives, not solutions**.

Rather than offering pre-built transforms like `MapTransform` or `FilterTransform`, we provide `RecordSet.Map()` and `RecordSet.Filter()` as building blocks. You compose these primitives to build transforms that fit your specific needs.

This approach ensures:

- **No framework lock-in** — Your transforms are plain Go code
- **Full control** — You decide exactly how data flows
- **Testability** — Simple functions are easy to test
- **Predictability** — No magic, no hidden behavior

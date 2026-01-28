# Changelog

All notable changes to this project will be documented in this file.

## v0.1.0

### Added

#### Core Domain
- `DataSchema` for defining typed data structures
- `SchemaColumn` interface with `SchemaColumnSingle` and `SchemaColumnArray` implementations
- Native types: `NativeTypeString`, `NativeTypeInt`, `NativeTypeFloat`, `NativeTypeDate`, `NativeTypeBool`
- `CustomType` for user-defined nested types
- `Record` with typed getters: `GetString`, `GetInt`, `GetFloat`, `GetDate`, `GetBool`, `GetArray`, `GetRecord`
- `RecordSet` with functional operations: `Map`, `Filter`, `Reduce`, `ForEach`, `Any`, `All`, `Take`, `Skip`, `First`, `Last`
- Value types: `StringValue`, `IntValue`, `FloatValue`, `DateValue`, `BoolValue`, `NullValue`, `ArrayValue`, `RecordValue`

#### Ports (Interfaces)
- `SourcePort` for data loading
- `TransformPort` for data transformation
- `StorePort` for data persistence

#### Adapters
- `JSONSource` for loading data from JSON files
- `JSONStore` for saving data to JSON files
- `TransformBuilder` for chaining multiple transforms

#### Services
- `DataPipeline` with `Run()` and `RunWithResult()` methods

#### Samples
- `stocks/inflation` - Price multiplication with Map
- `stocks/inflation_complex_object` - Nested objects with arrays
- `stocks/filter_products` - Data validation and correction
- `logs/utc_to_paris` - Timezone conversion
- `logs/count_by_hour` - Aggregation with Reduce

#### CI/CD
- GitHub Actions workflow for unit tests
- GitHub Actions workflow for sample tests
- GitHub Actions workflow for automated releases on tag push
- Reusable composite action for running sample tests

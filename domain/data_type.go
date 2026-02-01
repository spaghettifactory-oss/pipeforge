// Package domain contains the core domain types for data schema management.
package domain

// SchemaType defines the interface for all data types in a schema.
// It can represent either native types (string, int, etc.) or custom types.
type SchemaType interface {
	// GetTypeName returns the name of the type.
	GetTypeName() string
	// IsNative returns true if this is a built-in native type.
	IsNative() bool
}

// NativeType represents built-in primitive types.
type NativeType string

const (
	NativeTypeString NativeType = "string"
	NativeTypeInt    NativeType = "int"
	NativeTypeFloat  NativeType = "float"
	NativeTypeDate   NativeType = "date"
	NativeTypeBool   NativeType = "bool"
)

func (n NativeType) GetTypeName() string { return string(n) }
func (n NativeType) IsNative() bool      { return true }

// CustomType represents a user-defined type that references another schema.
// For example, a CVE type that contains CPE references.
type CustomType struct {
	Name   string      // The name of the custom type (e.g., "CPE", "CVE")
	Schema *DataSchema // Pointer to the schema definition
}

func (c CustomType) GetTypeName() string { return c.Name }
func (c CustomType) IsNative() bool      { return false }

// SchemaColumn defines the interface for columns in a schema.
type SchemaColumn interface {
	// GetID returns the column identifier.
	GetID() string
	// GetType returns the data type of this column.
	GetType() SchemaType
	// IsArray returns true if this column contains multiple values.
	IsArray() bool
}

// DataSchema represents a data structure definition with typed columns.
type DataSchema struct {
	ID      string         // Unique identifier for this schema
	Columns []SchemaColumn // List of columns in this schema
}

// SchemaColumnSingle represents a column with a single value.
type SchemaColumnSingle struct {
	ID         string     // Column identifier
	SchemaType SchemaType // Data type of the column
}

func (s SchemaColumnSingle) GetID() string       { return s.ID }
func (s SchemaColumnSingle) GetType() SchemaType { return s.SchemaType }
func (s SchemaColumnSingle) IsArray() bool       { return false }

// SchemaColumnArray represents a column containing an array of values.
type SchemaColumnArray struct {
	ID        string     // Column identifier
	RefSchema SchemaType // Type of elements in the array
}

func (s SchemaColumnArray) GetID() string       { return s.ID }
func (s SchemaColumnArray) GetType() SchemaType { return s.RefSchema }
func (s SchemaColumnArray) IsArray() bool       { return true }

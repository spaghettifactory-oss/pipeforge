package domain

import "time"

// Value represents a typed value in a Record.
type Value interface {
	// GetType returns the SchemaType of this value.
	GetType() SchemaType
	// IsNull returns true if this value is null.
	IsNull() bool
}

// StringValue represents a string value.
type StringValue string

func (v StringValue) GetType() SchemaType { return NativeTypeString }
func (v StringValue) IsNull() bool        { return false }

// IntValue represents an integer value.
type IntValue int64

func (v IntValue) GetType() SchemaType { return NativeTypeInt }
func (v IntValue) IsNull() bool        { return false }

// FloatValue represents a floating-point value.
type FloatValue float64

func (v FloatValue) GetType() SchemaType { return NativeTypeFloat }
func (v FloatValue) IsNull() bool        { return false }

// DateValue represents a date/time value.
type DateValue time.Time

func (v DateValue) GetType() SchemaType { return NativeTypeDate }
func (v DateValue) IsNull() bool        { return false }

// BoolValue represents a boolean value.
type BoolValue bool

func (v BoolValue) GetType() SchemaType { return NativeTypeBool }
func (v BoolValue) IsNull() bool        { return false }

// NullValue represents a null value for any type.
type NullValue struct {
	Type SchemaType
}

func (v NullValue) GetType() SchemaType { return v.Type }
func (v NullValue) IsNull() bool        { return true }

// ArrayValue represents an array of values.
type ArrayValue struct {
	ElementType SchemaType
	Elements    []Value
}

func (v ArrayValue) GetType() SchemaType { return v.ElementType }
func (v ArrayValue) IsNull() bool        { return false }

// RecordValue represents a nested record value (for custom types).
type RecordValue struct {
	Record *Record
}

func (v RecordValue) GetType() SchemaType {
	if v.Record == nil || v.Record.Schema == nil {
		return nil
	}
	return CustomType{Name: v.Record.Schema.ID, Schema: v.Record.Schema}
}
func (v RecordValue) IsNull() bool { return v.Record == nil }

// RawRecord represents raw data before validation and typing.
type RawRecord struct {
	Source string         // Origin of the data (e.g., "file.json", "api/cve")
	Data   map[string]any // Raw untyped data
}

// Record represents a typed data record conforming to a DataSchema.
type Record struct {
	Schema *DataSchema      // Schema this record conforms to
	Values map[string]Value // Column ID -> Value mapping
}

// NewRecord creates a new empty Record for the given schema.
func NewRecord(schema *DataSchema) *Record {
	return &Record{
		Schema: schema,
		Values: make(map[string]Value),
	}
}

// Get returns the value for the given column ID.
func (r *Record) Get(columnID string) Value {
	return r.Values[columnID]
}

// Set sets the value for the given column ID.
func (r *Record) Set(columnID string, value Value) {
	r.Values[columnID] = value
}

// GetString returns the string value for the given column ID.
// Returns empty string if the value is not a StringValue or is null.
func (r *Record) GetString(columnID string) string {
	v, ok := r.Values[columnID].(StringValue)
	if !ok {
		return ""
	}
	return string(v)
}

// GetInt returns the int value for the given column ID.
// Returns 0 if the value is not an IntValue or is null.
func (r *Record) GetInt(columnID string) int64 {
	v, ok := r.Values[columnID].(IntValue)
	if !ok {
		return 0
	}
	return int64(v)
}

// GetFloat returns the float value for the given column ID.
// Returns 0 if the value is not a FloatValue or is null.
func (r *Record) GetFloat(columnID string) float64 {
	v, ok := r.Values[columnID].(FloatValue)
	if !ok {
		return 0
	}
	return float64(v)
}

// GetDate returns the date value for the given column ID.
// Returns zero time if the value is not a DateValue or is null.
func (r *Record) GetDate(columnID string) time.Time {
	v, ok := r.Values[columnID].(DateValue)
	if !ok {
		return time.Time{}
	}
	return time.Time(v)
}

// GetBool returns the boolean value for the given column ID.
// Returns false if the value is not a BoolValue or is null.
func (r *Record) GetBool(columnID string) bool {
	v, ok := r.Values[columnID].(BoolValue)
	if !ok {
		return false
	}
	return bool(v)
}

// GetArray returns the array value for the given column ID.
// Returns nil if the value is not an ArrayValue or is null.
func (r *Record) GetArray(columnID string) []Value {
	v, ok := r.Values[columnID].(ArrayValue)
	if !ok {
		return nil
	}
	return v.Elements
}

// GetRecord returns the nested record value for the given column ID.
// Returns nil if the value is not a RecordValue or is null.
func (r *Record) GetRecord(columnID string) *Record {
	v, ok := r.Values[columnID].(RecordValue)
	if !ok {
		return nil
	}
	return v.Record
}

package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStringValue_GetType(t *testing.T) {
	t.Run("should return NativeTypeString", func(t *testing.T) {
		v := StringValue("hello")

		assert.Equal(t, NativeTypeString, v.GetType())
	})
}

func TestStringValue_IsNull(t *testing.T) {
	t.Run("should return false", func(t *testing.T) {
		v := StringValue("hello")

		assert.False(t, v.IsNull())
	})
}

func TestIntValue_GetType(t *testing.T) {
	t.Run("should return NativeTypeInt", func(t *testing.T) {
		v := IntValue(42)

		assert.Equal(t, NativeTypeInt, v.GetType())
	})
}

func TestIntValue_IsNull(t *testing.T) {
	t.Run("should return false", func(t *testing.T) {
		v := IntValue(42)

		assert.False(t, v.IsNull())
	})
}

func TestFloatValue_GetType(t *testing.T) {
	t.Run("should return NativeTypeFloat", func(t *testing.T) {
		v := FloatValue(3.14)

		assert.Equal(t, NativeTypeFloat, v.GetType())
	})
}

func TestFloatValue_IsNull(t *testing.T) {
	t.Run("should return false", func(t *testing.T) {
		v := FloatValue(3.14)

		assert.False(t, v.IsNull())
	})
}

func TestDateValue_GetType(t *testing.T) {
	t.Run("should return NativeTypeDate", func(t *testing.T) {
		v := DateValue(time.Now())

		assert.Equal(t, NativeTypeDate, v.GetType())
	})
}

func TestDateValue_IsNull(t *testing.T) {
	t.Run("should return false", func(t *testing.T) {
		v := DateValue(time.Now())

		assert.False(t, v.IsNull())
	})
}

func TestBoolValue_GetType(t *testing.T) {
	t.Run("should return NativeTypeBool", func(t *testing.T) {
		v := BoolValue(true)

		assert.Equal(t, NativeTypeBool, v.GetType())
	})
}

func TestBoolValue_IsNull(t *testing.T) {
	t.Run("should return false", func(t *testing.T) {
		v := BoolValue(false)

		assert.False(t, v.IsNull())
	})
}

func TestNullValue_GetType(t *testing.T) {
	tests := []struct {
		name     string
		nullType SchemaType
	}{
		{"null string", NativeTypeString},
		{"null int", NativeTypeInt},
		{"null float", NativeTypeFloat},
		{"null date", NativeTypeDate},
		{"null bool", NativeTypeBool},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NullValue{Type: tt.nullType}

			assert.Equal(t, tt.nullType, v.GetType())
		})
	}
}

func TestNullValue_IsNull(t *testing.T) {
	t.Run("should return true", func(t *testing.T) {
		v := NullValue{Type: NativeTypeString}

		assert.True(t, v.IsNull())
	})
}

func TestArrayValue_GetType(t *testing.T) {
	t.Run("should return element type for native array", func(t *testing.T) {
		v := ArrayValue{
			ElementType: NativeTypeString,
			Elements:    []Value{StringValue("a"), StringValue("b")},
		}

		assert.Equal(t, NativeTypeString, v.GetType())
	})

	t.Run("should return element type for custom array", func(t *testing.T) {
		customType := CustomType{Name: "CPE"}
		v := ArrayValue{
			ElementType: customType,
			Elements:    []Value{},
		}

		assert.Equal(t, customType, v.GetType())
	})
}

func TestArrayValue_IsNull(t *testing.T) {
	t.Run("should return false", func(t *testing.T) {
		v := ArrayValue{ElementType: NativeTypeString, Elements: []Value{}}

		assert.False(t, v.IsNull())
	})
}

func TestRecordValue_GetType(t *testing.T) {
	t.Run("should return CustomType for nested record", func(t *testing.T) {
		schema := &DataSchema{ID: "CPE"}
		record := &Record{Schema: schema}
		v := RecordValue{Record: record}

		schemaType := v.GetType()

		assert.NotNil(t, schemaType)
		assert.Equal(t, "CPE", schemaType.GetTypeName())
		assert.False(t, schemaType.IsNative())
	})

	t.Run("should return nil for nil record", func(t *testing.T) {
		v := RecordValue{Record: nil}

		assert.Nil(t, v.GetType())
	})

	t.Run("should return nil for record with nil schema", func(t *testing.T) {
		v := RecordValue{Record: &Record{Schema: nil}}

		assert.Nil(t, v.GetType())
	})
}

func TestRecordValue_IsNull(t *testing.T) {
	t.Run("should return true for nil record", func(t *testing.T) {
		v := RecordValue{Record: nil}

		assert.True(t, v.IsNull())
	})

	t.Run("should return false for non-nil record", func(t *testing.T) {
		v := RecordValue{Record: &Record{}}

		assert.False(t, v.IsNull())
	})
}

func TestValue_Interface(t *testing.T) {
	t.Run("StringValue should implement Value", func(t *testing.T) {
		var v Value = StringValue("test")

		assert.Equal(t, NativeTypeString, v.GetType())
		assert.False(t, v.IsNull())
	})

	t.Run("IntValue should implement Value", func(t *testing.T) {
		var v Value = IntValue(42)

		assert.Equal(t, NativeTypeInt, v.GetType())
		assert.False(t, v.IsNull())
	})

	t.Run("FloatValue should implement Value", func(t *testing.T) {
		var v Value = FloatValue(3.14)

		assert.Equal(t, NativeTypeFloat, v.GetType())
		assert.False(t, v.IsNull())
	})

	t.Run("DateValue should implement Value", func(t *testing.T) {
		var v Value = DateValue(time.Now())

		assert.Equal(t, NativeTypeDate, v.GetType())
		assert.False(t, v.IsNull())
	})

	t.Run("BoolValue should implement Value", func(t *testing.T) {
		var v Value = BoolValue(true)

		assert.Equal(t, NativeTypeBool, v.GetType())
		assert.False(t, v.IsNull())
	})

	t.Run("NullValue should implement Value", func(t *testing.T) {
		var v Value = NullValue{Type: NativeTypeString}

		assert.Equal(t, NativeTypeString, v.GetType())
		assert.True(t, v.IsNull())
	})

	t.Run("ArrayValue should implement Value", func(t *testing.T) {
		var v Value = ArrayValue{ElementType: NativeTypeString}

		assert.Equal(t, NativeTypeString, v.GetType())
		assert.False(t, v.IsNull())
	})

	t.Run("RecordValue should implement Value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		var v Value = RecordValue{Record: &Record{Schema: schema}}

		assert.Equal(t, "Test", v.GetType().GetTypeName())
		assert.False(t, v.IsNull())
	})
}

func TestNewRecord(t *testing.T) {
	t.Run("should create empty record with schema", func(t *testing.T) {
		schema := &DataSchema{
			ID: "Test",
			Columns: []SchemaColumn{
				SchemaColumnSingle{ID: "name", SchemaType: NativeTypeString},
			},
		}

		record := NewRecord(schema)

		assert.NotNil(t, record)
		assert.Equal(t, schema, record.Schema)
		assert.NotNil(t, record.Values)
		assert.Empty(t, record.Values)
	})
}

func TestRecord_GetSet(t *testing.T) {
	t.Run("should set and get value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		record.Set("name", StringValue("John"))

		assert.Equal(t, StringValue("John"), record.Get("name"))
	})

	t.Run("should return nil for unknown column", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		assert.Nil(t, record.Get("unknown"))
	})
}

func TestRecord_GetString(t *testing.T) {
	t.Run("should return string value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("name", StringValue("John"))

		assert.Equal(t, "John", record.GetString("name"))
	})

	t.Run("should return empty string for non-string value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("count", IntValue(42))

		assert.Equal(t, "", record.GetString("count"))
	})

	t.Run("should return empty string for unknown column", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		assert.Equal(t, "", record.GetString("unknown"))
	})
}

func TestRecord_GetInt(t *testing.T) {
	t.Run("should return int value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("count", IntValue(42))

		assert.Equal(t, int64(42), record.GetInt("count"))
	})

	t.Run("should return 0 for non-int value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("name", StringValue("John"))

		assert.Equal(t, int64(0), record.GetInt("name"))
	})

	t.Run("should return 0 for unknown column", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		assert.Equal(t, int64(0), record.GetInt("unknown"))
	})
}

func TestRecord_GetFloat(t *testing.T) {
	t.Run("should return float value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("score", FloatValue(3.14))

		assert.Equal(t, 3.14, record.GetFloat("score"))
	})

	t.Run("should return 0 for non-float value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("name", StringValue("John"))

		assert.Equal(t, float64(0), record.GetFloat("name"))
	})

	t.Run("should return 0 for unknown column", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		assert.Equal(t, float64(0), record.GetFloat("unknown"))
	})
}

func TestRecord_GetDate(t *testing.T) {
	t.Run("should return date value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		now := time.Now()
		record.Set("created", DateValue(now))

		assert.Equal(t, now, record.GetDate("created"))
	})

	t.Run("should return zero time for non-date value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("name", StringValue("John"))

		assert.True(t, record.GetDate("name").IsZero())
	})

	t.Run("should return zero time for unknown column", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		assert.True(t, record.GetDate("unknown").IsZero())
	})
}

func TestRecord_GetBool(t *testing.T) {
	t.Run("should return bool value true", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("active", BoolValue(true))

		assert.True(t, record.GetBool("active"))
	})

	t.Run("should return bool value false", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("active", BoolValue(false))

		assert.False(t, record.GetBool("active"))
	})

	t.Run("should return false for non-bool value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("name", StringValue("John"))

		assert.False(t, record.GetBool("name"))
	})

	t.Run("should return false for unknown column", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		assert.False(t, record.GetBool("unknown"))
	})
}

func TestRecord_GetArray(t *testing.T) {
	t.Run("should return array elements", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		elements := []Value{StringValue("a"), StringValue("b")}
		record.Set("tags", ArrayValue{ElementType: NativeTypeString, Elements: elements})

		result := record.GetArray("tags")

		assert.Len(t, result, 2)
		assert.Equal(t, StringValue("a"), result[0])
		assert.Equal(t, StringValue("b"), result[1])
	})

	t.Run("should return nil for non-array value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("name", StringValue("John"))

		assert.Nil(t, record.GetArray("name"))
	})

	t.Run("should return nil for unknown column", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		assert.Nil(t, record.GetArray("unknown"))
	})
}

func TestRecord_GetRecord(t *testing.T) {
	t.Run("should return nested record", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		nestedSchema := &DataSchema{ID: "Nested"}
		nestedRecord := NewRecord(nestedSchema)
		nestedRecord.Set("field", StringValue("value"))

		record := NewRecord(schema)
		record.Set("nested", RecordValue{Record: nestedRecord})

		result := record.GetRecord("nested")

		assert.NotNil(t, result)
		assert.Equal(t, nestedSchema, result.Schema)
		assert.Equal(t, "value", result.GetString("field"))
	})

	t.Run("should return nil for non-record value", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)
		record.Set("name", StringValue("John"))

		assert.Nil(t, record.GetRecord("name"))
	})

	t.Run("should return nil for unknown column", func(t *testing.T) {
		schema := &DataSchema{ID: "Test"}
		record := NewRecord(schema)

		assert.Nil(t, record.GetRecord("unknown"))
	})
}

func TestRawRecord(t *testing.T) {
	t.Run("should store source and raw data", func(t *testing.T) {
		raw := RawRecord{
			Source: "file.json",
			Data: map[string]any{
				"id":   "CVE-2021-44228",
				"name": "Log4Shell",
			},
		}

		assert.Equal(t, "file.json", raw.Source)
		assert.Equal(t, "CVE-2021-44228", raw.Data["id"])
		assert.Equal(t, "Log4Shell", raw.Data["name"])
	})
}

func TestRecord_ComplexExample(t *testing.T) {
	t.Run("should handle Order with nested Product records", func(t *testing.T) {
		// Define Product schema
		productSchema := &DataSchema{
			ID: "Product",
			Columns: []SchemaColumn{
				SchemaColumnSingle{ID: "name", SchemaType: NativeTypeString},
				SchemaColumnSingle{ID: "price", SchemaType: NativeTypeFloat},
				SchemaColumnSingle{ID: "quantity", SchemaType: NativeTypeInt},
			},
		}

		// Define Order schema
		orderSchema := &DataSchema{
			ID: "Order",
			Columns: []SchemaColumn{
				SchemaColumnSingle{ID: "id", SchemaType: NativeTypeString},
				SchemaColumnSingle{ID: "total", SchemaType: NativeTypeFloat},
				SchemaColumnArray{ID: "items", RefSchema: CustomType{Name: "Product", Schema: productSchema}},
			},
		}

		// Create Product record
		productRecord := NewRecord(productSchema)
		productRecord.Set("name", StringValue("Laptop"))
		productRecord.Set("price", FloatValue(999.99))
		productRecord.Set("quantity", IntValue(2))

		// Create Order record
		orderRecord := NewRecord(orderSchema)
		orderRecord.Set("id", StringValue("ORD-001"))
		orderRecord.Set("total", FloatValue(1999.98))
		orderRecord.Set("items", ArrayValue{
			ElementType: CustomType{Name: "Product", Schema: productSchema},
			Elements:    []Value{RecordValue{Record: productRecord}},
		})

		// Assertions
		assert.Equal(t, "ORD-001", orderRecord.GetString("id"))
		assert.Equal(t, 1999.98, orderRecord.GetFloat("total"))

		items := orderRecord.GetArray("items")
		assert.Len(t, items, 1)

		product := items[0].(RecordValue).Record
		assert.Equal(t, "Laptop", product.GetString("name"))
		assert.Equal(t, 999.99, product.GetFloat("price"))
		assert.Equal(t, int64(2), product.GetInt("quantity"))
	})
}

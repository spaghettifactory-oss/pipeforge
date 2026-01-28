package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNativeType_GetTypeName(t *testing.T) {
	tests := []struct {
		name     string
		nType    NativeType
		expected string
	}{
		{"string type", NativeTypeString, "string"},
		{"int type", NativeTypeInt, "int"},
		{"float type", NativeTypeFloat, "float"},
		{"date type", NativeTypeDate, "date"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.nType.GetTypeName())
		})
	}
}

func TestNativeType_IsNative(t *testing.T) {
	nativeTypes := []NativeType{
		NativeTypeString,
		NativeTypeInt,
		NativeTypeFloat,
		NativeTypeDate,
	}

	for _, nType := range nativeTypes {
		t.Run(string(nType), func(t *testing.T) {
			assert.True(t, nType.IsNative(), "NativeType should return true for IsNative()")
		})
	}
}

func TestCustomType_GetTypeName(t *testing.T) {
	tests := []struct {
		name     string
		custom   CustomType
		expected string
	}{
		{
			name:     "CPE type",
			custom:   CustomType{Name: "CPE", Schema: nil},
			expected: "CPE",
		},
		{
			name:     "CVE type",
			custom:   CustomType{Name: "CVE", Schema: nil},
			expected: "CVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.custom.GetTypeName())
		})
	}
}

func TestCustomType_IsNative(t *testing.T) {
	t.Run("should return false for custom type", func(t *testing.T) {
		custom := CustomType{Name: "CPE", Schema: nil}

		assert.False(t, custom.IsNative(), "CustomType should return false for IsNative()")
	})
}

func TestCustomType_WithSchema(t *testing.T) {
	t.Run("should reference a DataSchema", func(t *testing.T) {
		schema := &DataSchema{
			ID: "CPE",
			Columns: []SchemaColumn{
				SchemaColumnSingle{ID: "vendor", SchemaType: NativeTypeString},
				SchemaColumnSingle{ID: "product", SchemaType: NativeTypeString},
			},
		}

		custom := CustomType{Name: "CPE", Schema: schema}

		assert.NotNil(t, custom.Schema)
		assert.Equal(t, "CPE", custom.GetTypeName())
		assert.Equal(t, "CPE", custom.Schema.ID)
		assert.Len(t, custom.Schema.Columns, 2)
	})
}

func TestSchemaType_Interface(t *testing.T) {
	t.Run("NativeType should implement SchemaType", func(t *testing.T) {
		var native SchemaType = NativeTypeString
		assert.True(t, native.IsNative())
		assert.Equal(t, "string", native.GetTypeName())
	})

	t.Run("CustomType should implement SchemaType", func(t *testing.T) {
		var custom SchemaType = CustomType{Name: "Test"}
		assert.False(t, custom.IsNative())
		assert.Equal(t, "Test", custom.GetTypeName())
	})
}

func TestSchemaColumnSingle_GetID(t *testing.T) {
	tests := []struct {
		name     string
		column   SchemaColumnSingle
		expected string
	}{
		{"vendor column", SchemaColumnSingle{ID: "vendor", SchemaType: NativeTypeString}, "vendor"},
		{"age column", SchemaColumnSingle{ID: "age", SchemaType: NativeTypeInt}, "age"},
		{"price column", SchemaColumnSingle{ID: "price", SchemaType: NativeTypeFloat}, "price"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.column.GetID())
		})
	}
}

func TestSchemaColumnSingle_GetType(t *testing.T) {
	tests := []struct {
		name       string
		column     SchemaColumnSingle
		expected   SchemaType
	}{
		{"string column", SchemaColumnSingle{ID: "name", SchemaType: NativeTypeString}, NativeTypeString},
		{"int column", SchemaColumnSingle{ID: "count", SchemaType: NativeTypeInt}, NativeTypeInt},
		{"float column", SchemaColumnSingle{ID: "score", SchemaType: NativeTypeFloat}, NativeTypeFloat},
		{"date column", SchemaColumnSingle{ID: "created", SchemaType: NativeTypeDate}, NativeTypeDate},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.column.GetType())
		})
	}
}

func TestSchemaColumnSingle_IsArray(t *testing.T) {
	t.Run("should return false for single column", func(t *testing.T) {
		column := SchemaColumnSingle{ID: "name", SchemaType: NativeTypeString}

		assert.False(t, column.IsArray())
	})
}

func TestSchemaColumnArray_GetID(t *testing.T) {
	tests := []struct {
		name     string
		column   SchemaColumnArray
		expected string
	}{
		{"tags column", SchemaColumnArray{ID: "tags", RefSchema: NativeTypeString}, "tags"},
		{"cpes column", SchemaColumnArray{ID: "cpes", RefSchema: CustomType{Name: "CPE"}}, "cpes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.column.GetID())
		})
	}
}

func TestSchemaColumnArray_GetType(t *testing.T) {
	t.Run("should return native type for array of primitives", func(t *testing.T) {
		column := SchemaColumnArray{ID: "tags", RefSchema: NativeTypeString}

		assert.Equal(t, NativeTypeString, column.GetType())
		assert.True(t, column.GetType().IsNative())
	})

	t.Run("should return custom type for array of custom types", func(t *testing.T) {
		cpeType := CustomType{Name: "CPE", Schema: nil}
		column := SchemaColumnArray{ID: "cpes", RefSchema: cpeType}

		assert.Equal(t, cpeType, column.GetType())
		assert.False(t, column.GetType().IsNative())
		assert.Equal(t, "CPE", column.GetType().GetTypeName())
	})
}

func TestSchemaColumnArray_IsArray(t *testing.T) {
	t.Run("should return true for array column", func(t *testing.T) {
		column := SchemaColumnArray{ID: "items", RefSchema: NativeTypeString}

		assert.True(t, column.IsArray())
	})
}

func TestSchemaColumn_Interface(t *testing.T) {
	t.Run("SchemaColumnSingle should implement SchemaColumn", func(t *testing.T) {
		var col SchemaColumn = SchemaColumnSingle{ID: "name", SchemaType: NativeTypeString}

		assert.Equal(t, "name", col.GetID())
		assert.Equal(t, NativeTypeString, col.GetType())
		assert.False(t, col.IsArray())
	})

	t.Run("SchemaColumnArray should implement SchemaColumn", func(t *testing.T) {
		var col SchemaColumn = SchemaColumnArray{ID: "tags", RefSchema: NativeTypeString}

		assert.Equal(t, "tags", col.GetID())
		assert.Equal(t, NativeTypeString, col.GetType())
		assert.True(t, col.IsArray())
	})
}

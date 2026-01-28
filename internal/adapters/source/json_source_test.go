package source

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spaghettifactory-oss/pipeforge/internal/core/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONSource_Load(t *testing.T) {
	t.Run("should load simple JSON array", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeFloat},
				domain.SchemaColumnSingle{ID: "quantity", SchemaType: domain.NativeTypeInt},
			},
		}

		jsonData := `[
			{"name": "Laptop", "price": 999.99, "quantity": 5},
			{"name": "Phone", "price": 499.99, "quantity": 10}
		]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		require.NoError(t, err)
		assert.Equal(t, 2, result.Count())

		first := result.First()
		assert.Equal(t, "Laptop", first.GetString("name"))
		assert.Equal(t, 999.99, first.GetFloat("price"))
		assert.Equal(t, int64(5), first.GetInt("quantity"))

		last := result.Last()
		assert.Equal(t, "Phone", last.GetString("name"))
	})

	t.Run("should load JSON with arrays", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Article",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "title", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnArray{ID: "tags", RefSchema: domain.NativeTypeString},
			},
		}

		jsonData := `[
			{"title": "Go Tutorial", "tags": ["go", "programming", "tutorial"]}
		]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		require.NoError(t, err)
		assert.Equal(t, 1, result.Count())

		record := result.First()
		assert.Equal(t, "Go Tutorial", record.GetString("title"))

		tags := record.GetArray("tags")
		assert.Len(t, tags, 3)
		assert.Equal(t, domain.StringValue("go"), tags[0])
		assert.Equal(t, domain.StringValue("programming"), tags[1])
	})

	t.Run("should load JSON with nested objects", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "city", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "country", SchemaType: domain.NativeTypeString},
			},
		}

		userSchema := &domain.DataSchema{
			ID: "User",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		jsonData := `[
			{"name": "John", "address": {"city": "Paris", "country": "France"}}
		]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, userSchema)

		result, err := source.Load()

		require.NoError(t, err)
		assert.Equal(t, 1, result.Count())

		user := result.First()
		assert.Equal(t, "John", user.GetString("name"))

		address := user.GetRecord("address")
		require.NotNil(t, address)
		assert.Equal(t, "Paris", address.GetString("city"))
		assert.Equal(t, "France", address.GetString("country"))
	})

	t.Run("should handle null values", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "description", SchemaType: domain.NativeTypeString},
			},
		}

		jsonData := `[
			{"name": "Laptop", "description": null}
		]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		require.NoError(t, err)
		record := result.First()
		assert.Equal(t, "Laptop", record.GetString("name"))
		assert.True(t, record.Get("description").IsNull())
	})

	t.Run("should handle missing fields", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "description", SchemaType: domain.NativeTypeString},
			},
		}

		jsonData := `[{"name": "Laptop"}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		require.NoError(t, err)
		record := result.First()
		assert.Equal(t, "Laptop", record.GetString("name"))
		assert.Nil(t, record.Get("description"))
	})

	t.Run("should return error for non-existent file", func(t *testing.T) {
		schema := &domain.DataSchema{ID: "Test"}
		source := NewJSONSource("/non/existent/file.json", schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to read file")
	})

	t.Run("should return error for invalid JSON", func(t *testing.T) {
		schema := &domain.DataSchema{ID: "Test"}
		filePath := createTempFile(t, "not valid json")
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to parse JSON")
	})

	t.Run("should return error for type mismatch", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeFloat},
			},
		}

		jsonData := `[{"price": "not a number"}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "expected number")
	})
}

func TestJSONSource_Load_Dates(t *testing.T) {
	t.Run("should parse RFC3339 dates", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Event",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "date", SchemaType: domain.NativeTypeDate},
			},
		}

		jsonData := `[{"name": "Meeting", "date": "2024-01-15T10:30:00Z"}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		require.NoError(t, err)
		record := result.First()
		assert.Equal(t, "Meeting", record.GetString("name"))
		assert.Equal(t, 2024, record.GetDate("date").Year())
		assert.Equal(t, 1, int(record.GetDate("date").Month()))
		assert.Equal(t, 15, record.GetDate("date").Day())
	})

	t.Run("should return error for invalid date format", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Event",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "date", SchemaType: domain.NativeTypeDate},
			},
		}

		jsonData := `[{"date": "not-a-date"}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "invalid date format")
	})

	t.Run("should return error for non-string date", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Event",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "date", SchemaType: domain.NativeTypeDate},
			},
		}

		jsonData := `[{"date": 12345}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "expected date string")
	})
}

func TestJSONSource_Load_UnknownType(t *testing.T) {
	t.Run("should return error for unknown native type", func(t *testing.T) {
		unknownType := domain.NativeType("unknown")
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "field", SchemaType: unknownType},
			},
		}

		jsonData := `[{"field": "value"}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unknown native type")
	})
}

func TestJSONSource_Load_Strings(t *testing.T) {
	t.Run("should return error when string field receives non-string", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}

		jsonData := `[{"name": 12345}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "expected string")
	})
}

func TestJSONSource_Load_Integers(t *testing.T) {
	t.Run("should return error when int field receives non-number", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "quantity", SchemaType: domain.NativeTypeInt},
			},
		}

		jsonData := `[{"quantity": "not a number"}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "expected number")
	})
}

func TestJSONSource_Load_CustomTypes(t *testing.T) {
	t.Run("should return error when nested object has invalid data", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "zipcode", SchemaType: domain.NativeTypeInt},
			},
		}

		userSchema := &domain.DataSchema{
			ID: "User",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		jsonData := `[{"address": {"zipcode": "not a number"}}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, userSchema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "expected number")
	})

	t.Run("should return error when custom type has no schema", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "User",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: nil}},
			},
		}

		jsonData := `[{"address": {"city": "Paris"}}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "has no schema")
	})

	t.Run("should return error when expected object but got primitive", func(t *testing.T) {
		addressSchema := &domain.DataSchema{ID: "Address"}
		schema := &domain.DataSchema{
			ID: "User",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		jsonData := `[{"address": "not an object"}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "expected object")
	})
}

func TestJSONSource_Load_Arrays(t *testing.T) {
	t.Run("should return error when expected array but got primitive", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Article",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnArray{ID: "tags", RefSchema: domain.NativeTypeString},
			},
		}

		jsonData := `[{"tags": "not an array"}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "expected array")
	})

	t.Run("should return error for invalid array element", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnArray{ID: "prices", RefSchema: domain.NativeTypeFloat},
			},
		}

		jsonData := `[{"prices": [10.5, "invalid", 20.5]}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, schema)

		result, err := source.Load()

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "element 1")
	})

	t.Run("should load array of nested objects", func(t *testing.T) {
		itemSchema := &domain.DataSchema{
			ID: "Item",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}

		orderSchema := &domain.DataSchema{
			ID: "Order",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnArray{ID: "items", RefSchema: domain.CustomType{Name: "Item", Schema: itemSchema}},
			},
		}

		jsonData := `[{"items": [{"name": "Laptop"}, {"name": "Phone"}]}]`

		filePath := createTempFile(t, jsonData)
		source := NewJSONSource(filePath, orderSchema)

		result, err := source.Load()

		require.NoError(t, err)
		order := result.First()
		items := order.GetArray("items")
		assert.Len(t, items, 2)

		item0 := items[0].(domain.RecordValue).Record
		assert.Equal(t, "Laptop", item0.GetString("name"))
	})
}

func createTempFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.json")
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)
	return filePath
}

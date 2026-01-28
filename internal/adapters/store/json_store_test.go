package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spaghettifactory-oss/pipeforge/internal/core/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJSONStore(t *testing.T) {
	t.Run("should create store with file path and indent enabled", func(t *testing.T) {
		store := NewJSONStore("/path/to/file.json")

		assert.Equal(t, "/path/to/file.json", store.FilePath)
		assert.True(t, store.Indent)
	})
}

func TestJSONStore_Store(t *testing.T) {
	t.Run("should store simple records", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeFloat},
				domain.SchemaColumnSingle{ID: "quantity", SchemaType: domain.NativeTypeInt},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("name", domain.StringValue("Laptop"))
		record.Set("price", domain.FloatValue(999.99))
		record.Set("quantity", domain.IntValue(5))
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		assert.Len(t, result, 1)
		assert.Equal(t, "Laptop", result[0]["name"])
		assert.Equal(t, 999.99, result[0]["price"])
		assert.Equal(t, float64(5), result[0]["quantity"])
	})

	t.Run("should store multiple records", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}

		recordSet := domain.NewRecordSet(schema)

		record1 := domain.NewRecord(schema)
		record1.Set("name", domain.StringValue("Laptop"))
		recordSet.Add(record1)

		record2 := domain.NewRecord(schema)
		record2.Set("name", domain.StringValue("Phone"))
		recordSet.Add(record2)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		assert.Len(t, result, 2)
		assert.Equal(t, "Laptop", result[0]["name"])
		assert.Equal(t, "Phone", result[1]["name"])
	})

	t.Run("should store dates in RFC3339 format", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Event",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "date", SchemaType: domain.NativeTypeDate},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		date := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		record.Set("date", domain.DateValue(date))
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		assert.Equal(t, "2024-01-15T10:30:00Z", result[0]["date"])
	})

	t.Run("should store arrays", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Article",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnArray{ID: "tags", RefSchema: domain.NativeTypeString},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("tags", domain.ArrayValue{
			ElementType: domain.NativeTypeString,
			Elements: []domain.Value{
				domain.StringValue("go"),
				domain.StringValue("programming"),
			},
		})
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		tags := result[0]["tags"].([]any)
		assert.Len(t, tags, 2)
		assert.Equal(t, "go", tags[0])
		assert.Equal(t, "programming", tags[1])
	})

	t.Run("should store nested objects", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "city", SchemaType: domain.NativeTypeString},
			},
		}

		userSchema := &domain.DataSchema{
			ID: "User",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		recordSet := domain.NewRecordSet(userSchema)
		record := domain.NewRecord(userSchema)
		record.Set("name", domain.StringValue("John"))

		addressRecord := domain.NewRecord(addressSchema)
		addressRecord.Set("city", domain.StringValue("Paris"))
		record.Set("address", domain.RecordValue{Record: addressRecord})

		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		assert.Equal(t, "John", result[0]["name"])
		address := result[0]["address"].(map[string]any)
		assert.Equal(t, "Paris", address["city"])
	})

	t.Run("should store null values as null", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "description", SchemaType: domain.NativeTypeString},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("name", domain.StringValue("Laptop"))
		record.Set("description", domain.NullValue{Type: domain.NativeTypeString})
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		assert.Equal(t, "Laptop", result[0]["name"])
		assert.Nil(t, result[0]["description"])
	})

	t.Run("should store nil RecordValue as null", func(t *testing.T) {
		addressSchema := &domain.DataSchema{ID: "Address"}
		schema := &domain.DataSchema{
			ID: "User",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("address", domain.RecordValue{Record: nil})
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		assert.Nil(t, result[0]["address"])
	})

	t.Run("should store empty RecordSet as empty array", func(t *testing.T) {
		schema := &domain.DataSchema{ID: "Product"}
		recordSet := domain.NewRecordSet(schema)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		assert.Equal(t, "[]", string(content))
	})

	t.Run("should store without indent when disabled", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("name", domain.StringValue("Laptop"))
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)
		store.Indent = false

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		assert.Equal(t, `[{"name":"Laptop"}]`, string(content))
	})

	t.Run("should return error for nil RecordSet", func(t *testing.T) {
		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot store nil RecordSet")
	})

	t.Run("should return error for invalid file path", func(t *testing.T) {
		schema := &domain.DataSchema{ID: "Product"}
		recordSet := domain.NewRecordSet(schema)

		store := NewJSONStore("/nonexistent/directory/file.json")

		err := store.Store(recordSet)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to write file")
	})
}

func TestJSONStore_Store_Errors(t *testing.T) {
	t.Run("should handle nil value in record", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "field", SchemaType: domain.NativeTypeString},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("field", nil)
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		assert.Nil(t, result[0]["field"])
	})

	t.Run("should return error for unsupported value type", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "field", SchemaType: domain.NativeTypeString},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("field", unsupportedValue{})
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported value type")
	})

	t.Run("should return error for unsupported value type in array", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnArray{ID: "items", RefSchema: domain.NativeTypeString},
			},
		}

		recordSet := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("items", domain.ArrayValue{
			ElementType: domain.NativeTypeString,
			Elements:    []domain.Value{unsupportedValue{}},
		})
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "element 0")
	})
}

// unsupportedValue is a test helper for triggering unsupported type errors.
type unsupportedValue struct{}

func (v unsupportedValue) GetType() domain.SchemaType { return domain.NativeTypeString }
func (v unsupportedValue) IsNull() bool              { return false }

func TestJSONStore_Store_Arrays(t *testing.T) {
	t.Run("should store array of nested objects", func(t *testing.T) {
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

		recordSet := domain.NewRecordSet(orderSchema)
		record := domain.NewRecord(orderSchema)

		item1 := domain.NewRecord(itemSchema)
		item1.Set("name", domain.StringValue("Laptop"))

		item2 := domain.NewRecord(itemSchema)
		item2.Set("name", domain.StringValue("Phone"))

		record.Set("items", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Item", Schema: itemSchema},
			Elements: []domain.Value{
				domain.RecordValue{Record: item1},
				domain.RecordValue{Record: item2},
			},
		})
		recordSet.Add(record)

		filePath := tempFilePath(t)
		store := NewJSONStore(filePath)

		err := store.Store(recordSet)

		require.NoError(t, err)

		content := readFile(t, filePath)
		var result []map[string]any
		require.NoError(t, json.Unmarshal(content, &result))

		items := result[0]["items"].([]any)
		assert.Len(t, items, 2)
		assert.Equal(t, "Laptop", items[0].(map[string]any)["name"])
		assert.Equal(t, "Phone", items[1].(map[string]any)["name"])
	})
}

func tempFilePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "output.json")
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return content
}

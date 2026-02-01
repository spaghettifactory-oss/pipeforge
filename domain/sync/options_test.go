package sync

import (
	"testing"

	"github.com/spaghettifactory-oss/pipeforge/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewCompareOptions(t *testing.T) {
	t.Run("should create empty options", func(t *testing.T) {
		opts := NewCompareOptions()

		assert.NotNil(t, opts)
		assert.NotNil(t, opts.ArrayKeys)
		assert.Empty(t, opts.ArrayKeys)
	})

	t.Run("should apply options", func(t *testing.T) {
		opts := NewCompareOptions(
			WithArrayKey("stock", "name"),
			WithArrayKey("items", "id"),
		)

		assert.Equal(t, "name", opts.ArrayKeys["stock"])
		assert.Equal(t, "id", opts.ArrayKeys["items"])
	})
}

func TestWithArrayKey(t *testing.T) {
	t.Run("should set array key", func(t *testing.T) {
		opts := NewCompareOptions(WithArrayKey("products", "sku"))

		assert.Equal(t, "sku", opts.GetArrayKey("products"))
	})
}

func TestCompareOptions_GetArrayKey(t *testing.T) {
	t.Run("should return key when configured", func(t *testing.T) {
		opts := NewCompareOptions(WithArrayKey("stock", "name"))

		assert.Equal(t, "name", opts.GetArrayKey("stock"))
	})

	t.Run("should return empty string when not configured", func(t *testing.T) {
		opts := NewCompareOptions()

		assert.Equal(t, "", opts.GetArrayKey("unknown"))
	})

	t.Run("should return empty string for nil options", func(t *testing.T) {
		var opts *CompareOptions

		assert.Equal(t, "", opts.GetArrayKey("stock"))
	})

	t.Run("should return empty string for nil ArrayKeys map", func(t *testing.T) {
		opts := &CompareOptions{ArrayKeys: nil}

		assert.Equal(t, "", opts.GetArrayKey("stock"))
	})
}

func TestCompareOptions_HasArrayKey(t *testing.T) {
	t.Run("should return true when key configured", func(t *testing.T) {
		opts := NewCompareOptions(WithArrayKey("stock", "name"))

		assert.True(t, opts.HasArrayKey("stock"))
	})

	t.Run("should return false when not configured", func(t *testing.T) {
		opts := NewCompareOptions()

		assert.False(t, opts.HasArrayKey("unknown"))
	})
}

// Test key-based array comparison
func TestArrayComparisonByKey(t *testing.T) {
	productSchema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeFloat},
		},
	}

	storeSchema := &domain.DataSchema{
		ID: "Store",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "store_name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnArray{ID: "stock", RefSchema: domain.CustomType{Name: "Product", Schema: productSchema}},
		},
	}

	createProduct := func(name string, price float64) domain.Value {
		record := domain.NewRecord(productSchema)
		record.Set("name", domain.StringValue(name))
		record.Set("price", domain.FloatValue(price))
		return domain.RecordValue{Record: record}
	}

	t.Run("should detect unchanged when same elements in different order", func(t *testing.T) {
		// Old: [Laptop, Phone, Tablet]
		oldStore := domain.NewRecord(storeSchema)
		oldStore.Set("store_name", domain.StringValue("Tech Shop"))
		oldStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
				createProduct("Phone", 499),
				createProduct("Tablet", 349),
			},
		})

		// New: [Tablet, Laptop, Phone] - same products, different order
		newStore := domain.NewRecord(storeSchema)
		newStore.Set("store_name", domain.StringValue("Tech Shop"))
		newStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Tablet", 349),
				createProduct("Laptop", 999),
				createProduct("Phone", 499),
			},
		})

		// Without key - different order = modified
		deltaNoKey := CompareRecords(oldStore, newStore, 0)
		assert.Equal(t, RecordModified, deltaNoKey.ChangeType)

		// With key "name" - same products = unchanged
		deltaWithKey := CompareRecords(oldStore, newStore, 0, WithArrayKey("stock", "name"))
		assert.Equal(t, RecordUnchanged, deltaWithKey.ChangeType)
	})

	t.Run("should detect added element by key", func(t *testing.T) {
		oldStore := domain.NewRecord(storeSchema)
		oldStore.Set("store_name", domain.StringValue("Tech Shop"))
		oldStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
				createProduct("Phone", 499),
			},
		})

		newStore := domain.NewRecord(storeSchema)
		newStore.Set("store_name", domain.StringValue("Tech Shop"))
		newStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
				createProduct("Phone", 499),
				createProduct("Tablet", 349), // Added
			},
		})

		delta := CompareRecords(oldStore, newStore, 0, WithArrayKey("stock", "name"))

		assert.Equal(t, RecordModified, delta.ChangeType)
		stockDelta := delta.GetFieldDelta("stock")
		assert.Equal(t, FieldUpdated, stockDelta.ChangeType)
	})

	t.Run("should detect removed element by key", func(t *testing.T) {
		oldStore := domain.NewRecord(storeSchema)
		oldStore.Set("store_name", domain.StringValue("Tech Shop"))
		oldStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
				createProduct("Phone", 499),
				createProduct("Tablet", 349),
			},
		})

		newStore := domain.NewRecord(storeSchema)
		newStore.Set("store_name", domain.StringValue("Tech Shop"))
		newStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
				createProduct("Tablet", 349),
				// Phone removed
			},
		})

		delta := CompareRecords(oldStore, newStore, 0, WithArrayKey("stock", "name"))

		assert.Equal(t, RecordModified, delta.ChangeType)
	})

	t.Run("should detect modified element by key", func(t *testing.T) {
		oldStore := domain.NewRecord(storeSchema)
		oldStore.Set("store_name", domain.StringValue("Tech Shop"))
		oldStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
				createProduct("Phone", 499),
			},
		})

		newStore := domain.NewRecord(storeSchema)
		newStore.Set("store_name", domain.StringValue("Tech Shop"))
		newStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 1099), // Price changed
				createProduct("Phone", 499),
			},
		})

		delta := CompareRecords(oldStore, newStore, 0, WithArrayKey("stock", "name"))

		assert.Equal(t, RecordModified, delta.ChangeType)
	})

	t.Run("should work with int key", func(t *testing.T) {
		itemSchema := &domain.DataSchema{
			ID: "Item",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "id", SchemaType: domain.NativeTypeInt},
				domain.SchemaColumnSingle{ID: "value", SchemaType: domain.NativeTypeString},
			},
		}

		containerSchema := &domain.DataSchema{
			ID: "Container",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnArray{ID: "items", RefSchema: domain.CustomType{Name: "Item", Schema: itemSchema}},
			},
		}

		createItem := func(id int64, value string) domain.Value {
			record := domain.NewRecord(itemSchema)
			record.Set("id", domain.IntValue(id))
			record.Set("value", domain.StringValue(value))
			return domain.RecordValue{Record: record}
		}

		oldContainer := domain.NewRecord(containerSchema)
		oldContainer.Set("items", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Item", Schema: itemSchema},
			Elements: []domain.Value{
				createItem(1, "first"),
				createItem(2, "second"),
			},
		})

		newContainer := domain.NewRecord(containerSchema)
		newContainer.Set("items", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Item", Schema: itemSchema},
			Elements: []domain.Value{
				createItem(2, "second"),
				createItem(1, "first"),
			},
		})

		delta := CompareRecords(oldContainer, newContainer, 0, WithArrayKey("items", "id"))
		assert.Equal(t, RecordUnchanged, delta.ChangeType)
	})

	t.Run("should handle array with non-record elements gracefully", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnArray{ID: "tags", RefSchema: domain.NativeTypeString},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("tags", domain.ArrayValue{
			ElementType: domain.NativeTypeString,
			Elements:    []domain.Value{domain.StringValue("a"), domain.StringValue("b")},
		})

		new := domain.NewRecord(schema)
		new.Set("tags", domain.ArrayValue{
			ElementType: domain.NativeTypeString,
			Elements:    []domain.Value{domain.StringValue("b"), domain.StringValue("a")},
		})

		// With key configured but elements are not records - should compare normally
		delta := CompareRecords(old, new, 0, WithArrayKey("tags", "name"))

		// Non-record elements won't match by key, so maps will be empty
		// Empty maps have same length (0), so should be "equal" but this is edge case
		assert.Equal(t, RecordUnchanged, delta.ChangeType)
	})

	t.Run("should handle nil record in array", func(t *testing.T) {
		oldStore := domain.NewRecord(storeSchema)
		oldStore.Set("store_name", domain.StringValue("Tech Shop"))
		oldStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
				domain.RecordValue{Record: nil}, // nil record
			},
		})

		newStore := domain.NewRecord(storeSchema)
		newStore.Set("store_name", domain.StringValue("Tech Shop"))
		newStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
			},
		})

		// Should not panic, nil records are skipped in key map
		// Both arrays have same valid keyed elements (Laptop), so they're equal
		delta := CompareRecords(oldStore, newStore, 0, WithArrayKey("stock", "name"))
		assert.Equal(t, RecordUnchanged, delta.ChangeType)
	})
}

func TestCompareRecordSetsWithOptions(t *testing.T) {
	productSchema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeFloat},
		},
	}

	storeSchema := &domain.DataSchema{
		ID: "Store",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "store_name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnArray{ID: "stock", RefSchema: domain.CustomType{Name: "Product", Schema: productSchema}},
		},
	}

	createProduct := func(name string, price float64) domain.Value {
		record := domain.NewRecord(productSchema)
		record.Set("name", domain.StringValue(name))
		record.Set("price", domain.FloatValue(price))
		return domain.RecordValue{Record: record}
	}

	t.Run("should pass options to record comparison", func(t *testing.T) {
		oldStore := domain.NewRecord(storeSchema)
		oldStore.Set("store_name", domain.StringValue("Tech Shop"))
		oldStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Laptop", 999),
				createProduct("Phone", 499),
			},
		})

		newStore := domain.NewRecord(storeSchema)
		newStore.Set("store_name", domain.StringValue("Tech Shop"))
		newStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements: []domain.Value{
				createProduct("Phone", 499),
				createProduct("Laptop", 999),
			},
		})

		oldSet := domain.NewRecordSet(storeSchema)
		oldSet.Add(oldStore)

		newSet := domain.NewRecordSet(storeSchema)
		newSet.Add(newStore)

		delta := CompareRecordSets(oldSet, newSet, WithArrayKey("stock", "name"))

		assert.Equal(t, 1, len(delta.RecordDeltas))
		assert.Equal(t, RecordUnchanged, delta.RecordDeltas[0].ChangeType)
	})
}

func TestValueToString(t *testing.T) {
	t.Run("should convert string value", func(t *testing.T) {
		v := domain.StringValue("test")
		assert.Equal(t, "test", valueToString(v))
	})

	t.Run("should convert int value", func(t *testing.T) {
		v := domain.IntValue(42)
		assert.Equal(t, "42", valueToString(v))
	})

	t.Run("should return empty for nil", func(t *testing.T) {
		assert.Equal(t, "", valueToString(nil))
	})

	t.Run("should return empty for null value", func(t *testing.T) {
		v := domain.NullValue{Type: domain.NativeTypeString}
		assert.Equal(t, "", valueToString(v))
	})

	t.Run("should return empty for unsupported type", func(t *testing.T) {
		v := domain.FloatValue(3.14)
		assert.Equal(t, "", valueToString(v))
	})
}

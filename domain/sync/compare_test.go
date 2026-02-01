package sync

import (
	"testing"
	"time"

	"github.com/spaghettifactory-oss/pipeforge/domain"
	"github.com/stretchr/testify/assert"
)

func createTestSchema() *domain.DataSchema {
	return &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeFloat},
			domain.SchemaColumnSingle{ID: "quantity", SchemaType: domain.NativeTypeInt},
		},
	}
}

func createTestRecord(schema *domain.DataSchema, name string, price float64) *domain.Record {
	record := domain.NewRecord(schema)
	record.Set("name", domain.StringValue(name))
	record.Set("price", domain.FloatValue(price))
	return record
}

func createTestRecordWithQuantity(schema *domain.DataSchema, name string, price float64, quantity int64) *domain.Record {
	record := domain.NewRecord(schema)
	record.Set("name", domain.StringValue(name))
	record.Set("price", domain.FloatValue(price))
	record.Set("quantity", domain.IntValue(quantity))
	return record
}

// === FieldChangeType Tests ===

func TestFieldChangeType_String(t *testing.T) {
	t.Run("should return correct string for each type", func(t *testing.T) {
		assert.Equal(t, "unchanged", FieldUnchanged.String())
		assert.Equal(t, "added", FieldAdded.String())
		assert.Equal(t, "updated", FieldUpdated.String())
		assert.Equal(t, "deleted", FieldDeleted.String())
	})
}

// === RecordChangeType Tests ===

func TestRecordChangeType_String(t *testing.T) {
	t.Run("should return correct string for each type", func(t *testing.T) {
		assert.Equal(t, "unchanged", RecordUnchanged.String())
		assert.Equal(t, "added", RecordAdded.String())
		assert.Equal(t, "modified", RecordModified.String())
		assert.Equal(t, "deleted", RecordDeleted.String())
	})
}

// === CompareRecords Tests ===

func TestCompareRecords(t *testing.T) {
	t.Run("should detect added record when old is nil", func(t *testing.T) {
		schema := createTestSchema()
		newRecord := createTestRecord(schema, "Product", 100)

		delta := CompareRecords(nil, newRecord, 0)

		assert.Equal(t, RecordAdded, delta.ChangeType)
		assert.Nil(t, delta.OldRecord)
		assert.Equal(t, newRecord, delta.NewRecord)
		assert.Equal(t, 0, delta.Index)
	})

	t.Run("should detect deleted record when new is nil", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecord(schema, "Product", 100)

		delta := CompareRecords(oldRecord, nil, 5)

		assert.Equal(t, RecordDeleted, delta.ChangeType)
		assert.Equal(t, oldRecord, delta.OldRecord)
		assert.Nil(t, delta.NewRecord)
		assert.Equal(t, 5, delta.Index)
	})

	t.Run("should detect unchanged record when values are the same", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecord(schema, "Product", 100)
		newRecord := createTestRecord(schema, "Product", 100)

		delta := CompareRecords(oldRecord, newRecord, 0)

		assert.Equal(t, RecordUnchanged, delta.ChangeType)
		assert.False(t, delta.HasChanges())
	})

	t.Run("should detect modified record when values differ", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecord(schema, "Product", 100)
		newRecord := createTestRecord(schema, "Product", 150)

		delta := CompareRecords(oldRecord, newRecord, 0)

		assert.Equal(t, RecordModified, delta.ChangeType)
		assert.True(t, delta.HasChanges())
		assert.Contains(t, delta.UpdatedFields(), "price")
	})

	t.Run("should detect added field", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecord(schema, "Product", 100)
		newRecord := createTestRecordWithQuantity(schema, "Product", 100, 50)

		delta := CompareRecords(oldRecord, newRecord, 0)

		assert.Equal(t, RecordModified, delta.ChangeType)
		assert.Contains(t, delta.AddedFields(), "quantity")
	})

	t.Run("should detect deleted field", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecordWithQuantity(schema, "Product", 100, 50)
		newRecord := createTestRecord(schema, "Product", 100)

		delta := CompareRecords(oldRecord, newRecord, 0)

		assert.Equal(t, RecordModified, delta.ChangeType)
		assert.Contains(t, delta.DeletedFields(), "quantity")
	})

	t.Run("should return unchanged when both are nil", func(t *testing.T) {
		delta := CompareRecords(nil, nil, 0)

		assert.Equal(t, RecordUnchanged, delta.ChangeType)
	})
}

// === RecordDelta Methods Tests ===

func TestRecordDelta_GetFieldDelta(t *testing.T) {
	t.Run("should return field delta by column ID", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecord(schema, "Old", 100)
		newRecord := createTestRecord(schema, "New", 150)

		delta := CompareRecords(oldRecord, newRecord, 0)

		nameDelta := delta.GetFieldDelta("name")
		assert.NotNil(t, nameDelta)
		assert.Equal(t, FieldUpdated, nameDelta.ChangeType)

		priceDelta := delta.GetFieldDelta("price")
		assert.NotNil(t, priceDelta)
		assert.Equal(t, FieldUpdated, priceDelta.ChangeType)
	})

	t.Run("should return nil for non-existent column", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecord(schema, "Product", 100)
		newRecord := createTestRecord(schema, "Product", 100)

		delta := CompareRecords(oldRecord, newRecord, 0)

		assert.Nil(t, delta.GetFieldDelta("nonexistent"))
	})
}

func TestRecordDelta_ChangedFields(t *testing.T) {
	t.Run("should return all changed fields", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecord(schema, "Old", 100)
		newRecord := createTestRecordWithQuantity(schema, "New", 150, 10)

		delta := CompareRecords(oldRecord, newRecord, 0)

		changed := delta.ChangedFields()
		assert.Contains(t, changed, "name")
		assert.Contains(t, changed, "price")
		assert.Contains(t, changed, "quantity")
	})

	t.Run("should return empty slice when no changes", func(t *testing.T) {
		schema := createTestSchema()
		oldRecord := createTestRecord(schema, "Product", 100)
		newRecord := createTestRecord(schema, "Product", 100)

		delta := CompareRecords(oldRecord, newRecord, 0)

		assert.Empty(t, delta.ChangedFields())
	})
}

// === CompareRecordSets Tests ===

func TestCompareRecordSets(t *testing.T) {
	t.Run("should compare records by index", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))
		oldSet.Add(createTestRecord(schema, "B", 200))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))
		newSet.Add(createTestRecord(schema, "B", 250)) // Modified

		delta := CompareRecordSets(oldSet, newSet)

		assert.Equal(t, 2, len(delta.RecordDeltas))
		assert.Equal(t, RecordUnchanged, delta.RecordDeltas[0].ChangeType)
		assert.Equal(t, RecordModified, delta.RecordDeltas[1].ChangeType)
	})

	t.Run("should detect added records when new set is larger", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))
		newSet.Add(createTestRecord(schema, "B", 200)) // Added

		delta := CompareRecordSets(oldSet, newSet)

		assert.Equal(t, 2, len(delta.RecordDeltas))
		assert.Equal(t, RecordUnchanged, delta.RecordDeltas[0].ChangeType)
		assert.Equal(t, RecordAdded, delta.RecordDeltas[1].ChangeType)
	})

	t.Run("should detect deleted records when old set is larger", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))
		oldSet.Add(createTestRecord(schema, "B", 200))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))

		delta := CompareRecordSets(oldSet, newSet)

		assert.Equal(t, 2, len(delta.RecordDeltas))
		assert.Equal(t, RecordUnchanged, delta.RecordDeltas[0].ChangeType)
		assert.Equal(t, RecordDeleted, delta.RecordDeltas[1].ChangeType)
	})

	t.Run("should handle nil old set", func(t *testing.T) {
		schema := createTestSchema()

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))

		delta := CompareRecordSets(nil, newSet)

		assert.Equal(t, 1, len(delta.RecordDeltas))
		assert.Equal(t, RecordAdded, delta.RecordDeltas[0].ChangeType)
	})

	t.Run("should handle nil new set", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))

		delta := CompareRecordSets(oldSet, nil)

		assert.Equal(t, 1, len(delta.RecordDeltas))
		assert.Equal(t, RecordDeleted, delta.RecordDeltas[0].ChangeType)
	})

	t.Run("should handle both nil sets", func(t *testing.T) {
		delta := CompareRecordSets(nil, nil)

		assert.Empty(t, delta.RecordDeltas)
		assert.False(t, delta.HasChanges())
	})
}

// === RecordSetDelta Methods Tests ===

func TestRecordSetDelta_HasChanges(t *testing.T) {
	t.Run("should return true when there are changes", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 150))

		delta := CompareRecordSets(oldSet, newSet)

		assert.True(t, delta.HasChanges())
	})

	t.Run("should return false when no changes", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))

		delta := CompareRecordSets(oldSet, newSet)

		assert.False(t, delta.HasChanges())
	})
}

func TestRecordSetDelta_Summary(t *testing.T) {
	t.Run("should return correct summary", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100)) // Unchanged
		oldSet.Add(createTestRecord(schema, "B", 200)) // Modified
		oldSet.Add(createTestRecord(schema, "C", 300)) // Deleted

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100)) // Unchanged
		newSet.Add(createTestRecord(schema, "B", 250)) // Modified
		newSet.Add(createTestRecord(schema, "D", 400)) // Added (replaces C at index 2)

		delta := CompareRecordSets(oldSet, newSet)
		summary := delta.Summary()

		assert.Equal(t, 3, summary.Total)
		assert.Equal(t, 1, summary.Unchanged)
		assert.Equal(t, 2, summary.Modified) // B modified, C->D is also modification at index 2
	})

	t.Run("should count added and deleted correctly", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))
		oldSet.Add(createTestRecord(schema, "B", 200))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))
		newSet.Add(createTestRecord(schema, "B", 200))
		newSet.Add(createTestRecord(schema, "C", 300)) // Added

		delta := CompareRecordSets(oldSet, newSet)
		summary := delta.Summary()

		assert.Equal(t, 3, summary.Total)
		assert.Equal(t, 2, summary.Unchanged)
		assert.Equal(t, 1, summary.Added)
		assert.Equal(t, 0, summary.Deleted)
	})
}

func TestRecordSetDelta_FilterMethods(t *testing.T) {
	t.Run("should filter added records", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))
		newSet.Add(createTestRecord(schema, "B", 200))

		delta := CompareRecordSets(oldSet, newSet)
		added := delta.AddedRecords()

		assert.Equal(t, 1, len(added))
		assert.Equal(t, 1, added[0].Index)
	})

	t.Run("should filter modified records", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))
		oldSet.Add(createTestRecord(schema, "B", 200))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))
		newSet.Add(createTestRecord(schema, "B", 250))

		delta := CompareRecordSets(oldSet, newSet)
		modified := delta.ModifiedRecords()

		assert.Equal(t, 1, len(modified))
		assert.Equal(t, 1, modified[0].Index)
	})

	t.Run("should filter deleted records", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))
		oldSet.Add(createTestRecord(schema, "B", 200))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))

		delta := CompareRecordSets(oldSet, newSet)
		deleted := delta.DeletedRecords()

		assert.Equal(t, 1, len(deleted))
		assert.Equal(t, 1, deleted[0].Index)
	})

	t.Run("should filter unchanged records", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))
		oldSet.Add(createTestRecord(schema, "B", 200))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))
		newSet.Add(createTestRecord(schema, "B", 250))

		delta := CompareRecordSets(oldSet, newSet)
		unchanged := delta.UnchangedRecords()

		assert.Equal(t, 1, len(unchanged))
		assert.Equal(t, 0, unchanged[0].Index)
	})
}

func TestRecordSetDelta_Get(t *testing.T) {
	t.Run("should return record delta by index", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))
		oldSet.Add(createTestRecord(schema, "B", 200))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))
		newSet.Add(createTestRecord(schema, "B", 250))

		delta := CompareRecordSets(oldSet, newSet)

		rd := delta.Get(1)
		assert.NotNil(t, rd)
		assert.Equal(t, RecordModified, rd.ChangeType)
	})

	t.Run("should return nil for non-existent index", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))

		delta := CompareRecordSets(oldSet, newSet)

		assert.Nil(t, delta.Get(5))
	})
}

// === Value Comparison Tests ===

func TestValuesEqual(t *testing.T) {
	t.Run("should compare string values", func(t *testing.T) {
		schema := createTestSchema()
		old := createTestRecord(schema, "Hello", 100)
		new := createTestRecord(schema, "Hello", 100)

		delta := CompareRecords(old, new, 0)

		nameDelta := delta.GetFieldDelta("name")
		assert.Equal(t, FieldUnchanged, nameDelta.ChangeType)
	})

	t.Run("should compare int values", func(t *testing.T) {
		schema := createTestSchema()
		old := createTestRecordWithQuantity(schema, "A", 100, 50)
		new := createTestRecordWithQuantity(schema, "A", 100, 50)

		delta := CompareRecords(old, new, 0)

		qtyDelta := delta.GetFieldDelta("quantity")
		assert.Equal(t, FieldUnchanged, qtyDelta.ChangeType)
	})

	t.Run("should compare float values", func(t *testing.T) {
		schema := createTestSchema()
		old := createTestRecord(schema, "A", 99.99)
		new := createTestRecord(schema, "A", 99.99)

		delta := CompareRecords(old, new, 0)

		priceDelta := delta.GetFieldDelta("price")
		assert.Equal(t, FieldUnchanged, priceDelta.ChangeType)
	})

	t.Run("should compare bool values", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "active", SchemaType: domain.NativeTypeBool},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("active", domain.BoolValue(true))

		new := domain.NewRecord(schema)
		new.Set("active", domain.BoolValue(true))

		delta := CompareRecords(old, new, 0)

		activeDelta := delta.GetFieldDelta("active")
		assert.Equal(t, FieldUnchanged, activeDelta.ChangeType)
	})

	t.Run("should detect bool value change", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "active", SchemaType: domain.NativeTypeBool},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("active", domain.BoolValue(true))

		new := domain.NewRecord(schema)
		new.Set("active", domain.BoolValue(false))

		delta := CompareRecords(old, new, 0)

		activeDelta := delta.GetFieldDelta("active")
		assert.Equal(t, FieldUpdated, activeDelta.ChangeType)
	})
}

func TestDateValueComparison(t *testing.T) {
	t.Run("should compare date values as equal", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "created", SchemaType: domain.NativeTypeDate},
			},
		}

		date := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

		old := domain.NewRecord(schema)
		old.Set("created", domain.DateValue(date))

		new := domain.NewRecord(schema)
		new.Set("created", domain.DateValue(date))

		delta := CompareRecords(old, new, 0)

		dateDelta := delta.GetFieldDelta("created")
		assert.Equal(t, FieldUnchanged, dateDelta.ChangeType)
	})

	t.Run("should detect date value change", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "created", SchemaType: domain.NativeTypeDate},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("created", domain.DateValue(time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)))

		new := domain.NewRecord(schema)
		new.Set("created", domain.DateValue(time.Date(2024, 1, 16, 10, 30, 0, 0, time.UTC)))

		delta := CompareRecords(old, new, 0)

		dateDelta := delta.GetFieldDelta("created")
		assert.Equal(t, FieldUpdated, dateDelta.ChangeType)
	})
}

func TestRecordValueComparison(t *testing.T) {
	t.Run("should compare nested record values as equal", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "city", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "zip", SchemaType: domain.NativeTypeString},
			},
		}

		personSchema := &domain.DataSchema{
			ID: "Person",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		address1 := domain.NewRecord(addressSchema)
		address1.Set("city", domain.StringValue("Paris"))
		address1.Set("zip", domain.StringValue("75001"))

		address2 := domain.NewRecord(addressSchema)
		address2.Set("city", domain.StringValue("Paris"))
		address2.Set("zip", domain.StringValue("75001"))

		old := domain.NewRecord(personSchema)
		old.Set("name", domain.StringValue("John"))
		old.Set("address", domain.RecordValue{Record: address1})

		new := domain.NewRecord(personSchema)
		new.Set("name", domain.StringValue("John"))
		new.Set("address", domain.RecordValue{Record: address2})

		delta := CompareRecords(old, new, 0)

		assert.Equal(t, RecordUnchanged, delta.ChangeType)
		addressDelta := delta.GetFieldDelta("address")
		assert.Equal(t, FieldUnchanged, addressDelta.ChangeType)
	})

	t.Run("should detect nested record value change", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "city", SchemaType: domain.NativeTypeString},
			},
		}

		personSchema := &domain.DataSchema{
			ID: "Person",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		address1 := domain.NewRecord(addressSchema)
		address1.Set("city", domain.StringValue("Paris"))

		address2 := domain.NewRecord(addressSchema)
		address2.Set("city", domain.StringValue("Lyon"))

		old := domain.NewRecord(personSchema)
		old.Set("name", domain.StringValue("John"))
		old.Set("address", domain.RecordValue{Record: address1})

		new := domain.NewRecord(personSchema)
		new.Set("name", domain.StringValue("John"))
		new.Set("address", domain.RecordValue{Record: address2})

		delta := CompareRecords(old, new, 0)

		assert.Equal(t, RecordModified, delta.ChangeType)
		addressDelta := delta.GetFieldDelta("address")
		assert.Equal(t, FieldUpdated, addressDelta.ChangeType)
	})

	t.Run("should handle nil nested records", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "city", SchemaType: domain.NativeTypeString},
			},
		}

		personSchema := &domain.DataSchema{
			ID: "Person",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		address := domain.NewRecord(addressSchema)
		address.Set("city", domain.StringValue("Paris"))

		old := domain.NewRecord(personSchema)
		old.Set("address", domain.RecordValue{Record: nil})

		new := domain.NewRecord(personSchema)
		new.Set("address", domain.RecordValue{Record: address})

		delta := CompareRecords(old, new, 0)

		addressDelta := delta.GetFieldDelta("address")
		assert.Equal(t, FieldAdded, addressDelta.ChangeType)
	})

	t.Run("should compare nested records with different column counts", func(t *testing.T) {
		schema1 := &domain.DataSchema{
			ID: "Data",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "a", SchemaType: domain.NativeTypeString},
			},
		}

		schema2 := &domain.DataSchema{
			ID: "Data",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "a", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "b", SchemaType: domain.NativeTypeString},
			},
		}

		parentSchema := &domain.DataSchema{
			ID: "Parent",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "data", SchemaType: domain.CustomType{Name: "Data", Schema: schema1}},
			},
		}

		rec1 := domain.NewRecord(schema1)
		rec1.Set("a", domain.StringValue("value"))

		rec2 := domain.NewRecord(schema2)
		rec2.Set("a", domain.StringValue("value"))
		rec2.Set("b", domain.StringValue("extra"))

		old := domain.NewRecord(parentSchema)
		old.Set("data", domain.RecordValue{Record: rec1})

		new := domain.NewRecord(parentSchema)
		new.Set("data", domain.RecordValue{Record: rec2})

		delta := CompareRecords(old, new, 0)

		dataDelta := delta.GetFieldDelta("data")
		assert.Equal(t, FieldUpdated, dataDelta.ChangeType)
	})

	t.Run("should compare nested records with missing column in one", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Data",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "a", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "b", SchemaType: domain.NativeTypeString},
			},
		}

		parentSchema := &domain.DataSchema{
			ID: "Parent",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "data", SchemaType: domain.CustomType{Name: "Data", Schema: schema}},
			},
		}

		rec1 := domain.NewRecord(schema)
		rec1.Set("a", domain.StringValue("value"))
		rec1.Set("b", domain.StringValue("other"))

		rec2 := domain.NewRecord(schema)
		rec2.Set("a", domain.StringValue("value"))
		// b is missing

		old := domain.NewRecord(parentSchema)
		old.Set("data", domain.RecordValue{Record: rec1})

		new := domain.NewRecord(parentSchema)
		new.Set("data", domain.RecordValue{Record: rec2})

		delta := CompareRecords(old, new, 0)

		dataDelta := delta.GetFieldDelta("data")
		assert.Equal(t, FieldUpdated, dataDelta.ChangeType)
	})
}

func TestUnknownChangeTypeString(t *testing.T) {
	t.Run("should return unknown for invalid FieldChangeType", func(t *testing.T) {
		invalidType := FieldChangeType(99)
		assert.Equal(t, "unknown", invalidType.String())
	})

	t.Run("should return unknown for invalid RecordChangeType", func(t *testing.T) {
		invalidType := RecordChangeType(99)
		assert.Equal(t, "unknown", invalidType.String())
	})
}

func TestDifferentValueTypes(t *testing.T) {
	t.Run("should detect change when value types differ", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "value", SchemaType: domain.NativeTypeString},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("value", domain.StringValue("123"))

		new := domain.NewRecord(schema)
		new.Set("value", domain.IntValue(123))

		delta := CompareRecords(old, new, 0)

		valueDelta := delta.GetFieldDelta("value")
		assert.Equal(t, FieldUpdated, valueDelta.ChangeType)
	})
}

func TestNullValueComparison(t *testing.T) {
	t.Run("should handle NullValue comparison", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "value", SchemaType: domain.NativeTypeString},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("value", domain.NullValue{Type: domain.NativeTypeString})

		new := domain.NewRecord(schema)
		new.Set("value", domain.NullValue{Type: domain.NativeTypeString})

		delta := CompareRecords(old, new, 0)

		valueDelta := delta.GetFieldDelta("value")
		assert.Equal(t, FieldUnchanged, valueDelta.ChangeType)
	})

	t.Run("should detect change from null to value", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "value", SchemaType: domain.NativeTypeString},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("value", domain.NullValue{Type: domain.NativeTypeString})

		new := domain.NewRecord(schema)
		new.Set("value", domain.StringValue("hello"))

		delta := CompareRecords(old, new, 0)

		valueDelta := delta.GetFieldDelta("value")
		assert.Equal(t, FieldAdded, valueDelta.ChangeType)
	})
}

// customValue is a test type that implements domain.Value but is not handled by valuesEqual
type customValue struct{}

func (c customValue) GetType() domain.SchemaType { return domain.NativeTypeString }
func (c customValue) IsNull() bool               { return false }

func TestUnknownValueType(t *testing.T) {
	t.Run("should detect change for unknown value types", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "custom", SchemaType: domain.NativeTypeString},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("custom", customValue{})

		new := domain.NewRecord(schema)
		new.Set("custom", customValue{})

		delta := CompareRecords(old, new, 0)

		customDelta := delta.GetFieldDelta("custom")
		// Unknown types return false from valuesEqual, so they're considered updated
		assert.Equal(t, FieldUpdated, customDelta.ChangeType)
	})
}

// customRecordValue implements domain.Value and returns same type as RecordValue
// but fails type assertion
type customRecordValue struct{}

func (c customRecordValue) GetType() domain.SchemaType {
	return domain.CustomType{Name: "Fake", Schema: nil}
}
func (c customRecordValue) IsNull() bool { return false }

// fakeRecordValue looks like RecordValue (same GetType) but isn't
type fakeRecordValue struct {
	schema *domain.DataSchema
}

func (f fakeRecordValue) GetType() domain.SchemaType {
	return domain.CustomType{Name: f.schema.ID, Schema: f.schema}
}
func (f fakeRecordValue) IsNull() bool { return false }

func TestValuesEqualTypeAssertionFailure(t *testing.T) {
	t.Run("should return false when RecordValue type assertion fails", func(t *testing.T) {
		nestedSchema := &domain.DataSchema{
			ID: "Nested",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "value", SchemaType: domain.NativeTypeString},
			},
		}

		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "data", SchemaType: domain.CustomType{Name: "Nested", Schema: nestedSchema}},
			},
		}

		nestedRecord := domain.NewRecord(nestedSchema)
		nestedRecord.Set("value", domain.StringValue("test"))

		old := domain.NewRecord(schema)
		old.Set("data", domain.RecordValue{Record: nestedRecord})

		new := domain.NewRecord(schema)
		new.Set("data", fakeRecordValue{schema: nestedSchema})

		delta := CompareRecords(old, new, 0)

		dataDelta := delta.GetFieldDelta("data")
		assert.Equal(t, FieldUpdated, dataDelta.ChangeType)
	})
}

func TestRecordsEqual(t *testing.T) {
	t.Run("should return true when both records are nil", func(t *testing.T) {
		assert.True(t, RecordsEqual(nil, nil))
	})

	t.Run("should return false when only first record is nil", func(t *testing.T) {
		schema := createTestSchema()
		record := createTestRecord(schema, "Test", 100)
		assert.False(t, RecordsEqual(nil, record))
	})

	t.Run("should return false when only second record is nil", func(t *testing.T) {
		schema := createTestSchema()
		record := createTestRecord(schema, "Test", 100)
		assert.False(t, RecordsEqual(record, nil))
	})

	t.Run("should return true when records have same values", func(t *testing.T) {
		schema := createTestSchema()
		a := createTestRecord(schema, "Test", 100)
		b := createTestRecord(schema, "Test", 100)
		assert.True(t, RecordsEqual(a, b))
	})

	t.Run("should return false when records have different values", func(t *testing.T) {
		schema := createTestSchema()
		a := createTestRecord(schema, "Test", 100)
		b := createTestRecord(schema, "Test", 200)
		assert.False(t, RecordsEqual(a, b))
	})

	t.Run("should return false when records have different column counts", func(t *testing.T) {
		schema := createTestSchema()
		a := createTestRecord(schema, "Test", 100)
		b := createTestRecordWithQuantity(schema, "Test", 100, 50)
		assert.False(t, RecordsEqual(a, b))
	})

	t.Run("should return false when column is missing in second record", func(t *testing.T) {
		schema := createTestSchema()
		a := domain.NewRecord(schema)
		a.Set("name", domain.StringValue("Test"))
		a.Set("price", domain.FloatValue(100))

		b := domain.NewRecord(schema)
		b.Set("name", domain.StringValue("Test"))
		b.Set("quantity", domain.IntValue(50)) // different column

		assert.False(t, RecordsEqual(a, b))
	})
}

func TestBothNilNestedRecords(t *testing.T) {
	t.Run("should compare both nil nested records as equal", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "city", SchemaType: domain.NativeTypeString},
			},
		}

		personSchema := &domain.DataSchema{
			ID: "Person",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		old := domain.NewRecord(personSchema)
		old.Set("address", domain.RecordValue{Record: nil})

		new := domain.NewRecord(personSchema)
		new.Set("address", domain.RecordValue{Record: nil})

		delta := CompareRecords(old, new, 0)

		addressDelta := delta.GetFieldDelta("address")
		assert.Equal(t, FieldUnchanged, addressDelta.ChangeType)
	})

	t.Run("should detect change when only old nested record is nil", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "city", SchemaType: domain.NativeTypeString},
			},
		}

		personSchema := &domain.DataSchema{
			ID: "Person",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		address := domain.NewRecord(addressSchema)
		address.Set("city", domain.StringValue("Paris"))

		old := domain.NewRecord(personSchema)
		old.Set("address", domain.RecordValue{Record: nil})

		new := domain.NewRecord(personSchema)
		new.Set("address", domain.RecordValue{Record: address})

		delta := CompareRecords(old, new, 0)

		addressDelta := delta.GetFieldDelta("address")
		assert.Equal(t, FieldAdded, addressDelta.ChangeType)
	})

	t.Run("should detect change when only new nested record is nil", func(t *testing.T) {
		addressSchema := &domain.DataSchema{
			ID: "Address",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "city", SchemaType: domain.NativeTypeString},
			},
		}

		personSchema := &domain.DataSchema{
			ID: "Person",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "address", SchemaType: domain.CustomType{Name: "Address", Schema: addressSchema}},
			},
		}

		address := domain.NewRecord(addressSchema)
		address.Set("city", domain.StringValue("Paris"))

		old := domain.NewRecord(personSchema)
		old.Set("address", domain.RecordValue{Record: address})

		new := domain.NewRecord(personSchema)
		new.Set("address", domain.RecordValue{Record: nil})

		delta := CompareRecords(old, new, 0)

		addressDelta := delta.GetFieldDelta("address")
		assert.Equal(t, FieldDeleted, addressDelta.ChangeType)
	})
}

func TestRecordSetDeltaSummaryAllCases(t *testing.T) {
	t.Run("should count all change types in summary", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100)) // Index 0: Unchanged
		oldSet.Add(createTestRecord(schema, "B", 200)) // Index 1: Modified
		oldSet.Add(createTestRecord(schema, "C", 300)) // Index 2: Modified (C->D)

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100)) // Index 0: Unchanged
		newSet.Add(createTestRecord(schema, "B", 250)) // Index 1: Modified
		newSet.Add(createTestRecord(schema, "D", 400)) // Index 2: Modified (different from C)

		delta := CompareRecordSets(oldSet, newSet)
		summary := delta.Summary()

		assert.Equal(t, 3, summary.Total)
		assert.Equal(t, 1, summary.Unchanged)
		assert.Equal(t, 2, summary.Modified)
	})

	t.Run("should handle deleted records in summary", func(t *testing.T) {
		schema := createTestSchema()

		oldSet := domain.NewRecordSet(schema)
		oldSet.Add(createTestRecord(schema, "A", 100))
		oldSet.Add(createTestRecord(schema, "B", 200))
		oldSet.Add(createTestRecord(schema, "C", 300))

		newSet := domain.NewRecordSet(schema)
		newSet.Add(createTestRecord(schema, "A", 100))

		delta := CompareRecordSets(oldSet, newSet)
		summary := delta.Summary()

		assert.Equal(t, 3, summary.Total)
		assert.Equal(t, 1, summary.Unchanged)
		assert.Equal(t, 2, summary.Deleted)
		assert.Equal(t, 0, summary.Added)
		assert.Equal(t, 0, summary.Modified)
	})
}

func TestArrayValueComparison(t *testing.T) {
	t.Run("should compare array values", func(t *testing.T) {
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
			Elements:    []domain.Value{domain.StringValue("a"), domain.StringValue("b")},
		})

		delta := CompareRecords(old, new, 0)

		tagsDelta := delta.GetFieldDelta("tags")
		assert.Equal(t, FieldUnchanged, tagsDelta.ChangeType)
	})

	t.Run("should detect array value change", func(t *testing.T) {
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
			Elements:    []domain.Value{domain.StringValue("a"), domain.StringValue("c")}, // Changed
		})

		delta := CompareRecords(old, new, 0)

		tagsDelta := delta.GetFieldDelta("tags")
		assert.Equal(t, FieldUpdated, tagsDelta.ChangeType)
	})

	t.Run("should detect array length change", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnArray{ID: "tags", RefSchema: domain.NativeTypeString},
			},
		}

		old := domain.NewRecord(schema)
		old.Set("tags", domain.ArrayValue{
			ElementType: domain.NativeTypeString,
			Elements:    []domain.Value{domain.StringValue("a")},
		})

		new := domain.NewRecord(schema)
		new.Set("tags", domain.ArrayValue{
			ElementType: domain.NativeTypeString,
			Elements:    []domain.Value{domain.StringValue("a"), domain.StringValue("b")},
		})

		delta := CompareRecords(old, new, 0)

		tagsDelta := delta.GetFieldDelta("tags")
		assert.Equal(t, FieldUpdated, tagsDelta.ChangeType)
	})
}

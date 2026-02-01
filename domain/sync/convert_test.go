package sync

import (
	"testing"

	"github.com/spaghettifactory-oss/pipeforge/domain"
	"github.com/stretchr/testify/assert"
)

func TestRecordSetDeltaToRecordSet(t *testing.T) {
	t.Run("should convert delta to recordset", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
				domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeFloat},
			},
		}

		oldRecord := domain.NewRecord(schema)
		oldRecord.Set("name", domain.StringValue("Laptop"))
		oldRecord.Set("price", domain.FloatValue(999))

		newRecord := domain.NewRecord(schema)
		newRecord.Set("name", domain.StringValue("Laptop"))
		newRecord.Set("price", domain.FloatValue(1099))

		delta := &RecordSetDelta{
			Schema: schema,
			RecordDeltas: []RecordDelta{
				{
					Index:      0,
					ChangeType: RecordModified,
					OldRecord:  oldRecord,
					NewRecord:  newRecord,
				},
			},
		}

		result := delta.ToRecordSet()

		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result.Records))
		assert.Equal(t, "Delta", result.Schema.ID)

		record := result.Records[0]
		assert.Equal(t, int64(0), record.GetInt("index"))
		assert.Equal(t, "modified", record.GetString("change_type"))

		oldVal := record.Get("old").(domain.RecordValue)
		assert.Equal(t, "Laptop", oldVal.Record.GetString("name"))
		assert.Equal(t, float64(999), oldVal.Record.GetFloat("price"))

		newVal := record.Get("new").(domain.RecordValue)
		assert.Equal(t, float64(1099), newVal.Record.GetFloat("price"))
	})

	t.Run("should handle added record", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}

		newRecord := domain.NewRecord(schema)
		newRecord.Set("name", domain.StringValue("Phone"))

		delta := &RecordSetDelta{
			Schema: schema,
			RecordDeltas: []RecordDelta{
				{
					Index:      0,
					ChangeType: RecordAdded,
					OldRecord:  nil,
					NewRecord:  newRecord,
				},
			},
		}

		result := delta.ToRecordSet()

		record := result.Records[0]
		assert.Equal(t, "added", record.GetString("change_type"))

		oldVal := record.Get("old").(domain.RecordValue)
		assert.Nil(t, oldVal.Record)

		newVal := record.Get("new").(domain.RecordValue)
		assert.Equal(t, "Phone", newVal.Record.GetString("name"))
	})

	t.Run("should handle deleted record", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}

		oldRecord := domain.NewRecord(schema)
		oldRecord.Set("name", domain.StringValue("Tablet"))

		delta := &RecordSetDelta{
			Schema: schema,
			RecordDeltas: []RecordDelta{
				{
					Index:      0,
					ChangeType: RecordDeleted,
					OldRecord:  oldRecord,
					NewRecord:  nil,
				},
			},
		}

		result := delta.ToRecordSet()

		record := result.Records[0]
		assert.Equal(t, "deleted", record.GetString("change_type"))

		oldVal := record.Get("old").(domain.RecordValue)
		assert.Equal(t, "Tablet", oldVal.Record.GetString("name"))

		newVal := record.Get("new").(domain.RecordValue)
		assert.Nil(t, newVal.Record)
	})

	t.Run("should handle nil schema", func(t *testing.T) {
		delta := &RecordSetDelta{
			Schema: nil,
			RecordDeltas: []RecordDelta{
				{
					Index:      0,
					ChangeType: RecordUnchanged,
					OldRecord:  nil,
					NewRecord:  nil,
				},
			},
		}

		result := delta.ToRecordSet()

		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result.Records))
	})

	t.Run("should handle empty delta", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}

		delta := &RecordSetDelta{
			Schema:       schema,
			RecordDeltas: []RecordDelta{},
		}

		result := delta.ToRecordSet()

		assert.NotNil(t, result)
		assert.Equal(t, 0, len(result.Records))
	})

	t.Run("should handle multiple deltas", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}

		delta := &RecordSetDelta{
			Schema: schema,
			RecordDeltas: []RecordDelta{
				{Index: 0, ChangeType: RecordUnchanged},
				{Index: 1, ChangeType: RecordModified},
				{Index: 2, ChangeType: RecordDeleted},
				{Index: 3, ChangeType: RecordAdded},
			},
		}

		result := delta.ToRecordSet()

		assert.Equal(t, 4, len(result.Records))
		assert.Equal(t, "unchanged", result.Records[0].GetString("change_type"))
		assert.Equal(t, "modified", result.Records[1].GetString("change_type"))
		assert.Equal(t, "deleted", result.Records[2].GetString("change_type"))
		assert.Equal(t, "added", result.Records[3].GetString("change_type"))
	})
}

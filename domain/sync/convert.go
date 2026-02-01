package sync

import "github.com/spaghettifactory-oss/pipeforge/domain"

// ToRecordSet converts a RecordSetDelta into a standard RecordSet.
// Each delta record contains: index, change_type, old (record), new (record).
// This allows storing and processing deltas like any other data.
func (rsd *RecordSetDelta) ToRecordSet() *domain.RecordSet {
	deltaSchema := rsd.createDeltaSchema()
	result := domain.NewRecordSet(deltaSchema)

	for _, rd := range rsd.RecordDeltas {
		record := domain.NewRecord(deltaSchema)
		record.Set("index", domain.IntValue(int64(rd.Index)))
		record.Set("change_type", domain.StringValue(rd.ChangeType.String()))
		record.Set("old", domain.RecordValue{Record: rd.OldRecord})
		record.Set("new", domain.RecordValue{Record: rd.NewRecord})
		result.Add(record)
	}

	return result
}

// createDeltaSchema creates the schema for delta records.
func (rsd *RecordSetDelta) createDeltaSchema() *domain.DataSchema {
	var refSchema domain.SchemaType
	if rsd.Schema != nil {
		refSchema = domain.CustomType{Name: rsd.Schema.ID, Schema: rsd.Schema}
	} else {
		refSchema = domain.NativeTypeString // fallback
	}

	return &domain.DataSchema{
		ID: "Delta",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "index", SchemaType: domain.NativeTypeInt},
			domain.SchemaColumnSingle{ID: "change_type", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "old", SchemaType: refSchema},
			domain.SchemaColumnSingle{ID: "new", SchemaType: refSchema},
		},
	}
}

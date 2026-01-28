package main

import (
	"project/internal/core/domain"
)

// FixNegativeStockTransform resets negative stock values to 0.
type FixNegativeStockTransform struct {
	FieldID string
}

// NewFixNegativeStockTransform creates a new FixNegativeStockTransform.
func NewFixNegativeStockTransform(fieldID string) *FixNegativeStockTransform {
	return &FixNegativeStockTransform{FieldID: fieldID}
}

// Transform resets negative values to 0.
func (t *FixNegativeStockTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	if input == nil {
		return nil, nil
	}

	result := domain.NewRecordSet(input.Schema)

	for _, record := range input.Records {
		newRecord := domain.NewRecord(record.Schema)

		for colID, value := range record.Values {
			if colID == t.FieldID {
				newRecord.Set(colID, t.fixNegative(value))
			} else {
				newRecord.Set(colID, value)
			}
		}

		result.Add(newRecord)
	}

	return result, nil
}

func (t *FixNegativeStockTransform) fixNegative(value domain.Value) domain.Value {
	if value == nil || value.IsNull() {
		return value
	}

	intVal, ok := value.(domain.IntValue)
	if !ok {
		return value
	}

	if int64(intVal) < 0 {
		return domain.IntValue(0)
	}

	return value
}

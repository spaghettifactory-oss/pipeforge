package transform

import (
	"fmt"

	"github.com/spaghettifactory-oss/pipeforge/internal/core/domain"
)

// AddIntTransform adds a fixed integer value to a specified int field in each record.
type AddIntTransform struct {
	FieldID string
	Value   int64
}

// NewAddIntTransform creates a new AddIntTransform.
func NewAddIntTransform(fieldID string, value int64) *AddIntTransform {
	return &AddIntTransform{
		FieldID: fieldID,
		Value:   value,
	}
}

// Transform adds the configured value to the specified field in each record.
func (t *AddIntTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	if input == nil {
		return nil, nil
	}

	result := domain.NewRecordSet(input.Schema)

	for _, record := range input.Records {
		newRecord := domain.NewRecord(record.Schema)

		// Copy all values from the original record
		for colID, value := range record.Values {
			newRecord.Set(colID, value)
		}

		// Add the value to the specified int field
		currentValue := record.Get(t.FieldID)
		if currentValue != nil {
			intValue, ok := currentValue.(domain.IntValue)
			if !ok {
				return nil, fmt.Errorf("field %s is not an int", t.FieldID)
			}
			newRecord.Set(t.FieldID, domain.IntValue(int64(intValue)+t.Value))
		}

		result.Add(newRecord)
	}

	return result, nil
}

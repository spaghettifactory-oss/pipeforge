package main

import (
	"fmt"

	"project/internal/core/domain"
)

// MultiplyTransform multiplies a numeric field by a given factor.
type MultiplyTransform struct {
	FieldID string
	Factor  float64
}

// NewMultiplyTransform creates a new MultiplyTransform.
func NewMultiplyTransform(fieldID string, factor float64) *MultiplyTransform {
	return &MultiplyTransform{
		FieldID: fieldID,
		Factor:  factor,
	}
}

// Transform multiplies the specified field by the factor in each record.
func (t *MultiplyTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
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

		// Multiply the specified field
		currentValue := record.Get(t.FieldID)
		if currentValue != nil {
			multiplied, err := t.multiplyValue(currentValue)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", t.FieldID, err)
			}
			newRecord.Set(t.FieldID, multiplied)
		}

		result.Add(newRecord)
	}

	return result, nil
}

func (t *MultiplyTransform) multiplyValue(value domain.Value) (domain.Value, error) {
	switch v := value.(type) {
	case domain.IntValue:
		return domain.IntValue(int64(float64(v) * t.Factor)), nil
	case domain.FloatValue:
		return domain.FloatValue(float64(v) * t.Factor), nil
	default:
		return nil, fmt.Errorf("cannot multiply type %T", value)
	}
}

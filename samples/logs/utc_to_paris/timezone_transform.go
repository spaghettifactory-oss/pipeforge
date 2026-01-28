package main

import (
	"fmt"
	"time"

	"github.com/spaghettifactory-oss/pipeforge/internal/core/domain"
)

// TimezoneTransform converts a date field from one timezone to another.
type TimezoneTransform struct {
	FieldID  string
	Location *time.Location
}

// NewTimezoneTransform creates a new TimezoneTransform.
func NewTimezoneTransform(fieldID string, targetTimezone string) (*TimezoneTransform, error) {
	loc, err := time.LoadLocation(targetTimezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone %s: %w", targetTimezone, err)
	}
	return &TimezoneTransform{
		FieldID:  fieldID,
		Location: loc,
	}, nil
}

// Transform converts timestamps to the target timezone.
func (t *TimezoneTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	if input == nil {
		return nil, nil
	}

	result := domain.NewRecordSet(input.Schema)

	for _, record := range input.Records {
		newRecord := domain.NewRecord(record.Schema)

		for colID, value := range record.Values {
			if colID == t.FieldID {
				converted, err := t.convertTimezone(value)
				if err != nil {
					return nil, fmt.Errorf("record field %s: %w", colID, err)
				}
				newRecord.Set(colID, converted)
			} else {
				newRecord.Set(colID, value)
			}
		}

		result.Add(newRecord)
	}

	return result, nil
}

func (t *TimezoneTransform) convertTimezone(value domain.Value) (domain.Value, error) {
	if value == nil || value.IsNull() {
		return value, nil
	}

	dateVal, ok := value.(domain.DateValue)
	if !ok {
		return nil, fmt.Errorf("expected date, got %T", value)
	}

	utcTime := time.Time(dateVal)
	parisTime := utcTime.In(t.Location)

	return domain.DateValue(parisTime), nil
}

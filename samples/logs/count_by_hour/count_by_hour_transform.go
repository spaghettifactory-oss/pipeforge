package main

import (
	"github.com/spaghettifactory-oss/pipeforge/domain"
)

// CountByHourTransform aggregates logs by hour frame using Reduce.
type CountByHourTransform struct{}

// Transform counts logs by hour and returns a RecordSet with hour/count pairs.
func (t *CountByHourTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	// Define output schema
	outputSchema := &domain.DataSchema{
		ID: "HourCount",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "hour", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "count", SchemaType: domain.NativeTypeInt},
		},
	}

	// Use Reduce to count logs by hour frame
	countByHour := input.Reduce(
		make(map[string]int),
		func(acc any, r *domain.Record) any {
			counts := acc.(map[string]int)
			ts := r.GetDate("timestamp")
			hourFrame := ts.Format("2006-01-02 15:00")
			counts[hourFrame]++
			return counts
		},
	).(map[string]int)

	// Convert map to RecordSet
	result := domain.NewRecordSet(outputSchema)
	for hour, count := range countByHour {
		record := domain.NewRecord(outputSchema)
		record.Set("hour", domain.StringValue(hour))
		record.Set("count", domain.IntValue(count))
		result.Add(record)
	}

	return result, nil
}

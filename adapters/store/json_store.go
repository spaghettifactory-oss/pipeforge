package store

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spaghettifactory-oss/pipeforge/domain"
)

// JSONStore writes a RecordSet to a JSON file.
type JSONStore struct {
	FilePath string
	Indent   bool
}

// NewJSONStore creates a new JSONStore.
func NewJSONStore(filePath string) *JSONStore {
	return &JSONStore{
		FilePath: filePath,
		Indent:   true,
	}
}

// Store writes the RecordSet to the JSON file.
func (s *JSONStore) Store(data *domain.RecordSet) error {
	if data == nil {
		return fmt.Errorf("cannot store nil RecordSet")
	}

	rawData := make([]map[string]any, 0, len(data.Records))

	for _, record := range data.Records {
		mapped, err := s.mapRecord(record)
		if err != nil {
			return fmt.Errorf("failed to map record: %w", err)
		}
		rawData = append(rawData, mapped)
	}

	var jsonBytes []byte
	var err error

	if s.Indent {
		jsonBytes, err = json.MarshalIndent(rawData, "", "  ")
	} else {
		jsonBytes, err = json.Marshal(rawData)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	if err := os.WriteFile(s.FilePath, jsonBytes, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func (s *JSONStore) mapRecord(record *domain.Record) (map[string]any, error) {
	result := make(map[string]any)

	for colID, value := range record.Values {
		mapped, err := s.mapValue(value)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", colID, err)
		}
		result[colID] = mapped
	}

	return result, nil
}

func (s *JSONStore) mapValue(value domain.Value) (any, error) {
	if value == nil || value.IsNull() {
		return nil, nil
	}

	switch v := value.(type) {
	case domain.StringValue:
		return string(v), nil

	case domain.IntValue:
		return int64(v), nil

	case domain.FloatValue:
		return float64(v), nil

	case domain.DateValue:
		return time.Time(v).Format(time.RFC3339), nil

	case domain.ArrayValue:
		return s.mapArrayValue(v)

	case domain.RecordValue:
		if v.Record == nil {
			return nil, nil
		}
		return s.mapRecord(v.Record)

	default:
		return nil, fmt.Errorf("unsupported value type: %T", value)
	}
}

func (s *JSONStore) mapArrayValue(arr domain.ArrayValue) ([]any, error) {
	result := make([]any, 0, len(arr.Elements))

	for i, elem := range arr.Elements {
		mapped, err := s.mapValue(elem)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		result = append(result, mapped)
	}

	return result, nil
}

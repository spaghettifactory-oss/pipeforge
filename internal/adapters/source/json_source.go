package source

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"project/internal/core/domain"
)

// JSONSource reads data from a JSON file.
type JSONSource struct {
	FilePath string
	Schema   *domain.DataSchema
}

// NewJSONSource creates a new JSONSource.
func NewJSONSource(filePath string, schema *domain.DataSchema) *JSONSource {
	return &JSONSource{
		FilePath: filePath,
		Schema:   schema,
	}
}

// Load reads the JSON file and returns a RecordSet.
func (s *JSONSource) Load() (*domain.RecordSet, error) {
	data, err := os.ReadFile(s.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var rawData []map[string]any
	if err := json.Unmarshal(data, &rawData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	recordSet := domain.NewRecordSet(s.Schema)

	for _, item := range rawData {
		record, err := s.mapToRecord(item)
		if err != nil {
			return nil, fmt.Errorf("failed to map record: %w", err)
		}
		recordSet.Add(record)
	}

	return recordSet, nil
}

func (s *JSONSource) mapToRecord(data map[string]any) (*domain.Record, error) {
	record := domain.NewRecord(s.Schema)

	for _, col := range s.Schema.Columns {
		value, exists := data[col.GetID()]
		if !exists {
			continue
		}

		mappedValue, err := s.mapValue(value, col.GetType(), col.IsArray())
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.GetID(), err)
		}

		record.Set(col.GetID(), mappedValue)
	}

	return record, nil
}

func (s *JSONSource) mapValue(value any, schemaType domain.SchemaType, isArray bool) (domain.Value, error) {
	if value == nil {
		return domain.NullValue{Type: schemaType}, nil
	}

	if isArray {
		return s.mapArrayValue(value, schemaType)
	}

	return s.mapSingleValue(value, schemaType)
}

func (s *JSONSource) mapSingleValue(value any, schemaType domain.SchemaType) (domain.Value, error) {
	if schemaType.IsNative() {
		return s.mapNativeValue(value, schemaType.(domain.NativeType))
	}

	// Custom type - expect a nested object
	nestedData, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object for custom type %s", schemaType.GetTypeName())
	}

	customType := schemaType.(domain.CustomType)
	if customType.Schema == nil {
		return nil, fmt.Errorf("custom type %s has no schema", customType.Name)
	}

	nestedSource := &JSONSource{Schema: customType.Schema}
	nestedRecord, err := nestedSource.mapToRecord(nestedData)
	if err != nil {
		return nil, err
	}

	return domain.RecordValue{Record: nestedRecord}, nil
}

func (s *JSONSource) mapNativeValue(value any, nativeType domain.NativeType) (domain.Value, error) {
	switch nativeType {
	case domain.NativeTypeString:
		str, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", value)
		}
		return domain.StringValue(str), nil

	case domain.NativeTypeInt:
		// JSON numbers are float64
		num, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected number, got %T", value)
		}
		return domain.IntValue(int64(num)), nil

	case domain.NativeTypeFloat:
		num, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected number, got %T", value)
		}
		return domain.FloatValue(num), nil

	case domain.NativeTypeDate:
		str, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected date string, got %T", value)
		}
		t, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return nil, fmt.Errorf("invalid date format: %w", err)
		}
		return domain.DateValue(t), nil

	default:
		return nil, fmt.Errorf("unknown native type: %s", nativeType)
	}
}

func (s *JSONSource) mapArrayValue(value any, elementType domain.SchemaType) (domain.Value, error) {
	arr, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array, got %T", value)
	}

	elements := make([]domain.Value, 0, len(arr))
	for i, item := range arr {
		elem, err := s.mapSingleValue(item, elementType)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		elements = append(elements, elem)
	}

	return domain.ArrayValue{
		ElementType: elementType,
		Elements:    elements,
	}, nil
}

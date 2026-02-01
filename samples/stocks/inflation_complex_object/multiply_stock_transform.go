package main

import (
	"fmt"

	"github.com/spaghettifactory-oss/pipeforge/domain"
)

// MultiplyStockTransform multiplies pricing in nested stock arrays.
type MultiplyStockTransform struct {
	Factor float64
}

// NewMultiplyStockTransform creates a new MultiplyStockTransform.
func NewMultiplyStockTransform(factor float64) *MultiplyStockTransform {
	return &MultiplyStockTransform{Factor: factor}
}

// Transform multiplies pricing in each stock item.
func (t *MultiplyStockTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	if input == nil {
		return nil, nil
	}

	result := domain.NewRecordSet(input.Schema)

	for _, record := range input.Records {
		newRecord, err := t.transformStore(record)
		if err != nil {
			return nil, err
		}
		result.Add(newRecord)
	}

	return result, nil
}

func (t *MultiplyStockTransform) transformStore(record *domain.Record) (*domain.Record, error) {
	newRecord := domain.NewRecord(record.Schema)

	for colID, value := range record.Values {
		if colID == "stock" {
			newStock, err := t.transformStock(value)
			if err != nil {
				return nil, err
			}
			newRecord.Set(colID, newStock)
		} else {
			newRecord.Set(colID, value)
		}
	}

	return newRecord, nil
}

func (t *MultiplyStockTransform) transformStock(value domain.Value) (domain.Value, error) {
	arr, ok := value.(domain.ArrayValue)
	if !ok {
		return nil, fmt.Errorf("stock is not an array")
	}

	newElements := make([]domain.Value, 0, len(arr.Elements))

	for i, elem := range arr.Elements {
		recVal, ok := elem.(domain.RecordValue)
		if !ok {
			return nil, fmt.Errorf("stock element %d is not a record", i)
		}

		newItem, err := t.transformStockItem(recVal.Record)
		if err != nil {
			return nil, fmt.Errorf("stock element %d: %w", i, err)
		}

		newElements = append(newElements, domain.RecordValue{Record: newItem})
	}

	return domain.ArrayValue{
		ElementType: arr.ElementType,
		Elements:    newElements,
	}, nil
}

func (t *MultiplyStockTransform) transformStockItem(record *domain.Record) (*domain.Record, error) {
	newRecord := domain.NewRecord(record.Schema)

	for colID, value := range record.Values {
		if colID == "pricing" {
			intVal, ok := value.(domain.IntValue)
			if !ok {
				return nil, fmt.Errorf("pricing is not an int")
			}
			newRecord.Set(colID, domain.IntValue(int64(float64(intVal)*t.Factor)))
		} else {
			newRecord.Set(colID, value)
		}
	}

	return newRecord, nil
}

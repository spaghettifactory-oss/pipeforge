package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func createTestSchema() *DataSchema {
	return &DataSchema{
		ID: "Product",
		Columns: []SchemaColumn{
			SchemaColumnSingle{ID: "name", SchemaType: NativeTypeString},
			SchemaColumnSingle{ID: "price", SchemaType: NativeTypeFloat},
			SchemaColumnSingle{ID: "quantity", SchemaType: NativeTypeInt},
		},
	}
}

func createTestRecord(schema *DataSchema, name string, price float64) *Record {
	record := NewRecord(schema)
	record.Set("name", StringValue(name))
	record.Set("price", FloatValue(price))
	return record
}

func createTestRecordWithQuantity(schema *DataSchema, name string, price float64, quantity int64) *Record {
	record := NewRecord(schema)
	record.Set("name", StringValue(name))
	record.Set("price", FloatValue(price))
	record.Set("quantity", IntValue(quantity))
	return record
}

func TestNewRecordSet(t *testing.T) {
	t.Run("should create empty RecordSet with schema", func(t *testing.T) {
		schema := createTestSchema()

		rs := NewRecordSet(schema)

		assert.NotNil(t, rs)
		assert.Equal(t, schema, rs.Schema)
		assert.Empty(t, rs.Records)
	})
}

func TestRecordSet_Count(t *testing.T) {
	t.Run("should return 0 for empty set", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		assert.Equal(t, 0, rs.Count())
	})

	t.Run("should return correct count", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))
		rs.Add(createTestRecord(schema, "B", 20))

		assert.Equal(t, 2, rs.Count())
	})
}

func TestRecordSet_IsEmpty(t *testing.T) {
	t.Run("should return true for empty set", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		assert.True(t, rs.IsEmpty())
	})

	t.Run("should return false for non-empty set", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))

		assert.False(t, rs.IsEmpty())
	})
}

func TestRecordSet_Add(t *testing.T) {
	t.Run("should add record to set", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		record := createTestRecord(schema, "Laptop", 999.99)

		rs.Add(record)

		assert.Equal(t, 1, rs.Count())
		assert.Equal(t, record, rs.Records[0])
	})
}

func TestRecordSet_Get(t *testing.T) {
	t.Run("should return record at index", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		record := createTestRecord(schema, "Laptop", 999.99)
		rs.Add(record)

		assert.Equal(t, record, rs.Get(0))
	})

	t.Run("should return nil for negative index", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		assert.Nil(t, rs.Get(-1))
	})

	t.Run("should return nil for out of bounds index", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		assert.Nil(t, rs.Get(0))
		assert.Nil(t, rs.Get(10))
	})
}

func TestRecordSet_First(t *testing.T) {
	t.Run("should return first record", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		first := createTestRecord(schema, "First", 10)
		second := createTestRecord(schema, "Second", 20)
		rs.Add(first)
		rs.Add(second)

		assert.Equal(t, first, rs.First())
	})

	t.Run("should return nil for empty set", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		assert.Nil(t, rs.First())
	})
}

func TestRecordSet_Last(t *testing.T) {
	t.Run("should return last record", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		first := createTestRecord(schema, "First", 10)
		second := createTestRecord(schema, "Second", 20)
		rs.Add(first)
		rs.Add(second)

		assert.Equal(t, second, rs.Last())
	})

	t.Run("should return nil for empty set", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		assert.Nil(t, rs.Last())
	})
}

func TestRecordSet_Filter(t *testing.T) {
	t.Run("should filter records matching predicate", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "Cheap", 50))
		rs.Add(createTestRecord(schema, "Expensive", 500))
		rs.Add(createTestRecord(schema, "VeryExpensive", 1000))

		filtered := rs.Filter(func(r *Record) bool {
			return r.GetFloat("price") > 100
		})

		assert.Equal(t, 2, filtered.Count())
		assert.Equal(t, "Expensive", filtered.Get(0).GetString("name"))
		assert.Equal(t, "VeryExpensive", filtered.Get(1).GetString("name"))
	})

	t.Run("should return empty set when no matches", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "Cheap", 10))

		filtered := rs.Filter(func(r *Record) bool {
			return r.GetFloat("price") > 1000
		})

		assert.True(t, filtered.IsEmpty())
		assert.Equal(t, schema, filtered.Schema)
	})
}

func TestRecordSet_Map(t *testing.T) {
	t.Run("should transform all records", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 100))
		rs.Add(createTestRecord(schema, "B", 200))

		doubled := rs.Map(func(r *Record) *Record {
			newRecord := NewRecord(schema)
			newRecord.Set("name", r.Get("name"))
			newRecord.Set("price", FloatValue(r.GetFloat("price")*2))
			return newRecord
		})

		assert.Equal(t, 2, doubled.Count())
		assert.Equal(t, 200.0, doubled.Get(0).GetFloat("price"))
		assert.Equal(t, 400.0, doubled.Get(1).GetFloat("price"))
	})
}

func TestRecordSet_ForEach(t *testing.T) {
	t.Run("should apply function to each record", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))
		rs.Add(createTestRecord(schema, "B", 20))

		var names []string
		rs.ForEach(func(r *Record) {
			names = append(names, r.GetString("name"))
		})

		assert.Equal(t, []string{"A", "B"}, names)
	})
}

func TestRecordSet_Any(t *testing.T) {
	t.Run("should return true if any record matches", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "Cheap", 10))
		rs.Add(createTestRecord(schema, "Expensive", 1000))

		hasExpensive := rs.Any(func(r *Record) bool {
			return r.GetFloat("price") > 500
		})

		assert.True(t, hasExpensive)
	})

	t.Run("should return false if no record matches", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "Cheap", 10))

		hasExpensive := rs.Any(func(r *Record) bool {
			return r.GetFloat("price") > 500
		})

		assert.False(t, hasExpensive)
	})

	t.Run("should return false for empty set", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		result := rs.Any(func(r *Record) bool { return true })

		assert.False(t, result)
	})
}

func TestRecordSet_All(t *testing.T) {
	t.Run("should return true if all records match", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 100))
		rs.Add(createTestRecord(schema, "B", 200))

		allPositive := rs.All(func(r *Record) bool {
			return r.GetFloat("price") > 0
		})

		assert.True(t, allPositive)
	})

	t.Run("should return false if any record does not match", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "Cheap", 10))
		rs.Add(createTestRecord(schema, "Expensive", 1000))

		allExpensive := rs.All(func(r *Record) bool {
			return r.GetFloat("price") > 500
		})

		assert.False(t, allExpensive)
	})

	t.Run("should return true for empty set", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		result := rs.All(func(r *Record) bool { return false })

		assert.True(t, result)
	})
}

func TestRecordSet_Take(t *testing.T) {
	t.Run("should return first n records", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))
		rs.Add(createTestRecord(schema, "B", 20))
		rs.Add(createTestRecord(schema, "C", 30))

		taken := rs.Take(2)

		assert.Equal(t, 2, taken.Count())
		assert.Equal(t, "A", taken.Get(0).GetString("name"))
		assert.Equal(t, "B", taken.Get(1).GetString("name"))
	})

	t.Run("should return all records if n exceeds count", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))

		taken := rs.Take(10)

		assert.Equal(t, 1, taken.Count())
	})

	t.Run("should return empty set if n is 0", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))

		taken := rs.Take(0)

		assert.True(t, taken.IsEmpty())
	})
}

func TestRecordSet_Skip(t *testing.T) {
	t.Run("should skip first n records", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))
		rs.Add(createTestRecord(schema, "B", 20))
		rs.Add(createTestRecord(schema, "C", 30))

		skipped := rs.Skip(1)

		assert.Equal(t, 2, skipped.Count())
		assert.Equal(t, "B", skipped.Get(0).GetString("name"))
		assert.Equal(t, "C", skipped.Get(1).GetString("name"))
	})

	t.Run("should return empty set if n exceeds count", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))

		skipped := rs.Skip(10)

		assert.True(t, skipped.IsEmpty())
	})

	t.Run("should return all records if n is 0", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))

		skipped := rs.Skip(0)

		assert.Equal(t, 1, skipped.Count())
	})
}

func TestRecordSet_Chaining(t *testing.T) {
	t.Run("should support method chaining for pagination", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		for i := 0; i < 100; i++ {
			rs.Add(createTestRecord(schema, "Item", float64(i)))
		}

		// Page 3 with 10 items per page
		page3 := rs.Skip(20).Take(10)

		assert.Equal(t, 10, page3.Count())
		assert.Equal(t, 20.0, page3.First().GetFloat("price"))
		assert.Equal(t, 29.0, page3.Last().GetFloat("price"))
	})

	t.Run("should support filter then map", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "Cheap", 50))
		rs.Add(createTestRecord(schema, "Medium", 150))
		rs.Add(createTestRecord(schema, "Expensive", 500))

		result := rs.
			Filter(func(r *Record) bool {
				return r.GetFloat("price") >= 100
			}).
			Map(func(r *Record) *Record {
				newRecord := NewRecord(schema)
				newRecord.Set("name", r.Get("name"))
				newRecord.Set("price", FloatValue(r.GetFloat("price")*1.1)) // +10% tax
				return newRecord
			})

		assert.Equal(t, 2, result.Count())
		assert.Equal(t, 165.0, result.Get(0).GetFloat("price"))
		assert.Equal(t, 550.0, result.Get(1).GetFloat("price"))
	})
}

func TestRecordSet_Reduce(t *testing.T) {
	t.Run("should reduce records to single value (sum)", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecordWithQuantity(schema, "A", 10, 5))
		rs.Add(createTestRecordWithQuantity(schema, "B", 20, 3))
		rs.Add(createTestRecordWithQuantity(schema, "C", 30, 2))

		total := rs.Reduce(int64(0), func(acc any, r *Record) any {
			return acc.(int64) + r.GetInt("quantity")
		})

		assert.Equal(t, int64(10), total)
	})

	t.Run("should return initial value for empty set", func(t *testing.T) {
		rs := NewRecordSet(createTestSchema())

		total := rs.Reduce(int64(100), func(acc any, r *Record) any {
			return acc.(int64) + r.GetInt("quantity")
		})

		assert.Equal(t, int64(100), total)
	})

	t.Run("should concatenate strings", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecord(schema, "A", 10))
		rs.Add(createTestRecord(schema, "B", 20))
		rs.Add(createTestRecord(schema, "C", 30))

		names := rs.Reduce("", func(acc any, r *Record) any {
			if acc.(string) == "" {
				return r.GetString("name")
			}
			return acc.(string) + "," + r.GetString("name")
		})

		assert.Equal(t, "A,B,C", names)
	})

	t.Run("should find max value", func(t *testing.T) {
		schema := createTestSchema()
		rs := NewRecordSet(schema)
		rs.Add(createTestRecordWithQuantity(schema, "A", 10, 50))
		rs.Add(createTestRecordWithQuantity(schema, "B", 20, 5))
		rs.Add(createTestRecordWithQuantity(schema, "C", 30, 25))

		max := rs.Reduce(int64(0), func(acc any, r *Record) any {
			if v := r.GetInt("quantity"); v > acc.(int64) {
				return v
			}
			return acc
		})

		assert.Equal(t, int64(50), max)
	})
}

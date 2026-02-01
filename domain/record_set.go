package domain

// RecordSet represents a collection of records sharing the same schema.
type RecordSet struct {
	Schema  *DataSchema // Schema all records conform to
	Records []*Record   // Collection of records
}

// NewRecordSet creates a new empty RecordSet for the given schema.
func NewRecordSet(schema *DataSchema) *RecordSet {
	return &RecordSet{
		Schema:  schema,
		Records: make([]*Record, 0),
	}
}

// Count returns the number of records in the set.
func (rs *RecordSet) Count() int {
	return len(rs.Records)
}

// IsEmpty returns true if the set contains no records.
func (rs *RecordSet) IsEmpty() bool {
	return len(rs.Records) == 0
}

// Add appends a record to the set.
func (rs *RecordSet) Add(record *Record) {
	rs.Records = append(rs.Records, record)
}

// Get returns the record at the given index, or nil if out of bounds.
func (rs *RecordSet) Get(index int) *Record {
	if index < 0 || index >= len(rs.Records) {
		return nil
	}
	return rs.Records[index]
}

// First returns the first record, or nil if empty.
func (rs *RecordSet) First() *Record {
	return rs.Get(0)
}

// Last returns the last record, or nil if empty.
func (rs *RecordSet) Last() *Record {
	return rs.Get(len(rs.Records) - 1)
}

// Filter returns a new RecordSet containing only records that match the predicate.
func (rs *RecordSet) Filter(predicate func(*Record) bool) *RecordSet {
	result := NewRecordSet(rs.Schema)
	for _, r := range rs.Records {
		if predicate(r) {
			result.Add(r)
		}
	}
	return result
}

// Map applies a transformation to each record and returns a new RecordSet.
func (rs *RecordSet) Map(transform func(*Record) *Record) *RecordSet {
	result := NewRecordSet(rs.Schema)
	for _, r := range rs.Records {
		result.Add(transform(r))
	}
	return result
}

// ForEach applies a function to each record.
func (rs *RecordSet) ForEach(fn func(*Record)) {
	for _, r := range rs.Records {
		fn(r)
	}
}

// Any returns true if at least one record matches the predicate.
func (rs *RecordSet) Any(predicate func(*Record) bool) bool {
	for _, r := range rs.Records {
		if predicate(r) {
			return true
		}
	}
	return false
}

// All returns true if all records match the predicate.
func (rs *RecordSet) All(predicate func(*Record) bool) bool {
	for _, r := range rs.Records {
		if !predicate(r) {
			return false
		}
	}
	return true
}

// Take returns a new RecordSet with at most n records from the beginning.
func (rs *RecordSet) Take(n int) *RecordSet {
	result := NewRecordSet(rs.Schema)
	for i := 0; i < n && i < len(rs.Records); i++ {
		result.Add(rs.Records[i])
	}
	return result
}

// Skip returns a new RecordSet without the first n records.
func (rs *RecordSet) Skip(n int) *RecordSet {
	result := NewRecordSet(rs.Schema)
	for i := n; i < len(rs.Records); i++ {
		result.Add(rs.Records[i])
	}
	return result
}

// Reduce aggregates all records into a single value using an accumulator function.
// The initial value is the starting point, and the reducer combines each record with the accumulator.
//
// Example - Sum:
//
//	total := rs.Reduce(int64(0), func(acc any, r *Record) any {
//	    return acc.(int64) + r.GetInt("quantity")
//	}).(int64)
//
// Example - Max:
//
//	max := rs.Reduce(int64(0), func(acc any, r *Record) any {
//	    if v := r.GetInt("price"); v > acc.(int64) { return v }
//	    return acc
//	}).(int64)
func (rs *RecordSet) Reduce(initial any, reducer func(accumulator any, record *Record) any) any {
	result := initial
	for _, r := range rs.Records {
		result = reducer(result, r)
	}
	return result
}

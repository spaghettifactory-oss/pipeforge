package sync

import (
	"strconv"
	"time"

	"github.com/spaghettifactory-oss/pipeforge/domain"
)

// CompareRecords compares two records and returns a RecordDelta.
// The index parameter is used to identify the position in a RecordSet.
// Optional CompareOption can be passed to configure comparison behavior.
func CompareRecords(oldRecord, newRecord *domain.Record, index int, opts ...CompareOption) RecordDelta {
	options := NewCompareOptions(opts...)

	// Handle nil cases (added/deleted)
	if oldRecord == nil && newRecord == nil {
		return RecordDelta{
			Index:      index,
			ChangeType: RecordUnchanged,
		}
	}

	if oldRecord == nil {
		return RecordDelta{
			Index:      index,
			ChangeType: RecordAdded,
			NewRecord:  newRecord,
		}
	}

	if newRecord == nil {
		return RecordDelta{
			Index:      index,
			ChangeType: RecordDeleted,
			OldRecord:  oldRecord,
		}
	}

	// Both records exist, compare field by field
	fieldDeltas := compareFields(oldRecord, newRecord, "", options)

	// Determine if there are any changes
	hasChanges := false
	for _, fd := range fieldDeltas {
		if fd.ChangeType != FieldUnchanged {
			hasChanges = true
			break
		}
	}

	changeType := RecordUnchanged
	if hasChanges {
		changeType = RecordModified
	}

	return RecordDelta{
		Index:       index,
		ChangeType:  changeType,
		OldRecord:   oldRecord,
		NewRecord:   newRecord,
		FieldDeltas: fieldDeltas,
	}
}

// compareFields compares all fields between two records.
func compareFields(oldRecord, newRecord *domain.Record, parentPath string, options *CompareOptions) []FieldDelta {
	var deltas []FieldDelta

	// Collect all column IDs from both records
	allColumns := make(map[string]bool)
	for colID := range oldRecord.Values {
		allColumns[colID] = true
	}
	for colID := range newRecord.Values {
		allColumns[colID] = true
	}

	// Compare each column
	for colID := range allColumns {
		oldValue := oldRecord.Values[colID]
		newValue := newRecord.Values[colID]

		fieldPath := colID
		if parentPath != "" {
			fieldPath = parentPath + "." + colID
		}

		delta := compareFieldValues(colID, oldValue, newValue, fieldPath, options)
		deltas = append(deltas, delta)
	}

	return deltas
}

// compareFieldValues compares two values and returns a FieldDelta.
func compareFieldValues(columnID string, oldValue, newValue domain.Value, fieldPath string, options *CompareOptions) FieldDelta {
	oldIsNull := oldValue == nil || oldValue.IsNull()
	newIsNull := newValue == nil || newValue.IsNull()

	// Both null = unchanged
	if oldIsNull && newIsNull {
		return FieldDelta{
			ColumnID:   columnID,
			ChangeType: FieldUnchanged,
			OldValue:   oldValue,
			NewValue:   newValue,
		}
	}

	// Old null, new has value = added
	if oldIsNull && !newIsNull {
		return FieldDelta{
			ColumnID:   columnID,
			ChangeType: FieldAdded,
			OldValue:   oldValue,
			NewValue:   newValue,
		}
	}

	// Old has value, new null = deleted
	if !oldIsNull && newIsNull {
		return FieldDelta{
			ColumnID:   columnID,
			ChangeType: FieldDeleted,
			OldValue:   oldValue,
			NewValue:   newValue,
		}
	}

	// Both have values, compare them
	if valuesEqualWithOptions(oldValue, newValue, fieldPath, options) {
		return FieldDelta{
			ColumnID:   columnID,
			ChangeType: FieldUnchanged,
			OldValue:   oldValue,
			NewValue:   newValue,
		}
	}

	return FieldDelta{
		ColumnID:   columnID,
		ChangeType: FieldUpdated,
		OldValue:   oldValue,
		NewValue:   newValue,
	}
}

// valuesEqual compares two non-null values for equality (without options, for backward compatibility).
func valuesEqual(a, b domain.Value) bool {
	return valuesEqualWithOptions(a, b, "", nil)
}

// valuesEqualWithOptions compares two non-null values for equality with options.
func valuesEqualWithOptions(a, b domain.Value, fieldPath string, options *CompareOptions) bool {
	// Different types = not equal
	if a.GetType() != b.GetType() {
		return false
	}

	switch va := a.(type) {
	case domain.StringValue:
		vb, ok := b.(domain.StringValue)
		return ok && va == vb

	case domain.IntValue:
		vb, ok := b.(domain.IntValue)
		return ok && va == vb

	case domain.FloatValue:
		vb, ok := b.(domain.FloatValue)
		return ok && va == vb

	case domain.BoolValue:
		vb, ok := b.(domain.BoolValue)
		return ok && va == vb

	case domain.DateValue:
		vb, ok := b.(domain.DateValue)
		return ok && time.Time(va).Equal(time.Time(vb))

	case domain.ArrayValue:
		vb, ok := b.(domain.ArrayValue)
		if !ok {
			return false
		}
		return arraysEqual(va, vb, fieldPath, options)

	case domain.RecordValue:
		vb, ok := b.(domain.RecordValue)
		if !ok {
			return false
		}
		return RecordsEqual(va.Record, vb.Record)

	default:
		return false
	}
}

// arraysEqual compares two arrays, using key-based matching if configured.
func arraysEqual(a, b domain.ArrayValue, fieldPath string, options *CompareOptions) bool {
	// Check if we have a key configured for this field
	keyColumn := ""
	if options != nil {
		keyColumn = options.GetArrayKey(fieldPath)
	}

	// If no key configured, compare by index
	if keyColumn == "" {
		if len(a.Elements) != len(b.Elements) {
			return false
		}
		for i := range a.Elements {
			if !valuesEqualWithOptions(a.Elements[i], b.Elements[i], fieldPath, options) {
				return false
			}
		}
		return true
	}

	// Key-based comparison
	return arraysEqualByKey(a, b, keyColumn, fieldPath, options)
}

// arraysEqualByKey compares two arrays using a key column to match elements.
func arraysEqualByKey(a, b domain.ArrayValue, keyColumn, fieldPath string, options *CompareOptions) bool {
	// Build maps of key -> element for both arrays
	aMap := buildArrayKeyMap(a.Elements, keyColumn)
	bMap := buildArrayKeyMap(b.Elements, keyColumn)

	// Check if they have the same keys
	if len(aMap) != len(bMap) {
		return false
	}

	// Compare elements by key
	for key, aElem := range aMap {
		bElem, exists := bMap[key]
		if !exists {
			return false
		}
		if !valuesEqualWithOptions(aElem, bElem, fieldPath, options) {
			return false
		}
	}

	return true
}

// buildArrayKeyMap creates a map from key value to element for RecordValue arrays.
func buildArrayKeyMap(elements []domain.Value, keyColumn string) map[string]domain.Value {
	result := make(map[string]domain.Value)
	for _, elem := range elements {
		rv, ok := elem.(domain.RecordValue)
		if !ok || rv.Record == nil {
			continue
		}
		keyVal := rv.Record.Get(keyColumn)
		if keyVal == nil {
			continue
		}
		// Convert key to string for map lookup
		keyStr := valueToString(keyVal)
		result[keyStr] = elem
	}
	return result
}

// valueToString converts a Value to string for use as map key.
func valueToString(v domain.Value) string {
	if v == nil || v.IsNull() {
		return ""
	}
	switch val := v.(type) {
	case domain.StringValue:
		return string(val)
	case domain.IntValue:
		return strconv.FormatInt(int64(val), 10)
	default:
		return ""
	}
}

// RecordsEqual compares two records for equality.
func RecordsEqual(a, b *domain.Record) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Check if they have the same columns
	if len(a.Values) != len(b.Values) {
		return false
	}

	for colID, valA := range a.Values {
		valB, exists := b.Values[colID]
		if !exists {
			return false
		}
		if !valuesEqual(valA, valB) {
			return false
		}
	}

	return true
}

// CompareRecordSets compares two RecordSets by index and returns a RecordSetDelta.
// Optional CompareOption can be passed to configure comparison behavior.
func CompareRecordSets(oldSet, newSet *domain.RecordSet, opts ...CompareOption) *RecordSetDelta {
	var schema *domain.DataSchema
	if newSet != nil && newSet.Schema != nil {
		schema = newSet.Schema
	} else if oldSet != nil && oldSet.Schema != nil {
		schema = oldSet.Schema
	}

	delta := &RecordSetDelta{
		Schema: schema,
	}

	oldLen := 0
	newLen := 0
	if oldSet != nil {
		oldLen = len(oldSet.Records)
	}
	if newSet != nil {
		newLen = len(newSet.Records)
	}

	maxLen := oldLen
	if newLen > maxLen {
		maxLen = newLen
	}

	for i := 0; i < maxLen; i++ {
		var oldRecord *domain.Record
		var newRecord *domain.Record

		if oldSet != nil && i < len(oldSet.Records) {
			oldRecord = oldSet.Records[i]
		}
		if newSet != nil && i < len(newSet.Records) {
			newRecord = newSet.Records[i]
		}

		recordDelta := CompareRecords(oldRecord, newRecord, i, opts...)
		delta.RecordDeltas = append(delta.RecordDeltas, recordDelta)
	}

	return delta
}

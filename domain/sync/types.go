package sync

import "github.com/spaghettifactory-oss/pipeforge/domain"

// String constants for change types.
const (
	strUnchanged = "unchanged"
	strAdded     = "added"
	strUpdated   = "updated"
	strDeleted   = "deleted"
	strModified  = "modified"
	strUnknown   = "unknown"
)

// FieldChangeType represents the type of change on a field.
type FieldChangeType int

const (
	// FieldUnchanged indicates the field value is the same.
	FieldUnchanged FieldChangeType = iota
	// FieldAdded indicates the field was null/absent and now has a value.
	FieldAdded
	// FieldUpdated indicates the field value has changed.
	FieldUpdated
	// FieldDeleted indicates the field had a value and is now null/absent.
	FieldDeleted
)

// String returns the string representation of the FieldChangeType.
func (t FieldChangeType) String() string {
	switch t {
	case FieldUnchanged:
		return strUnchanged
	case FieldAdded:
		return strAdded
	case FieldUpdated:
		return strUpdated
	case FieldDeleted:
		return strDeleted
	default:
		return strUnknown
	}
}

// FieldDelta represents the delta on a specific field.
type FieldDelta struct {
	ColumnID   string
	ChangeType FieldChangeType
	OldValue   domain.Value // nil if Added
	NewValue   domain.Value // nil if Deleted
}

// RecordChangeType represents the type of change on a record.
type RecordChangeType int

const (
	// RecordUnchanged indicates the record has no changes.
	RecordUnchanged RecordChangeType = iota
	// RecordAdded indicates a new record (exists only in new set).
	RecordAdded
	// RecordModified indicates the record exists in both sets with field changes.
	RecordModified
	// RecordDeleted indicates the record was removed (exists only in old set).
	RecordDeleted
)

// String returns the string representation of the RecordChangeType.
func (t RecordChangeType) String() string {
	switch t {
	case RecordUnchanged:
		return strUnchanged
	case RecordAdded:
		return strAdded
	case RecordModified:
		return strModified
	case RecordDeleted:
		return strDeleted
	default:
		return strUnknown
	}
}

// RecordDelta represents the result of comparing two records.
type RecordDelta struct {
	Index       int
	ChangeType  RecordChangeType
	OldRecord   *domain.Record // nil if Added
	NewRecord   *domain.Record // nil if Deleted
	FieldDeltas []FieldDelta   // Field-level changes (empty if Added/Deleted)
}

// HasChanges returns true if this record has any changes.
func (rd *RecordDelta) HasChanges() bool {
	return rd.ChangeType != RecordUnchanged
}

// ChangedFields returns the list of column IDs that were modified.
func (rd *RecordDelta) ChangedFields() []string {
	var fields []string
	for _, fd := range rd.FieldDeltas {
		if fd.ChangeType != FieldUnchanged {
			fields = append(fields, fd.ColumnID)
		}
	}
	return fields
}

// AddedFields returns the list of column IDs that were added.
func (rd *RecordDelta) AddedFields() []string {
	var fields []string
	for _, fd := range rd.FieldDeltas {
		if fd.ChangeType == FieldAdded {
			fields = append(fields, fd.ColumnID)
		}
	}
	return fields
}

// UpdatedFields returns the list of column IDs that were updated.
func (rd *RecordDelta) UpdatedFields() []string {
	var fields []string
	for _, fd := range rd.FieldDeltas {
		if fd.ChangeType == FieldUpdated {
			fields = append(fields, fd.ColumnID)
		}
	}
	return fields
}

// DeletedFields returns the list of column IDs that were deleted.
func (rd *RecordDelta) DeletedFields() []string {
	var fields []string
	for _, fd := range rd.FieldDeltas {
		if fd.ChangeType == FieldDeleted {
			fields = append(fields, fd.ColumnID)
		}
	}
	return fields
}

// GetFieldDelta returns the FieldDelta for the given column ID, or nil if not found.
func (rd *RecordDelta) GetFieldDelta(columnID string) *FieldDelta {
	for i := range rd.FieldDeltas {
		if rd.FieldDeltas[i].ColumnID == columnID {
			return &rd.FieldDeltas[i]
		}
	}
	return nil
}

// RecordSetDelta represents the delta for an entire RecordSet.
type RecordSetDelta struct {
	Schema       *domain.DataSchema
	RecordDeltas []RecordDelta
}

// HasChanges returns true if any record has changes.
func (rsd *RecordSetDelta) HasChanges() bool {
	for _, rd := range rsd.RecordDeltas {
		if rd.HasChanges() {
			return true
		}
	}
	return false
}

// AddedRecords returns all RecordDeltas for added records.
func (rsd *RecordSetDelta) AddedRecords() []RecordDelta {
	var result []RecordDelta
	for _, rd := range rsd.RecordDeltas {
		if rd.ChangeType == RecordAdded {
			result = append(result, rd)
		}
	}
	return result
}

// ModifiedRecords returns all RecordDeltas for modified records.
func (rsd *RecordSetDelta) ModifiedRecords() []RecordDelta {
	var result []RecordDelta
	for _, rd := range rsd.RecordDeltas {
		if rd.ChangeType == RecordModified {
			result = append(result, rd)
		}
	}
	return result
}

// DeletedRecords returns all RecordDeltas for deleted records.
func (rsd *RecordSetDelta) DeletedRecords() []RecordDelta {
	var result []RecordDelta
	for _, rd := range rsd.RecordDeltas {
		if rd.ChangeType == RecordDeleted {
			result = append(result, rd)
		}
	}
	return result
}

// UnchangedRecords returns all RecordDeltas for unchanged records.
func (rsd *RecordSetDelta) UnchangedRecords() []RecordDelta {
	var result []RecordDelta
	for _, rd := range rsd.RecordDeltas {
		if rd.ChangeType == RecordUnchanged {
			result = append(result, rd)
		}
	}
	return result
}

// DeltaSummary contains statistics about the changes.
type DeltaSummary struct {
	Added     int
	Modified  int
	Deleted   int
	Unchanged int
	Total     int
}

// Summary returns statistics about the changes in this delta.
func (rsd *RecordSetDelta) Summary() DeltaSummary {
	summary := DeltaSummary{
		Total: len(rsd.RecordDeltas),
	}
	for _, rd := range rsd.RecordDeltas {
		switch rd.ChangeType {
		case RecordAdded:
			summary.Added++
		case RecordModified:
			summary.Modified++
		case RecordDeleted:
			summary.Deleted++
		case RecordUnchanged:
			summary.Unchanged++
		}
	}
	return summary
}

// Get returns the RecordDelta at the given index, or nil if out of bounds.
func (rsd *RecordSetDelta) Get(index int) *RecordDelta {
	for i := range rsd.RecordDeltas {
		if rsd.RecordDeltas[i].Index == index {
			return &rsd.RecordDeltas[i]
		}
	}
	return nil
}

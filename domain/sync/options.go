package sync

// CompareOptions holds configuration for record comparison.
type CompareOptions struct {
	// ArrayKeys maps field paths to their key columns for matching array elements.
	// Example: {"stock": "name"} means array elements in "stock" field are matched by "name".
	ArrayKeys map[string]string
}

// CompareOption is a functional option for configuring comparison behavior.
type CompareOption func(*CompareOptions)

// NewCompareOptions creates default CompareOptions.
func NewCompareOptions(opts ...CompareOption) *CompareOptions {
	options := &CompareOptions{
		ArrayKeys: make(map[string]string),
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// WithArrayKey configures key-based matching for an array field.
// Instead of comparing by index, array elements will be matched by the specified key column.
//
// Example:
//
//	sync.CompareRecords(old, new, 0, sync.WithArrayKey("stock", "name"))
//
// This will match products in "stock" array by their "name" field.
func WithArrayKey(fieldPath, keyColumn string) CompareOption {
	return func(o *CompareOptions) {
		o.ArrayKeys[fieldPath] = keyColumn
	}
}

// GetArrayKey returns the key column for the given field path, or empty string if not configured.
func (o *CompareOptions) GetArrayKey(fieldPath string) string {
	if o == nil || o.ArrayKeys == nil {
		return ""
	}
	return o.ArrayKeys[fieldPath]
}

// HasArrayKey returns true if a key is configured for the given field path.
func (o *CompareOptions) HasArrayKey(fieldPath string) bool {
	return o.GetArrayKey(fieldPath) != ""
}

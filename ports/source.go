package ports

import "github.com/spaghettifactory-oss/pipeforge/domain"

// SourcePort defines the interface for loading data from external sources.
type SourcePort interface {
	// Load reads data from the source and returns a RecordSet.
	Load() (*domain.RecordSet, error)
}

package ports

import "project/internal/core/domain"

// StorePort defines the interface for storing/writing data.
type StorePort interface {
	// Store writes the RecordSet to the destination.
	Store(data *domain.RecordSet) error
}

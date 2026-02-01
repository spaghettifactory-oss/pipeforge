package ports

import "github.com/spaghettifactory-oss/pipeforge/domain"

// StorePort defines the interface for storing/writing data.
type StorePort interface {
	// Store writes the RecordSet to the destination.
	Store(data *domain.RecordSet) error
}

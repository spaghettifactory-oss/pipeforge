package ports

import "github.com/spaghettifactory-oss/pipeforge/domain"

// TransformPort defines the interface for transforming data.
type TransformPort interface {
	// Transform takes a RecordSet as input and returns a transformed RecordSet.
	Transform(input *domain.RecordSet) (*domain.RecordSet, error)
}

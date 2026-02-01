package transform

import (
	"github.com/spaghettifactory-oss/pipeforge/domain"
	"github.com/spaghettifactory-oss/pipeforge/ports"
)

// TransformBuilder allows chaining multiple transforms into a single pipeline.
// If no transforms are added, it passes through the input unchanged.
type TransformBuilder struct {
	transforms []ports.TransformPort
}

// NewTransformBuilder creates a new empty TransformBuilder.
func NewTransformBuilder() *TransformBuilder {
	return &TransformBuilder{
		transforms: make([]ports.TransformPort, 0),
	}
}

// Add appends a transform to the pipeline and returns the builder for chaining.
func (b *TransformBuilder) Add(t ports.TransformPort) *TransformBuilder {
	b.transforms = append(b.transforms, t)
	return b
}

// Build returns the TransformBuilder as a TransformPort.
// The builder itself implements TransformPort.
func (b *TransformBuilder) Build() ports.TransformPort {
	return b
}

// Transform executes all transforms in sequence.
// If no transforms were added, returns the input unchanged.
func (b *TransformBuilder) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	if len(b.transforms) == 0 {
		return input, nil
	}

	result := input
	for _, t := range b.transforms {
		var err error
		result, err = t.Transform(result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

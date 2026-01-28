package services

import (
	"errors"

	"project/internal/core/domain"
	"project/internal/core/ports"
)

type DataPipeline struct {
	Source    ports.SourcePort
	Transform ports.TransformPort
	Store     ports.StorePort
}

// Run executes the pipeline: Load → Transform → Store.
func (s *DataPipeline) Run() error {
	_, err := s.RunWithResult()
	return err
}

// RunWithResult executes the pipeline and returns the final RecordSet.
func (s *DataPipeline) RunWithResult() (*domain.RecordSet, error) {
	if s.Source == nil || s.Transform == nil || s.Store == nil {
		return nil, errors.New("Empty source, transform or store")
	}

	// Load data from source
	data, err := s.Source.Load()
	if err != nil {
		return nil, err
	}

	// Transform data
	transformed, err := s.Transform.Transform(data)
	if err != nil {
		return nil, err
	}

	// Store data
	err = s.Store.Store(transformed)
	if err != nil {
		return nil, err
	}

	return transformed, nil
}

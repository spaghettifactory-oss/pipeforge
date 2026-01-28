package transform

import "project/internal/core/domain"

type EmptyTransform struct{}

func (s EmptyTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	return input, nil
}

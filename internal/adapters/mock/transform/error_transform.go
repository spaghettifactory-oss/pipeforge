package transform

import (
	"errors"

	"project/internal/core/domain"
)

type ErrorTransform struct{}

func (s ErrorTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	return nil, errors.New("transform error")
}

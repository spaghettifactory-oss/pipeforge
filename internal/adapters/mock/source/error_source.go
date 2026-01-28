package source

import (
	"errors"

	"project/internal/core/domain"
)

type ErrorSource struct{}

func (s ErrorSource) Load() (*domain.RecordSet, error) {
	return nil, errors.New("source load error")
}

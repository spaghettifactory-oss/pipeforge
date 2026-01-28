package store

import (
	"errors"

	"project/internal/core/domain"
)

type ErrorStore struct{}

func (s ErrorStore) Store(data *domain.RecordSet) error {
	return errors.New("store error")
}

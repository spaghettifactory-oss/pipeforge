package store

import "project/internal/core/domain"

type EmptyStore struct{}

func (s EmptyStore) Store(data *domain.RecordSet) error {
	return nil
}

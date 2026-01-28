package store

import "github.com/spaghettifactory-oss/pipeforge/internal/core/domain"

type EmptyStore struct{}

func (s EmptyStore) Store(data *domain.RecordSet) error {
	return nil
}

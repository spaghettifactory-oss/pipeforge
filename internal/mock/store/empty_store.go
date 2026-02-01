package store

import "github.com/spaghettifactory-oss/pipeforge/domain"

type EmptyStore struct{}

func (s EmptyStore) Store(data *domain.RecordSet) error {
	return nil
}

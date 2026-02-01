package store

import (
	"errors"

	"github.com/spaghettifactory-oss/pipeforge/domain"
)

type ErrorStore struct{}

func (s ErrorStore) Store(data *domain.RecordSet) error {
	return errors.New("store error")
}

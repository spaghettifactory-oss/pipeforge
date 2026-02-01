package source

import (
	"errors"

	"github.com/spaghettifactory-oss/pipeforge/domain"
)

type ErrorSource struct{}

func (s ErrorSource) Load() (*domain.RecordSet, error) {
	return nil, errors.New("source load error")
}

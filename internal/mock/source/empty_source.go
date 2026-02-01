package source

import "github.com/spaghettifactory-oss/pipeforge/domain"

type EmptySource struct{}

func (s EmptySource) Load() (*domain.RecordSet, error) {
	return domain.NewRecordSet(nil), nil
}

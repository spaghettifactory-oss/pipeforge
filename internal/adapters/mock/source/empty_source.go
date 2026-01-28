package source

import "project/internal/core/domain"

type EmptySource struct{}

func (s EmptySource) Load() (*domain.RecordSet, error) {
	return domain.NewRecordSet(nil), nil
}

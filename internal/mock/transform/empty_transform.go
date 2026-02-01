package transform

import "github.com/spaghettifactory-oss/pipeforge/domain"

type EmptyTransform struct{}

func (s EmptyTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	return input, nil
}

package services

import (
	"testing"

	"github.com/spaghettifactory-oss/pipeforge/internal/adapters/mock/source"
	"github.com/spaghettifactory-oss/pipeforge/internal/adapters/mock/store"
	"github.com/spaghettifactory-oss/pipeforge/internal/adapters/mock/transform"

	"github.com/stretchr/testify/assert"
)

func TestRun(t *testing.T) {
	t.Run("should execute pipeline successfully", func(t *testing.T) {
		pipeline := DataPipeline{
			Source:    &source.EmptySource{},
			Transform: &transform.EmptyTransform{},
			Store:     &store.EmptyStore{},
		}

		err := pipeline.Run()

		assert.NoError(t, err)
	})

	t.Run("should return error when not initialized", func(t *testing.T) {
		pipeline := DataPipeline{}

		err := pipeline.Run()

		assert.Error(t, err)
	})

	t.Run("should return error when source fails", func(t *testing.T) {
		pipeline := DataPipeline{
			Source:    &source.ErrorSource{},
			Transform: &transform.EmptyTransform{},
			Store:     &store.EmptyStore{},
		}

		err := pipeline.Run()

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "source")
	})

	t.Run("should return error when transform fails", func(t *testing.T) {
		pipeline := DataPipeline{
			Source:    &source.EmptySource{},
			Transform: &transform.ErrorTransform{},
			Store:     &store.EmptyStore{},
		}

		err := pipeline.Run()

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "transform")
	})

	t.Run("should return error when store fails", func(t *testing.T) {
		pipeline := DataPipeline{
			Source:    &source.EmptySource{},
			Transform: &transform.EmptyTransform{},
			Store:     &store.ErrorStore{},
		}

		err := pipeline.Run()

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store")
	})
}

func TestRunWithResult(t *testing.T) {
	t.Run("should return RecordSet on success", func(t *testing.T) {
		pipeline := DataPipeline{
			Source:    &source.EmptySource{},
			Transform: &transform.EmptyTransform{},
			Store:     &store.EmptyStore{},
		}

		result, err := pipeline.RunWithResult()

		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("should return nil and error when not initialized", func(t *testing.T) {
		pipeline := DataPipeline{}

		result, err := pipeline.RunWithResult()

		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

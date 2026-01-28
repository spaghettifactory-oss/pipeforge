package transform

import (
	"testing"

	"project/internal/adapters/mock/transform"
	"project/internal/core/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTransformBuilder(t *testing.T) {
	t.Run("should create empty builder", func(t *testing.T) {
		builder := NewTransformBuilder()

		assert.NotNil(t, builder)
		assert.Empty(t, builder.transforms)
	})
}

func TestTransformBuilder_Add(t *testing.T) {
	t.Run("should add transform and return builder for chaining", func(t *testing.T) {
		builder := NewTransformBuilder()

		result := builder.Add(&transform.EmptyTransform{})

		assert.Same(t, builder, result)
		assert.Len(t, builder.transforms, 1)
	})

	t.Run("should allow chaining multiple adds", func(t *testing.T) {
		builder := NewTransformBuilder().
			Add(&transform.EmptyTransform{}).
			Add(&transform.EmptyTransform{}).
			Add(&transform.EmptyTransform{})

		assert.Len(t, builder.transforms, 3)
	})
}

func TestTransformBuilder_Build(t *testing.T) {
	t.Run("should return builder as TransformPort", func(t *testing.T) {
		builder := NewTransformBuilder()

		result := builder.Build()

		assert.Same(t, builder, result)
	})
}

func TestTransformBuilder_Transform(t *testing.T) {
	t.Run("should pass through unchanged when no transforms", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			},
		}
		input := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("name", domain.StringValue("Laptop"))
		input.Add(record)

		builder := NewTransformBuilder()

		result, err := builder.Transform(input)

		require.NoError(t, err)
		assert.Same(t, input, result)
	})

	t.Run("should execute single transform", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "quantity", SchemaType: domain.NativeTypeInt},
			},
		}
		input := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("quantity", domain.IntValue(10))
		input.Add(record)

		builder := NewTransformBuilder().
			Add(transform.NewAddIntTransform("quantity", 5))

		result, err := builder.Transform(input)

		require.NoError(t, err)
		assert.Equal(t, int64(15), result.First().GetInt("quantity"))
	})

	t.Run("should chain multiple transforms", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "quantity", SchemaType: domain.NativeTypeInt},
			},
		}
		input := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("quantity", domain.IntValue(10))
		input.Add(record)

		builder := NewTransformBuilder().
			Add(transform.NewAddIntTransform("quantity", 5)).
			Add(transform.NewAddIntTransform("quantity", 3)).
			Add(transform.NewAddIntTransform("quantity", 2))

		result, err := builder.Transform(input)

		require.NoError(t, err)
		assert.Equal(t, int64(20), result.First().GetInt("quantity"))
	})

	t.Run("should stop and return error when transform fails", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Product",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "quantity", SchemaType: domain.NativeTypeInt},
			},
		}
		input := domain.NewRecordSet(schema)
		record := domain.NewRecord(schema)
		record.Set("quantity", domain.IntValue(10))
		input.Add(record)

		builder := NewTransformBuilder().
			Add(transform.NewAddIntTransform("quantity", 5)).
			Add(&transform.ErrorTransform{}).
			Add(transform.NewAddIntTransform("quantity", 3))

		result, err := builder.Transform(input)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "transform error")
	})
}

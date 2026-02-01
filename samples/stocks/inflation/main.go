package main

import (
	"fmt"
	"log"

	"github.com/spaghettifactory-oss/pipeforge/adapters/source"
	"github.com/spaghettifactory-oss/pipeforge/adapters/store"
	"github.com/spaghettifactory-oss/pipeforge/adapters/transform"
	"github.com/spaghettifactory-oss/pipeforge/domain"
	"github.com/spaghettifactory-oss/pipeforge/pipeline"
)

func main() {
	// Define the product schema
	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "pricing", SchemaType: domain.NativeTypeInt},
		},
	}

	// Create the pipeline: Load JSON -> Multiply pricing by 3 -> Store JSON
	p := pipeline.DataPipeline{
		Source: source.NewJSONSource("samples/stocks/inflation/products.json", schema),
		Transform: transform.NewTransformBuilder().
			Add(NewMultiplyTransform("pricing", 3)).
			Build(),
		Store: store.NewJSONStore("samples/stocks/inflation/products_inflated.json"),
	}

	// Run the pipeline
	result, err := p.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	// Display results
	fmt.Println("Inflation applied (x3):")
	fmt.Println("------------------------")
	for _, record := range result.Records {
		fmt.Printf("%s: %d EUR\n", record.GetString("name"), record.GetInt("pricing"))
	}
}

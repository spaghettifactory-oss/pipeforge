package main

import (
	"fmt"
	"log"

	"github.com/spaghettifactory-oss/pipeforge/internal/adapters/source"
	"github.com/spaghettifactory-oss/pipeforge/internal/adapters/store"
	"github.com/spaghettifactory-oss/pipeforge/internal/adapters/transform"
	"github.com/spaghettifactory-oss/pipeforge/internal/core/domain"
	"github.com/spaghettifactory-oss/pipeforge/internal/core/services"
)

func main() {
	// Define the product schema (nested in stock array)
	productSchema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "pricing", SchemaType: domain.NativeTypeInt},
		},
	}

	// Define the store schema
	storeSchema := &domain.DataSchema{
		ID: "Store",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "store_name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnArray{ID: "stock", RefSchema: domain.CustomType{Name: "Product", Schema: productSchema}},
		},
	}

	// Create the pipeline
	pipeline := services.DataPipeline{
		Source: source.NewJSONSource("samples/stocks/inflation_complex_object/store.json", storeSchema),
		Transform: transform.NewTransformBuilder().
			Add(NewMultiplyStockTransform(3)).
			Build(),
		Store: store.NewJSONStore("samples/stocks/inflation_complex_object/store_inflated.json"),
	}

	// Run the pipeline
	result, err := pipeline.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	// Display results
	fmt.Println("Inflation applied (x3):")
	fmt.Println("========================")
	for _, storeRecord := range result.Records {
		fmt.Printf("\n%s:\n", storeRecord.GetString("store_name"))
		fmt.Println("------------------------")

		stock := storeRecord.GetArray("stock")
		for _, item := range stock {
			product := item.(domain.RecordValue).Record
			fmt.Printf("  %s: %d EUR\n", product.GetString("name"), product.GetInt("pricing"))
		}
	}
}

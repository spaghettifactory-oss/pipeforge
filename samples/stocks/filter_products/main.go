package main

import (
	"fmt"
	"log"

	"project/internal/adapters/source"
	"project/internal/adapters/store"
	"project/internal/adapters/transform"
	"project/internal/core/domain"
	"project/internal/core/services"
)

func main() {
	// Define the product schema
	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "stock", SchemaType: domain.NativeTypeInt},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	// Create the pipeline
	pipeline := services.DataPipeline{
		Source: source.NewJSONSource("samples/stocks/filter_products/products.json", schema),
		Transform: transform.NewTransformBuilder().
			Add(NewFixNegativeStockTransform("stock")).
			Build(),
		Store: store.NewJSONStore("samples/stocks/filter_products/products_fixed.json"),
	}

	// Run the pipeline
	result, err := pipeline.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	// Display results
	fmt.Println("Negative stock fixed to 0:")
	fmt.Println("==========================")
	for _, record := range result.Records {
		stock := record.GetInt("stock")
		marker := ""
		if stock == 0 {
			marker = " (fixed)"
		}
		fmt.Printf("%s: stock=%d%s\n", record.GetString("name"), stock, marker)
	}
}

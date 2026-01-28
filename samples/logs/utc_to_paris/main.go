package main

import (
	"fmt"
	"log"
	"time"

	"project/internal/adapters/source"
	"project/internal/adapters/store"
	"project/internal/adapters/transform"
	"project/internal/core/domain"
	"project/internal/core/services"
)

func main() {
	// Define the log schema
	logSchema := &domain.DataSchema{
		ID: "Log",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "level", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "message", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "timestamp", SchemaType: domain.NativeTypeDate},
		},
	}

	// Create timezone transform (UTC -> Paris)
	tzTransform, err := NewTimezoneTransform("timestamp", "Europe/Paris")
	if err != nil {
		log.Fatalf("Failed to create transform: %v", err)
	}

	// Create the pipeline
	pipeline := services.DataPipeline{
		Source: source.NewJSONSource("samples/logs/utc_to_paris/logs.json", logSchema),
		Transform: transform.NewTransformBuilder().
			Add(tzTransform).
			Build(),
		Store: store.NewJSONStore("samples/logs/utc_to_paris/logs_paris.json"),
	}

	// Run the pipeline
	result, err := pipeline.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	// Display results
	fmt.Println("UTC -> Europe/Paris conversion:")
	fmt.Println("================================")
	for _, record := range result.Records {
		ts := record.GetDate("timestamp")
		fmt.Printf("[%s] %s - %s\n",
			record.GetString("level"),
			ts.Format(time.RFC3339),
			record.GetString("message"),
		)
	}
}

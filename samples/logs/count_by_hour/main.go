package main

import (
	"fmt"
	"log"
	"sort"

	mockstore "project/internal/adapters/mock/store"
	"project/internal/adapters/source"
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

	// Create the pipeline with CountByHourTransform
	pipeline := services.DataPipeline{
		Source:    source.NewJSONSource("samples/logs/count_by_hour/logs.json", logSchema),
		Transform: &CountByHourTransform{},
		Store:     &mockstore.EmptyStore{},
	}

	// Run the pipeline
	result, err := pipeline.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	// Sort hours for display
	type hourCount struct {
		hour  string
		count int64
	}
	var hours []hourCount
	for _, r := range result.Records {
		hours = append(hours, hourCount{
			hour:  r.GetString("hour"),
			count: r.GetInt("count"),
		})
	}
	sort.Slice(hours, func(i, j int) bool {
		return hours[i].hour < hours[j].hour
	})

	// Display results
	fmt.Println("Logs count by hour:")
	fmt.Println("====================")
	var total int64
	for _, hc := range hours {
		total += hc.count
		bar := ""
		for i := int64(0); i < hc.count; i++ {
			bar += "█"
		}
		fmt.Printf("%s | %2d %s\n", hc.hour, hc.count, bar)
	}
	fmt.Println("====================")
	fmt.Printf("Total: %d logs\n", total)
}

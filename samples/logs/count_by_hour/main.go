package main

import (
	"fmt"
	"log"
	"sort"

	mockstore "github.com/spaghettifactory-oss/pipeforge/internal/mock/store"
	"github.com/spaghettifactory-oss/pipeforge/adapters/source"
	"github.com/spaghettifactory-oss/pipeforge/domain"
	"github.com/spaghettifactory-oss/pipeforge/pipeline"
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
	p := pipeline.DataPipeline{
		Source:    source.NewJSONSource("samples/logs/count_by_hour/logs.json", logSchema),
		Transform: &CountByHourTransform{},
		Store:     &mockstore.EmptyStore{},
	}

	// Run the pipeline
	result, err := p.RunWithResult()
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

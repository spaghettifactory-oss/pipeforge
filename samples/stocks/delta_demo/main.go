package main

import (
	"fmt"
	"log"

	"github.com/spaghettifactory-oss/pipeforge/adapters/source"
	"github.com/spaghettifactory-oss/pipeforge/domain"
	"github.com/spaghettifactory-oss/pipeforge/domain/sync"
)

func main() {
	// Define schemas
	productSchema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "pricing", SchemaType: domain.NativeTypeInt},
		},
	}

	storeSchema := &domain.DataSchema{
		ID: "Store",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "store_name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnArray{ID: "stock", RefSchema: domain.CustomType{Name: "Product", Schema: productSchema}},
		},
	}

	// Load original data
	jsonSource := source.NewJSONSource("samples/stocks/delta_demo/store.json", storeSchema)
	original, err := jsonSource.Load()
	if err != nil {
		log.Fatalf("Failed to load data: %v", err)
	}

	fmt.Println("=== ORIGINAL DATA ===")
	printStores(original)

	// Step 1: Filter products with pricing >= 50
	// Step 2: Apply x3 transformation on pricing
	transformed := transformStores(original, productSchema, storeSchema)

	fmt.Println("\n=== TRANSFORMED DATA (pricing > 100, then x3) ===")
	printStores(transformed)

	// Step 3: Compare with Delta
	fmt.Println("\n=== DELTA COMPARISON ===")

	// Compare by index (default)
	delta := sync.CompareRecordSets(original, transformed)

	fmt.Printf("\nSummary: %+v\n", delta.Summary())

	for _, rd := range delta.RecordDeltas {
		storeName := ""
		if rd.OldRecord != nil {
			storeName = rd.OldRecord.GetString("store_name")
		} else if rd.NewRecord != nil {
			storeName = rd.NewRecord.GetString("store_name")
		}

		fmt.Printf("\nStore[%d] %s: %s\n", rd.Index, storeName, rd.ChangeType.String())

		if rd.ChangeType == sync.RecordModified {
			for _, fd := range rd.FieldDeltas {
				if fd.ChangeType != sync.FieldUnchanged {
					fmt.Printf("  - Field '%s': %s\n", fd.ColumnID, fd.ChangeType.String())

					// Show array changes for stock
					if fd.ColumnID == "stock" {
						showStockChanges(fd, productSchema)
					}
				}
			}
		}
	}

	// Compare with key-based matching for stock array
	fmt.Println("\n=== DELTA WITH KEY-BASED MATCHING (stock by 'name') ===")
	deltaWithKey := sync.CompareRecordSets(original, transformed, sync.WithArrayKey("stock", "name"))

	for _, rd := range deltaWithKey.RecordDeltas {
		storeName := ""
		if rd.OldRecord != nil {
			storeName = rd.OldRecord.GetString("store_name")
		} else if rd.NewRecord != nil {
			storeName = rd.NewRecord.GetString("store_name")
		}

		fmt.Printf("\nStore[%d] %s: %s\n", rd.Index, storeName, rd.ChangeType.String())

		if rd.ChangeType == sync.RecordModified {
			for _, fd := range rd.FieldDeltas {
				if fd.ChangeType != sync.FieldUnchanged {
					fmt.Printf("  - Field '%s': %s\n", fd.ColumnID, fd.ChangeType.String())
				}
			}
		}
	}
}

// transformStores filters products >= 50 pricing and applies x3
// Also demonstrates: keep Paris, delete Lyon, delete Bordeaux, add Marseille
func transformStores(original *domain.RecordSet, productSchema, storeSchema *domain.DataSchema) *domain.RecordSet {
	result := domain.NewRecordSet(storeSchema)

	for _, storeRecord := range original.Records {
		storeName := storeRecord.GetString("store_name")

		// Skip Lyon (simulates deletion)
		if storeName == "Tech Shop Lyon" {
			continue
		}

		// Skip Bordeaux (simulates deletion)
		if storeName == "Tech Shop Bordeaux" {
			continue
		}

		newStore := domain.NewRecord(storeSchema)
		newStore.Set("store_name", storeRecord.Get("store_name"))

		// Filter and transform stock
		oldStock := storeRecord.GetArray("stock")
		var newStock []domain.Value

		for _, item := range oldStock {
			product := item.(domain.RecordValue).Record
			pricing := product.GetInt("pricing")
			productName := product.GetString("name")

			// Delete Phone from stock (simulates stock deletion)
			if productName == "Phone" {
				continue
			}

			// Filter: only products > 100
			if pricing > 100 {
				// Transform: x3
				newProduct := domain.NewRecord(productSchema)
				newProduct.Set("name", product.Get("name"))
				newProduct.Set("pricing", domain.IntValue(pricing*3))
				newStock = append(newStock, domain.RecordValue{Record: newProduct})
			}
		}

		// Add SSD to Paris stock (simulates stock addition)
		if storeName == "Tech Shop Paris" {
			newStock = append(newStock, domain.RecordValue{
				Record: createProduct(productSchema, "SSD", 199),
			})
		}

		newStore.Set("stock", domain.ArrayValue{
			ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
			Elements:    newStock,
		})

		result.Add(newStore)
	}

	// Add new store Marseille (simulates addition)
	marseille := domain.NewRecord(storeSchema)
	marseille.Set("store_name", domain.StringValue("Tech Shop Marseille"))
	marseille.Set("stock", domain.ArrayValue{
		ElementType: domain.CustomType{Name: "Product", Schema: productSchema},
		Elements: []domain.Value{
			domain.RecordValue{Record: createProduct(productSchema, "Camera", 599)},
			domain.RecordValue{Record: createProduct(productSchema, "Drone", 899)},
		},
	})
	result.Add(marseille)

	return result
}

func createProduct(schema *domain.DataSchema, name string, pricing int64) *domain.Record {
	product := domain.NewRecord(schema)
	product.Set("name", domain.StringValue(name))
	product.Set("pricing", domain.IntValue(pricing))
	return product
}

func printStores(rs *domain.RecordSet) {
	for _, storeRecord := range rs.Records {
		fmt.Printf("\n%s:\n", storeRecord.GetString("store_name"))
		stock := storeRecord.GetArray("stock")
		for _, item := range stock {
			product := item.(domain.RecordValue).Record
			fmt.Printf("  - %s: %d EUR\n", product.GetString("name"), product.GetInt("pricing"))
		}
	}
}

func showStockChanges(fd sync.FieldDelta, productSchema *domain.DataSchema) {
	if fd.OldValue == nil || fd.NewValue == nil {
		return
	}

	oldArray, ok1 := fd.OldValue.(domain.ArrayValue)
	newArray, ok2 := fd.NewValue.(domain.ArrayValue)
	if !ok1 || !ok2 {
		return
	}

	fmt.Println("    Old stock:")
	for _, item := range oldArray.Elements {
		if rv, ok := item.(domain.RecordValue); ok && rv.Record != nil {
			fmt.Printf("      - %s: %d EUR\n", rv.Record.GetString("name"), rv.Record.GetInt("pricing"))
		}
	}

	fmt.Println("    New stock:")
	for _, item := range newArray.Elements {
		if rv, ok := item.(domain.RecordValue); ok && rv.Record != nil {
			fmt.Printf("      - %s: %d EUR\n", rv.Record.GetString("name"), rv.Record.GetInt("pricing"))
		}
	}
}

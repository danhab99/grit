package export

import (
	"encoding/csv"
	"os"

	"grit/db"
)

func exportResourceTableCSV(database db.Database, outputPath string, resourceName string) {
	if resourceName != "" {
		exportLogger.Printf("Exporting resources with name '%s' to CSV: %s\n", resourceName, outputPath)
	} else {
		exportLogger.Printf("Exporting all resources to CSV: %s\n", outputPath)
	}

	var writer *csv.Writer
	if outputPath == "-" {
		writer = csv.NewWriter(os.Stdout)
	} else {
		outFile, err := os.Create(outputPath)
		if err != nil {
			exportLogger.Printf("Failed to create output file: %v\n", err)
			os.Exit(1)
		}
		defer outFile.Close()
		writer = csv.NewWriter(outFile)
	}
	defer writer.Flush()

	var resourceChan chan db.Resource
	if resourceName != "" {
		resourceChan = database.GetResourcesByName(resourceName)
	} else {
		resourceChan = database.GetAllResources()
	}

	header := []string{"id", "name", "object_hash", "created_at"}
	if err := writer.Write(header); err != nil {
		exportLogger.Printf("Failed to write CSV header: %v\n", err)
		os.Exit(1)
	}

	resourceCount := 0
	for resource := range resourceChan {
		row := []string{
			resource.ID,
			resource.Name,
			resource.ObjectHash,
			resource.CreatedAt,
		}
		if err := writer.Write(row); err != nil {
			exportLogger.Printf("Failed to write CSV row: %v\n", err)
			os.Exit(1)
		}
		resourceCount++
	}

	exportLogger.Printf("Exported %d resources to CSV\n", resourceCount)
}


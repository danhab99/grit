package export

import (
	"encoding/csv"
	"os"
	"strconv"

	"grit/db"
)

func exportResourceTableCSV(database db.Database, outputPath string) {
	exportLogger.Printf("Exporting resource table to CSV: %s\n", outputPath)

	// Get all column definitions
	columns, err := database.ListAllColumns()
	if err != nil {
		exportLogger.Printf("Failed to list columns: %v\n", err)
		os.Exit(1)
	}

	// Create output file
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

	// Write header row
	header := []string{"id", "name", "object_hash", "created_at"}
	for _, col := range columns {
		header = append(header, col.Name)
	}
	if err := writer.Write(header); err != nil {
		exportLogger.Printf("Failed to write CSV header: %v\n", err)
		os.Exit(1)
	}

	// Write resource rows
	resourceCount := 0
	for resource := range database.GetAllResources() {
		row := []string{
			strconv.FormatInt(resource.ID, 10),
			resource.Name,
			resource.ObjectHash,
			resource.CreatedAt,
		}

		// Get column values for this resource
		for _, col := range columns {
			colValue, err := database.GetColumnValue(col.ID, resource.ID)
			if err != nil {
				exportLogger.Printf("Failed to get column value: %v\n", err)
				row = append(row, "")
				continue
			}
			if colValue != nil {
				// Get the actual value content
				data, err := database.GetObject(colValue.ObjectHash)
				if err != nil {
					row = append(row, "")
				} else {
					row = append(row, string(data))
				}
			} else {
				row = append(row, "")
			}
		}

		if err := writer.Write(row); err != nil {
			exportLogger.Printf("Failed to write CSV row: %v\n", err)
			os.Exit(1)
		}
		resourceCount++
	}

	exportLogger.Printf("Exported %d resources to CSV\n", resourceCount)
}

package export

import (
	"os"

	"grit/db"

	"github.com/fatih/color"
)

func exportResourceByHash(database db.Database, hash string) {
	exportLogger.Printf("Exporting resource with hash: %s\n", color.MagentaString(hash[:16]+"..."))

	// Check if object exists
	if !database.ObjectExists(hash) {
		exportLogger.Printf("Object with hash '%s' not found\n", hash)
		os.Exit(1)
	}

	// Get object data
	data, err := database.GetObject(hash)
	if err != nil {
		exportLogger.Printf("Failed to get object %s: %v\n", hash[:16], err)
		os.Exit(1)
	}

	// Write raw content to stdout
	os.Stdout.Write(data)

	exportLogger.Printf("Exported %d bytes\n", len(data))
}


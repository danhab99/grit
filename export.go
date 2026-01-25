package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
)

var exportLogger = NewColorLogger("[EXPORT] ", color.New(color.FgGreen, color.Bold))

func exportResourcesByName(database Database, resourceName string) {
	exportLogger.Printf("Listing resources with name: %s", color.MagentaString(resourceName))

	// List all resources with the given name
	resourceCount := 0
	for resource := range database.GetResourcesByName(resourceName) {
		resourceCount++
		// Output hash and metadata
		fmt.Fprintf(os.Stdout, "%s\t%s\t%s\n", resource.ObjectHash, resource.Name, resource.CreatedAt)
	}

	if resourceCount == 0 {
		exportLogger.Errorf("No resources found with name '%s'", resourceName)
		os.Exit(1)
	} else {
		exportLogger.Successf("Listed %d resource(s)", resourceCount)
	}
}

func exportResourceByHash(database Database, hash string) {
	exportLogger.Printf("Exporting resource with hash: %s", color.MagentaString(hash[:16]+"..."))

	// Check if object exists
	if !database.ObjectExists(hash) {
		exportLogger.Errorf("Object with hash '%s' not found", hash)
		os.Exit(1)
	}

	// Get object data
	data, err := database.GetObject(hash)
	if err != nil {
		exportLogger.Errorf("Failed to get object %s: %v", hash[:16], err)
		os.Exit(1)
	}

	// Write raw content to stdout
	os.Stdout.Write(data)
	
	exportLogger.Successf("Exported %d bytes", len(data))
}

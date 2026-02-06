package export

import (
	"fmt"
	"os"

	"grit/db"

	"github.com/fatih/color"
)

func exportResourcesByName(database db.Database, resourceName string) {
	exportLogger.Printf("Listing resources with name: %s\n", color.MagentaString(resourceName))

	// List all resources with the given name
	resourceCount := 0
	for resource := range database.GetResourcesByName(resourceName) {
		resourceCount++
		// Output hash and metadata
		fmt.Fprintf(os.Stdout, "%s\t%s\t%s\n", resource.ObjectHash, resource.Name, resource.CreatedAt)
	}

	if resourceCount == 0 {
		exportLogger.Printf("No resources found with name '%s'\n", resourceName)
		os.Exit(1)
	} else {
		exportLogger.Printf("Listed %d resource(s)\n", resourceCount)
	}
}

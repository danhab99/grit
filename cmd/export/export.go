// Description: Export resources from the database
package export

import (
	"flag"
	"fmt"
	"os"

	"grit/db"
	"grit/log"

	"github.com/fatih/color"
)

var exportLogger = log.NewLogger("EXPORT")

// Command flags
var (
	dbPath *string
	name   *string
	hash   *string
)

// RegisterFlags sets up the flags for the export command
func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
	name = fs.String("name", "", "list resource hashes by name")
	hash = fs.String("hash", "", "export file content by hash")
}

// Execute runs the export command
func Execute() {
	if (*name == "" && *hash == "") || (*name != "" && *hash != "") {
		fmt.Fprintf(os.Stderr, "Error: specify exactly one of -name or -hash\n")
		os.Exit(1)
	}

	exportLogger.Printf("Initializing database at: %s\n", *dbPath)
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	if *name != "" {
		exportResourcesByName(database, *name)
	} else if *hash != "" {
		exportResourceByHash(database, *hash)
	}
}

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

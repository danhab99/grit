// Description: Export resources from the database
package export

import (
	"flag"
	"fmt"
	"os"

	"grit/db"
	"grit/log"
)

var exportLogger = log.NewLogger("EXPORT")

// Command flags
var (
	dbPath     *string
	name       *string
	hash       *string
	tarOut     *string
	compressed *bool
	csvOut     *string
)

// RegisterFlags sets up the flags for the export command
func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
	name = fs.String("name", "", "list resource hashes by name")
	hash = fs.String("hash", "", "export file content by hash")
	tarOut = fs.String("tar", "", "export all resources to tarball")
	compressed = fs.Bool("compressed", true, "Compress the tarball")
	csvOut = fs.String("csv", "", "export resource table as CSV (use '-' for stdout)")
}

// Execute runs the export command
func Execute() {
	if *name == "" && *hash == "" && *tarOut == "" && *csvOut == "" {
		fmt.Fprintf(os.Stderr, "Error: specify one of -name, -hash, -tar, or -csv\n")
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
	} else if *tarOut != "" {
		exportTarball(database, *tarOut, *compressed, []string{})
	} else if *csvOut != "" {
		exportResourceTableCSV(database, *csvOut)
	}
}

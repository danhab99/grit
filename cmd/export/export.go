// Description: Export resources from the database
package export

import (
	"flag"
	"fmt"
	"os"

	"grit/db"
	"grit/log"

	"github.com/danhab99/idk/chans"
)

var exportLogger = log.NewLogger("EXPORT")

// Command flags
var (
	dbPath     *string
	name       *string
	hash       *string
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

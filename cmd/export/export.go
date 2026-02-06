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
	tarOut     *string
	compressed *bool
)

// RegisterFlags sets up the flags for the export command
func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
	name = fs.String("name", "", "list resource hashes by name")
	hash = fs.String("hash", "", "export file content by hash")
	tarOut = fs.String("tar", "export.tar.gz", "export file content by hash")
	compressed = fs.Bool("--compressed", true, "Compress the tarball")
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
	} else if *tarOut != "" {
		exportTarball(database, *tarOut, *compressed, []string{})
	}
}

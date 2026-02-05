// Description: Show pipeline status and statistics
package status

import (
	"flag"
	"fmt"
	"os"

	"grit/db"
	"grit/log"
)

var statusLogger = log.NewLogger("STATUS")

// Command flags
var (
	dbPath *string
)

// RegisterFlags sets up the flags for the status command
func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
}

// Execute shows the pipeline status
func Execute() {
	statusLogger.Printf("Checking pipeline status at: %s\n", *dbPath)

	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	// Get pipeline statistics
	complete, totalTasks, processedTasks, err := database.GetPipelineStatus()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting status: %v\n", err)
		os.Exit(1)
	}

	resourceCount, err := database.CountResources()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error counting resources: %v\n", err)
		os.Exit(1)
	}

	stepCount, err := database.CountSteps()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error counting steps: %v\n", err)
		os.Exit(1)
	}

	// Display status
	fmt.Printf("\n📊 Pipeline Status\n")
	fmt.Printf("==================\n\n")
	fmt.Printf("Steps:          %d total\n", stepCount)
	fmt.Printf("Tasks:          %d total, %d processed, %d remaining\n", totalTasks, processedTasks, totalTasks-processedTasks)
	fmt.Printf("Resources:      %d total\n", resourceCount)
	fmt.Printf("Complete:       %v\n", complete)
	fmt.Printf("\n")
}

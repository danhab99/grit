package progress

import (
	"flag"
	"fmt"
	"math"

	"grit/db"
	"grit/log"

	"github.com/danhab99/idk/chans"
)

var progressLogger = log.NewLogger("progress")

// Command flags
var (
	dbPath *string
)

// RegisterFlags sets up the flags for the status command
func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
}

// Execute runs the command
func Execute() {
	db, err := db.NewDatabase(*dbPath)
	if err != nil {
		panic(err)
	}

	fmt.Println("Progress...")

	allSteps := <-chans.Accumulate(db.ListSteps())

	padLen := 0
	for _, step := range allSteps {
		padLen = max(len(step.Name), padLen)
	}

	for _, step := range allSteps {

		totalSteps, err := db.CountTasksForStep(step.ID)
		if err != nil {
			panic(err)
		}

		uncompletedSteps, err := db.CountUnprocessedTasksForStep(step.ID)
		if err != nil {
			panic(err)
		}

		completedSteps := totalSteps - uncompletedSteps
		completedPercentage := (float64(completedSteps) / float64(totalSteps)) * 100

		if math.IsNaN(completedPercentage) {
			completedPercentage = 0
		}

		fmt.Printf("  %*s => %3.4f%%\n", padLen, step.Name, completedPercentage)
	}
}

// Description: Run the pipeline
package run

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"grit/db"
	"grit/exec"
	"grit/fuse"
	"grit/log"
	"grit/manifest"
	"grit/pipeline"
	"grit/utils"

	"github.com/pelletier/go-toml"
)

var runLogger = log.NewLogger("RUN")

// Command flags
var (
	manifestPath *string
	dbPath       *string
	parallel     *int
	enabledSteps stringSlice
)

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// RegisterFlags sets up the flags for the run command
func RegisterFlags(fs *flag.FlagSet) {
	manifestPath = fs.String("manifest", "", "manifest path (required)")
	dbPath = fs.String("db", "./db", "database path")
	parallel = fs.Int("parallel", runtime.NumCPU(), "number of processes to run in parallel")
	fs.Var(&enabledSteps, "step", "steps to run (can be specified multiple times)")
}

// Execute runs the pipeline
func Execute() {
	if *manifestPath == "" {
		fmt.Fprintf(os.Stderr, "Error: -manifest is required\n")
		os.Exit(1)
	}

	runLogger.Printf("Loading manifest from: %s\n", *manifestPath)

	manifestToml, err := os.ReadFile(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading manifest: %v\n", err)
		os.Exit(1)
	}

	var m manifest.Manifest
	err = toml.Unmarshal(manifestToml, &m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing manifest: %v\n", err)
		os.Exit(1)
	}
	runLogger.Printf("Loaded %d steps from manifest\n", len(m.Steps))

	// Check disk space before opening database
	utils.CheckDiskSpace(*dbPath)

	runLogger.Printf("Initializing database at: %s\n", *dbPath)
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	run(m, database, *parallel, enabledSteps)
}

func constructRunnerPipeline(m manifest.Manifest, database db.Database, enabledSteps []string) ([]db.Step, *pipeline.Pipeline, *fuse.FuseWatcher, func()) {
	steps := m.RegisterSteps(&database, enabledSteps)

	runLogger.Printf("Registered %d steps\n", len(m.Steps))

	outputChan := database.MakeResourceConsumer()

	fuseWatcher, err := fuse.NewTempDirFuseWatcher(outputChan)
	if err != nil {
		panic(err)
	}

	executor := exec.NewScriptExecutor(&database, fuseWatcher.GetMountPath())

	// Create pipeline with single FUSE server
	pipeline, err := pipeline.NewPipeline(executor, &database)
	if err != nil {
		panic(err)
	}

	return steps, pipeline, fuseWatcher, func() {
		fuseWatcher.Stop()
		fuseWatcher.WaitForWrites()
	}
}

func run(m manifest.Manifest, database db.Database, parallel int, enabledSteps []string) {
	startTime := time.Now()

	steps, pipeline, fuseWatcher, stop := constructRunnerPipeline(m, database, enabledSteps)
	defer stop()

	// Check if we need to seed
	resourceCount, err := database.CountResources()
	if err != nil {
		panic(err)
	}

	// Execute all steps
	var totalExecutions int64

	if resourceCount == 0 {
		runLogger.Printf("No resources found, running seed steps\n")

		for step := range database.GetStepsWithZeroInputs() {
			totalExecutions += pipeline.ExecuteStep(step, parallel)
		}
	}

	// run twice to check that everything is done
	for range 2 {
		for _, step := range steps {
			executions := pipeline.ExecuteStep(step, parallel)
			totalExecutions += executions

			if executions > 0 {
				runLogger.Printf("Step %s: executed %d tasks\n", step.Name, executions)
			}
			fuseWatcher.WaitForWrites()
		}
	}

	duration := time.Since(startTime)
	runLogger.Printf("Pipeline complete: %d tasks executed in %s\n", totalExecutions, duration.Round(time.Millisecond))
}

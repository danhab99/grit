// Description: Run the pipeline
package run

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"grit/db"
	"grit/exec"
	"grit/types"
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
	pprofAddr    *string
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
	pprofAddr = fs.String("pprof", "", "enable pprof HTTP server on this address (e.g. :6060)")
}

// Execute runs the pipeline
func Execute() {
	// Set before any allocations so every allocation is sampled.
	runtime.MemProfileRate = 1

	if *pprofAddr != "" {
		go func() {
			runLogger.Printf("pprof listening on http://%s/debug/pprof/\n", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				runLogger.Printf("pprof server error: %v\n", err)
			}
		}()
	}

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

func constructRunnerPipeline(m manifest.Manifest, database db.Database, enabledSteps []string) ([]types.Step, *pipeline.Pipeline, func()) {
	steps := m.RegisterSteps(&database, enabledSteps)

	runLogger.Printf("Registered %d steps\n", len(steps))

	executor := exec.NewScriptExecutor(&database)

	// Create pipeline
	pipeline, err := pipeline.NewPipeline(executor, &database)
	if err != nil {
		panic(err)
	}

	stop := func() {
		database.Close()
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGABRT, os.Interrupt, os.Kill)
		<-c
		stop()
	}()

	return steps, pipeline, stop
}

func run(m manifest.Manifest, database db.Database, parallel int, enabledSteps []string) {
	startTime := time.Now()

	// Ingest CSV files before pipeline execution
	if len(m.CsvFiles) > 0 {
		csvCount, err := m.IngestCsvFiles(&database)
		if err != nil {
			runLogger.Printf("Error ingesting CSV files: %v\n", err)
			panic(err)
		}
		if csvCount > 0 {
			runLogger.Printf("Ingested %d rows from %d CSV file(s)\n", csvCount, len(m.CsvFiles))
		}
	}

	steps, pipeline, stop := constructRunnerPipeline(m, database, enabledSteps)
	defer stop()

	// Check if we need to seed
	resourceCount, err := database.CountResources()
	if err != nil {
		panic(err)
	}

	// Execute all steps
	var totalStepExecutions int64

	if resourceCount == 0 {
		runLogger.Printf("No resources found, running seed steps\n")

		for step := range database.GetStepsWithZeroInputs() {
			totalStepExecutions += pipeline.ExecuteStep(step, parallel)
		}
	}

	resourceCount, err = database.CountResources()
	if err != nil {
		panic(err)
	}

	if resourceCount == 0 {
		panic("No resources were seeded")
	}

	// run twice to check that everything is done
	for range 2 {
		for _, step := range steps {
			executions := pipeline.ExecuteStep(step, parallel)
			totalStepExecutions += executions

			if executions > 0 {
				runLogger.Printf("Step %s: executed %d tasks\n", step.Name, executions)
			}
		}
	}

	duration := time.Since(startTime)
	runLogger.Printf("Pipeline complete: %d step tasks executed in %s\n", totalStepExecutions, duration.Round(time.Millisecond))
}

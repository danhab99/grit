// Description: Run the pipeline
package run

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
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
	manifestPath   *string
	dbPath         *string
	parallel       *int
	enabledSteps   stringSlice
	enabledColumns stringSlice
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
	fs.Var(&enabledColumns, "column", "columns to run (can be specified multiple times)")
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
	if err = toml.Unmarshal(manifestToml, &m); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing manifest: %v\n", err)
		os.Exit(1)
	}
	runLogger.Printf("Loaded %d steps and %d columns from manifest\n", len(m.Steps), len(m.Columns))

	utils.CheckDiskSpace(*dbPath)

	runLogger.Printf("Initializing database at: %s\n", *dbPath)
	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	startTime := time.Now()

	if len(m.CsvFiles) > 0 {
		csvCount, err := m.IngestCsvFiles(&database)
		if err != nil {
			panic(err)
		}
		if csvCount > 0 {
			runLogger.Printf("Ingested %d rows from %d CSV file(s)\n", csvCount, len(m.CsvFiles))
		}
	}

	steps := m.RegisterSteps(&database, enabledSteps)
	columns := m.RegisterColumns(&database, enabledColumns)
	runLogger.Printf("Registered %d steps and %d columns\n", len(steps), len(columns))

	outputChan := database.MakeResourceConsumer()
	fuseWatcher, err := fuse.NewTempDirFuseWatcher(outputChan)
	if err != nil {
		panic(err)
	}

	executor := exec.NewScriptExecutor(&database, fuseWatcher.GetMountPath())
	p, err := pipeline.NewPipeline(executor, &database)
	if err != nil {
		panic(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGABRT, os.Interrupt)
	stop := func() {
		signal.Stop(sigCh)
		fuseWatcher.Stop()
		fuseWatcher.WaitForWrites()
		close(outputChan)
		database.Close()
	}
	go func() {
		<-sigCh
		runLogger.Println("Interrupted, shutting down...")
		stop()
		os.Exit(1)
	}()
	defer stop()

	resourceCount, err := database.CountResources()
	if err != nil {
		panic(err)
	}

	var totalStepExecutions int64
	var totalColumnExecutions int64

	if resourceCount == 0 {
		runLogger.Println("No resources found, running seed steps")
		for step := range database.GetStepsWithZeroInputs() {
			totalStepExecutions += p.ExecuteStep(step, *parallel)
		}
		fuseWatcher.WaitForWrites()
		database.WaitForResourceCommit()

		resourceCount, err = database.CountResources()
		if err != nil {
			panic(err)
		}
		if resourceCount == 0 {
			panic("No resources were seeded")
		}
	}

	// Run twice to ensure all dependent steps complete.
	for range 2 {
		for _, step := range steps {
			executions := p.ExecuteStep(step, *parallel)
			totalStepExecutions += executions
			if executions > 0 {
				runLogger.Printf("Step %s: executed %d tasks\n", step.Name, executions)
				fuseWatcher.WaitForWrites()
				database.WaitForResourceCommit()
			}
		}
		for _, column := range columns {
			executions := p.ExecuteColumn(column, *parallel)
			totalColumnExecutions += executions
			if executions > 0 {
				runLogger.Printf("Column %s: executed %d tasks\n", column.Name, executions)
			}
		}
	}

	duration := time.Since(startTime)
	runLogger.Printf("Pipeline complete: %d step tasks, %d column tasks executed in %s\n", totalStepExecutions, totalColumnExecutions, duration.Round(time.Millisecond))
}

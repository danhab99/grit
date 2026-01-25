package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/fatih/color"
	"github.com/pelletier/go-toml"
)

const LOG_FLAGS = log.Lshortfile | log.Lmicroseconds | log.Ldate

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

var mainLogger = NewColorLogger("[MAIN] ", color.New(color.FgMagenta, color.Bold))

func main() {
	manifest_path := flag.String("manifest", "", "manifest path")
	db_path := flag.String("db", "./db", "database path")
	parallel := flag.Int("parallel", runtime.NumCPU(), "number of processes to run in parallel")
	exportName := flag.String("export", "", "list resource hashes by name")
	exportHash := flag.String("export-hash", "", "export file content by hash")
	runPipeline := flag.Bool("run", false, "run the pipeline")
	startStep := flag.String("start", "", "step to start from (optional, defaults to start step in manifest)")
	verbose := flag.Bool("verbose", false, "enable verbose logging")
	quiet := flag.Bool("quiet", false, "minimal output (overrides verbose)")

	var enabledSteps stringSlice
	flag.Var(&enabledSteps, "step", "steps to run")

	flag.Parse()

	// Set log level based on flags
	if *quiet {
		SetLogLevel(LogLevelQuiet)
	} else if *verbose {
		SetLogLevel(LogLevelVerbose)
	} else {
		SetLogLevel(LogLevelNormal)
	}

	mainLogger.Printf("Loading manifest from: %s", *manifest_path)

	manifest_toml, err := os.ReadFile(*manifest_path)
	if err != nil {
		panic(err)
	}

	var manifest Manifest
	err = toml.Unmarshal(manifest_toml, &manifest)
	if err != nil {
		panic(err)
	}
	mainLogger.Successf("Loaded %d steps from manifest", len(manifest.Steps))

	// Check disk space before opening database
	checkDiskSpace(*db_path)

	mainLogger.Verbosef("Initializing database at: %s", *db_path)
	database, err := NewDatabase(*db_path)
	if err != nil {
		panic(err)
	}

	if *runPipeline {
		run(manifest, database, *parallel, *startStep, enabledSteps)
	} else if exportName != nil && *exportName != "" {
		exportResourcesByName(database, *exportName)
	} else if exportHash != nil && *exportHash != "" {
		exportResourceByHash(database, *exportHash)
	}
}

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/pelletier/go-toml"
)

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

var mainLogger = log.New(os.Stderr, "[MAIN] ", log.Ldate|log.Ltime|log.Lmsgprefix)

func main() {
	manifest_path := flag.String("manifest", "", "manifest path")
	db_path := flag.String("db", "./db", "database path")
	parallel := flag.Int("parallel", runtime.NumCPU(), "number of processes to run in parallel")
	exportName := flag.String("export", "", "export a specific step")
	exportMode := flag.String("export-mode", "output", "export mode: 'input' or 'output' (default: output)")
	runPipeline := flag.Bool("run", false, "run the pipeline")
	startStep := flag.String("start", "", "step to start from (optional, defaults to start step in manifest)")

	var enabledSteps stringSlice
	flag.Var(&enabledSteps, "step", "steps to run")

	flag.Parse()

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
	mainLogger.Printf("Loaded %d steps from manifest", len(manifest.Steps))

	mainLogger.Printf("Initializing database at: %s", *db_path)
	database, err := NewDatabase(*db_path)
	if err != nil {
		panic(err)
	}

	if *runPipeline {
		run(manifest, database, *parallel, *startStep, enabledSteps)
	} else if exportName != nil && *exportName != "" {
		exportResults(database, *exportName, *exportMode)
	}
}

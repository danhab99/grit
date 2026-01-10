package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/pelletier/go-toml"
)

func main() {
	manifest_path := flag.String("manifest", "", "manifest path")
	db_path := flag.String("db", "./db", "database path")
	parallel := flag.Int("parallel", runtime.NumCPU(), "number of processes to run in parallel")
	exportName := flag.String("export", "", "export a specific task")
	runPipeline := flag.Bool("run", false, "run the pipeline")

	flag.Parse()

	fmt.Printf("Loading manifest from: %s\n", *manifest_path)

	manifest_toml, err := os.ReadFile(*manifest_path)
	if err != nil {
		panic(err)
	}

	var manifest Manifest
	err = toml.Unmarshal(manifest_toml, &manifest)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d tasks from manifest\n", len(manifest.Tasks))

	fmt.Printf("Initializing database at: %s\n", *db_path)
	database, err := NewDatabase(*db_path)
	if err != nil {
		panic(err)
	}

	if *runPipeline {
		run(manifest, database, *parallel)
	} else if export != nil && *exportName != "" {
		export(manifest, database, *exportName)
	}
}

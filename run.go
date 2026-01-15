package main

import (
	"log"
	"os"
	"strings"
)

var runLogger = log.New(os.Stderr, "[RUN] ", log.Ldate|log.Ltime|log.Lmsgprefix)

func extractStepName(filename string) string {
	base := filename
	if idx := strings.LastIndex(filename, "."); idx != -1 {
		base = filename[:idx]
	}

	if idx := strings.Index(base, "_"); idx != -1 {
		return base[:idx]
	}

	return base
}

func run(manifest Manifest, database Database, parallel int, startStepName string, enabledSteps []string) {
	runLogger.Println("Registered steps")

	es := make([]Step, len(enabledSteps))

	for _, step := range manifest.Steps {
		s := Step{
			Name:     step.Name,
			Script:   step.Script,
			IsStart:  step.Start,
			Parallel: step.Parallel,
		}

		database.CreateStep(s)
		es = append(es, s)
	}

	runLogger.Println("Stubbed done task")
	database.CreateStep(Step{
		Name:     "done",
		Script:   "true",
		IsStart:  false,
		Parallel: nil,
	})

	pipeline := NewPipeline(&database, es)

	totalExecCount := int64(0)
	execCount := int64(1)
	for execCount > 0 {
		runLogger.Println("--- BEGIN EXECUTION ---")
		c := pipeline.Execute(startStepName, parallel)
		execCount = c
		totalExecCount += c
	}

	runLogger.Printf("Completed processing %d tasks", totalExecCount)
}

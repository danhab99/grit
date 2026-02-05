package main

import (
	"grit/db"
	"grit/exec"
	"grit/fuse"
	"grit/log"
	"grit/pipeline"
	"time"
)

var runLogger = log.NewLogger("RUN")

func constructRunnerPipeline(manifest Manifest, database db.Database, enabledSteps []string) ([]db.Step, *pipeline.Pipeline) {
	steps := manifest.RegisterSteps(&database, enabledSteps)

	runLogger.Printf("Registered %d steps\n", len(manifest.Steps))

	outputChan := make(chan fuse.FileData)

	fuseWatcher, err := fuse.NewTempDirFuseWatcher(outputChan)
	if err != nil {
		panic(err)
	}

	executor := exec.NewScriptExecutor(&database, fuseWatcher.GetMountPath(), outputChan)

	// Create pipeline with single FUSE server
	pipeline, err := pipeline.NewPipeline(executor, &database)
	if err != nil {
		panic(err)
	}

	return steps, pipeline
}

func run(manifest Manifest, database db.Database, parallel int, enabledSteps []string) {
	startTime := time.Now()

	steps, pipeline := constructRunnerPipeline(manifest, database, enabledSteps)

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
		}
	}

	duration := time.Since(startTime)
	runLogger.Printf("Pipeline complete: %d tasks executed in %s\n", totalExecutions, duration.Round(time.Millisecond))
}

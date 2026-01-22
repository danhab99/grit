package main

import (
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/danhab99/idk/chans"
	"github.com/danhab99/idk/workers"
	"github.com/fatih/color"
)

type Pipeline struct {
	db           *Database
	enabledSteps []Step
}

func NewPipeline(d *Database, steps []Step) Pipeline {
	return Pipeline{d, steps}
}

var pipelineLogger = NewColorLogger("[PIPELINE] ", color.New(color.FgCyan, color.Bold))

func (p *Pipeline) Execute(startStepName string, maxParallel int) int64 {
	db := p.db
	var numberOfExecutions int64

	steps := <-chans.Accumulate(db.ListSteps())
	stepsIndex := make(map[int64]Step)
	for _, s := range steps {
		if len(p.enabledSteps) > 0 {
			if slices.ContainsFunc(p.enabledSteps, func(step Step) bool {
				return s.Name == step.Name
			}) {
				stepsIndex[s.ID] = s
			}
		} else {
			stepsIndex[s.ID] = s
		}
	}

	p.Seed()

	for _, step := range stepsIndex {
		numberOfExecutions += p.ExecuteStep(step, maxParallel)
	}

	return numberOfExecutions
}

func (p Pipeline) ExecuteTask(t Task) {
	db := p.db

	step, err := db.GetStep(*t.StepID)
	if err != nil {
		panic(err)
	}

	t.Processed = true

	// Execute the script
	executor := NewScriptExecutor(db, &p)
	execErr := executor.Execute(t, *step)

	// Update task status
	var errorMsg *string
	if execErr != nil {
		msg := execErr.Error()
		errorMsg = &msg
	}
	err = db.UpdateTaskStatus(t.ID, true, errorMsg)
	if err != nil {
		panic(err)
	}
}

func (p Pipeline) Seed() {
	db := p.db

	startStep, err := db.GetStartingStep()
	if err != nil {
		panic(err)
	}
	if startStep == nil {
		panic("start step cannot be nil")
	}

	// Try to find an unprocessed task first
	var seedTask *Task
	for task := range db.GetUnprocessedTasks(startStep.ID) {
		seedTask = &task
		break
	}

	if seedTask == nil {
		// No unprocessed tasks, create a new one
		prestartTask := Task{
			StepID: &startStep.ID,
		}

		startTaskId, err := db.CreateTask(prestartTask)
		if err != nil {
			panic(err)
		}

		seedTask, err = db.GetTask(startTaskId)
		if err != nil {
			panic(err)
		}
	}

	p.ExecuteTask(*seedTask)

	err = db.UpdateStepStatus(startStep.ID, true)
	if err != nil {
		panic(err)
	}
}

func (p Pipeline) ExecuteStep(s Step, maxParallel int) int64 {
	db := p.db

	// Force save WAL before executing step
	if err := db.ForceSaveWAL(); err != nil {
		pipelineLogger.Errorf("Failed to checkpoint WAL before step '%s': %v", s.Name, err)
		panic(err)
	}

	color.New(color.FgCyan, color.Bold).Fprintf(os.Stderr, "\n▶ ")
	pipelineLogger.Printf("Step: %s", color.New(color.FgMagenta, color.Bold).Sprint(s.Name))

	numberOfExecutions := int64(0)
	numberOfUnprocessedTasks := int64(0)

	// Get unprocessed tasks channel once and collect them
	unprocessedTasksList := []Task{}
	for task := range db.GetUnprocessedTasks(s.ID) {
		if !task.Processed {
			numberOfUnprocessedTasks++
			unprocessedTasksList = append(unprocessedTasksList, task)
		}
	}
	pipelineLogger.Verbosef("  Collected %d unprocessed tasks for step '%s'", numberOfUnprocessedTasks, s.Name)

	if numberOfUnprocessedTasks == 0 {
		pipelineLogger.Verbosef("  No unprocessed tasks for step '%s'", s.Name)
		return 0
	}

	// Count total tasks and already-processed tasks for this step
	totalTasks, processedTasks, err := db.GetTaskCountsForStep(s.ID)
	if err != nil {
		pipelineLogger.Errorf("Failed to get task counts: %v", err)
		panic(err)
	}

	pipelineLogger.Printf("  %s: Starting (%d/%d already completed)", s.Name, processedTasks, totalTasks)

	par := s.Parallel
	if par == nil {
		par = &maxParallel
	}

	// Simple progress tracking without progress bar
	var completedCount int64
	var mu sync.Mutex
	lastPrint := time.Now()

	// Create a channel from the collected tasks for workers
	unprocessedTasks := make(chan Task)
	go func() {
		defer close(unprocessedTasks)
		for _, task := range unprocessedTasksList {
			unprocessedTasks <- task
		}
	}()

	workers.Parallel0(unprocessedTasks, *par, func(task Task) {
		if task.Processed {
			return
		}

		p.ExecuteTask(task)
		numberOfExecutions++

		mu.Lock()
		completedCount++
		elapsed := time.Since(lastPrint)
		// Print progress update every 2 seconds or every 100 tasks
		if elapsed > 2*time.Second || completedCount%100 == 0 {
			pipelineLogger.Printf("  %s: %d/%d tasks completed", s.Name, completedCount, numberOfUnprocessedTasks)
			lastPrint = time.Now()
		}
		mu.Unlock()
	})

	// Print final status
	pipelineLogger.Printf("  %s: %d/%d tasks completed", s.Name, completedCount, numberOfUnprocessedTasks)

	pipelineLogger.Successf("  Step '%s' complete: %d/%d tasks", s.Name, numberOfExecutions, numberOfUnprocessedTasks)
	err = db.UpdateStepStatus(s.ID, true)
	if err != nil {
		panic(err)
	}

	fmt.Println("returning")

	return numberOfExecutions
}

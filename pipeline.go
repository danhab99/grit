package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"

	"sync"

	"github.com/danhab99/idk/chans"
	"github.com/danhab99/idk/workers"
	"github.com/fatih/color"
)

type Pipeline struct {
	db           *Database
	enabledSteps []Step
	tracker      *TaskTracker
}

func NewPipeline(d *Database, steps []Step) Pipeline {
	return Pipeline{d, steps, NewTaskTracker()}
}

var pipelineLogger = NewColorLogger("[PIPELINE] ", color.New(color.FgCyan, color.Bold))

func (p *Pipeline) Execute(startStepName string, maxParallel int) int64 {
	db := p.db
	var numberOfExecutions int64

	steps := <-chans.Accumulate(db.ListSteps())
	stepsIndex := make(map[int64]Step)
	for _, s := range steps {
		// if s.IsStart {
		// 	continue
		// }
		// if slices.ContainsFunc(p.enabledSteps, func(e Step) bool {
		// 	return s.Name == e.Name
		// }) {
		// 	continue
		// }
		// if s.Processed {
		// 	continue
		// }
		stepsIndex[s.ID] = s
	}

	// runtime.Breakpoint()
	p.Seed()

	for _, step := range stepsIndex {
		numberOfExecutions += p.ExecuteStep(step)
	}

	p.tracker.PrintPipelineSummary(len(stepsIndex), numberOfExecutions)

	return numberOfExecutions
}

func (p Pipeline) ExecuteTask(t Task) {
	db := p.db

	step, err := db.GetStep(*t.StepID)
	if err != nil {
		panic(err)
	}

	p.tracker.StartTask(t.ID, step.Name, t.ObjectHash)

	t.Processed = true

	inputFile, err := os.CreateTemp("/tmp", "input-*")
	if err != nil {
		panic(err)
	}
	defer os.Remove(inputFile.Name())

	if t.ObjectHash != "" {
		objectPath := db.GetObjectPath(t.ObjectHash)
		data, err := os.Open(objectPath)
		if err != nil {
			panic(err)
		}
		n, err := io.Copy(inputFile, data)
		if err != nil {
			panic(err)
		}
		pipelineLogger.Verbosef("    Input: %d bytes from %s", n, t.ObjectHash[:16]+"...")

	} else {
		pipelineLogger.Verbosef("    Input: (empty - start step)")
	}
	inputFile.Close()

	outputDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(outputDir)

	pipelineLogger.Verbosef("    Executing: %s", step.Script)
	cmd := exec.Command("sh", "-c", step.Script)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile.Name()),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

	if err := cmd.Start(); err != nil {
		pipelineLogger.Errorf("    Error starting script: %v", err)
		panic(err)
	}

	scriptLogger := NewColorLogger(fmt.Sprintf("[SCRIPT:%s] ", step.Name), color.New(color.FgYellow))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Verboseln(scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stderr] %s", scanner.Text())
		}
	}()

	wg.Wait()

	if err := cmd.Wait(); err != nil {
		pipelineLogger.Errorf("    Error executing script: %v", err)
	}

	// runtime.Breakpoint()
	err = db.UpdateStepStatus(t.ID, true)
	if err != nil {
		panic(err)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		panic(err)
	}

	entriesChan := make(chan os.DirEntry)

	go func() {
		defer close(entriesChan)
		for _, entry := range entries {
			entriesChan <- entry
		}
	}()

	err = db.UpdateTaskStatus(t.ID, true, nil)
	if err != nil {
		panic(err)
	}

	// Track scheduled tasks before completing
	scheduledBefore := p.tracker.GetScheduledSummary()

	type UnfinishedTask struct {
		task  Task
		step  *Step
		entry os.DirEntry
	}

	unfinishedTasksChan := make(chan UnfinishedTask)

	go func() {
		defer close(unfinishedTasksChan)

		workers.Parallel(entriesChan, unfinishedTasksChan, runtime.NumCPU(), func(entry os.DirEntry) UnfinishedTask {
			if entry.IsDir() {
				return UnfinishedTask{}
			}

			filename := entry.Name()
			stepName := extractStepName(filename)
			filePath := fmt.Sprintf("%s/%s", outputDir, filename)

			var isCompleted bool

			nextStep, err := db.GetStepByName(stepName)
			if err != nil {
				panic(err)
			}
			if nextStep != nil {
				isCompleted, err = db.IsTaskCompletedInNextStep(nextStep.ID, t.ID)
				if err != nil {
					panic(err)
				}

				if isCompleted {
					pipelineLogger.Verbosef("		Task %d already completed in next step", t.ID)
					return UnfinishedTask{}
				}
			}

			pipelineLogger.Verbosef("		Output: %s -> %s", filename, color.MagentaString(stepName))

			hash, err := hashFileSHA256(filePath)
			if err != nil {
				panic(err)
			}

			// Only set InputTaskID if current task has a valid DB ID
			var inputTaskID *int64
			if t.ID > 0 {
				inputTaskID = &t.ID
			}

			pTask := Task{
				ObjectHash:  hash,
				InputTaskID: inputTaskID,
				Processed:   isCompleted,
				StepID:      &nextStep.ID,
			}

			return UnfinishedTask{pTask, nextStep, entry}
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var buffer []UnfinishedTask
		var bufferTasks []Task

		for unfinishedTask := range unfinishedTasksChan {
			buffer = append(buffer, unfinishedTask)
			bufferTasks = append(bufferTasks, unfinishedTask.task)

			if len(buffer) >= 100 {
				completedTasks, err := db.BatchInsertTasks(bufferTasks)
				if err != nil {
					panic(err)
				}

				for i, cTask := range completedTasks {
					uTask := buffer[i]
					buffer[i].task = cTask
					if uTask.step != nil {
						p.tracker.ScheduleTask(unfinishedTask.step.Name)
					}

					filename := unfinishedTask.entry.Name()
					filePath := fmt.Sprintf("%s/%s", outputDir, filename)

					objectPath := db.GetObjectPath(unfinishedTask.task.ObjectHash)
					_, err = copyFileWithSHA256(filePath, objectPath)
					if err != nil {
						panic(err)
					}
				}
			}
		}
	}()

	// Show what was scheduled
	scheduledAfter := p.tracker.GetScheduledSummary()
	newTasksScheduled := make(map[string]int64)
	for stepName, count := range scheduledAfter {
		if beforeCount, exists := scheduledBefore[stepName]; exists {
			if count > beforeCount {
				newTasksScheduled[stepName] = count - beforeCount
			}
		} else if count > 0 {
			newTasksScheduled[stepName] = count
		}
	}

	wg.Wait()

	p.tracker.CompleteTask(t.ID, true)

	// Show scheduled tasks for this execution
	if len(newTasksScheduled) > 0 && GetLogLevel() >= LogLevelNormal {
		for stepName, count := range newTasksScheduled {
			color.New(color.FgBlue).Fprintf(os.Stderr, "│   ")
			color.New(color.FgCyan).Fprintf(os.Stderr, "+%d queued", count)
			color.New(color.FgWhite).Fprintf(os.Stderr, " → %s\n", color.MagentaString(stepName))
		}
	}

}

// func (p Pipeline) IterateUnprocessed() chan Task {
// 	db := p.db

// 	var tasksChans []chan Task

// 	for step := range db.ListSteps() {
// 		if !slices.Contains(p.enabledSteps, step) {
// 			continue
// 		}
// 		if step.IsStart {
// 			continue
// 		}

// 		c := db.GetUnprocessedTasks(step.ID)
// 		tasksChans = append(tasksChans, c)
// 	}

// 	return chans.Merge(tasksChans...)
// }

func (p Pipeline) Seed() {
	db := p.db

	startStep, err := db.GetStartingStep()
	if err != nil {
		panic(err)
	}
	if startStep == nil {
		panic("start step cannot be nil")
	}

	processedTaskCount := 0

	for task := range db.GetTasksForStep(startStep.ID) {
		if task.Processed {
			processedTaskCount++
		}
	}

	if processedTaskCount == 0 {
		prestartTask := Task{
			StepID: &startStep.ID,
		}

		startTaskId, err := db.CreateTask(prestartTask)
		if err != nil {
			panic(err)
		}

		startTask, err := db.GetTask(startTaskId)
		p.ExecuteTask(*startTask)
	}

	err = db.UpdateStepStatus(startStep.ID, true)
	if err != nil {
		panic(err)
	}
}

func (p Pipeline) ExecuteStep(s Step) int64 {
	db := p.db

	numberOfExecutions := int64(0)
	numberOfUnprocessedTasks := int64(0)

	// Count unprocessed tasks first
	for task := range db.GetUnprocessedTasks(s.ID) {
		if !task.Processed {
			numberOfUnprocessedTasks++
		}
	}

	if numberOfUnprocessedTasks == 0 {
		if GetLogLevel() >= LogLevelVerbose {
			color.New(color.FgBlue).Fprintf(os.Stderr, "│ ")
			color.New(color.FgYellow).Fprintf(os.Stderr, "skipping")
			color.New(color.FgWhite).Fprintf(os.Stderr, " %s (no pending tasks)\n", s.Name)
		}
		return 0
	}

	// Start step tracking
	p.tracker.StartStep(s.Name, numberOfUnprocessedTasks)

	par := s.Parallel
	if par == nil {
		x := runtime.NumCPU()
		par = &x
	}
	workers.Parallel0(db.GetUnprocessedTasks(s.ID), int(*par), func(task Task) {
		if task.Processed {
			return
		}

		p.ExecuteTask(task)
		numberOfExecutions++
	})

	p.tracker.FinishStep(true)

	err := db.UpdateStepStatus(s.ID, true)
	if err != nil {
		panic(err)
	}

	return numberOfExecutions
}

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

var runLogger = log.New(os.Stderr, "[RUN] ", log.Ldate|log.Ltime|log.Lmsgprefix)

func extractTaskName(filename string) string {
	base := filename
	if idx := strings.LastIndex(filename, "."); idx != -1 {
		base = filename[:idx]
	}

	if idx := strings.Index(base, "_"); idx != -1 {
		return base[:idx]
	}

	return base
}

func run(manifest Manifest, database Database, parallel int, startTaskName string) {
	runLogger.Println("Registering tasks...")
	for _, task := range manifest.Tasks {
		if task.Start {
			runLogger.Printf("Task: %s (START TASK)", task.Name)
		} else {
			runLogger.Printf("Task: %s", task.Name)
		}
		if task.Parallel != nil {
			runLogger.Printf("  Parallel limit: %d", *task.Parallel)
		}
		database.RegisterTask(task.Name, task.Script, task.Start, task.Parallel)
	}

	// Determine which task to start from
	var startTask *Task
	var err error

	if startTaskName != "" {
		runLogger.Printf("Starting from task: %s", startTaskName)
		startTask, err = database.GetTaskByName(startTaskName)
		if err != nil {
			panic(err)
		}
		if startTask == nil {
			panic(fmt.Sprintf("Task '%s' not found", startTaskName))
		}

		// Mark all results for this task as unprocessed to re-run them
		count, err := database.MarkTaskResultsUnprocessed(startTaskName)
		if err != nil {
			panic(err)
		}
		if count > 0 {
			runLogger.Printf("Marked %d existing results as unprocessed for task '%s'", count, startTaskName)
		} else {
			// No existing results, create an initial empty one
			_, _, err := database.InsertResult("", &startTask.ID, nil)
			if err != nil {
				panic(err)
			}
			runLogger.Printf("Created initial result for task '%s' (no existing results found)", startTask.Name)
		}
	} else {
		startTask, err = database.GetStartTask()
		if err != nil {
			panic(err)
		}
		if startTask == nil {
			panic("No start task found in manifest")
		}
		runLogger.Printf("Starting from default start task: %s", startTask.Name)

		// Create initial result for the start task if it doesn't exist
		_, isNew, err := database.InsertResult("", &startTask.ID, nil)
		if err != nil {
			panic(err)
		}
		if isNew {
			runLogger.Printf("Created initial result for task '%s'", startTask.Name)
		} else {
			runLogger.Printf("Initial result for task '%s' already exists", startTask.Name)
		}
	}

	// Track semaphores for per-task parallelism limits
	taskSemaphores := make(map[int64]chan struct{})
	var semMutex sync.Mutex

	var wg sync.WaitGroup
	jobs := make(chan Result, parallel)

	// Worker function that respects per-task parallelism
	processWithLimit := func(result Result, db Database) {
		// Get task to check for parallelism limit
		task, err := db.GetTaskByID(*result.TaskID)
		if err != nil {
			panic(err)
		}

		// Acquire slot from task-specific semaphore if limit is set
		var sem chan struct{}
		if task.Parallel != nil {
			semMutex.Lock()
			if taskSemaphores[task.ID] == nil {
				taskSemaphores[task.ID] = make(chan struct{}, *task.Parallel)
			}
			sem = taskSemaphores[task.ID]
			semMutex.Unlock()

			sem <- struct{}{}        // Acquire
			defer func() { <-sem }() // Release
		}

		result.deriveResults(db)
	}

	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for result := range jobs {
				processWithLimit(result, database)
			}
		}()
	}

	totalProcessed := 0
	for {
		unprocessed, err := database.GetUnprocessedResults()
		if err != nil {
			panic(err)
		}

		if len(unprocessed) == 0 {
			break
		}

		runLogger.Printf("Processing %d unprocessed results...", len(unprocessed))
		for _, result := range unprocessed {
			jobs <- result
			totalProcessed++
		}

		close(jobs)
		wg.Wait()

		jobs = make(chan Result, parallel)
		for i := 0; i < parallel; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for result := range jobs {
					processWithLimit(result, database)
				}
			}()
		}
	}

	close(jobs)
	wg.Wait()

	runLogger.Printf("Completed processing %d results", totalProcessed)
}

func (r Result) deriveResults(db Database) {
	task, err := db.GetTaskByID(*r.TaskID)
	if err != nil {
		panic(err)
	}

	runLogger.Printf("Processing result %d for task '%s'", r.ID, task.Name)

	err = db.MarkResultProcessed(r.ID)
	if err != nil {
		panic(err)
	}

	inputFile, err := os.CreateTemp("/tmp", "input-*")
	if err != nil {
		panic(err)
	}
	defer os.Remove(inputFile.Name())

	if r.ObjectHash != "" {
		objectPath := db.GetObjectPath(r.ObjectHash)
		data, err := os.ReadFile(objectPath)
		if err != nil {
			panic(err)
		}
		runLogger.Printf("  Input: %d bytes from %s", len(data), r.ObjectHash[:16]+"...")
		_, err = inputFile.Write(data)
		if err != nil {
			panic(err)
		}
	} else {
		runLogger.Println("  Input: (empty - start task)")
	}
	inputFile.Close()

	outputDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(outputDir)

	runLogger.Printf("  Executing script for task '%s'", task.Name)
	cmd := exec.Command("sh", "-c", task.Script)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile.Name()),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)

	// Capture stdout and stderr
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		panic(err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		runLogger.Printf("  Error starting script: %v", err)
		panic(err)
	}

	// Create a script logger for this specific task
	scriptLogger := log.New(os.Stderr, fmt.Sprintf("[SCRIPT:%s] ", task.Name), log.Ldate|log.Ltime|log.Lmsgprefix)

	// Stream stdout
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Println(scanner.Text())
		}
	}()

	// Stream stderr
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Printf("[stderr] %s", scanner.Text())
		}
	}()

	// Wait for output streaming to complete
	wg.Wait()

	// Wait for command to complete
	if err := cmd.Wait(); err != nil {
		runLogger.Printf("  Error executing script: %v", err)
		// panic(err)
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		panic(err)
	}

	newCount := 0
	skippedCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		taskName := extractTaskName(filename)
		filePath := fmt.Sprintf("%s/%s", outputDir, filename)

		runLogger.Printf("  Output: %s -> task '%s'", filename, taskName)

		targetTask, err := db.GetTaskByName(taskName)
		if err != nil {
			runLogger.Printf("    Warning: Error looking up task '%s': %v", taskName, err)
			continue
		}

		var taskID *int64
		if targetTask != nil {
			taskID = &targetTask.ID
		} else {
			runLogger.Printf("    (terminal output - no task '%s')", taskName)
			// taskID remains nil for terminal results
		}

		hash, err := hashFileSHA256(filePath)
		if err != nil {
			panic(err)
		}

		objectPath := db.GetObjectPath(hash)
		_, err = copyFileWithSHA256(filePath, objectPath)
		if err != nil {
			panic(err)
		}

		_, isNew, err := db.InsertResult(hash, taskID, &r.ID)
		if err != nil {
			panic(err)
		}

		if isNew {
			newCount++
		} else {
			skippedCount++
		}
	}

	runLogger.Printf("  Created %d new results, %d already existed", newCount, skippedCount)
}

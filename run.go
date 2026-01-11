package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
)

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

func run(manifest Manifest, database Database, parallel int) {
	fmt.Println("Registering tasks...")
	for _, task := range manifest.Tasks {
		fmt.Printf("  - %s", task.Name)
		if task.Start {
			fmt.Printf(" (START TASK)")
		}
		fmt.Println()
		database.RegisterTask(task.Name, task.Script, task.Start)
	}

	startTask, err := database.GetStartTask()
	if err != nil {
		panic(err)
	}

	if startTask != nil {
		unprocessed, err := database.GetUnprocessedResults()
		if err != nil {
			panic(err)
		}

		if len(unprocessed) == 0 {
			fmt.Println("Seeding start task:", startTask.Name)
			database.InsertResult("", startTask.ID, nil)
		}
	}

	var wg sync.WaitGroup
	jobs := make(chan Result, parallel)

	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for result := range jobs {
				processResult(result, database)
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

		fmt.Printf("\nProcessing %d results...\n", len(unprocessed))
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
					processResult(result, database)
				}
			}()
		}
	}

	close(jobs)
	wg.Wait()

	fmt.Printf("\nCompleted processing %d results\n", totalProcessed)
}

func processResult(r Result, db Database) {
	task, err := db.GetTaskByID(r.TaskID)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Processing result %d for task '%s'\n", r.ID, task.Name)

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
		fmt.Printf("  Input: %d bytes from %s\n", len(data), r.ObjectHash[:16]+"...")
		_, err = inputFile.Write(data)
		if err != nil {
			panic(err)
		}
	} else {
		fmt.Printf("  Input: (empty - start task)\n")
	}
	inputFile.Close()

	outputDir, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(outputDir)

	cmd := exec.Command("sh", "-c", task.Script)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile.Name()),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("  Error executing script: %s\n", string(output))
		panic(err)
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

		fmt.Printf("  Output: %s -> task '%s'\n", filename, taskName)

		targetTask, err := db.GetTaskByName(taskName)
		if err != nil {
			fmt.Printf("    Warning: Error looking up task '%s': %v\n", taskName, err)
			continue
		}
		if targetTask == nil {
			fmt.Printf("    Warning: Task '%s' not found, skipping\n", taskName)
			continue
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

		_, isNew, err := db.InsertResult(hash, targetTask.ID, &r.ID)
		if err != nil {
			panic(err)
		}

		if isNew {
			newCount++
		} else {
			skippedCount++
		}
	}

	fmt.Printf("  Created %d new results, %d already existed\n", newCount, skippedCount)
}

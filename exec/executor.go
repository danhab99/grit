package exec

import (
	"bufio"
	"fmt"
	"grit/db"
	"grit/fuse"
	"grit/log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/danhab99/idk/workers"
)

type ScriptExecutor struct {
	db         *db.Database
	mountPath  string
	outputChan chan fuse.FileData
}

func NewScriptExecutor(db *db.Database, mountPath string, outputChan chan fuse.FileData) *ScriptExecutor {
	return &ScriptExecutor{db, mountPath, outputChan}
}

var executeLogger = log.NewLogger("EXEC")

// func (e *ScriptExecutor) ExecuteStep(step db.Step, defaultParallel int) error {
// 	database := e.db
// 	executeLogger.Println("Running unfinished tasks for step", step.Name)

// 	p := defaultParallel
// 	if step.Parallel != nil {
// 		p = min(defaultParallel, *step.Parallel)
// 	}

// 	workers.Parallel0(database.GetUnprocessedTasks(step.ID), p, func(task db.Task) {
// 		err := e.Execute(task, step)
// 		if err != nil {
// 			panic(err)
// 		}
// 	})

// 	return nil
// }

func (e *ScriptExecutor) Execute(task db.Task, step db.Step) error {
	// executeLogger.Printf("Executing task ID=%d for step '%s' (step_id=%d)\n", task.ID, step.Name, task.StepID)
	start := time.Now()

	// Create input file
	inputFile, err := os.CreateTemp("/tmp", "input-*")
	if err != nil {
		return fmt.Errorf("failed to create input file: %w", err)
	}
	defer os.Remove(inputFile.Name())

	// Write input data if exists
	if err := e.prepareInput(task, inputFile); err != nil {
		return err
	}
	inputFile.Close()

	// Execute the script
	executeLogger.Verbosef("Executing: %s\n", step.Script)
	cmd := e.buildCommand(step, inputFile.Name(), e.mountPath)

	// Run script and capture output
	if err := e.runScript(cmd, step); err != nil {
		return err
	}

	elapsedTime := time.Since(start)

	executeLogger.Printf("Executed task ID=%d for step '%s' successfully in %s\n", task.ID, step.Name, elapsedTime.String())
	return nil
}

func (e *ScriptExecutor) prepareInput(task db.Task, inputFile *os.File) error {
	// Get input resource if task has one
	if task.InputResourceID != nil {
		inputResource, err := e.db.GetResource(*task.InputResourceID)
		if err != nil {
			return fmt.Errorf("failed to get input resource: %w", err)
		}

		data, err := e.db.GetObject(inputResource.ObjectHash)
		if err != nil {
			return fmt.Errorf("failed to get object: %w", err)
		}

		n, err := inputFile.Write(data)
		if err != nil {
			return fmt.Errorf("failed to write input data: %w", err)
		}
		executeLogger.Verbosef("Input: %d bytes from resource '%s' (hash: %s)\n", n, inputResource.Name, inputResource.ObjectHash[:16]+"...")
	} else {
		executeLogger.Verbosef("Input: (empty - start step)\n")
	}

	return nil
}

func (e *ScriptExecutor) buildCommand(step db.Step, inputFile, outputDir string) *exec.Cmd {
	cmd := exec.Command("sh", "-c", step.Script)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)
	return cmd
}

func (e *ScriptExecutor) runScript(cmd *exec.Cmd, step db.Step) error {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		executeLogger.Printf("Error starting script: %v\n", err)
		return fmt.Errorf("failed to start script: %w", err)
	}

	scriptLogger := executeLogger.Context(step.Name)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stdout] %s\n", scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			scriptLogger.Verbosef("[stderr] %s\n", scanner.Text())
		}
	}()

	// Wait for command to finish (closes pipes)
	err = cmd.Wait()

	// Then wait for goroutines to finish reading
	wg.Wait()

	if err != nil {
		executeLogger.Printf("Error executing script: %v\n", err)
		return fmt.Errorf("script execution failed: %w", err)
	}

	return nil
}

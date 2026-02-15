package exec

import (
	"bufio"
	"fmt"
	"grit/db"
	"grit/log"
	"os"
	"os/exec"
	"sync"
	"time"
)

type ScriptExecutor struct {
	db        *db.Database
	mountPath string
}

func NewScriptExecutor(db *db.Database, mountPath string) *ScriptExecutor {
	return &ScriptExecutor{db, mountPath}
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
	cmd := e.buildCommand(step, inputFile.Name(), e.mountPath, task.ID)

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

func (e *ScriptExecutor) buildCommand(step db.Step, inputFile, outputDir string, taskID int64) *exec.Cmd {
	cmd := exec.Command("sh", "-c", step.Script)
	outdir := fmt.Sprintf("%s/task_%d", outputDir, taskID)

	// Ensure the per-task output directory exists (creates via FUSE)
	if err := os.MkdirAll(outdir, 0755); err != nil {
		executeLogger.Printf("Error creating output dir %s: %v\n", outdir, err)
		panic(err)
	}

	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", inputFile),
		fmt.Sprintf("OUTPUT_DIR=%s", outdir),
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

func (e *ScriptExecutor) runColumnScript(cmd *exec.Cmd, column db.Column) error {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		executeLogger.Printf("Error starting column script: %v\n", err)
		return fmt.Errorf("failed to start column script: %w", err)
	}

	scriptLogger := executeLogger.Context("col:" + column.Name)

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
		executeLogger.Printf("Error executing column script: %v\n", err)
		return fmt.Errorf("column script execution failed: %w", err)
	}

	return nil
}

// ExecuteColumnTask executes a column task for computing a column value for a resource
func (e *ScriptExecutor) ExecuteColumnTask(task db.ColumnTask, column db.Column) error {
	start := time.Now()

	// Create input directory for dependency column values
	inputDir, err := os.MkdirTemp("", "col-input-*")
	if err != nil {
		return fmt.Errorf("failed to create input directory: %w", err)
	}
	defer os.RemoveAll(inputDir)

	// Get the resource for this task
	resource, err := e.db.GetResource(task.ResourceID)
	if err != nil {
		return fmt.Errorf("failed to get resource: %w", err)
	}

	// Write resource data to input directory as "data" file (the implicit dependency)
	resourceData, err := e.db.GetObject(resource.ObjectHash)
	if err != nil {
		return fmt.Errorf("failed to get resource object: %w", err)
	}

	dataFile := fmt.Sprintf("%s/data", inputDir)
	if err := os.WriteFile(dataFile, resourceData, 0644); err != nil {
		return fmt.Errorf("failed to write resource data: %w", err)
	}
	executeLogger.Verbosef("Column input: %d bytes from resource '%s' (hash: %s) -> data\n", len(resourceData), resource.Name, resource.ObjectHash[:16]+"...")

	// Write dependency column values to input directory
	for _, depName := range column.Dependencies {
		colValue, err := e.db.GetColumnValueByColumnName(depName, task.ResourceID)
		if err != nil {
			return fmt.Errorf("failed to get column value for %s: %w", depName, err)
		}
		if colValue == nil {
			return fmt.Errorf("missing column value for dependency %s", depName)
		}

		depData, err := e.db.GetObject(colValue.ObjectHash)
		if err != nil {
			return fmt.Errorf("failed to get object for column %s: %w", depName, err)
		}

		depFile := fmt.Sprintf("%s/%s", inputDir, depName)
		if err := os.WriteFile(depFile, depData, 0644); err != nil {
			return fmt.Errorf("failed to write dependency data for %s: %w", depName, err)
		}
		executeLogger.Verbosef("Column input: %d bytes from column '%s' -> %s\n", len(depData), depName, depName)
	}

	// Create output directory
	outputDir, err := os.MkdirTemp("", "col-output-*")
	if err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}
	defer os.RemoveAll(outputDir)

	// Build and execute the command
	cmd := e.buildColumnCommand(column, inputDir, outputDir)

	if err := e.runColumnScript(cmd, column); err != nil {
		return err
	}

	// Read output file (should be named after the column)
	outputFile := fmt.Sprintf("%s/%s", outputDir, column.Name)
	outputData, err := os.ReadFile(outputFile)
	if err != nil {
		return fmt.Errorf("failed to read column output file %s: %w", outputFile, err)
	}

	// Store the output as a column value
	hash, err := e.db.StoreObjectAndGetHash(outputData)
	if err != nil {
		return fmt.Errorf("failed to store column value: %w", err)
	}

	_, err = e.db.CreateColumnValue(column.ID, task.ResourceID, hash)
	if err != nil {
		return fmt.Errorf("failed to create column value record: %w", err)
	}

	elapsedTime := time.Since(start)

	executeLogger.Printf("Executed column task ID=%d for column '%s' resource=%d successfully in %s\n", task.ID, column.Name, task.ResourceID, elapsedTime.String())
	return nil
}

func (e *ScriptExecutor) buildColumnCommand(column db.Column, inputDir, outputDir string) *exec.Cmd {
	cmd := exec.Command("sh", "-c", column.Script)

	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_DIR=%s", inputDir),
		fmt.Sprintf("OUTPUT_DIR=%s", outputDir),
	)
	return cmd
}

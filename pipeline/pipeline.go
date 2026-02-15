package pipeline

import (
	"runtime"
	"sync/atomic"

	"grit/db"
	"grit/exec"
	"grit/log"

	"github.com/danhab99/idk/workers"
)

var pipelineLogger = log.NewLogger("PIPELINE")

type Pipeline struct {
	database *db.Database
	executor *exec.ScriptExecutor
}

func NewPipeline(executor *exec.ScriptExecutor, database *db.Database) (*Pipeline, error) {
	return &Pipeline{database, executor}, nil
}

func (p *Pipeline) ExecuteStep(step db.Step, maxParallel int) int64 {
	database := p.database

	if len(step.Inputs) == 0 {
		pipelineLogger.Printf("Executing seed step %s\n", step.Name)

		var startTask db.Task

		startStepCount, err := database.CountTasksForStep(step.ID)
		if err != nil {
			panic(err)
		}

		if startStepCount > 0 {
			startTask = <-database.GetTasksForStep(step.ID)
		} else {
			t, err := database.CreateAndGetTask(db.Task{
				StepID: step.ID,
			})
			if err != nil {
				panic(err)
			}
			startTask = *t
		}

		if !startTask.Processed {
			err = p.executor.Execute(startTask, step)
			var errorMsg *string
			if err != nil {
				msg := err.Error()
				errorMsg = &msg
				pipelineLogger.Printf("Seed task %d failed: %v\n", startTask.ID, err)
			}

			// Mark the seed task as processed
			err = database.UpdateTaskStatus(startTask.ID, true, errorMsg)
			if err != nil {
				pipelineLogger.Printf("Error updating seed task %d: %v\n", startTask.ID, err)
			}
			return 1
		}

		return 0
	}

	// Schedule new tasks for this step
	tasksCreated, err := database.ScheduleTasksForStep(step.ID)
	if err != nil {
		pipelineLogger.Printf("Error scheduling tasks for step %s: %v\n", step.Name, err)
		return 0
	}

	if tasksCreated > 0 {
		pipelineLogger.Printf("Step %s: scheduled %d new tasks\n", step.Name, tasksCreated)
	}

	err = database.ForceSaveWAL()
	if err != nil {
		panic(err)
	}
	taskChan := database.GetUnprocessedTasks(step.ID)

	var executionCount atomic.Int64
	pr := step.Parallel
	if pr == nil {
		x := runtime.NumCPU()
		pr = &x
	}

	workers.Parallel0(taskChan, *pr, func(task db.Task) {
		pipelineLogger.Verbosef("Executing task %d for step %s\n", task.ID, step.Name)

		execErr := p.executor.Execute(task, step)

		var errorMsg *string
		if execErr != nil {
			msg := execErr.Error()
			errorMsg = &msg
			pipelineLogger.Printf("Task %d failed: %v\n", task.ID, execErr)
		}

		err = database.UpdateTaskStatus(task.ID, true, errorMsg)
		if err != nil {
			pipelineLogger.Printf("Error updating task %d: %v\n", task.ID, err)
		}

		executionCount.Add(1)
	})

	return executionCount.Load()
}

// ExecuteColumn executes all pending tasks for a column
func (p *Pipeline) ExecuteColumn(column db.Column, maxParallel int) int64 {
	database := p.database

	// Schedule new column tasks
	tasksCreated, err := database.ScheduleColumnTasksForColumn(column.ID)
	if err != nil {
		pipelineLogger.Printf("Error scheduling tasks for column %s: %v\n", column.Name, err)
		return 0
	}

	if tasksCreated > 0 {
		pipelineLogger.Printf("Column %s: scheduled %d new tasks\n", column.Name, tasksCreated)
	}

	err = database.ForceSaveWAL()
	if err != nil {
		panic(err)
	}
	taskChan := database.GetUnprocessedColumnTasks(column.ID)

	var executionCount atomic.Int64
	pr := column.Parallel
	if pr == nil {
		x := runtime.NumCPU()
		pr = &x
	}

	workers.Parallel0(taskChan, *pr, func(task db.ColumnTask) {
		pipelineLogger.Verbosef("Executing column task %d for column %s (resource=%d)\n", task.ID, column.Name, task.ResourceID)

		execErr := p.executor.ExecuteColumnTask(task, column)

		var errorMsg *string
		if execErr != nil {
			msg := execErr.Error()
			errorMsg = &msg
			pipelineLogger.Printf("Column task %d failed: %v\n", task.ID, execErr)
		}

		err = database.UpdateColumnTaskStatus(task.ID, true, errorMsg)
		if err != nil {
			pipelineLogger.Printf("Error updating column task %d: %v\n", task.ID, err)
		}

		executionCount.Add(1)
	})

	return executionCount.Load()
}

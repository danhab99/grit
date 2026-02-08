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
			if err != nil {
				panic(err)
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

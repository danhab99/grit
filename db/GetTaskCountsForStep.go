package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetTaskCountsForStep(stepID int64) (int64, int64, error) {
	totalTasks, err := d.CountTasksForStep(stepID)
	if err != nil {
		return 0, 0, err
	}

	unprocessedTasks, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return totalTasks, 0, err
	}

	processedTasks := totalTasks - unprocessedTasks
	return totalTasks, processedTasks, nil
}

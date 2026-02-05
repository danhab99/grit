package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetUnprocessedTasks(stepID int64) chan Task {
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)
		var taskCount int64 = 0
		defer func() {
			dbLogger.Verbosef("GetUnprocessedTasks(step=%d) found %d unprocessed tasks\n", stepID, taskCount)
		}()

		// Get all unprocessed tasks for this step
		rows, err := d.db.Query(`
			SELECT t.id, t.step_id, t.input_resource_id, t.processed, t.error
			FROM task t
			WHERE t.step_id = ? 
			  AND t.processed = 0
			ORDER BY t.id
		`, stepID)
		if err != nil {
			dbLogger.Verbosef("Error querying unprocessed tasks for step %d: %v\n", stepID, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.ID, &t.StepID, &t.InputResourceID, &t.Processed, &t.Error); err != nil {
				dbLogger.Verbosef("Error scanning task for step %d: %v\n", stepID, err)
				return
			}
			taskCount++
			taskChan <- t
		}

		if err := rows.Err(); err != nil {
			dbLogger.Verbosef("Error iterating tasks for step %d: %v\n", stepID, err)
		}
	}()

	return taskChan
}

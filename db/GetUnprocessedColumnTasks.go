package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetUnprocessedColumnTasks(columnID int64) chan ColumnTask {
	taskChan := make(chan ColumnTask)

	go func() {
		defer close(taskChan)
		var taskCount int64 = 0
		defer func() {
			dbLogger.Verbosef("GetUnprocessedColumnTasks(column=%d) found %d unprocessed column tasks\n", columnID, taskCount)
		}()

		// Get all unprocessed column tasks for this column
		rows, err := d.db.Query(`
			SELECT ct.id, ct.column_id, ct.resource_id, ct.processed, ct.error
			FROM column_task ct
			WHERE ct.column_id = ? 
			  AND ct.processed = 0
			ORDER BY ct.id
		`, columnID)
		if err != nil {
			dbLogger.Verbosef("Error querying unprocessed column tasks for column %d: %v\n", columnID, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var t ColumnTask
			if err := rows.Scan(&t.ID, &t.ColumnID, &t.ResourceID, &t.Processed, &t.Error); err != nil {
				dbLogger.Verbosef("Error scanning column task for column %d: %v\n", columnID, err)
				return
			}
			taskCount++
			taskChan <- t
		}

		if err := rows.Err(); err != nil {
			dbLogger.Verbosef("Error iterating column tasks for column %d: %v\n", columnID, err)
		}
	}()

	return taskChan
}

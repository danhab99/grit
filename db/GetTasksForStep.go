package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetTasksForStep(stepID int64) chan Task {
	taskChan := make(chan Task)

	go func() {
		defer close(taskChan)

		rows, err := d.db.Query(`
			SELECT id, step_id, input_resource_id, processed, error 
			FROM task 
			WHERE step_id = ?
			ORDER BY id
		`, stepID)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := rows.Scan(&t.ID, &t.StepID, &t.InputResourceID, &t.Processed, &t.Error); err != nil {
				panic(err)
			}
			taskChan <- t
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return taskChan
}

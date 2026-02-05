package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) MarkStepTasksUnprocessed(stepID int64) error {
	// runtime.Breakpoint()
	_, err := d.db.Exec(`
UPDATE task 
SET processed = 0, error = NULL
WHERE step_id = ?
`, stepID)
	return err
}

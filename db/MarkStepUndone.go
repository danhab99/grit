package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) MarkStepUndone(stepID int64) error {
	// Delete all tasks and resources for this step
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete all tasks for this step
	result, err := tx.Exec("DELETE FROM task WHERE step_id = ?", stepID)
	if err != nil {
		return err
	}

	tasksDeleted, _ := result.RowsAffected()

	if err := tx.Commit(); err != nil {
		return err
	}

	dbLogger.Verbosef("Marked step %d as undone: deleted %d tasks and their resources\n", stepID, tasksDeleted)
	return nil
}

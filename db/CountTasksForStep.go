package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CountTasksForStep(stepID int64) (int64, error) {
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE step_id = ?", stepID)
	var count int64
	err := row.Scan(&count)
	return count, err
}

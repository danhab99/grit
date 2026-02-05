package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CountUnprocessedTasksForStep(stepID int64) (int64, error) {
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE step_id = ? AND processed = 0", stepID)
	var count int64
	err := row.Scan(&count)
	return count, err
}

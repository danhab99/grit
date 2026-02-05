package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CountUnprocessedTasks() (int64, error) {
	row := d.db.QueryRow("SELECT COUNT(*) FROM task WHERE processed = 0")
	var count int64
	err := row.Scan(&count)
	return count, err
}

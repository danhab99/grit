package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) TaskExists(id int64) (bool, error) {
	var exists bool
	err := d.db.QueryRow("SELECT EXISTS(SELECT 1 FROM task WHERE id = ?)", id).Scan(&exists)
	return exists, err
}

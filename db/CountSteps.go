package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CountSteps() (int64, error) {
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM step").Scan(&count)
	return count, err
}

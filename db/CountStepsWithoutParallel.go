package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CountStepsWithoutParallel() (int64, error) {
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM step WHERE parallel IS NOT NULL").Scan(&count)
	return count, err
}

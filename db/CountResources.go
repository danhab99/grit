package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CountResources() (int64, error) {
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM resource").Scan(&count)
	return count, err
}

package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) DeleteStep(id int64) error {
	_, err := d.db.Exec("DELETE FROM step WHERE id = ?", id)
	return err
}

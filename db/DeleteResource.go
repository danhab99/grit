package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) DeleteResource(id int64) error {
	_, err := d.db.Exec("DELETE FROM resource WHERE id = ?", id)
	return err
}

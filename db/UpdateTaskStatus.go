package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) UpdateTaskStatus(id int64, processed bool, errorMsg *string) error {
	_, err := d.db.Exec(`
UPDATE task 
SET processed = ?, error = ?
WHERE id = ?
`, processed, errorMsg, id)
	return err
}

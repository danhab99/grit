package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) UpdateColumnTaskStatus(id int64, processed bool, errorMsg *string) error {
	p := 0
	if processed {
		p = 1
	}
	_, err := d.execWithBusyRetry("UPDATE column_task SET processed = ?, error = ? WHERE id = ?", p, errorMsg, id)
	return err
}

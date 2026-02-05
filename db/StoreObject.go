package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) StoreObject(hash string, data []byte) error {
	// Use WriteBatch for better performance even for single writes
	wb := d.badgerDB.NewWriteBatch()
	defer wb.Cancel()

	if err := wb.Set([]byte(hash), data); err != nil {
		return err
	}

	return wb.Flush()
}

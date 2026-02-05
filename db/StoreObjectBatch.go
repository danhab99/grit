package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) StoreObjectBatch(objects map[string][]byte) error {
	wb := d.badgerDB.NewWriteBatch()
	defer wb.Cancel()

	for hash, data := range objects {
		if err := wb.Set([]byte(hash), data); err != nil {
			return err
		}
	}

	return wb.Flush()
}

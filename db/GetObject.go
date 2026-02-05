package db

import (
	badger "github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetObject(hash string) ([]byte, error) {
	var data []byte
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(hash))
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	return data, err
}

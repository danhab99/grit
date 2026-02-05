package db

import (
	badger "github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) ObjectExists(hash string) bool {
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(hash))
		return err
	})
	return err == nil
}

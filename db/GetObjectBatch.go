package db

import (
	badger "github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetObjectBatch(hashes []string) (map[string][]byte, error) {
	results := make(map[string][]byte)

	err := d.badgerDB.View(func(txn *badger.Txn) error {
		for _, hash := range hashes {
			item, err := txn.Get([]byte(hash))
			if err != nil {
				return err
			}
			data, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			results[hash] = data
		}
		return nil
	})

	return results, err
}

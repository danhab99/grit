package db

import (
	"encoding/hex"

	badger "github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetObject(hash string) ([]byte, error) {
	// Decode hex string to bytes since that's how the key is stored
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}

	var data []byte
	err = d.badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(hashBytes)
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	return data, err
}

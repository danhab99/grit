package db

import (
	"encoding/hex"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) StoreObject(hash string, data []byte) error {
	// Decode hex string to bytes since that's how the key should be stored
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}

	// Use WriteBatch for better performance even for single writes
	wb := d.badgerDB.NewWriteBatch()
	defer wb.Cancel()

	if err := wb.Set(hashBytes, data); err != nil {
		return err
	}

	return wb.Flush()
}

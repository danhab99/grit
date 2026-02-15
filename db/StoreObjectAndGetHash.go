package db

import (
	"crypto/sha256"
	"encoding/hex"

	_ "github.com/mattn/go-sqlite3"
)

// StoreObjectAndGetHash stores the data and returns its hash
func (d Database) StoreObjectAndGetHash(data []byte) (string, error) {
	hash := sha256.Sum256(data)
	hashStr := hex.EncodeToString(hash[:])

	err := d.StoreObject(hashStr, data)
	if err != nil {
		return "", err
	}

	return hashStr, nil
}

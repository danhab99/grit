package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CreateResourceFromReader(name string, reader io.Reader) (int64, string, error) {
	// Read all data and calculate hash
	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read data: %w", err)
	}

	// Calculate hash
	hasher := sha256.New()
	hasher.Write(data)
	hashBytes := hasher.Sum(nil)
	hash := hex.EncodeToString(hashBytes)

	// Check if object already exists in BadgerDB
	if !d.ObjectExists(hash) {
		// Store in BadgerDB
		if err := d.StoreObject(hash, data); err != nil {
			return 0, "", fmt.Errorf("failed to store object: %w", err)
		}
	}

	// Create resource record in SQLite
	resourceID, err := d.CreateResource(name, hash)
	if err != nil {
		return 0, "", fmt.Errorf("failed to create resource record: %w", err)
	}

	return resourceID, hash, nil
}

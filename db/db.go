package db

import (
	"fmt"
	"grit/log"
	"os"

	badger "github.com/dgraph-io/badger/v4"
)

var dbLogger = log.NewLogger("DB")

func NewDatabase(repo_path string) (Database, error) {
	err := os.MkdirAll(repo_path, 0755)
	if err != nil {
		return Database{}, err
	}

	if err := os.MkdirAll(repo_path+"/objects", 0755); err != nil {
		return Database{}, err
	}

	dbLogger.Verbosef("Opening BadgerDB at %s\n", repo_path)
	badgerOpts := badger.DefaultOptions(repo_path + "/db")
	badgerOpts.Logger = nil

	// Performance tuning
	badgerOpts.SyncWrites = false
	badgerOpts.NumVersionsToKeep = 1
	badgerOpts.CompactL0OnClose = false
	badgerOpts.ValueLogFileSize = 512 << 20
	badgerOpts.MemTableSize = 128 << 20
	badgerOpts.NumMemtables = 3
	badgerOpts.NumLevelZeroTables = 5
	badgerOpts.NumLevelZeroTablesStall = 10
	badgerOpts.ValueThreshold = 1024
	badgerOpts.NumCompactors = 2

	badgerDB, err := badger.Open(badgerOpts)
	if err != nil {
		return Database{}, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	dbLogger.Println("Database ready")
	return Database{repo_path, badgerDB}, nil
}

func (d Database) Close() error {
	if err := d.badgerDB.Close(); err != nil {
		return fmt.Errorf("failed to close BadgerDB: %w", err)
	}
	return nil
}

// ForceSaveWAL is a no-op. BadgerDB handles compaction automatically.
func (d Database) ForceSaveWAL() error {
	return nil
}


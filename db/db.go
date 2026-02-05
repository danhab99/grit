package db

import (
	"database/sql"
	"fmt"
	"grit/log"
	"os"
	"runtime"

	badger "github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
)

var dbLogger = log.NewLogger("DB")

func NewDatabase(repo_path string) (Database, error) {
	err := os.MkdirAll(repo_path, 0755)
	if err != nil {
		return Database{}, err
	}

	err = os.MkdirAll(repo_path+"/sqlite", 0755)
	if err != nil {
		return Database{}, err
	}

	dbLogger.Verbosef("Opening database at %s/db\n", repo_path)
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s/sqlite/db?timeout=600000", repo_path))
	if err != nil {
		return Database{}, err
	}

	// Set connection pool to reduce lock contention during checkpoint
	// Use multiple connections so readers/writers can make progress in parallel.
	numConns := runtime.NumCPU()
	db.SetMaxOpenConns(numConns)
	db.SetMaxIdleConns(numConns)

	// Force WAL checkpoint to clear the 173GB log before proceeding
	dbLogger.Println("Checkpointing WAL file (this may take a moment)...")
	// _, err = db.Exec("PRAGMA busy_timeout = 600000;")
	_, err = db.Exec("PRAGMA busy_timeout = 6;")
	if err != nil {
		return Database{}, err
	}

	// Checkpoint: restart to clear the wal file
	_, err = db.Exec("PRAGMA wal_autocheckpoint = 0;")
	if err != nil {
		return Database{}, err
	}

	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")
	db.Exec("PRAGMA foreign_keys=ON;")

	// Force checkpoint
	_, err = db.Exec("PRAGMA optimize;")
	if err != nil {
		dbLogger.Verbosef("Warning: PRAGMA optimize failed: %v\n", err)
	}

	dbLogger.Println("Initializing database schema")
	_, err = db.Exec(schema)
	if err != nil {
		return Database{}, err
	}

	// Initialize BadgerDB for object storage
	badgerPath := fmt.Sprintf("%s/objects_db", repo_path)
	dbLogger.Verbosef("Opening BadgerDB at %s\n", badgerPath)
	badgerOpts := badger.DefaultOptions(badgerPath)
	badgerOpts.Logger = nil // Disable BadgerDB's default logging

	// Performance tuning for sequential batch operations
	// Objects are written in batches during output processing, then read sequentially during task execution
	badgerOpts.SyncWrites = false           // Don't fsync on every write
	badgerOpts.NumVersionsToKeep = 1        // No version history for immutable data
	badgerOpts.CompactL0OnClose = false     // Faster shutdown
	badgerOpts.ValueLogFileSize = 512 << 20 // Larger value log (512MB) for batch writes
	badgerOpts.MemTableSize = 128 << 20     // Large memtable (128MB) for batch buffering
	badgerOpts.NumMemtables = 3             // More memtables for write-heavy batches
	badgerOpts.NumLevelZeroTables = 5       // Allow more L0 tables before compaction
	badgerOpts.NumLevelZeroTablesStall = 10 // Higher stall threshold
	badgerOpts.ValueThreshold = 1024        // Store larger values in value log for sequential read
	badgerOpts.NumCompactors = 2            // More compactors for background work

	badgerDB, err := badger.Open(badgerOpts)
	if err != nil {
		return Database{}, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	return Database{db, repo_path, badgerDB}, nil
}

// Step CRUD operations

// Resource CRUD operations

// CreateResourceFromReader reads data from an io.Reader, stores it in BadgerDB, and creates a resource record in SQLite.
// Returns the resource ID and the calculated hash.

// GetTaskInputResource returns the input resource for a task (if any)

// Task CRUD operations

// CreateTasksFromResources creates tasks for a given step from a set of input resources.
// Each resource will create a unique task (step_id, input_resource_id is unique).
// Returns the created task IDs.

// ScheduleTasksForStep creates tasks for all unconsumed resources matching the step's inputs.
// Uses a single SQL INSERT to efficiently schedule all tasks at once.
// Returns the number of new tasks created.

// Relational operators

// GetTaskCountsForStep returns (total tasks, processed tasks) for a given step

// Utility functions

// StoreObject stores object data in BadgerDB

// StoreObjectBatch stores multiple objects in a single batch (much faster)

// GetObject retrieves object data from BadgerDB

// GetObjectBatch retrieves multiple objects in a single transaction (faster for sequential reads)

// ObjectExists checks if an object exists in BadgerDB

// ForceSaveWAL performs a WAL checkpoint to ensure data is persisted to the database file

// Close closes both SQLite and BadgerDB connections

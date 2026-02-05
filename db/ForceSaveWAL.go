package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) ForceSaveWAL() error {
	dbLogger.Println("Checkpointing WAL...")
	_, err := d.db.Exec("PRAGMA wal_checkpoint(RESTART);")
	if err != nil {
		dbLogger.Verbosef("Error checkpointing WAL: %v\n", err)
		return err
	}
	dbLogger.Println("WAL checkpoint complete")
	return nil
}

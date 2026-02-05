package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) Close() error {
	if err := d.db.Close(); err != nil {
		return fmt.Errorf("failed to close SQLite: %w", err)
	}
	if err := d.badgerDB.Close(); err != nil {
		return fmt.Errorf("failed to close BadgerDB: %w", err)
	}
	return nil
}

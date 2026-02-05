package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) UpdateStepStatus(id int64, processed bool) error {
	// No-op: step processed status is no longer tracked
	return nil
}

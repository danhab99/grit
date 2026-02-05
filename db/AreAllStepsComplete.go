package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) AreAllStepsComplete() (bool, error) {
	// Step completion is no longer tracked, always return true
	return true, nil
}

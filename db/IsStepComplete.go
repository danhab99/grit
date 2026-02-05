package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) IsStepComplete(stepID int64) (bool, error) {
	count, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

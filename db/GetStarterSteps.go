package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetStarterSteps() chan Step {
	return d.GetStepsWithZeroInputs()
}

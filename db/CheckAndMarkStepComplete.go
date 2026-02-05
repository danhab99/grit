package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CheckAndMarkStepComplete(stepID int64) (bool, error) {
	isComplete, err := d.IsStepComplete(stepID)
	if err != nil {
		return false, err
	}

	if isComplete {
		step, err := d.GetStep(stepID)
		if err != nil {
			return false, err
		}

		// Only mark as processed if it wasn't already
		if step != nil {
			err = d.UpdateStepStatus(stepID, true)
			if err != nil {
				return false, err
			}
			dbLogger.Verbosef("Step %d (%s) marked as complete\n", stepID, step.Name)
		}
	}

	return isComplete, nil
}

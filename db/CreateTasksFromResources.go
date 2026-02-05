package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CreateTasksFromResources(stepID int64, resourceIDs []int64) ([]int64, error) {
	if len(resourceIDs) == 0 {
		return nil, nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO task (step_id, input_resource_id, processed, error)
		VALUES (?, ?, 0, NULL)
		ON CONFLICT(step_id, input_resource_id) DO NOTHING
	`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var taskIDs []int64
	for _, resourceID := range resourceIDs {
		res, err := stmt.Exec(stepID, resourceID)
		if err != nil {
			return nil, err
		}

		id, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}

		// If LastInsertId is 0, the insert was ignored (duplicate)
		if id > 0 {
			taskIDs = append(taskIDs, id)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return taskIDs, nil
}

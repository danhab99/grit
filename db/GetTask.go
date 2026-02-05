package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetTask(id int64) (*Task, error) {
	var t Task
	err := d.db.QueryRow("SELECT id, step_id, input_resource_id, processed, error FROM task WHERE id = ?", id).Scan(
		&t.ID, &t.StepID, &t.InputResourceID, &t.Processed, &t.Error,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

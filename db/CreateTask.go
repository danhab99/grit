package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CreateTask(task Task) (int64, error) {
	p := 0
	if task.Processed {
		p = 1
	}
	res, err := d.db.Exec(`
INSERT INTO task (step_id, input_resource_id, processed, error)
VALUES (?, ?, ?, ?)
`, task.StepID, task.InputResourceID, p, task.Error)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}

package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CreateColumnTask(task ColumnTask) (int64, error) {
	p := 0
	if task.Processed {
		p = 1
	}
	res, err := d.db.Exec(`
INSERT INTO column_task (column_id, resource_id, processed, error)
VALUES (?, ?, ?, ?)
`, task.ColumnID, task.ResourceID, p, task.Error)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}

func (d Database) CreateAndGetColumnTask(task ColumnTask) (*ColumnTask, error) {
	id, err := d.CreateColumnTask(task)
	if err != nil {
		return nil, err
	}
	task.ID = id
	return &task, nil
}

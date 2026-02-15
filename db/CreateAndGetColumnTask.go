package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CreateAndGetColumnTask(task ColumnTask) (*ColumnTask, error) {
	id, err := d.CreateColumnTask(task)
	if err != nil {
		return nil, err
	}
	task.ID = id
	return &task, nil
}

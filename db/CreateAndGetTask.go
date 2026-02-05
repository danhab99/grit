package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d *Database) CreateAndGetTask(t Task) (*Task, error) {
	taskId, err := d.CreateTask(t)
	if err != nil {
		return nil, err
	}

	return d.GetTask(taskId)
}

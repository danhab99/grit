package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) BatchInsertTasks(tasks []Task) ([]Task, error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
	INSERT INTO task (step_id, input_resource_id, processed, error)
	VALUES (?, ?, ?, ?)`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for i, task := range tasks {
		p := 0
		if task.Processed {
			p = 1
		}
		res, err := stmt.Exec(task.StepID, task.InputResourceID, p, task.Error)
		if err != nil {
			return nil, err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}
		tasks[i].ID = int64(id)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return tasks, nil
}

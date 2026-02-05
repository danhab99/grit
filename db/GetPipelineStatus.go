package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetPipelineStatus() (complete bool, totalTasks int64, processedTasks int64, err error) {
	err = d.db.QueryRow("SELECT COUNT(*) FROM task").Scan(&totalTasks)
	if err != nil {
		return false, 0, 0, err
	}

	err = d.db.QueryRow("SELECT COUNT(*) FROM task WHERE processed = 1").Scan(&processedTasks)
	if err != nil {
		return false, 0, 0, err
	}

	complete = totalTasks > 0 && totalTasks == processedTasks
	return complete, totalTasks, processedTasks, nil
}

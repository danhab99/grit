package db

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetTaskInputResource(taskID int64) (*Resource, error) {
	var r Resource
	err := d.db.QueryRow(`
		SELECT r.id, r.name, r.object_hash, r.created_at
		FROM resource r
		INNER JOIN task t ON r.id = t.input_resource_id
		WHERE t.id = ?
	`, taskID).Scan(&r.ID, &r.Name, &r.ObjectHash, &r.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

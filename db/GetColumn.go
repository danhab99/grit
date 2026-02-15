package db

import (
	"database/sql"
	"encoding/json"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetColumn(id int64) (*Column, error) {
	var c Column
	var parallel sql.NullInt64
	var depsJSON sql.NullString

	err := d.db.QueryRow(`
		SELECT id, name, resource_name, script, parallel, dependencies, version 
		FROM column_def WHERE id = ?
	`, id).Scan(&c.ID, &c.Name, &c.ResourceName, &c.Script, &parallel, &depsJSON, &c.Version)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if parallel.Valid {
		val := int(parallel.Int64)
		c.Parallel = &val
	}
	if depsJSON.Valid && depsJSON.String != "" {
		if err := json.Unmarshal([]byte(depsJSON.String), &c.Dependencies); err != nil {
			dbLogger.Verbosef("Warning: failed to unmarshal dependencies for column %d: %v\n", c.ID, err)
		}
	}

	return &c, nil
}

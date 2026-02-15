package db

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CreateColumn(column Column) (int64, error) {
	// Serialize dependencies to JSON for storage and comparison
	// Treat nil and empty slices the same way
	var depsStr string
	if len(column.Dependencies) > 0 {
		depsJSON, err := json.Marshal(column.Dependencies)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal dependencies: %w", err)
		}
		depsStr = string(depsJSON)
	} else {
		depsStr = "[]"
	}

	// Check if a column with the same name, resource_name, script, and dependencies already exists
	var existingID int64
	var existingDeps sql.NullString
	err := d.db.QueryRow("SELECT id, dependencies FROM column_def WHERE name = ? AND resource_name = ? AND script = ? ORDER BY version DESC LIMIT 1", column.Name, column.ResourceName, column.Script).Scan(&existingID, &existingDeps)
	if err == nil {
		// Column with same name, resource_name and script exists, check if dependencies match
		existingDepsStr := "[]"
		if existingDeps.Valid && existingDeps.String != "" {
			existingDepsStr = existingDeps.String
		}

		if existingDepsStr == depsStr {
			// Dependencies match, update parallel flag
			_, err := d.db.Exec("UPDATE column_def SET parallel = ? WHERE id = ?", column.Parallel, existingID)
			if err != nil {
				return 0, err
			}

			return existingID, nil
		}
		// Dependencies changed, need to create a new version
	}
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	// Get the max version for this column name and resource_name
	var maxVersion sql.NullInt64
	err = d.db.QueryRow("SELECT MAX(version) FROM column_def WHERE name = ? AND resource_name = ?", column.Name, column.ResourceName).Scan(&maxVersion)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	version := 1
	if maxVersion.Valid {
		version = int(maxVersion.Int64) + 1
	}

	res, err := d.db.Exec(`
INSERT INTO column_def (name, resource_name, script, parallel, dependencies, version)
VALUES (?, ?, ?, ?, ?, ?)
`, column.Name, column.ResourceName, column.Script, column.Parallel, depsStr, version)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

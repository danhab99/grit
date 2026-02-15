package db

import (
	"database/sql"
	"encoding/json"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) ListAllColumns() ([]Column, error) {
	rows, err := d.db.Query(`
		SELECT id, name, script, parallel, dependencies, version 
		FROM column_def 
		ORDER BY name, version DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []Column
	seenNames := make(map[string]bool)

	for rows.Next() {
		var column Column
		var parallel sql.NullInt64
		var depsJSON sql.NullString
		if err := rows.Scan(&column.ID, &column.Name, &column.Script, &parallel, &depsJSON, &column.Version); err != nil {
			return nil, err
		}

		// Only include the latest version of each column
		if seenNames[column.Name] {
			continue
		}
		seenNames[column.Name] = true

		if parallel.Valid {
			val := int(parallel.Int64)
			column.Parallel = &val
		}
		if depsJSON.Valid && depsJSON.String != "" {
			if err := json.Unmarshal([]byte(depsJSON.String), &column.Dependencies); err != nil {
				dbLogger.Verbosef("Warning: failed to unmarshal dependencies for column %d: %v\n", column.ID, err)
			}
		}
		columns = append(columns, column)
	}

	return columns, rows.Err()
}

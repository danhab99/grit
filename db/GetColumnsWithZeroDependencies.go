package db

import (
	"database/sql"
	"encoding/json"

	_ "github.com/mattn/go-sqlite3"
)

// GetColumnsWithZeroDependencies returns columns that have no dependencies
// These columns operate directly on the resource data
func (d Database) GetColumnsWithZeroDependencies() chan Column {
	columnChan := make(chan Column)

	go func() {
		defer close(columnChan)

		rows, err := d.db.Query(`
			SELECT id, name, script, parallel, dependencies, version 
			FROM column_def 
			WHERE dependencies IS NULL OR dependencies = '[]'
			ORDER BY version DESC
		`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var column Column
			var parallel sql.NullInt64
			var depsJSON sql.NullString
			if err := rows.Scan(&column.ID, &column.Name, &column.Script, &parallel, &depsJSON, &column.Version); err != nil {
				panic(err)
			}
			if parallel.Valid {
				val := int(parallel.Int64)
				column.Parallel = &val
			}
			if depsJSON.Valid && depsJSON.String != "" {
				if err := json.Unmarshal([]byte(depsJSON.String), &column.Dependencies); err != nil {
					dbLogger.Verbosef("Warning: failed to unmarshal dependencies for column %d: %v\n", column.ID, err)
				}
			}
			columnChan <- column
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return columnChan
}

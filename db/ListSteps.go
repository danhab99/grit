package db

import (
	"database/sql"
	"encoding/json"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) ListSteps() chan Step {
	stepChan := make(chan Step)

	go func() {
		defer close(stepChan)

		rows, err := d.db.Query("SELECT id, name, script, parallel, inputs, version FROM step ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var step Step
			var parallel sql.NullInt64
			var inputsJSON sql.NullString
			if err := rows.Scan(&step.ID, &step.Name, &step.Script, &parallel, &inputsJSON, &step.Version); err != nil {
				panic(err)
			}
			if parallel.Valid {
				val := int(parallel.Int64)
				step.Parallel = &val
			}
			if inputsJSON.Valid && inputsJSON.String != "" {
				if err := json.Unmarshal([]byte(inputsJSON.String), &step.Inputs); err != nil {
					dbLogger.Verbosef("Warning: failed to unmarshal inputs for step %d: %v\n", step.ID, err)
				}
			}
			stepChan <- step
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}()

	return stepChan
}

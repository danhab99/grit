package db

import (
	"database/sql"
	"encoding/json"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetTaintedSteps() chan Step {
	stepChan := make(chan Step)

	go func() {
		defer close(stepChan)

		// Find all steps where there's a newer version with a different script or inputs
		rows, err := d.db.Query(`
			SELECT s1.id, s1.name, s1.script, s1.parallel, s1.inputs, s1.version
			FROM step s1
			INNER JOIN step s2 ON s1.name = s2.name
			WHERE s1.version < s2.version
			  AND (s1.script != s2.script OR COALESCE(s1.inputs, '') != COALESCE(s2.inputs, ''))
			GROUP BY s1.id
			ORDER BY s1.name, s1.version
		`)
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

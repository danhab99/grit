package db

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetStepByName(name string) (*Step, error) {
	var step Step
	var parallel sql.NullInt64
	var inputsJSON sql.NullString
	err := d.db.QueryRow("SELECT id, name, script, parallel, inputs, version FROM step WHERE name = ? ORDER BY version DESC LIMIT 1", name).Scan(
		&step.ID, &step.Name, &step.Script, &parallel, &inputsJSON, &step.Version,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if parallel.Valid {
		val := int(parallel.Int64)
		step.Parallel = &val
	}
	if inputsJSON.Valid && inputsJSON.String != "" {
		if err := json.Unmarshal([]byte(inputsJSON.String), &step.Inputs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal inputs: %w", err)
		}
	}
	return &step, nil
}

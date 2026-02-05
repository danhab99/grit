package db

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CreateStep(step Step) (int64, error) {
	// Serialize inputs to JSON for storage and comparison
	// Treat nil and empty slices the same way
	var inputsStr string
	if len(step.Inputs) > 0 {
		inputsJSON, err := json.Marshal(step.Inputs)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal inputs: %w", err)
		}
		inputsStr = string(inputsJSON)
	} else {
		inputsStr = "[]"
	}

	// Check if a step with the same name, script, and inputs already exists
	var existingID int64
	var existingInputs sql.NullString
	err := d.db.QueryRow("SELECT id, inputs FROM step WHERE name = ? AND script = ? ORDER BY version DESC LIMIT 1", step.Name, step.Script).Scan(&existingID, &existingInputs)
	if err == nil {
		// Step with same name and script exists, check if inputs match
		existingInputsStr := "[]"
		if existingInputs.Valid && existingInputs.String != "" {
			existingInputsStr = existingInputs.String
		}

		if existingInputsStr == inputsStr {
			// Inputs match, update parallel flag
			_, err := d.db.Exec("UPDATE step SET parallel = ? WHERE id = ?", step.Parallel, existingID)
			if err != nil {
				return 0, err
			}

			return existingID, nil
		}
		// Inputs changed, need to create a new version
	}
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	// Get the max version for this step name
	var maxVersion sql.NullInt64
	err = d.db.QueryRow("SELECT MAX(version) FROM step WHERE name = ?", step.Name).Scan(&maxVersion)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	version := 1
	if maxVersion.Valid {
		version = int(maxVersion.Int64) + 1
	}

	res, err := d.db.Exec(`
INSERT INTO step (name, script, parallel, inputs, version)
VALUES (?, ?, ?, ?, ?)
`, step.Name, step.Script, step.Parallel, inputsStr, version)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

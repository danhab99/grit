package db

import (
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

func (d Database) ScheduleTasksForStep(stepID int64) (int64, error) {
	step, err := d.GetStep(stepID)
	if err != nil {
		return 0, err
	}

	if len(step.Inputs) == 0 {
		dbLogger.Verbosef("Step %d (%s) has no inputs, skipping scheduling\n", stepID, step.Name)
		return 0, nil
	}

	// Build IN clause for input resource names
	inputsJSON, err := json.Marshal(step.Inputs)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal inputs: %w", err)
	}

	dbLogger.Verbosef("Scheduling tasks for step %d (%s) with inputs: %s\n", stepID, step.Name, string(inputsJSON))

	// Single SQL statement to create tasks for all unconsumed resources
	// that were produced by the upstream steps listed in step.Inputs.
	// We join resource -> creating task -> creating step and match the
	// creating step's name against the input list.
	result, err := d.db.Exec(`
		INSERT INTO task (step_id, input_resource_id, processed, error)
		SELECT ?, r.id, 0, NULL
		FROM resource r
		JOIN task t_created ON r.created_by_task_id = t_created.id
		JOIN step s_created ON t_created.step_id = s_created.id
		WHERE s_created.name IN (SELECT value FROM json_each(?))
		  AND NOT EXISTS (
		      SELECT 1 FROM task t 
		      WHERE t.step_id = ? 
		        AND t.input_resource_id = r.id
		  )
	`, stepID, string(inputsJSON), stepID)

	if err != nil {
		return 0, fmt.Errorf("failed to schedule tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if rowsAffected > 0 {
		dbLogger.Verbosef("Scheduled %d new tasks for step %d (%s)\n", rowsAffected, stepID, step.Name)
	} else {
		dbLogger.Verbosef("No new tasks scheduled for step %d (%s) - no matching unconsumed resources\n", stepID, step.Name)
	}

	return rowsAffected, nil
}

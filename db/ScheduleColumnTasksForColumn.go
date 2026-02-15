package db

import (
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

// ScheduleColumnTasksForColumn schedules column tasks for all resources that need processing by this column.
// For columns with no dependencies, it schedules tasks for all resources.
// For columns with dependencies, it schedules tasks for resources that have values for all dependent columns.
func (d Database) ScheduleColumnTasksForColumn(columnID int64) (int64, error) {
	column, err := d.GetColumn(columnID)
	if err != nil {
		return 0, err
	}

	if len(column.Dependencies) == 0 {
		// Column has no dependencies - schedule for all resources that don't have a task yet
		dbLogger.Verbosef("Scheduling column tasks for column %d (%s) with no dependencies\n", columnID, column.Name)

		result, err := d.execWithBusyRetry(`
			INSERT INTO column_task (column_id, resource_id, processed, error)
			SELECT ?, r.id, 0, NULL
			FROM resource r
			WHERE NOT EXISTS (
			    SELECT 1 FROM column_task ct 
			    WHERE ct.column_id = ? 
			      AND ct.resource_id = r.id
			)
		`, columnID, columnID)

		if err != nil {
			return 0, fmt.Errorf("failed to schedule column tasks: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}

		if rowsAffected > 0 {
			dbLogger.Verbosef("Scheduled %d new column tasks for column %d (%s)\n", rowsAffected, columnID, column.Name)
		}

		return rowsAffected, nil
	}

	// Column has dependencies - schedule for resources that have all dependency values
	depsJSON, err := json.Marshal(column.Dependencies)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal dependencies: %w", err)
	}

	dbLogger.Verbosef("Scheduling column tasks for column %d (%s) with dependencies: %s\n", columnID, column.Name, string(depsJSON))

	// Schedule for resources where:
	// 1. A task doesn't already exist for this column and resource
	// 2. All dependency columns have values for this resource
	numDeps := len(column.Dependencies)
	result, err := d.execWithBusyRetry(`
		INSERT INTO column_task (column_id, resource_id, processed, error)
		SELECT ?, r.id, 0, NULL
		FROM resource r
		WHERE NOT EXISTS (
		    SELECT 1 FROM column_task ct 
		    WHERE ct.column_id = ? 
		      AND ct.resource_id = r.id
		)
		AND (
		    SELECT COUNT(DISTINCT cv.column_id)
		    FROM column_value cv
		    JOIN column_def cd ON cv.column_id = cd.id
		    WHERE cv.resource_id = r.id
		      AND cd.name IN (SELECT value FROM json_each(?))
		) = ?
	`, columnID, columnID, string(depsJSON), numDeps)

	if err != nil {
		return 0, fmt.Errorf("failed to schedule column tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if rowsAffected > 0 {
		dbLogger.Verbosef("Scheduled %d new column tasks for column %d (%s)\n", rowsAffected, columnID, column.Name)
	} else {
		dbLogger.Verbosef("No new column tasks scheduled for column %d (%s) - dependencies not satisfied\n", columnID, column.Name)
	}

	return rowsAffected, nil
}

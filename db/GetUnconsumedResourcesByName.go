package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetUnconsumedResourcesByName(name string, consumingStepID int64) chan Resource {
	resourceChan := make(chan Resource)

	go func() {
		defer close(resourceChan)

		// Find resources with this name that don't have a task in the consuming step that uses them as input
		rows, err := d.db.Query(`
			SELECT r.id, r.name, r.object_hash, r.created_at 
			FROM resource r
			WHERE r.name = ?
			AND NOT EXISTS (
				SELECT 1 FROM task t
				WHERE t.input_resource_id = r.id 
				AND t.step_id = ?
			)
			ORDER BY r.created_at DESC
		`, name, consumingStepID)
		if err != nil {
			dbLogger.Verbosef("Error querying unconsumed resources for name %s, step %d: %v\n", name, consumingStepID, err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var r Resource
			if err := rows.Scan(&r.ID, &r.Name, &r.ObjectHash, &r.CreatedAt); err != nil {
				dbLogger.Verbosef("Error scanning resource: %v\n", err)
				return
			}
			resourceChan <- r
		}

		if err := rows.Err(); err != nil {
			dbLogger.Verbosef("Error iterating resources: %v\n", err)
		}
	}()

	return resourceChan
}

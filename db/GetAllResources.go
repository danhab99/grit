package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetAllResources() chan Resource {
	resourceChan := make(chan Resource)

	go func() {
		defer close(resourceChan)

		rows, err := d.db.Query("SELECT id, name, object_hash, created_at FROM resource ORDER BY created_at DESC")
		if err != nil {
			dbLogger.Verbosef("Error querying all resources: %v\n", err)
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

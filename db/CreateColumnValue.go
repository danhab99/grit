package db

import (
	_ "github.com/mattn/go-sqlite3"
)

// CreateColumnValue creates or updates a column value for a resource
func (d Database) CreateColumnValue(columnID, resourceID int64, objectHash string) (int64, error) {
	// Upsert - update if exists, insert if not
	_, err := d.db.Exec(`
INSERT INTO column_value (column_id, resource_id, object_hash)
VALUES (?, ?, ?)
ON CONFLICT(column_id, resource_id) DO UPDATE SET object_hash = ?, created_at = CURRENT_TIMESTAMP
`, columnID, resourceID, objectHash, objectHash)
	if err != nil {
		return 0, err
	}

	var id int64
	row := d.db.QueryRow("SELECT id FROM column_value WHERE column_id = ? AND resource_id = ? LIMIT 1", columnID, resourceID)
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

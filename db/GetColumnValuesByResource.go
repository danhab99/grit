package db

import (
	_ "github.com/mattn/go-sqlite3"
)

// GetColumnValuesByResource retrieves all column values for a specific resource
func (d Database) GetColumnValuesByResource(resourceID int64) ([]ColumnValue, error) {
	rows, err := d.db.Query(`
		SELECT cv.id, cv.column_id, cv.resource_id, cv.object_hash, cv.created_at 
		FROM column_value cv
		WHERE cv.resource_id = ?
	`, resourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []ColumnValue
	for rows.Next() {
		var cv ColumnValue
		if err := rows.Scan(&cv.ID, &cv.ColumnID, &cv.ResourceID, &cv.ObjectHash, &cv.CreatedAt); err != nil {
			return nil, err
		}
		values = append(values, cv)
	}

	return values, rows.Err()
}

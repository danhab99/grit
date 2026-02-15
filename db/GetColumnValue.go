package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// GetColumnValue retrieves a column value for a specific column and resource
func (d Database) GetColumnValue(columnID, resourceID int64) (*ColumnValue, error) {
	var cv ColumnValue
	err := d.db.QueryRow(`
		SELECT id, column_id, resource_id, object_hash, created_at 
		FROM column_value 
		WHERE column_id = ? AND resource_id = ?
	`, columnID, resourceID).Scan(&cv.ID, &cv.ColumnID, &cv.ResourceID, &cv.ObjectHash, &cv.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &cv, nil
}

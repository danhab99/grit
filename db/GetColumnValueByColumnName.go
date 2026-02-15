package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// GetColumnValueByColumnName retrieves the column value for a resource by column name
func (d Database) GetColumnValueByColumnName(columnName string, resourceID int64) (*ColumnValue, error) {
	var cv ColumnValue
	err := d.db.QueryRow(`
		SELECT cv.id, cv.column_id, cv.resource_id, cv.object_hash, cv.created_at 
		FROM column_value cv
		JOIN column_def cd ON cv.column_id = cd.id
		WHERE cd.name = ? AND cv.resource_id = ?
		ORDER BY cd.version DESC
		LIMIT 1
	`, columnName, resourceID).Scan(&cv.ID, &cv.ColumnID, &cv.ResourceID, &cv.ObjectHash, &cv.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &cv, nil
}

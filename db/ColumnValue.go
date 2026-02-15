package db

import (
	"database/sql"

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

// GetColumnValuesByColumnName retrieves the column value for a resource by column name
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

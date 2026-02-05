package db

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetResource(id int64) (*Resource, error) {
	var r Resource
	err := d.db.QueryRow("SELECT id, name, object_hash, created_at FROM resource WHERE id = ?", id).Scan(
		&r.ID, &r.Name, &r.ObjectHash, &r.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

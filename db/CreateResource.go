package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) CreateResource(name string, objectHash string) (int64, error) {
	return d.CreateResourceWithTask(name, objectHash, nil)
}

func (d Database) CreateResourceWithTask(name string, objectHash string, createdByTaskID *int64) (int64, error) {
	// Use an upsert-like pattern to make this safe under concurrency:
	// INSERT ... ON CONFLICT DO NOTHING, then SELECT the id. This avoids
	// races where two goroutines attempt to insert the same resource.
	_, err := d.db.Exec(`
INSERT INTO resource (name, object_hash, created_by_task_id)
VALUES (?, ?, ?)
ON CONFLICT(name, object_hash) DO NOTHING
`, name, objectHash, createdByTaskID)
	if err != nil {
		return 0, err
	}

	// Now select the id (should exist either from this insert or a concurrent one)
	var id int64
	err = d.db.QueryRow("SELECT id FROM resource WHERE name = ? AND object_hash = ? LIMIT 1", name, objectHash).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

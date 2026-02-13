package db

func (d Database) CreateResource(name string, objectHash string) (int64, error) {
	return d.CreateResourceWithTask(name, objectHash, nil)
}

func (d Database) CreateResourceWithTask(name string, objectHash string, createdByTaskID *int64) (int64, error) {
	if _, err := d.db.Exec(`
INSERT INTO resource (name, object_hash, created_by_task_id)
VALUES (?, ?, ?)
ON CONFLICT(name, object_hash) DO NOTHING
`, name, objectHash, createdByTaskID); err != nil {
		return 0, err
	}

	var id int64
	row := d.db.QueryRow("SELECT id FROM resource WHERE name = ? AND object_hash = ? LIMIT 1", name, objectHash)
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

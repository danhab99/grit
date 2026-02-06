package db

import (
	_ "github.com/mattn/go-sqlite3"
)

func (d Database) GetAllResourceNames() chan string {
	nameChan := make(chan string)

	go func() {
		defer close(nameChan)

		rows, err := d.db.Query("SELECT DISTINCT name FROM resource ORDER BY name")
		if err != nil {
			dbLogger.Verbosef("Error querying resource names: %v\n", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				dbLogger.Verbosef("Error scanning resource name: %v\n", err)
				return
			}
			nameChan <- name
		}

		if err := rows.Err(); err != nil {
			dbLogger.Verbosef("Error iterating resource names: %v\n", err)
		}
	}()

	return nameChan
}

# Creating New Database Functions in `grit/db`

This package uses a fragmented file structure where each major `Database` method is located in its own file named after the method (e.g., `GetStep.go`, `CreateTask.go`).

## Guidelines for AI and Developers

### 1. File Placement
- Create a new file in `/db/` with the name `FunctionName.go`.
- Ensure it uses `package db`.

### 2. Method Signature
- Most methods should be value receivers: `func (d Database) FunctionName(...)`.
- If the method modifies the `Database` struct نفسه (rare), use a pointer receiver: `func (d *Database) FunctionName(...)`.

### 3. Return Patterns
- **Single Record**: Return `(*T, error)`. Return `(nil, nil)` if not found (idiomatic for this project).
- **Multiple Records**: Return `chan T`. Use a goroutine to fetch rows and send them to the channel. Always `defer close(stepChan)` and check `rows.Err()`.
- **Action/CRUD**: Return `(int64, error)` for IDs or just `error`.

### 4. Database Access
- Use `d.db` for SQLite operations.
- Use `d.badgerDB` for BadgerDB (object storage) operations.
- The schema is defined in [schema.go](/home/dan/Documents/go/src/grit/db/schema.go).

### 5. Common Snippets

#### Channel-based Query Pattern:
```go
func (d Database) ListSomething() chan Something {
    out := make(chan Something)
    go func() {
        defer close(out)
        rows, err := d.db.Query("SELECT ...")
        if err != nil {
            dbLogger.Verbosef("Error: %v", err)
            return
        }
        defer rows.Close()
        for rows.Next() {
            var s Something
            if err := rows.Scan(&s.ID, ...); err != nil {
                return
            }
            out <- s
        }
    }()
    return out
}
```

#### JSON Handling:
Inputs and other slices are stored as JSON strings in SQLite. Use `json.Marshal` and `json.Unmarshal`.

### 6. Updating the Schema
- If you need to change the table structure, update the `schema` constant in [schema.go](/home/dan/Documents/go/src/grit/db/schema.go).
- Update the corresponding Go structs in the same file.

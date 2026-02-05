package db

import (
	"database/sql"
	"fmt"
	badger "github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
)

const schema string = `
CREATE TABLE IF NOT EXISTS step (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  name      TEXT NOT NULL,
  script    TEXT NOT NULL,
  parallel  INTEGER,
  inputs    TEXT,
  version   INTEGER DEFAULT 1,
  UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS task (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  step_id          INTEGER NOT NULL,
  input_resource_id INTEGER,
  processed        INTEGER DEFAULT 0,
  error            TEXT,

  FOREIGN KEY(step_id) REFERENCES step(id),
  FOREIGN KEY(input_resource_id) REFERENCES resource(id),
  UNIQUE(step_id, input_resource_id)
);

CREATE TABLE IF NOT EXISTS resource (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  name             TEXT NOT NULL,
  object_hash      VARCHAR(64) NOT NULL,
  created_at       TEXT DEFAULT (CURRENT_TIMESTAMP),

  UNIQUE(name, object_hash)
);

CREATE INDEX IF NOT EXISTS idx_step_name ON step(name);
CREATE INDEX IF NOT EXISTS idx_task_step ON task(step_id);
CREATE INDEX IF NOT EXISTS idx_task_processed ON task(processed);
CREATE INDEX IF NOT EXISTS idx_resource_name ON resource(name);
CREATE INDEX IF NOT EXISTS idx_task_input_resource ON task(input_resource_id);
`

type Database struct {
	db        *sql.DB
	repo_path string
	badgerDB  *badger.DB
}

type Step struct {
	ID       int64
	Name     string
	Script   string
	Parallel *int
	Inputs   []string
	Version  int
}

type Task struct {
	ID              int64
	StepID          int64
	InputResourceID *int64
	Processed       bool
	Error           *string
}

type Resource struct {
	ID         int64
	Name       string
	ObjectHash string
	CreatedAt  string
}

func (t Task) String() string {
	var e string
	if t.Error == nil {
		e = "NIL"
	} else {
		e = *t.Error
	}

	return fmt.Sprintf("Task(id=%d step_id=%d processed=%v error=%s)", t.ID, t.StepID, t.Processed, e)
}

package db

import (
	"database/sql"
	"fmt"
	"grit/broadcast"

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
  created_by_task_id INTEGER,

  UNIQUE(name, object_hash),
  FOREIGN KEY(created_by_task_id) REFERENCES task(id)
);

CREATE TABLE IF NOT EXISTS column_def (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  name         TEXT NOT NULL,
  resource_name TEXT NOT NULL,
  script       TEXT NOT NULL,
  parallel     INTEGER,
  dependencies TEXT,
  version      INTEGER DEFAULT 1,
  UNIQUE(name, resource_name, version)
);

CREATE TABLE IF NOT EXISTS column_task (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  column_id        INTEGER NOT NULL,
  resource_id      INTEGER NOT NULL,
  processed        INTEGER DEFAULT 0,
  error            TEXT,

  FOREIGN KEY(column_id) REFERENCES column_def(id),
  FOREIGN KEY(resource_id) REFERENCES resource(id),
  UNIQUE(column_id, resource_id)
);

CREATE TABLE IF NOT EXISTS column_value (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  column_id        INTEGER NOT NULL,
  resource_id      INTEGER NOT NULL,
  object_hash      VARCHAR(64) NOT NULL,
  created_at       TEXT DEFAULT (CURRENT_TIMESTAMP),

  FOREIGN KEY(column_id) REFERENCES column_def(id),
  FOREIGN KEY(resource_id) REFERENCES resource(id),
  UNIQUE(column_id, resource_id)
);

CREATE INDEX IF NOT EXISTS idx_step_name ON step(name);
CREATE INDEX IF NOT EXISTS idx_task_step ON task(step_id);
CREATE INDEX IF NOT EXISTS idx_task_processed ON task(processed);
CREATE INDEX IF NOT EXISTS idx_resource_name ON resource(name);
CREATE INDEX IF NOT EXISTS idx_task_input_resource ON task(input_resource_id);
CREATE INDEX IF NOT EXISTS idx_column_def_name ON column_def(name);
CREATE INDEX IF NOT EXISTS idx_column_task_column ON column_task(column_id);
CREATE INDEX IF NOT EXISTS idx_column_task_processed ON column_task(processed);
CREATE INDEX IF NOT EXISTS idx_column_value_column ON column_value(column_id);
CREATE INDEX IF NOT EXISTS idx_column_value_resource ON column_value(resource_id);
`

type Database struct {
	db               *sql.DB
	repo_path        string
	badgerDB         *badger.DB
	resourceListener *broadcast.Broadcaster[any]
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

type Column struct {
	ID           int64
	Name         string
	ResourceName string
	Script       string
	Parallel     *int
	Dependencies []string
	Version      int
}

type ColumnTask struct {
	ID         int64
	ColumnID   int64
	ResourceID int64
	Processed  bool
	Error      *string
}

type ColumnValue struct {
	ID         int64
	ColumnID   int64
	ResourceID int64
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

func (ct ColumnTask) String() string {
	var e string
	if ct.Error == nil {
		e = "NIL"
	} else {
		e = *ct.Error
	}

	return fmt.Sprintf("ColumnTask(id=%d column_id=%d resource_id=%d processed=%v error=%s)", ct.ID, ct.ColumnID, ct.ResourceID, ct.Processed, e)
}

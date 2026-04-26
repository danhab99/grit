package db

import (
	"fmt"
	"grit/broadcast"

	badger "github.com/dgraph-io/badger/v4"
)

type Database struct {
	repo_path        string
	badgerDB         *badger.DB
	resourceListener *broadcast.Broadcaster[any]
}

type Step struct {
	ID       string `msgpack:"id"`
	Name     string `msgpack:"name"`
	Script   string `msgpack:"script"`
	Parallel *int   `msgpack:"parallel,omitempty"`
	Input    string `msgpack:"input,omitempty"`
	Version  int    `msgpack:"version"`
}

type Task struct {
	ID              string  `msgpack:"id"`
	StepID          string  `msgpack:"step_id"`
	InputResourceID *string `msgpack:"input_resource_id,omitempty"`
	Processed       bool    `msgpack:"processed"`
	Error           *string `msgpack:"error,omitempty"`
}

type Resource struct {
	ID              string  `msgpack:"id"`
	Name            string  `msgpack:"name"`
	ObjectHash      string  `msgpack:"object_hash"`
	CreatedAt       string  `msgpack:"created_at"`
	CreatedByTaskID *string `msgpack:"created_by_task_id,omitempty"`
}


func (t Task) String() string {
	var e string
	if t.Error == nil {
		e = "NIL"
	} else {
		e = *t.Error
	}
	return fmt.Sprintf("Task(id=%s step_id=%s processed=%v error=%s)", t.ID, t.StepID, t.Processed, e)
}



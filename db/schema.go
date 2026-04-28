package db

import (
	"grit/types"

	badger "github.com/dgraph-io/badger/v4"
)

type Database struct {
	repo_path string
	badgerDB  *badger.DB
}

// Type aliases so existing db internals compile unchanged until rewrite.
type Step = types.Step
type Task = types.Task
type Resource = types.Resource



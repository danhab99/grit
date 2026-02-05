package db

import (
	"crypto/sha256"
	"encoding/hex"
	"grit/fuse"
	"io"
	"runtime"
	"strings"

	"github.com/danhab99/idk/workers"
	_ "github.com/mattn/go-sqlite3"
)

func (db Database) MakeResourceConsumer() chan fuse.FileData {
	outputChan := make(chan fuse.FileData, 100) // Buffered to prevent deadlock

	// Jobs for background storage and DB insert
	type storeJob struct {
		hash string
		data []byte
		name string
	}
	type dbJob struct {
		name string
		hash string
	}

	storeChan := make(chan storeJob, runtime.NumCPU())
	dbJobChan := make(chan dbJob, 100)

	// Worker pool: read FileData, compute hash, and dispatch store + db jobs using workers.Parallel0
	numWorkers := runtime.NumCPU()
	go func() {
		workers.Parallel0(outputChan, numWorkers, func(fd fuse.FileData) {
			resourceName := strings.Split(fd.Name, "_")[0]
			data, err := io.ReadAll(fd.Reader)
			if err != nil {
				dbLogger.Verbosef("Error reading file %s: %v\n", fd.Name, err)
				return
			}

			// Compute hash
			hasher := sha256.New()
			hasher.Write(data)
			hash := hex.EncodeToString(hasher.Sum(nil))

			// Enqueue store job; if storeChan is full, spawn a goroutine so the worker doesn't block
			storeChan <- storeJob{hash: hash, data: data, name: fd.Name}

			// Enqueue DB job (should be quick)
			dbJobChan <- dbJob{name: resourceName, hash: hash}
		})

		// When output processing finishes, close the downstream channels
		close(storeChan)
		close(dbJobChan)
	}()

	// Store workers using workers.Parallel0
	numStoreWorkers := 2
	go func() {
		workers.Parallel0(storeChan, numStoreWorkers, func(s storeJob) {
			if !db.ObjectExists(s.hash) {
				if err := db.StoreObject(s.hash, s.data); err != nil {
					dbLogger.Verbosef("Error storing object %s: %v\n", s.hash[:16]+"...", err)
				}
			}
		})
	}()

	// DB inserter (parallel workers to improve SQLite concurrency)
	numDBWorkers := runtime.NumCPU()
	go func() {
		workers.Parallel0(dbJobChan, numDBWorkers, func(j dbJob) {
			if _, err := db.CreateResource(j.name, j.hash); err != nil {
				dbLogger.Verbosef("Error creating resource %s: %v\n", j.name, err)
				return
			}
			dbLogger.Verbosef("Created resource %s (hash: %s)\n", j.name, j.hash[:16]+"...")
		})
	}()

	return outputChan
}

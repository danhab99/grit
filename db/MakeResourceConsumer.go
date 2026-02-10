package db

import (
	"crypto/sha256"
	"encoding/hex"
	"grit/fuse"
	"io"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/danhab99/idk/workers"
	_ "github.com/mattn/go-sqlite3"
)

func (db Database) MakeResourceConsumer() chan fuse.FileData {
	inputChan := make(chan fuse.FileData, 100) // Buffered to prevent deadlock

	// Process FileData directly in the worker pool: compute hash, store object and
	// insert resource record immediately. This avoids any channel-based batching
	// or buffering for downstream storage/DB workers.
	// Worker pool: read FileData, compute hash, store object if needed, and create resource
	numWorkers := runtime.NumCPU()
	go func() {
		workers.Parallel0(inputChan, numWorkers, func(fd fuse.FileData) {
			// Parse path: "task_N/filename" or just "filename"
			var taskID *int64
			filename := fd.Name

			pathParts := strings.Split(fd.Name, "/")
			if len(pathParts) > 1 {
				// Has directory structure: extract task ID from "task_N"
				taskDir := pathParts[0]
				filename = pathParts[len(pathParts)-1]

				if strings.HasPrefix(taskDir, "task_") {
					if id, err := strconv.ParseInt(strings.TrimPrefix(taskDir, "task_"), 10, 64); err == nil {
						taskID = &id
					}
				}
			}

			resourceName := strings.Split(filename, "_")[0]
			data, err := io.ReadAll(fd.Reader)
			if err != nil {
				panic(err)
			}

			// Compute hash
			hasher := sha256.New()
			hasher.Write(data)
			hash := hex.EncodeToString(hasher.Sum(nil))

			// Store object if it doesn't already exist
			if !db.ObjectExists(hash) {
				if err := db.StoreObject(hash, data); err != nil {
					panic(err)
				}
			}

			// Insert resource record (with retries)
			if _, err := db.CreateResourceWithTask(resourceName, hash, taskID); err != nil {
				const maxRetries = 3
				var err2 error
				for attempt := 1; attempt <= maxRetries; attempt++ {
					_, err2 = db.CreateResourceWithTask(resourceName, hash, taskID)
					if err2 == nil {
						break
					}
					dbLogger.Println("Error while trying to create resource", err2, attempt)
					if attempt < maxRetries {
						time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
					}
				}
				if err2 != nil {
					panic(err2)
				}
			}

			dbLogger.Verbosef("Created resource %s (hash: %s)\n", resourceName, hash[:16]+"...")
		})
	}()

	return inputChan
}

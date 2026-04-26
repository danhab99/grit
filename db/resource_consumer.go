package db

import (
	"crypto/sha256"
	"encoding/hex"
	"grit/fuse"
	"grit/watchdog"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

type pendingResource struct {
	resourceName string
	hash         string
	data         []byte
	taskID       string
}

func (thisDB *Database) MakeResourceConsumer() chan fuse.FileData {
	// Keep this queue intentionally small: each entry can hold a large payload
	// referenced by a bytes.Reader from FUSE Release(). A large buffile:///tmp/pprof001.svgfer here
	// allows runaway in-flight memory when tasks finish faster than DB writes.
	const maxInFlightFiles = 8
	inputChan := make(chan fuse.FileData, maxInFlightFiles)

	dog := watchdog.NewWatchdog(100 * time.Millisecond)

	batchChan := make(chan []pendingResource)
	const maxBatchItems = 32
	const maxBatchBytes = 8 << 20 // 8 MiB cap for buffered payloads

	go func() {
		defer close(batchChan)
		defer dog.Stop()
		defer func() {
			if r := recover(); r != nil {
				dbLogger.Printf("Panic in resource producer: %v\n", r)
			}
		}()

		var batch []pendingResource
		var batchBytes int
		var totalProcessed int64

		flush := func() {
			if len(batch) > 0 {
				dbLogger.Verbosef("Flushing batch of %d resources (%d bytes)\n", len(batch), batchBytes)
				batchChan <- batch
				totalProcessed += int64(len(batch))
				batch = nil
				batchBytes = 0
			}
		}

		for {
			select {
			case in, ok := <-inputChan:
				if !ok {
					flush()
					dbLogger.Verbosef("Resource producer: input channel closed, total processed=%d\n", totalProcessed)
					return
				}
				dog.Pet()
				dbLogger.Verbosef("Producer: received file %s, current batch size=%d items (%d bytes)\n", in.Name, len(batch), batchBytes)

				parts := strings.Split(in.Name, "/")
				if len(parts) != 2 {
					dbLogger.Printf("Producer: invalid resource name format: %s\n", in.Name)
					continue
				}

				taskID := strings.Split(parts[0], "_")[1]
				resourceName := strings.Split(parts[1], "_")[0]

				data := in.Data

				hs := sha256.Sum256(data)
				hash := hex.EncodeToString(hs[:])

				batch = append(batch, pendingResource{resourceName, hash, data, taskID})
				batchBytes += len(data)

				if len(batch) >= maxBatchItems || batchBytes >= maxBatchBytes {
					flush()
				}

			case <-dog.Bark:
				dbLogger.Verbosef("Producer: watchdog barked, flushing batch size=%d\n", len(batch))
				flush()
			}
		}
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				dbLogger.Printf("Panic in resource writer: %v\n", r)
			}
		}()

		var batchCount int64
		var itemCount int64
		for batch := range batchChan {
			batchCount++
			itemCount += int64(len(batch))
			dbLogger.Verbosef("Writer: processing batch #%d with %d items (total=%d items)\n", batchCount, len(batch), itemCount)

			// First: store all objects via WriteBatch (no conflict possible)
			wb := thisDB.badgerDB.NewWriteBatch()
			for _, item := range batch {
				hashBytes, err := hex.DecodeString(item.hash)
				if err != nil {
					dbLogger.Printf("Writer: failed to decode hash: %v\n", err)
					continue
				}
				err = wb.Set(objectKey(hashBytes), item.data)
				if err != nil {
					dbLogger.Printf("Writer: failed to set object in batch: %v\n", err)
					continue
				}
			}
			err := wb.Flush()
			if err != nil {
				dbLogger.Printf("Writer: failed to flush write batch: %v\n", err)
				wb.Cancel()
				continue
			}
			wb.Cancel()

			// Second: create resource metadata + indexes.
			for _, item := range batch {
				thisDB.insertResourceWithRetry(item)
			}

			// Notify listeners
			thisDB.resourceListener.Broadcast(nil)
			if batchCount%10 == 0 {
				dbLogger.Verbosef("Writer: broadcast milestone - %d batches, %d total items processed\n", batchCount, itemCount)
			}
		}
	}()

	return inputChan
}

const maxConflictRetries = 10

// insertResourceWithRetry inserts a single resource with retry on transaction conflict.
func (thisDB *Database) insertResourceWithRetry(item pendingResource) {
	for attempt := range maxConflictRetries {
		err := thisDB.badgerDB.Update(func(txn *badger.Txn) error {
			hashIdxKey := idxResourceHashKey(item.resourceName, item.hash)
			existing, err := getVal(txn, hashIdxKey)
			if err != nil {
				return err
			}
			if existing != nil {
				return nil // already exists
			}

			id := newULID()
			res := Resource{
				ID:              id,
				Name:            item.resourceName,
				ObjectHash:      item.hash,
				CreatedAt:       nowTimestamp(),
				CreatedByTaskID: &item.taskID,
			}

			if err := putEntity(txn, resourceKey(id), &res); err != nil {
				return err
			}
			if err := txn.Set(idxResourceByNameKey(item.resourceName, id), nil); err != nil {
				return err
			}
			if err := txn.Set(hashIdxKey, []byte(id)); err != nil {
				return err
			}
			return nil
		})

		if err == nil {
			return
		}
		if err == badger.ErrConflict {
			dbLogger.Verbosef("Transaction conflict on resource insert (attempt %d), retrying...\n", attempt+1)
			continue
		}
		dbLogger.Printf("Failed to insert resource after %d attempts: %v\n", attempt+1, err)
		return
	}
}

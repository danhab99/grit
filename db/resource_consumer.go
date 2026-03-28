package db

import (
	"crypto/sha256"
	"encoding/hex"
	"grit/fuse"
	"grit/watchdog"
	"io"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func (thisDB *Database) MakeResourceConsumer() chan fuse.FileData {
	inputChan := make(chan fuse.FileData, 100)

	dog := watchdog.NewWatchdog(100 * time.Millisecond)

	type pendingResource struct {
		resourceName string
		hash         string
		data         []byte
		taskID       string
	}

	batchChan := make(chan []pendingResource)

	go func() {
		defer close(batchChan)

		var batch []pendingResource

		flush := func() {
			if len(batch) > 0 {
				batchChan <- batch
				batch = nil
			}
		}

		for {
			select {
			case in, ok := <-inputChan:
				if !ok {
					flush()
					return
				}
				dog.Pet()
				dbLogger.Verboseln("Adding resource to transaction", in.Name)

				parts := strings.Split(in.Name, "/")
				if len(parts) != 2 {
					panic("invalid resource name format")
				}

				taskID := strings.Split(parts[0], "_")[1]
				resourceName := strings.Split(parts[1], "_")[0]

				data, err := io.ReadAll(in.Reader)
				if err != nil {
					panic(err)
				}

				hs := sha256.Sum256(data)
				hash := hex.EncodeToString(hs[:])

				batch = append(batch, pendingResource{resourceName, hash, data, taskID})

				dbLogger.Verboseln("Added new resources", in.Name, hash, taskID)
				if len(batch) > 100 {
					flush()
				}

			case <-dog.Bark:
				dbLogger.Verboseln("Dog barked, flushing transactions")
				flush()
			}
		}
	}()

	go func() {
		for batch := range batchChan {
			dbLogger.Verboseln("Executing batch write")

			// First: store all objects
			wb := thisDB.badgerDB.NewWriteBatch()
			for _, item := range batch {
				hashBytes, err := hex.DecodeString(item.hash)
				if err != nil {
					panic(err)
				}
				err = wb.Set(objectKey(hashBytes), item.data)
				if err != nil {
					panic(err)
				}
			}
			err := wb.Flush()
			if err != nil {
				panic(err)
			}

			// Second: create resource metadata + indexes in a transaction
			err = thisDB.badgerDB.Update(func(txn *badger.Txn) error {
				for _, item := range batch {
					// Check unique constraint
					hashIdxKey := idxResourceHashKey(item.resourceName, item.hash)
					existing, err := getVal(txn, hashIdxKey)
					if err != nil {
						return err
					}
					if existing != nil {
						continue // resource already exists
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
					if err := txn.Set(idxResourceProducerKey(id), []byte(item.taskID)); err != nil {
						return err
					}

					// Look up producer step name for the index
					task, err := getEntity[Task](txn, taskKey(item.taskID))
					if err == nil && task != nil {
						step, err := getEntity[Step](txn, stepKey(task.StepID))
						if err == nil && step != nil {
							if err := txn.Set(idxResourceProdStepKey(step.Name, id), nil); err != nil {
								return err
							}
						}
					}
				}
				return nil
			})
			if err != nil {
				panic(err)
			}

			// Notify listeners
			thisDB.resourceListener.Broadcast(nil)
		}
	}()

	return inputChan
}

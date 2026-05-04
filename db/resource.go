package db

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	badger "github.com/dgraph-io/badger/v4"
)

func (d Database) CreateResource(name string, objectHash string) (string, error) {
	return d.CreateResourceWithTask(name, objectHash, nil)
}

func (d Database) CreateResourceWithTask(name string, objectHash string, createdByTaskID *string) (string, error) {
	var resultID string
	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		// Check unique constraint: (name, object_hash)
		hashKey := idxResourceHashKey(name, objectHash)
		existing, err := getVal(txn, hashKey)
		if err != nil {
			return err
		}
		if existing != nil {
			resultID = string(existing)
			return nil // already exists
		}

		id := newULID()
		res := Resource{
			ID:              id,
			Name:            name,
			ObjectHash:      objectHash,
			CreatedAt:       nowTimestamp(),
			CreatedByTaskID: createdByTaskID,
		}

		if err := putEntity(txn, resourceKey(id), &res); err != nil {
			return err
		}

		// Indexes
		if err := txn.Set(idxResourceByNameKey(name, id), nil); err != nil {
			return err
		}
		if err := txn.Set(hashKey, []byte(id)); err != nil {
			return err
		}

		resultID = id
		return nil
	})
	return resultID, err
}

func (d Database) CreateResourceFromReader(name string, reader io.Reader) (string, string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", "", fmt.Errorf("failed to read data: %w", err)
	}

	hasher := sha256.New()
	hasher.Write(data)
	hashBytes := hasher.Sum(nil)
	hash := hex.EncodeToString(hashBytes)

	if !d.ObjectExists(hash) {
		if err := d.StoreObject(hash, data); err != nil {
			return "", "", fmt.Errorf("failed to store object: %w", err)
		}
	}

	resourceID, err := d.CreateResource(name, hash)
	if err != nil {
		return "", "", fmt.Errorf("failed to create resource record: %w", err)
	}

	return resourceID, hash, nil
}

func (d Database) GetResource(id string) (*Resource, error) {
	var res *Resource
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		res, err = getEntity[Resource](txn, resourceKey(id))
		return err
	})
	return res, err
}

func (d Database) GetResourcesByName(name string) chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		prefix := idxResourceByNamePrefix(name)
		// Reverse scan: ULIDs are time-sorted, reverse gives newest first.
		// cursor starts at the top of the range; skipFirst skips the already-seen
		// cursor key on each subsequent batch (Seek in reverse lands on the key itself).
		cursor := append(append([]byte{}, prefix...), 0xFF)
		skipFirst := false
		for {
			var resources []Resource
			var lastKey []byte
			exhausted := false
			err := d.badgerDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				opts.Reverse = true
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				it.Seek(cursor)
				if skipFirst && it.ValidForPrefix(prefix) {
					it.Next()
				}
				var scanned int
				for ; it.ValidForPrefix(prefix); it.Next() {
					key := it.Item().KeyCopy(nil)
					lastKey = key
					scanned++
					resID := string(key[len(prefix):])
					r, err := getEntity[Resource](txn, resourceKey(resID))
					if err != nil {
						return err
					}
					if r != nil {
						resources = append(resources, *r)
					}
					if scanned >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying resources by name %s: %v\n", name, err)
				break
			}
			for _, r := range resources {
				ch <- r
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = lastKey
			skipFirst = true
		}
	}()
	return ch
}

func (d Database) GetAllResources() chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		prefix := []byte(prefixResource)
		cursor := append([]byte{}, prefix...)
		for {
			var resources []Resource
			var lastKey []byte
			exhausted := false
			err := d.badgerDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					lastKey = it.Item().KeyCopy(nil)
					var r Resource
					err := it.Item().Value(func(v []byte) error { return decode(v, &r) })
					if err == nil {
						resources = append(resources, r)
					}
					if len(resources) >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying all resources: %v\n", err)
				break
			}
			for _, r := range resources {
				ch <- r
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = append(lastKey, 0x00)
		}
	}()
	return ch
}

func (d Database) GetAllResourceNames() chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		prefix := []byte(prefixResource)
		cursor := append([]byte{}, prefix...)
		seen := make(map[string]bool) // global dedup across batches
		for {
			var names []string
			var lastKey []byte
			exhausted := false
			var scanned int
			err := d.badgerDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					lastKey = it.Item().KeyCopy(nil)
					scanned++
					var r Resource
					err := it.Item().Value(func(v []byte) error { return decode(v, &r) })
					if err == nil && !seen[r.Name] {
						seen[r.Name] = true
						names = append(names, r.Name)
					}
					if scanned >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying resource names: %v\n", err)
				break
			}
			for _, n := range names {
				ch <- n
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = append(lastKey, 0x00)
		}
	}()
	return ch
}

func (d Database) GetUnconsumedResourcesByName(name string, consumingStepID string) chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		prefix := idxResourceByNamePrefix(name)
		cursor := append(append([]byte{}, prefix...), 0xFF)
		skipFirst := false
		for {
			var resources []Resource
			var lastKey []byte
			exhausted := false
			err := d.badgerDB.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefix
				opts.Reverse = true
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				it.Seek(cursor)
				if skipFirst && it.ValidForPrefix(prefix) {
					it.Next()
				}
				var scanned int
				for ; it.ValidForPrefix(prefix); it.Next() {
					key := it.Item().KeyCopy(nil)
					lastKey = key
					scanned++
					resID := string(key[len(prefix):])
					uniqueKey := idxTaskUniqueKey(consumingStepID, resID)
					if keyExists(txn, uniqueKey) {
						if scanned >= scanBatchSize {
							return nil
						}
						continue
					}
					r, err := getEntity[Resource](txn, resourceKey(resID))
					if err != nil {
						return err
					}
					if r != nil {
						resources = append(resources, *r)
					}
					if scanned >= scanBatchSize {
						return nil
					}
				}
				exhausted = true
				return nil
			})
			if err != nil {
				dbLogger.Verbosef("Error querying unconsumed resources for name %s, step %s: %v\n", name, consumingStepID, err)
				break
			}
			for _, r := range resources {
				ch <- r
			}
			if exhausted || lastKey == nil {
				break
			}
			cursor = lastKey
			skipFirst = true
		}
	}()
	return ch
}

func (d Database) CountResources() (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		count, err = prefixCount(txn, []byte(prefixResource))
		return err
	})
	return count, err
}

func (d Database) DeleteResource(id string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		r, err := getEntity[Resource](txn, resourceKey(id))
		if err != nil || r == nil {
			return err
		}
		_ = txn.Delete(resourceKey(id))
		_ = txn.Delete(idxResourceByNameKey(r.Name, id))
		_ = txn.Delete(idxResourceHashKey(r.Name, r.ObjectHash))
		return nil
	})
}

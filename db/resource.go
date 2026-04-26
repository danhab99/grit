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
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			// Reverse scan: ULIDs are time-sorted, reverse gives newest first
			prefix := idxResourceByNamePrefix(name)
			return prefixScanReverse(txn, prefix, func(key, val []byte) (bool, error) {
				resID := string(key[len(prefix):])
				r, err := getEntity[Resource](txn, resourceKey(resID))
				if err != nil {
					return false, err
				}
				if r != nil {
					ch <- *r
				}
				return true, nil
			})
		})
		if err != nil {
			dbLogger.Verbosef("Error querying resources by name %s: %v\n", name, err)
		}
	}()
	return ch
}

func (d Database) GetAllResources() chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			return prefixScan(txn, []byte(prefixResource), func(key, val []byte) (bool, error) {
				var r Resource
				if err := decode(val, &r); err != nil {
					return true, nil
				}
				ch <- r
				return true, nil
			})
		})
		if err != nil {
			dbLogger.Verbosef("Error querying all resources: %v\n", err)
		}
	}()
	return ch
}

func (d Database) GetAllResourceNames() chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		seen := make(map[string]bool)
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			return prefixScan(txn, []byte(prefixResource), func(key, val []byte) (bool, error) {
				var r Resource
				if err := decode(val, &r); err != nil {
					return true, nil
				}
				if !seen[r.Name] {
					seen[r.Name] = true
					ch <- r.Name
				}
				return true, nil
			})
		})
		if err != nil {
			dbLogger.Verbosef("Error querying resource names: %v\n", err)
		}
	}()
	return ch
}

func (d Database) GetUnconsumedResourcesByName(name string, consumingStepID string) chan Resource {
	ch := make(chan Resource)
	go func() {
		defer close(ch)
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			prefix := idxResourceByNamePrefix(name)
			return prefixScanReverse(txn, prefix, func(key, val []byte) (bool, error) {
				resID := string(key[len(prefix):])
				// Check if this resource has already been consumed by this step
				uniqueKey := idxTaskUniqueKey(consumingStepID, resID)
				if keyExists(txn, uniqueKey) {
					return true, nil // already consumed
				}
				r, err := getEntity[Resource](txn, resourceKey(resID))
				if err != nil {
					return false, err
				}
				if r != nil {
					ch <- *r
				}
				return true, nil
			})
		})
		if err != nil {
			dbLogger.Verbosef("Error querying unconsumed resources for name %s, step %s: %v\n", name, consumingStepID, err)
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

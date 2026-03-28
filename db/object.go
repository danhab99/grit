package db

import (
	"crypto/sha256"
	"encoding/hex"

	badger "github.com/dgraph-io/badger/v4"
)

func (d Database) StoreObject(hash string, data []byte) error {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}
	wb := d.badgerDB.NewWriteBatch()
	defer wb.Cancel()
	if err := wb.Set(objectKey(hashBytes), data); err != nil {
		return err
	}
	return wb.Flush()
}

func (d Database) StoreObjectAndGetHash(data []byte) (string, error) {
	h := sha256.Sum256(data)
	hashStr := hex.EncodeToString(h[:])
	if err := d.StoreObject(hashStr, data); err != nil {
		return "", err
	}
	return hashStr, nil
}

func (d Database) StoreObjectBatch(objects map[string][]byte) error {
	wb := d.badgerDB.NewWriteBatch()
	defer wb.Cancel()
	for hash, data := range objects {
		hashBytes, err := hex.DecodeString(hash)
		if err != nil {
			// fall back to raw bytes if not hex
			hashBytes = []byte(hash)
		}
		if err := wb.Set(objectKey(hashBytes), data); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (d Database) GetObject(hash string) ([]byte, error) {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	var data []byte
	err = d.badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectKey(hashBytes))
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	return data, err
}

func (d Database) GetObjectBatch(hashes []string) (map[string][]byte, error) {
	results := make(map[string][]byte)
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		for _, hash := range hashes {
			hashBytes, err := hex.DecodeString(hash)
			if err != nil {
				return err
			}
			item, err := txn.Get(objectKey(hashBytes))
			if err != nil {
				return err
			}
			data, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			results[hash] = data
		}
		return nil
	})
	return results, err
}

func (d Database) ObjectExists(hash string) bool {
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return false
	}
	err = d.badgerDB.View(func(txn *badger.Txn) error {
		_, err := txn.Get(objectKey(hashBytes))
		return err
	})
	return err == nil
}

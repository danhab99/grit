package db

import (
	"crypto/rand"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/oklog/ulid/v2"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	ulidMu      sync.Mutex
	ulidEntropy = ulid.Monotonic(rand.Reader, 0)
)

func newULID() string {
	ulidMu.Lock()
	defer ulidMu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy).String()
}

func encode(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func decode(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// prefixScan iterates all keys with the given prefix and calls fn for each.
// Return true from fn to continue, false to stop.
func prefixScan(txn *badger.Txn, prefix []byte, fn func(key, val []byte) (bool, error)) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		var val []byte
		err := item.Value(func(v []byte) error {
			val = append([]byte{}, v...)
			return nil
		})
		if err != nil {
			return err
		}
		cont, err := fn(item.KeyCopy(nil), val)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

// prefixScanKeys iterates keys only (no value fetch) for the given prefix.
func prefixScanKeys(txn *badger.Txn, prefix []byte, fn func(key []byte) (bool, error)) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		cont, err := fn(it.Item().KeyCopy(nil))
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

// prefixScanReverse iterates keys in reverse order under the given prefix.
// Used when ULID ordering gives newest-first semantics.
func prefixScanReverse(txn *badger.Txn, prefix []byte, fn func(key, val []byte) (bool, error)) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()

	// For reverse iteration, seek to the end of the prefix range
	seekKey := append(append([]byte{}, prefix...), 0xFF)
	for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		var val []byte
		err := item.Value(func(v []byte) error {
			val = append([]byte{}, v...)
			return nil
		})
		if err != nil {
			return err
		}
		cont, err := fn(item.KeyCopy(nil), val)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

// prefixCount counts all keys with the given prefix.
func prefixCount(txn *badger.Txn, prefix []byte) (int64, error) {
	var count int64
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
	}
	return count, nil
}

// keyExists checks if a key exists in the transaction.
func keyExists(txn *badger.Txn, key []byte) bool {
	_, err := txn.Get(key)
	return err == nil
}

// getVal retrieves the raw value for a key. Returns nil, nil if not found.
func getVal(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return item.ValueCopy(nil)
}

// getEntity retrieves and decodes an entity by key.
func getEntity[T any](txn *badger.Txn, key []byte) (*T, error) {
	val, err := getVal(txn, key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var entity T
	if err := decode(val, &entity); err != nil {
		return nil, err
	}
	return &entity, nil
}

// putEntity encodes and stores an entity.
func putEntity(txn *badger.Txn, key []byte, v interface{}) error {
	data, err := encode(v)
	if err != nil {
		return err
	}
	return txn.Set(key, data)
}

// nowTimestamp returns the current time as an RFC3339 string.
func nowTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

package db

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	badger "github.com/dgraph-io/badger/v4"
)

// getCsvFileHash retrieves the stored hash for a CSV file path, or "" if none.
func (d *Database) getCsvFileHash(path string) (string, error) {
	var hash string
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		val, err := getVal(txn, metaCsvHashKey(path))
		if err != nil {
			return err
		}
		if val != nil {
			hash = string(val)
		}
		return nil
	})
	return hash, err
}

// setCsvFileHash stores the hash for a CSV file path.
func (d *Database) setCsvFileHash(path, hash string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		return txn.Set(metaCsvHashKey(path), []byte(hash))
	})
}

// hashFile streams through a file computing its SHA-256 without loading it all into memory.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file for hashing: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("failed to hash file: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// IngestCsvFile reads a CSV file line by line and creates a resource for each row.
// It hashes the file first to skip re-reading if the content hasn't changed.
// Returns the number of resources created and any error.
func (d *Database) IngestCsvFile(path string, outputName string) (int64, error) {
	dbLogger.Printf("Ingesting CSV file: %s → output name: %s\n", path, outputName)

	// First pass: hash the file
	fileHash, err := hashFile(path)
	if err != nil {
		return 0, err
	}

	// Check if file has already been ingested with this hash
	storedHash, err := d.getCsvFileHash(path)
	if err != nil {
		return 0, fmt.Errorf("failed to check stored CSV hash: %w", err)
	}

	if storedHash == fileHash {
		dbLogger.Printf("CSV file %s unchanged (hash %s), skipping\n", path, fileHash[:16])
		return 0, nil
	}

	dbLogger.Printf("CSV file %s changed or new, ingesting rows...\n", path)

	// Second pass: read line by line and create resources
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer f.Close()

	var count int64
	const batchSize = 500

	var objectBatch []struct {
		hash string
		data []byte
	}

	flushBatch := func() error {
		if len(objectBatch) == 0 {
			return nil
		}

		// Store objects via WriteBatch
		wb := d.badgerDB.NewWriteBatch()
		defer wb.Cancel()
		for _, item := range objectBatch {
			hashBytes, err := hex.DecodeString(item.hash)
			if err != nil {
				return fmt.Errorf("failed to decode hash: %w", err)
			}
			if err := wb.Set(objectKey(hashBytes), item.data); err != nil {
				return fmt.Errorf("failed to set object in batch: %w", err)
			}
		}
		if err := wb.Flush(); err != nil {
			return fmt.Errorf("failed to flush object batch: %w", err)
		}

		// Create resource records with producer step index
		for _, item := range objectBatch {
			err := d.badgerDB.Update(func(txn *badger.Txn) error {
				hashKey := idxResourceHashKey(outputName, item.hash)
				existing, err := getVal(txn, hashKey)
				if err != nil {
					return err
				}
				if existing != nil {
					return nil // already exists
				}

				id := newULID()
				res := Resource{
					ID:         id,
					Name:       outputName,
					ObjectHash: item.hash,
					CreatedAt:  nowTimestamp(),
				}

				if err := putEntity(txn, resourceKey(id), &res); err != nil {
					return err
				}
				if err := txn.Set(idxResourceByNameKey(outputName, id), nil); err != nil {
					return err
				}
				if err := txn.Set(hashKey, []byte(id)); err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to create resource: %w", err)
			}
		}

		objectBatch = objectBatch[:0]
		return nil
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		data := make([]byte, len(line))
		copy(data, line)

		h := sha256.Sum256(data)
		hash := hex.EncodeToString(h[:])

		objectBatch = append(objectBatch, struct {
			hash string
			data []byte
		}{hash, data})
		count++

		if len(objectBatch) >= batchSize {
			if err := flushBatch(); err != nil {
				return count, err
			}
			dbLogger.Verbosef("CSV ingest: %d rows processed so far\n", count)
		}
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading CSV file: %w", err)
	}

	// Flush remaining
	if err := flushBatch(); err != nil {
		return count, err
	}

	// Store the file hash so we skip next time
	if err := d.setCsvFileHash(path, fileHash); err != nil {
		return count, fmt.Errorf("failed to store CSV file hash: %w", err)
	}

	// Notify listeners that resources are available
	d.resourceListener.Broadcast(nil)

	dbLogger.Printf("CSV ingest complete: %d rows from %s\n", count, path)
	return count, nil
}

package db

import (
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

func (d Database) CreateColumn(column Column) (string, error) {
	var resultID string

	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		// Find latest version with same name and resource_name
		prefix := idxColumnByNameResPrefix(column.Name, column.ResourceName)
		var latestCol *Column
		var latestVersion int

		err := prefixScan(txn, prefix, func(key, val []byte) (bool, error) {
			// Key: ix:cn:{name}\x00{resource_name}\x00{version}\x00{ulid}
			parts := strings.Split(string(key[len(prefix):]), "\x00")
			if len(parts) < 2 {
				return true, nil
			}
			colULID := parts[len(parts)-1]
			c, err := getEntity[Column](txn, columnKey(colULID))
			if err != nil || c == nil {
				return true, nil
			}
			if c.Version > latestVersion {
				latestVersion = c.Version
				latestCol = c
			}
			return true, nil
		})
		if err != nil {
			return err
		}

		// Check if latest version matches (same script and dependencies)
		if latestCol != nil && latestCol.Script == column.Script && depsMatch(latestCol.Dependencies, column.Dependencies) {
			latestCol.Parallel = column.Parallel
			if err := putEntity(txn, columnKey(latestCol.ID), latestCol); err != nil {
				return err
			}
			resultID = latestCol.ID
			return nil
		}

		// Create new version
		version := latestVersion + 1
		if version == 0 {
			version = 1
		}

		id := newULID()
		column.ID = id
		column.Version = version

		if err := putEntity(txn, columnKey(id), &column); err != nil {
			return err
		}
		if err := txn.Set(idxColumnByNameKey(column.Name, column.ResourceName, version, id), nil); err != nil {
			return err
		}

		resultID = id
		return nil
	})
	return resultID, err
}

func depsMatch(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	am := make(map[string]bool, len(a))
	for _, v := range a {
		am[v] = true
	}
	for _, v := range b {
		if !am[v] {
			return false
		}
	}
	return true
}

func (d Database) CreateColumnTask(task ColumnTask) (string, error) {
	var resultID string
	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		id := newULID()
		task.ID = id

		if err := putEntity(txn, colTaskKey(id), &task); err != nil {
			return err
		}

		if task.Processed {
			if err := txn.Set(idxColTaskProcKey(task.ColumnID, id), nil); err != nil {
				return err
			}
		} else {
			if err := txn.Set(idxColTaskUnprocKey(task.ColumnID, id), nil); err != nil {
				return err
			}
		}

		// Unique constraint
		if err := txn.Set(idxColTaskUniqueKey(task.ColumnID, task.ResourceID), []byte(id)); err != nil {
			return err
		}

		resultID = id
		return nil
	})
	return resultID, err
}

func (d Database) CreateAndGetColumnTask(task ColumnTask) (*ColumnTask, error) {
	id, err := d.CreateColumnTask(task)
	if err != nil {
		return nil, err
	}
	task.ID = id
	return &task, nil
}

func (d Database) CreateColumnValue(columnID, resourceID string, objectHash string) (string, error) {
	var resultID string
	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		// Check for existing (upsert)
		existingKey := idxColValByColResKey(columnID, resourceID)
		existingVal, err := getVal(txn, existingKey)
		if err != nil {
			return err
		}

		if existingVal != nil {
			// Update existing
			cvID := string(existingVal)
			cv, err := getEntity[ColumnValue](txn, colValKey(cvID))
			if err != nil || cv == nil {
				return err
			}
			cv.ObjectHash = objectHash
			cv.CreatedAt = nowTimestamp()
			if err := putEntity(txn, colValKey(cvID), cv); err != nil {
				return err
			}
			resultID = cvID
			return nil
		}

		// Create new
		id := newULID()
		cv := ColumnValue{
			ID:         id,
			ColumnID:   columnID,
			ResourceID: resourceID,
			ObjectHash: objectHash,
			CreatedAt:  nowTimestamp(),
		}

		if err := putEntity(txn, colValKey(id), &cv); err != nil {
			return err
		}
		if err := txn.Set(existingKey, []byte(id)); err != nil {
			return err
		}
		if err := txn.Set(idxColValByResKey(resourceID, id), nil); err != nil {
			return err
		}

		// Column name index - look up the column name
		col, err := getEntity[Column](txn, columnKey(columnID))
		if err == nil && col != nil {
			if err := txn.Set(idxColValByColNameKey(col.Name, resourceID), []byte(id)); err != nil {
				return err
			}
		}

		resultID = id
		return nil
	})
	return resultID, err
}

func (d Database) GetColumn(id string) (*Column, error) {
	var col *Column
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		col, err = getEntity[Column](txn, columnKey(id))
		return err
	})
	return col, err
}

func (d Database) GetColumnByName(name string) (*Column, error) {
	var result *Column
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		prefix := idxColumnByNamePrefix(name)
		var lastKey []byte
		err := prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
			lastKey = key
			return true, nil
		})
		if err != nil {
			return err
		}
		if lastKey == nil {
			return nil
		}
		parts := strings.Split(string(lastKey[len(prefix):]), "\x00")
		if len(parts) < 3 {
			return nil
		}
		colULID := parts[len(parts)-1]
		result, err = getEntity[Column](txn, columnKey(colULID))
		return err
	})
	return result, err
}

func (d Database) GetColumnsWithZeroDependencies() chan Column {
	ch := make(chan Column)
	go func() {
		defer close(ch)
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			return prefixScan(txn, []byte(prefixColumn), func(key, val []byte) (bool, error) {
				var c Column
				if err := decode(val, &c); err != nil {
					return true, nil
				}
				if len(c.Dependencies) == 0 {
					ch <- c
				}
				return true, nil
			})
		})
		if err != nil {
			panic(err)
		}
	}()
	return ch
}

func (d Database) GetColumnValue(columnID, resourceID string) (*ColumnValue, error) {
	var result *ColumnValue
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		cvIDBytes, err := getVal(txn, idxColValByColResKey(columnID, resourceID))
		if err != nil || cvIDBytes == nil {
			return err
		}
		result, err = getEntity[ColumnValue](txn, colValKey(string(cvIDBytes)))
		return err
	})
	return result, err
}

func (d Database) GetColumnValueByColumnName(columnName string, resourceID string) (*ColumnValue, error) {
	var result *ColumnValue
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		cvIDBytes, err := getVal(txn, idxColValByColNameKey(columnName, resourceID))
		if err != nil || cvIDBytes == nil {
			return err
		}
		result, err = getEntity[ColumnValue](txn, colValKey(string(cvIDBytes)))
		return err
	})
	return result, err
}

func (d Database) GetColumnValuesByResource(resourceID string) ([]ColumnValue, error) {
	var values []ColumnValue
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		prefix := idxColValByResPrefix(resourceID)
		return prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
			cvID := string(key[len(prefix):])
			cv, err := getEntity[ColumnValue](txn, colValKey(cvID))
			if err != nil {
				return false, err
			}
			if cv != nil {
				values = append(values, *cv)
			}
			return true, nil
		})
	})
	return values, err
}

func (d Database) GetUnprocessedColumnTasks(columnID string) chan ColumnTask {
	ch := make(chan ColumnTask)
	go func() {
		defer close(ch)
		var taskCount int64
		defer func() {
			dbLogger.Verbosef("GetUnprocessedColumnTasks(column=%s) found %d unprocessed column tasks\n", columnID, taskCount)
		}()

		err := d.badgerDB.View(func(txn *badger.Txn) error {
			prefix := idxColTaskUnprocPrefix(columnID)
			return prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
				ctID := string(key[len(prefix):])
				ct, err := getEntity[ColumnTask](txn, colTaskKey(ctID))
				if err != nil {
					return false, err
				}
				if ct != nil {
					taskCount++
					ch <- *ct
				}
				return true, nil
			})
		})
		if err != nil {
			dbLogger.Verbosef("Error querying unprocessed column tasks for column %s: %v\n", columnID, err)
		}
	}()
	return ch
}

func (d Database) UpdateColumnTaskStatus(id string, processed bool, errorMsg *string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		ct, err := getEntity[ColumnTask](txn, colTaskKey(id))
		if err != nil || ct == nil {
			return err
		}

		wasProcessed := ct.Processed
		ct.Processed = processed
		ct.Error = errorMsg

		if err := putEntity(txn, colTaskKey(id), ct); err != nil {
			return err
		}

		if wasProcessed != processed {
			if processed {
				_ = txn.Delete(idxColTaskUnprocKey(ct.ColumnID, id))
				return txn.Set(idxColTaskProcKey(ct.ColumnID, id), nil)
			} else {
				_ = txn.Delete(idxColTaskProcKey(ct.ColumnID, id))
				return txn.Set(idxColTaskUnprocKey(ct.ColumnID, id), nil)
			}
		}
		return nil
	})
}

func (d Database) ListAllColumns() ([]Column, error) {
	var columns []Column
	seenKeys := make(map[string]bool)

	err := d.badgerDB.View(func(txn *badger.Txn) error {
		return prefixScan(txn, []byte(prefixColumn), func(key, val []byte) (bool, error) {
			var c Column
			if err := decode(val, &c); err != nil {
				return true, nil
			}
			k := c.ResourceName + ":" + c.Name
			if seenKeys[k] {
				return true, nil
			}
			seenKeys[k] = true
			columns = append(columns, c)
			return true, nil
		})
	})
	return columns, err
}

// ScheduleColumnTasksForColumn schedules column tasks for resources matching the column.
func (d Database) ScheduleColumnTasksForColumn(columnID string) (int64, error) {
	column, err := d.GetColumn(columnID)
	if err != nil || column == nil {
		return 0, err
	}

	dbLogger.Verbosef("Scheduling column tasks for column %s (%s) on resource '%s'\n", columnID, column.Name, column.ResourceName)

	var toSchedule []string

	err = d.badgerDB.View(func(txn *badger.Txn) error {
		// Find resources with matching name
		prefix := idxResourceByNamePrefix(column.ResourceName)
		return prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
			resID := string(key[len(prefix):])

			// Check unique constraint
			if keyExists(txn, idxColTaskUniqueKey(columnID, resID)) {
				return true, nil
			}

			// Check dependencies
			if len(column.Dependencies) > 0 {
				satisfiedCount := 0
				for _, depName := range column.Dependencies {
					if keyExists(txn, idxColValByColNameKey(depName, resID)) {
						satisfiedCount++
					}
				}
				if satisfiedCount < len(column.Dependencies) {
					return true, nil
				}
			}

			toSchedule = append(toSchedule, resID)
			return true, nil
		})
	})
	if err != nil {
		return 0, fmt.Errorf("failed to find resources for column scheduling: %w", err)
	}

	if len(toSchedule) == 0 {
		return 0, nil
	}

	err = d.badgerDB.Update(func(txn *badger.Txn) error {
		for _, resID := range toSchedule {
			if keyExists(txn, idxColTaskUniqueKey(columnID, resID)) {
				continue
			}

			id := newULID()
			ct := ColumnTask{
				ID:         id,
				ColumnID:   columnID,
				ResourceID: resID,
			}

			if err := putEntity(txn, colTaskKey(id), &ct); err != nil {
				return err
			}
			if err := txn.Set(idxColTaskUnprocKey(columnID, id), nil); err != nil {
				return err
			}
			if err := txn.Set(idxColTaskUniqueKey(columnID, resID), []byte(id)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to schedule column tasks: %w", err)
	}

	count := int64(len(toSchedule))
	if count > 0 {
		dbLogger.Verbosef("Scheduled %d new column tasks for column %s (%s)\n", count, columnID, column.Name)
	}
	return count, nil
}

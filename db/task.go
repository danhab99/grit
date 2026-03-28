package db

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

func (d Database) CreateTask(task Task) (string, error) {
	var resultID string
	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		id := newULID()
		task.ID = id

		if err := putEntity(txn, taskKey(id), &task); err != nil {
			return err
		}

		// Status index
		if task.Processed {
			if err := txn.Set(idxTaskByStepProcKey(task.StepID, id), nil); err != nil {
				return err
			}
		} else {
			if err := txn.Set(idxTaskByStepUnprocKey(task.StepID, id), nil); err != nil {
				return err
			}
		}

		// All-tasks-for-step index
		if err := txn.Set(idxTaskByStepAllKey(task.StepID, id), nil); err != nil {
			return err
		}

		// Unique constraint index and input relationship
		if task.InputResourceID != nil {
			if err := txn.Set(idxTaskUniqueKey(task.StepID, *task.InputResourceID), []byte(id)); err != nil {
				return err
			}
			if err := txn.Set(idxTaskByInputKey(*task.InputResourceID, id), nil); err != nil {
				return err
			}
		}

		resultID = id
		return nil
	})
	return resultID, err
}

func (d *Database) CreateAndGetTask(t Task) (*Task, error) {
	id, err := d.CreateTask(t)
	if err != nil {
		return nil, err
	}
	return d.GetTask(id)
}

func (d Database) CreateTasksFromResources(stepID string, resourceIDs []string) ([]string, error) {
	if len(resourceIDs) == 0 {
		return nil, nil
	}

	var taskIDs []string
	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		for _, resourceID := range resourceIDs {
			// Check unique constraint
			uniqueKey := idxTaskUniqueKey(stepID, resourceID)
			if keyExists(txn, uniqueKey) {
				continue // already exists
			}

			id := newULID()
			task := Task{
				ID:              id,
				StepID:          stepID,
				InputResourceID: &resourceID,
			}

			if err := putEntity(txn, taskKey(id), &task); err != nil {
				return err
			}
			if err := txn.Set(idxTaskByStepUnprocKey(stepID, id), nil); err != nil {
				return err
			}
			if err := txn.Set(idxTaskByStepAllKey(stepID, id), nil); err != nil {
				return err
			}
			if err := txn.Set(uniqueKey, []byte(id)); err != nil {
				return err
			}
			if err := txn.Set(idxTaskByInputKey(resourceID, id), nil); err != nil {
				return err
			}

			taskIDs = append(taskIDs, id)
		}
		return nil
	})
	return taskIDs, err
}

func (d Database) GetTask(id string) (*Task, error) {
	var task *Task
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		task, err = getEntity[Task](txn, taskKey(id))
		return err
	})
	return task, err
}

func (d Database) GetTasksForStep(stepID string) chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			prefix := idxTaskByStepAllPrefix(stepID)
			return prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
				taskID := string(key[len(prefix):])
				t, err := getEntity[Task](txn, taskKey(taskID))
				if err != nil {
					return false, err
				}
				if t != nil {
					ch <- *t
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

func (d Database) GetUnprocessedTasks(stepID string) chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		var taskCount int64
		defer func() {
			dbLogger.Verbosef("GetUnprocessedTasks(step=%s) found %d unprocessed tasks\n", stepID, taskCount)
		}()

		err := d.badgerDB.View(func(txn *badger.Txn) error {
			prefix := idxTaskByStepUnprocPrefix(stepID)
			return prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
				taskID := string(key[len(prefix):])
				t, err := getEntity[Task](txn, taskKey(taskID))
				if err != nil {
					return false, err
				}
				if t != nil {
					taskCount++
					ch <- *t
				}
				return true, nil
			})
		})
		if err != nil {
			dbLogger.Verbosef("Error querying unprocessed tasks for step %s: %v\n", stepID, err)
		}
	}()
	return ch
}

func (d Database) GetTaskInputResource(taskID string) (*Resource, error) {
	var resource *Resource
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		t, err := getEntity[Task](txn, taskKey(taskID))
		if err != nil || t == nil || t.InputResourceID == nil {
			return err
		}
		resource, err = getEntity[Resource](txn, resourceKey(*t.InputResourceID))
		return err
	})
	return resource, err
}

func (d Database) UpdateTaskStatus(id string, processed bool, errorMsg *string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		t, err := getEntity[Task](txn, taskKey(id))
		if err != nil || t == nil {
			return err
		}

		wasProcessed := t.Processed
		t.Processed = processed
		t.Error = errorMsg

		if err := putEntity(txn, taskKey(id), t); err != nil {
			return err
		}

		// Move between status indexes
		if wasProcessed != processed {
			if processed {
				_ = txn.Delete(idxTaskByStepUnprocKey(t.StepID, id))
				return txn.Set(idxTaskByStepProcKey(t.StepID, id), nil)
			} else {
				_ = txn.Delete(idxTaskByStepProcKey(t.StepID, id))
				return txn.Set(idxTaskByStepUnprocKey(t.StepID, id), nil)
			}
		}
		return nil
	})
}

func (d Database) CountTasksForStep(stepID string) (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		count, err = prefixCount(txn, idxTaskByStepAllPrefix(stepID))
		return err
	})
	return count, err
}

func (d Database) CountUnprocessedTasks() (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		// Sum unprocessed across all steps by scanning entire unprocessed index
		var err error
		count, err = prefixCount(txn, []byte(idxTaskByStepUnproc))
		return err
	})
	return count, err
}

func (d Database) CountUnprocessedTasksForStep(stepID string) (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		count, err = prefixCount(txn, idxTaskByStepUnprocPrefix(stepID))
		return err
	})
	return count, err
}

func (d Database) GetTaskCountsForStep(stepID string) (total int64, processed int64, err error) {
	total, err = d.CountTasksForStep(stepID)
	if err != nil {
		return 0, 0, err
	}
	unprocessed, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return total, 0, err
	}
	return total, total - unprocessed, nil
}

func (d Database) DeleteTask(id string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		t, err := getEntity[Task](txn, taskKey(id))
		if err != nil || t == nil {
			return err
		}

		if err := txn.Delete(taskKey(id)); err != nil {
			return err
		}
		_ = txn.Delete(idxTaskByStepAllKey(t.StepID, id))
		_ = txn.Delete(idxTaskByStepUnprocKey(t.StepID, id))
		_ = txn.Delete(idxTaskByStepProcKey(t.StepID, id))
		if t.InputResourceID != nil {
			_ = txn.Delete(idxTaskUniqueKey(t.StepID, *t.InputResourceID))
			_ = txn.Delete(idxTaskByInputKey(*t.InputResourceID, id))
		}
		return nil
	})
}

func (d Database) TaskExists(id string) (bool, error) {
	var exists bool
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		exists = keyExists(txn, taskKey(id))
		return nil
	})
	return exists, err
}

func (d Database) ListTasks() chan Task {
	ch := make(chan Task)
	go func() {
		defer close(ch)
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			return prefixScan(txn, []byte(prefixTask), func(key, val []byte) (bool, error) {
				var t Task
				if err := decode(val, &t); err != nil {
					return true, nil
				}
				ch <- t
				return true, nil
			})
		})
		if err != nil {
			panic(err)
		}
	}()
	return ch
}

func (d Database) MarkStepTasksUnprocessed(stepID string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		// Find all processed tasks for this step and mark them unprocessed
		prefix := idxTaskByStepProcPrefix(stepID)
		var taskIDs []string
		err := prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
			taskIDs = append(taskIDs, string(key[len(prefix):]))
			return true, nil
		})
		if err != nil {
			return err
		}

		for _, taskID := range taskIDs {
			t, err := getEntity[Task](txn, taskKey(taskID))
			if err != nil || t == nil {
				continue
			}
			t.Processed = false
			t.Error = nil
			if err := putEntity(txn, taskKey(taskID), t); err != nil {
				return err
			}
			_ = txn.Delete(idxTaskByStepProcKey(stepID, taskID))
			if err := txn.Set(idxTaskByStepUnprocKey(stepID, taskID), nil); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d Database) MarkStepUndone(stepID string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		prefix := idxTaskByStepAllPrefix(stepID)
		var taskIDs []string
		err := prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
			taskIDs = append(taskIDs, string(key[len(prefix):]))
			return true, nil
		})
		if err != nil {
			return err
		}

		for _, taskID := range taskIDs {
			t, err := getEntity[Task](txn, taskKey(taskID))
			if err != nil || t == nil {
				continue
			}
			// Delete task and all its indexes
			_ = txn.Delete(taskKey(taskID))
			_ = txn.Delete(idxTaskByStepAllKey(stepID, taskID))
			_ = txn.Delete(idxTaskByStepUnprocKey(stepID, taskID))
			_ = txn.Delete(idxTaskByStepProcKey(stepID, taskID))
			if t.InputResourceID != nil {
				_ = txn.Delete(idxTaskUniqueKey(stepID, *t.InputResourceID))
				_ = txn.Delete(idxTaskByInputKey(*t.InputResourceID, taskID))
			}
		}

		dbLogger.Verbosef("Marked step %s as undone: deleted %d tasks\n", stepID, len(taskIDs))
		return nil
	})
}

func (d Database) IsStepComplete(stepID string) (bool, error) {
	count, err := d.CountUnprocessedTasksForStep(stepID)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

func (d Database) CheckAndMarkStepComplete(stepID string) (bool, error) {
	isComplete, err := d.IsStepComplete(stepID)
	if err != nil {
		return false, err
	}
	if isComplete {
		step, err := d.GetStep(stepID)
		if err != nil {
			return false, err
		}
		if step != nil {
			dbLogger.Verbosef("Step %s (%s) marked as complete\n", stepID, step.Name)
		}
	}
	return isComplete, nil
}

func (d Database) GetPipelineStatus() (complete bool, totalTasks int64, processedTasks int64, err error) {
	err = d.badgerDB.View(func(txn *badger.Txn) error {
		totalTasks, err = prefixCount(txn, []byte(prefixTask))
		if err != nil {
			return err
		}
		var unprocessed int64
		unprocessed, err = prefixCount(txn, []byte(idxTaskByStepUnproc))
		if err != nil {
			return err
		}
		processedTasks = totalTasks - unprocessed
		return nil
	})
	complete = totalTasks > 0 && totalTasks == processedTasks
	return
}

// idxTaskByStepProcPrefix returns the prefix for processed tasks of a step.
func idxTaskByStepProcPrefix(stepID string) []byte {
	return []byte(idxTaskByStepProc + stepID + "\x00")
}

// ScheduleTasksForStep schedules tasks for a step by finding resources produced by input steps
// that haven't already been consumed by this step.
func (d Database) ScheduleTasksForStep(stepID string) (int64, error) {
	step, err := d.GetStep(stepID)
	if err != nil {
		return 0, err
	}
	if step == nil || len(step.Inputs) == 0 {
		dbLogger.Verbosef("Step %s (%s) has no inputs, skipping scheduling\n", stepID, step.Name)
		return 0, nil
	}

	dbLogger.Verbosef("Scheduling tasks for step %s (%s) with inputs: %v\n", stepID, step.Name, step.Inputs)

	// Phase 1: Find resources to schedule (read-only)
	var toSchedule []string
	err = d.badgerDB.View(func(txn *badger.Txn) error {
		for _, inputStepName := range step.Inputs {
			prefix := idxResourceProdStepPrefix(inputStepName)
			err := prefixScanKeys(txn, prefix, func(key []byte) (bool, error) {
				resourceID := string(key[len(prefix):])
				uniqueKey := idxTaskUniqueKey(stepID, resourceID)
				if !keyExists(txn, uniqueKey) {
					toSchedule = append(toSchedule, resourceID)
				}
				return true, nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to find resources for scheduling: %w", err)
	}

	if len(toSchedule) == 0 {
		dbLogger.Verbosef("No new tasks scheduled for step %s (%s)\n", stepID, step.Name)
		return 0, nil
	}

	// Phase 2: Create tasks (write)
	err = d.badgerDB.Update(func(txn *badger.Txn) error {
		for _, resourceID := range toSchedule {
			// Re-check unique constraint inside write txn
			uniqueKey := idxTaskUniqueKey(stepID, resourceID)
			if keyExists(txn, uniqueKey) {
				continue
			}

			id := newULID()
			resID := resourceID
			task := Task{
				ID:              id,
				StepID:          stepID,
				InputResourceID: &resID,
			}

			if err := putEntity(txn, taskKey(id), &task); err != nil {
				return err
			}
			if err := txn.Set(idxTaskByStepUnprocKey(stepID, id), nil); err != nil {
				return err
			}
			if err := txn.Set(idxTaskByStepAllKey(stepID, id), nil); err != nil {
				return err
			}
			if err := txn.Set(uniqueKey, []byte(id)); err != nil {
				return err
			}
			if err := txn.Set(idxTaskByInputKey(resourceID, id), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to schedule tasks: %w", err)
	}

	count := int64(len(toSchedule))
	if count > 0 {
		dbLogger.Verbosef("Scheduled %d new tasks for step %s (%s)\n", count, stepID, step.Name)
	}
	return count, nil
}

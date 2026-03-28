package db

import (
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

func (d Database) CreateStep(step Step) (string, error) {
	var resultID string

	err := d.badgerDB.Update(func(txn *badger.Txn) error {
		// Find latest version of step with this name and script
		prefix := idxStepByNamePrefix(step.Name)
		var latestStep *Step
		var latestVersion int

		err := prefixScan(txn, prefix, func(key, val []byte) (bool, error) {
			// Key format: ix:sn:{name}\x00{version}\x00{ulid}
			// Extract ULID from end of key
			parts := strings.Split(string(key[len(prefix):]), "\x00")
			if len(parts) < 2 {
				return true, nil
			}
			stepULID := parts[len(parts)-1]
			s, err := getEntity[Step](txn, stepKey(stepULID))
			if err != nil || s == nil {
				return true, nil
			}
			if s.Version > latestVersion {
				latestVersion = s.Version
				latestStep = s
			}
			return true, nil
		})
		if err != nil {
			return err
		}

		// Check if latest version matches (same script and inputs)
		if latestStep != nil && latestStep.Script == step.Script && inputsMatch(latestStep.Inputs, step.Inputs) {
			// Just update parallel if needed
			latestStep.Parallel = step.Parallel
			if err := putEntity(txn, stepKey(latestStep.ID), latestStep); err != nil {
				return err
			}
			resultID = latestStep.ID
			return nil
		}

		// Create new version
		version := latestVersion + 1
		if version == 0 {
			version = 1
		}

		id := newULID()
		step.ID = id
		step.Version = version

		if err := putEntity(txn, stepKey(id), &step); err != nil {
			return err
		}

		// Index: step by name + version
		if err := txn.Set(idxStepByNameKey(step.Name, version, id), nil); err != nil {
			return err
		}

		resultID = id
		return nil
	})

	return resultID, err
}

func inputsMatch(a, b []string) bool {
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

func (d Database) GetStep(id string) (*Step, error) {
	var step *Step
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		step, err = getEntity[Step](txn, stepKey(id))
		return err
	})
	return step, err
}

func (d Database) GetStepByName(name string) (*Step, error) {
	var result *Step
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		// Reverse scan to get highest version first (keys are sorted, version is zero-padded)
		prefix := idxStepByNamePrefix(name)
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
		// Extract ULID from last key (highest version)
		parts := strings.Split(string(lastKey[len(prefix):]), "\x00")
		if len(parts) < 2 {
			return nil
		}
		stepULID := parts[len(parts)-1]
		result, err = getEntity[Step](txn, stepKey(stepULID))
		return err
	})
	return result, err
}

func (d Database) GetStepsWithZeroInputs() chan Step {
	ch := make(chan Step)
	go func() {
		defer close(ch)
		// Scan all steps, filter those with no inputs
		seen := make(map[string]bool) // track latest version per name
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			prefix := []byte(prefixStep)
			return prefixScan(txn, prefix, func(key, val []byte) (bool, error) {
				var s Step
				if err := decode(val, &s); err != nil {
					return true, nil
				}
				if len(s.Inputs) == 0 {
					ch <- s
					seen[s.Name] = true
				}
				return true, nil
			})
		})
		if err != nil {
			dbLogger.Verbosef("Error in GetStepsWithZeroInputs: %v\n", err)
		}
	}()
	return ch
}

func (d Database) GetStarterSteps() chan Step {
	return d.GetStepsWithZeroInputs()
}

func (d Database) ListSteps() chan Step {
	ch := make(chan Step)
	go func() {
		defer close(ch)
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			prefix := []byte(prefixStep)
			return prefixScan(txn, prefix, func(key, val []byte) (bool, error) {
				var s Step
				if err := decode(val, &s); err != nil {
					return true, nil
				}
				ch <- s
				return true, nil
			})
		})
		if err != nil {
			dbLogger.Verbosef("Error in ListSteps: %v\n", err)
		}
	}()
	return ch
}

func (d Database) CountSteps() (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		count, err = prefixCount(txn, []byte(prefixStep))
		return err
	})
	return count, err
}

func (d Database) CountStepsWithoutParallel() (int64, error) {
	var count int64
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		prefix := []byte(prefixStep)
		return prefixScan(txn, prefix, func(key, val []byte) (bool, error) {
			var s Step
			if err := decode(val, &s); err != nil {
				return true, nil
			}
			if s.Parallel != nil {
				count++
			}
			return true, nil
		})
	})
	return count, err
}

func (d Database) DeleteStep(id string) error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		step, err := getEntity[Step](txn, stepKey(id))
		if err != nil {
			return err
		}
		if step == nil {
			return nil
		}
		// Delete primary
		if err := txn.Delete(stepKey(id)); err != nil {
			return err
		}
		// Delete name index
		return txn.Delete(idxStepByNameKey(step.Name, step.Version, id))
	})
}

func (d Database) UpdateStepStatus(id string, processed bool) error {
	// No-op: step processed status is no longer tracked
	return nil
}

func (d Database) GetTaintedSteps() chan Step {
	ch := make(chan Step)
	go func() {
		defer close(ch)
		err := d.badgerDB.View(func(txn *badger.Txn) error {
			// Collect all steps grouped by name
			stepsByName := make(map[string][]Step)
			prefix := []byte(prefixStep)
			err := prefixScan(txn, prefix, func(key, val []byte) (bool, error) {
				var s Step
				if err := decode(val, &s); err != nil {
					return true, nil
				}
				stepsByName[s.Name] = append(stepsByName[s.Name], s)
				return true, nil
			})
			if err != nil {
				return err
			}

			// Find tainted steps: older versions where script or inputs differ from newer
			for _, steps := range stepsByName {
				if len(steps) < 2 {
					continue
				}
				// Find max version
				var maxVersion int
				var maxStep Step
				for _, s := range steps {
					if s.Version > maxVersion {
						maxVersion = s.Version
						maxStep = s
					}
				}
				// Emit older versions that differ
				for _, s := range steps {
					if s.Version < maxVersion && (s.Script != maxStep.Script || !inputsMatch(s.Inputs, maxStep.Inputs)) {
						ch <- s
					}
				}
			}
			return nil
		})
		if err != nil {
			fmt.Printf("Error in GetTaintedSteps: %v\n", err)
		}
	}()
	return ch
}

package manifest

import (
	"fmt"
	"grit/db"
	"slices"
)

type Manifest struct {
	Steps    []ManifestStep    `toml:"step"`
	CsvFiles []ManifestCsvFile `toml:"csv"`
}

type ManifestCsvFile struct {
	Path   string `toml:"path"`
	Output string `toml:"output"`
}

type ManifestStep struct {
	Name     string `toml:"name"`
	Script   string `toml:"script"`
	Parallel *int   `toml:"parallel"`
	Input    string `toml:"input"`
}

func (manifest Manifest) RegisterSteps(database *db.Database, enabledSteps []string) []db.Step {
	// Register all steps from manifest
	var steps []db.Step
	for _, manifestStep := range manifest.Steps {
		step := db.Step{
			Name:     manifestStep.Name,
			Script:   manifestStep.Script,
			Parallel: manifestStep.Parallel,
			Input:    manifestStep.Input,
		}

		id, err := database.CreateStep(step)
		if err != nil {
			panic(err)
		}
		step.ID = id

		// Filter to enabled steps if specified
		if len(enabledSteps) > 0 {
			if slices.Contains(enabledSteps, step.Name) {
				steps = append(steps, step)
			}
		} else {
			steps = append(steps, step)
		}
	}

	return steps
}

// IngestCsvFiles reads all configured CSV files and creates resources from their rows.
// This runs before any pipeline steps. Returns total rows ingested.
func (manifest Manifest) IngestCsvFiles(database *db.Database) (int64, error) {
	var total int64
	for _, csvFile := range manifest.CsvFiles {
		count, err := database.IngestCsvFile(csvFile.Path, csvFile.Output)
		if err != nil {
			return total, fmt.Errorf("failed to ingest CSV file %s: %w", csvFile.Path, err)
		}
		total += count
	}
	return total, nil
}

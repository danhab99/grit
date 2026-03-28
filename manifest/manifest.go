package manifest

import (
	"fmt"
	"grit/db"
	"slices"
)

type Manifest struct {
	Steps    []ManifestStep    `toml:"step"`
	Columns  []ManifestColumn  `toml:"column"`
	CsvFiles []ManifestCsvFile `toml:"csv"`
}

type ManifestCsvFile struct {
	Path    string   `toml:"path"`
	Output  string   `toml:"output"`
	Columns []string `toml:"columns"`
}

type ManifestStep struct {
	Name     string   `toml:"name"`
	Script   string   `toml:"script"`
	Parallel *int     `toml:"parallel"`
	Inputs   []string `toml:"inputs"`
}

type ManifestColumn struct {
	Name         string   `toml:"name"`
	Resource     string   `toml:"resource"`
	Script       string   `toml:"script"`
	Parallel     *int     `toml:"parallel"`
	Dependencies []string `toml:"dependencies"`
}

func (manifest Manifest) RegisterSteps(database *db.Database, enabledSteps []string) []db.Step {
	// Register all steps from manifest
	var steps []db.Step
	for _, manifestStep := range manifest.Steps {
		step := db.Step{
			Name:     manifestStep.Name,
			Script:   manifestStep.Script,
			Parallel: manifestStep.Parallel,
			Inputs:   manifestStep.Inputs,
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

func (manifest Manifest) RegisterColumns(database *db.Database, enabledColumns []string) []db.Column {
	// Register all columns from manifest
	var columns []db.Column
	for _, manifestColumn := range manifest.Columns {
		column := db.Column{
			Name:         manifestColumn.Name,
			ResourceName: manifestColumn.Resource,
			Script:       manifestColumn.Script,
			Parallel:     manifestColumn.Parallel,
			Dependencies: manifestColumn.Dependencies,
		}

		id, err := database.CreateColumn(column)
		if err != nil {
			panic(err)
		}
		column.ID = id

		// Filter to enabled columns if specified
		if len(enabledColumns) > 0 {
			if slices.Contains(enabledColumns, column.Name) {
				columns = append(columns, column)
			}
		} else {
			columns = append(columns, column)
		}
	}

	return columns
}

// IngestCsvFiles reads all configured CSV files and creates resources from their rows.
// This runs before any pipeline steps. Returns total rows ingested.
func (manifest Manifest) IngestCsvFiles(database *db.Database) (int64, error) {
	var total int64
	for _, csvFile := range manifest.CsvFiles {
		count, err := database.IngestCsvFile(csvFile.Path, csvFile.Output, csvFile.Columns)
		if err != nil {
			return total, fmt.Errorf("failed to ingest CSV file %s: %w", csvFile.Path, err)
		}
		total += count
	}
	return total, nil
}

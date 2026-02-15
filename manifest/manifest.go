package manifest

import (
	"grit/db"
	"slices"
)

type Manifest struct {
	Steps   []ManifestStep   `toml:"step"`
	Columns []ManifestColumn `toml:"column"`
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

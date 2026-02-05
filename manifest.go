package main

import (
	"grit/db"
	"slices"
)

type Manifest struct {
	Steps []ManifestStep `toml:"step"`
}

type ManifestStep struct {
	Name     string   `toml:"name"`
	Script   string   `toml:"script"`
	Parallel *int     `toml:"parallel"`
	Inputs   []string `toml:"inputs"`
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

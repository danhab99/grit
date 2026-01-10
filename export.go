package main

import "fmt"

func export(manifest Manifest, database Database, name string) {
	steps, err := database.IterateTasks(name, false)
	if err != nil { panic (err) }

	for step := range steps {
		fmt.Println("%s -> %s", step.PrevStep.ObjectHash, step.ObjectHash)
	}
}

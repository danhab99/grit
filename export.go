package main

import (
	"fmt"
	"os"
)

func exportResults(database Database, taskName string, mode string) {
	fmt.Fprintf(os.Stderr, "Exporting %s for task '%s'\n", mode, taskName)
	
	results, err := database.GetResultsByTaskName(taskName, mode)
	if err != nil {
		panic(err)
	}

	fmt.Fprintf(os.Stderr, "Found %d results\n", len(results))
	
	for _, hash := range results {
		objectPath := database.GetObjectPath(hash)
		fmt.Println(objectPath)
	}
}

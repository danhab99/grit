package main

import (
	"flag"
	"os"
	"runtime"

	"github.com/danhab99/idk/chans"
	"github.com/pelletier/go-toml"
)

func main() {
	manifest_path := flag.String("manifest", "", "manifest path")
	db_path := flag.String("db", "./db", "database path")
	parallel := flag.Int("parallel", runtime.NumCPU(), "number of processes to run in parallel")

	flag.Parse()

	manifest_toml, err := os.ReadFile(*manifest_path)
	if err != nil {
		panic(err)
	}

	var manifest Manifest
	err = toml.Unmarshal(manifest_toml, &manifest)
	if err != nil {
		panic(err)
	}

	database, err := NewDatabase(*db_path)
	if err != nil {
		panic(err)
	}

	bus := make([]chan Step, len(manifest.Tasks))

	for i, task := range manifest.Tasks {
		database.RegisterTask(task.Name, task.Script.String)
		bus[i], err = database.IterateTasks(task.Name)
	}


	tasks := chans.Merge(bus...)

	for goid := 0; goid < *parallel; goid++ {
		go func() {
			for task := range tasks {
				stepCount++
				runStep(&task, database)
			}
		}()
	}

}

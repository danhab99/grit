package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/danhab99/idk/chans"
)

func run(manifest Manifest, database Database, parallel int) {
	iterate := func() chan Step {
		bus := make([]chan Step, len(manifest.Tasks))

		fmt.Println("Registering tasks...")
		for i, task := range manifest.Tasks {
			fmt.Printf("	- %s\n", task.Name)
			database.RegisterTask(task.Name, task.Script)

			var err error
			bus[i], err = database.IterateTasks(task.Name, true)
			if err != nil {
				panic(err)
			}
		}

		fmt.Println("Processing steps...")

		return chans.Merge(bus...)
	}

	tasks := make(chan Step)

	for range parallel {
		go func() {
			for task := range tasks {
				runStep(&task, database)
			}
		}()
	}

	totalSteps := 0
	runCount := 1
	for runCount > 0 {
		runCount = 0
		for step := range iterate() {
			runCount++
			totalSteps++
			tasks <- step
		}
	}
	close(tasks)

	fmt.Printf("Completed processing %d steps\n", totalSteps)
}

func runStep(s *Step, db Database) {
	fmt.Printf("Running step %d for task: %s\n", s.ID, s.Task.Name)
	cmd := exec.Command("sh", "-c", s.Task.Script)

	input_file, err := os.CreateTemp("/tmp", "*")
	if err != nil {
		panic(err)
	}
	output_dir := os.TempDir()

	input_file.Write(s.Object)

	cmd.Env = append(os.Environ(),
		fmt.Sprintf("INPUT_FILE=%s", input_file.Name()),
		fmt.Sprintf("OUTPUT_DIR=%s", output_dir),
	)

	fmt.Printf("  Executing script with INPUT_FILE=%s\n", input_file.Name())
	_, err = cmd.CombinedOutput()
	if err != nil {
		panic(err)
	}

	dirs, err := os.ReadDir(output_dir)
	if err != nil {
		panic(err)
	}

	outputCount := 0
	for _, file := range dirs {
		if file.IsDir() {
			continue
		}

		body, err := os.ReadFile(fmt.Sprintf("%s/%s", output_dir, file.Name()))
		if err != nil {
			panic(err)
		}

		err = db.InsertStep(Step{
			TaskID:   s.TaskID,
			Object:   body,
			PrevStep: s,
		})
		if err != nil {
			panic(err)
		}
		outputCount++
	}
	fmt.Printf("  Generated %d output files\n", outputCount)

}

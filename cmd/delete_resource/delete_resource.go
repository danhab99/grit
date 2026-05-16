// Description: Delete resources and unreferenced object blobs
package delete_resource

import (
	"flag"
	"fmt"
	"os"

	"grit/db"
)

var (
	dbPath *string
	id     *string
	name   *string
)

func RegisterFlags(fs *flag.FlagSet) {
	dbPath = fs.String("db", "./db", "database path")
	id = fs.String("id", "", "resource ID to delete")
	name = fs.String("name", "", "delete all resources with this name")
}

func Execute() {
	if (*id == "" && *name == "") || (*id != "" && *name != "") {
		fmt.Fprintln(os.Stderr, "Error: specify exactly one of -id or -name")
		os.Exit(1)
	}

	database, err := db.NewDatabase(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer database.Close()

	if *id != "" {
		result, err := database.DeleteResourceHard(*id)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting resource %s: %v\n", *id, err)
			os.Exit(1)
		}
		if !result.ResourceDeleted {
			fmt.Printf("No resource found for id=%s\n", *id)
			return
		}

		fmt.Printf("Deleted resource id=%s name=%s hash=%s\n", result.ResourceID, result.Name, result.ObjectHash)
		if result.ObjectDeleted {
			fmt.Printf("Deleted object hash=%s (remaining_refs=%d)\n", result.ObjectHash, result.RemainingObjectRefs)
		} else {
			fmt.Printf("Kept object hash=%s (remaining_refs=%d)\n", result.ObjectHash, result.RemainingObjectRefs)
		}
		return
	}

	resourceIDs := make([]string, 0)
	for r := range database.GetResourcesByName(*name) {
		resourceIDs = append(resourceIDs, r.ID)
	}

	if len(resourceIDs) == 0 {
		fmt.Printf("No resources found for name=%s\n", *name)
		return
	}

	resourcesDeleted := 0
	objectsDeleted := 0
	for _, resourceID := range resourceIDs {
		result, err := database.DeleteResourceHard(resourceID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting resource %s: %v\n", resourceID, err)
			os.Exit(1)
		}
		if !result.ResourceDeleted {
			continue
		}
		resourcesDeleted++
		if result.ObjectDeleted {
			objectsDeleted++
		}
	}

	fmt.Printf("Deleted %d resources for name=%s\n", resourcesDeleted, *name)
	fmt.Printf("Deleted %d unreferenced objects\n", objectsDeleted)
}

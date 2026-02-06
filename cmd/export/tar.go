package export

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"grit/db"

	"github.com/danhab99/idk/chans"
)

func exportTarball(database db.Database, outputPath string, compressed bool, resourceNames []string) {

	outFile, err := os.Open(outputPath)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	var outputWriter io.Writer = outFile
	if compressed {
		g := gzip.NewWriter(outFile)
		defer g.Close()
		outputWriter = g
	}

	tarWriter := tar.NewWriter(outputWriter)
	defer tarWriter.Close()

	page := 0
	const PAGE_MAX = 10000
	pageCount := PAGE_MAX

	names := resourceNames
	if len(resourceNames) == 0 {
		names = <-chans.Accumulate(database.GetAllResourceNames())
	}

	exportLogger.Println("Exporting steps", names)

	for _, resourceName := range names {
		for resource := range database.GetResourcesByName(resourceName) {
			data, err := database.GetObject(resource.ObjectHash)
			if err != nil {
				panic(err)
			}

			err = tarWriter.WriteHeader(&tar.Header{
				Name: fmt.Sprintf("%s/%d/%s", resource.Name, page, resource.ObjectHash),
				Size: int64(len(data)),
				Mode: 0644,
			})
			if err != nil {
				panic(err)
			}

			_, err = tarWriter.Write(data)
			if err != nil {
				panic(err)
			}

			pageCount--
			if pageCount <= 0 {
				page++
				pageCount = PAGE_MAX
				exportLogger.Printf("Exported %d resources\n", page*PAGE_MAX)
			}
		}

		pageCount++
	}
}

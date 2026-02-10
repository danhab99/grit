package fuse

import (
	"grit/log"
	"io"
	"os"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// FuseWatcher watches a FUSE mount point and consumes files written to it
type FuseWatcher struct {
	mountPath  string
	server     *fuse.Server
	mu         sync.Mutex
	files      map[string]*fileData
	closed     bool
	outputChan chan<- FileData
	openFiles  sync.WaitGroup // Track open files
}

// FileData contains the filename and content of a file written to the FUSE mount
type FileData struct {
	Name   string
	Reader io.Reader
}

type fileData struct {
	content []byte
	mu      sync.Mutex
}

var fuseLogger = log.NewLogger("FUSE")

// NewFuseWatcher creates a new FUSE watcher that mounts at the specified path
// Backpressure is controlled by the capacity of outputChan
func NewFuseWatcher(mountPath string, outputChan chan<- FileData) (*FuseWatcher, error) {
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return nil, err
	}

	fuseLogger.Println("New FUSE watcher at", mountPath)

	fw := &FuseWatcher{
		mountPath:  mountPath,
		files:      make(map[string]*fileData),
		outputChan: outputChan,
	}

	fs := pathfs.NewPathNodeFs(&fuseFS{
		FileSystem: pathfs.NewDefaultFileSystem(),
		watcher:    fw,
	}, nil)
	server, _, err := nodefs.MountRoot(mountPath, fs.Root(), &nodefs.Options{
		AttrTimeout:  time.Second,
		EntryTimeout: time.Second,
	})
	if err != nil {
		return nil, err
	}

	fw.server = server

	fw.Start()

	return fw, nil
}

func NewTempDirFuseWatcher(outputChan chan<- FileData) (*FuseWatcher, error) {
	d, err := os.MkdirTemp("/tmp", "output-*")
	if err != nil {
		return nil, err
	}

	return NewFuseWatcher(d, outputChan)
}

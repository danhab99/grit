package fuse

import (
	"os"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// Start begins serving the FUSE filesystem
func (fw *FuseWatcher) Start() {
	fuseLogger.Verbosef("Starting server at %s\n", fw.mountPath)
	go fw.server.Serve()
}

// GetMountPath returns the mount path for this FUSE watcher
func (fw *FuseWatcher) GetMountPath() string {
	return fw.mountPath
}

// WaitForWrites blocks until all open files have been closed
func (fw *FuseWatcher) WaitForWrites() {
	if fw == nil {
		panic("how is this a nil")
	}
	fw.openFiles.Wait()
}

// Stop unmounts the filesystem, waits for open files to be released, and cleans up the mount directory
func (fw *FuseWatcher) Stop() error {
	fw.mu.Lock()
	if fw.closed {
		fw.mu.Unlock()
		return nil
	}
	fw.closed = true
	fw.mu.Unlock()

	fuseLogger.Verbosef("Stopping server at %s\n", fw.mountPath)

	// Wait for any open files to be closed (with short timeout)
	done := make(chan struct{})
	go func() {
		fw.openFiles.Wait()
		close(done)
	}()

	select {
	case <-done:
		fuseLogger.Verbosef("All files closed gracefully\n")
	case <-time.After(2 * time.Second):
		fuseLogger.Verbosef("Timeout waiting for open files, continuing shutdown\n")
	}

	// Unmount the filesystem
	err := fw.server.Unmount()
	if err != nil {
		fuseLogger.Verbosef("Error unmounting: %v\n", err)
	}

	// Clean up the mount directory
	if err := os.RemoveAll(fw.mountPath); err != nil {
		fuseLogger.Verbosef("Error removing mount directory %s: %v\n", fw.mountPath, err)
		return err
	}

	fuseLogger.Verbosef("Cleaned up mount directory %s\n", fw.mountPath)

	return nil
}

// fuseFS implements the FUSE filesystem interface
type fuseFS struct {
	pathfs.FileSystem
	watcher *FuseWatcher
}

func (fs *fuseFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		// Root directory - write-only
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0200,
		}, fuse.OK
	}

	// File paths (task_x/filename): return ENOENT so the kernel calls Create.
	if strings.Contains(name, "/") {
		return nil, fuse.ENOENT
	}

	// Single-component paths: treat task_* as directories so scripts can create files beneath them.
	// All single-component names are directories (task_* subdirs).
	return &fuse.Attr{
		Mode: fuse.S_IFDIR | 0200,
	}, fuse.OK
}

func (fs *fuseFS) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	// Deny directory listing - write-only directory
	fuseLogger.Verbosef("opendir refused %s\n", name)
	return nil, fuse.EACCES
}

func (fs *fuseFS) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	// Allow directory creation for task subdirectories
	fuseLogger.Verbosef("mkdir %s\n", name)
	return fuse.OK
}

func (fs *fuseFS) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	fs.watcher.mu.Lock()
	defer fs.watcher.mu.Unlock()

	if fs.watcher.closed {
		return nil, fuse.EROFS
	}

	// Check if opening for read - deny read access (write-only filesystem)
	accessMode := flags & 0x3 // O_RDONLY=0, O_WRONLY=1, O_RDWR=2
	if accessMode == 0 {      // O_RDONLY
		fuseLogger.Verbosef("open %s denied - read access not permitted\n", name)
		return nil, fuse.EACCES
	}

	// For write-only filesystem: allow opening any file for write
	// Each open creates fresh content (like O_TRUNC behavior)
	fd := &fileData{content: make([]byte, 0)}
	fs.watcher.openFiles.Add(1)

	fuseLogger.Verbosef("open %s flags=0x%x (write)\n", name, flags)

	return &fuseFile{
		File:    nodefs.NewDefaultFile(),
		name:    name,
		data:    fd,
		watcher: fs.watcher,
	}, fuse.OK
}

func (fs *fuseFS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	fs.watcher.mu.Lock()
	defer fs.watcher.mu.Unlock()

	if fs.watcher.closed {
		return nil, fuse.EROFS
	}

	fd := &fileData{content: make([]byte, 0)}
	fs.watcher.openFiles.Add(1)

	fuseLogger.Verbosef("create %s flags=%d mode=%d\n", name, flags, mode)

	return &fuseFile{
		File:    nodefs.NewDefaultFile(),
		name:    name,
		data:    fd,
		watcher: fs.watcher,
	}, fuse.OK
}

func (fs *fuseFS) Unlink(name string, context *fuse.Context) fuse.Status {
	fuseLogger.Verbosef("unlink %s\n", name)
	return fuse.OK
}

// fuseFile represents an open file in the FUSE filesystem
type fuseFile struct {
	nodefs.File
	name    string
	data    *fileData
	watcher *FuseWatcher
}

func (f *fuseFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()

	newSize := int(off) + len(data)
	if newSize > cap(f.data.content) {
		newCap := cap(f.data.content) * 2
		if newCap < newSize {
			newCap = newSize
		}
		if newCap < 4096 {
			newCap = 4096
		}
		newContent := make([]byte, newSize, newCap)
		copy(newContent, f.data.content)
		f.data.content = newContent
	} else if newSize > len(f.data.content) {
		f.data.content = f.data.content[:newSize]
	}

	// Only log first write to avoid spam for large files
	if off == 0 {
		fuseLogger.Verbosef("write %s started\n", f.name)
	}
	copy(f.data.content[off:], data)
	return uint32(len(data)), fuse.OK
}

func (f *fuseFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *fuseFile) Fsync(flags int) fuse.Status {
	return fuse.OK
}

func (f *fuseFile) Truncate(size uint64) fuse.Status {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()

	sz := int(size)
	if sz <= cap(f.data.content) {
		f.data.content = f.data.content[:sz]
	} else {
		newContent := make([]byte, sz)
		copy(newContent, f.data.content)
		f.data.content = newContent
	}
	return fuse.OK
}

func (f *fuseFile) GetAttr(out *fuse.Attr) fuse.Status {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()

	out.Mode = fuse.S_IFREG | 0200 // Write-only file
	out.Size = uint64(len(f.data.content))
	return fuse.OK
}

func (f *fuseFile) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
	// Pre-allocate space if needed
	f.data.mu.Lock()
	defer f.data.mu.Unlock()

	requiredSize := int(off + size)
	if requiredSize > cap(f.data.content) {
		newCap := cap(f.data.content) * 2
		if newCap < requiredSize {
			newCap = requiredSize
		}
		newContent := make([]byte, len(f.data.content), newCap)
		copy(newContent, f.data.content)
		f.data.content = newContent
	}
	return fuse.OK
}

func (f *fuseFile) Release() {
	// Steal ownership of the buffer — no copy needed.
	f.data.mu.Lock()
	content := f.data.content[:len(f.data.content):len(f.data.content)]
	f.data.content = nil
	f.data.mu.Unlock()

	f.watcher.mu.Lock()
	closed := f.watcher.closed
	f.watcher.mu.Unlock()

	if !closed {
		if len(content) > 0 && f.watcher.outputChan != nil {
			// Send file data to output channel - blocks until consumed
			f.watcher.outputChan <- FileData{Name: f.name, Data: content}
		}

		// Proactively invalidate kernel dentries so the kernel sends FORGET
		// promptly, preventing pathInode accumulation in go-fuse's inode tree.
		parts := strings.SplitN(f.name, "/", 2)
		if len(parts) == 2 {
			dirName, fileName := parts[0], parts[1]
			f.watcher.pathNodeFs.EntryNotify(dirName, fileName)
			f.watcher.pathNodeFs.EntryNotify("", dirName)
		}
	}

	fuseLogger.Verbosef("release %s\n", f.name)

	// Signal that this file is closed
	f.watcher.openFiles.Done()
}

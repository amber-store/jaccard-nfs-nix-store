//go:build linux

package materialize

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// syncFile is called on every file that was written, before it is closed.
// On Linux it does nothing: syncTree syncs them all at once.
func syncFile(*os.File) error { return nil }

// syncTree makes the partial tree at path, a directory or a single file,
// durable when it is whole: one syncfs of the file system it is on, which
// costs less than a sync of each of its files.
func syncTree(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := unix.Syncfs(int(f.Fd())); err != nil {
		f.Close()
		return fmt.Errorf("syncing the file system of %s: %w", path, err)
	}
	return f.Close()
}

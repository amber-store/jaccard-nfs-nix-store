//go:build !linux

package materialize

import (
	"fmt"
	"os"
)

// syncFile is called on every file that was written, before it is closed.
// Where there is no syncfs, which is where only tests run, this is what
// makes the file durable.
func syncFile(f *os.File) error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing %s: %w", f.Name(), err)
	}
	return nil
}

// syncTree is called on the partial tree when it is whole. Every file in
// it has been synced by syncFile already.
func syncTree(string) error { return nil }

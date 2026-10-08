package refs

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
)

// originFile is the file of a cache directory that says where the cache
// was filled from.
const originFile = "origin"

// origin is what the origin file of a cache holds: the server and the
// prefix, each quoted on a line of its own, so that neither can be taken
// for part of the other.
func origin(server, prefix string) []byte {
	return fmt.Appendf(nil, "server %s\nprefix %s\n", strconv.Quote(server), strconv.Quote(prefix))
}

// claim makes the cache in dir one of this server and prefix, or finds
// that it is: a new cache has its origin written, and one that was filled
// from another server or under another prefix is refused.
//
// A pin is a name and the root it had, and is answered without asking
// anybody. Served under another prefix, or from another server, the name
// would stand for another reference and still get the old one's tree.
func claim(dir, server, prefix string) error {
	path := filepath.Join(dir, originFile)
	want := origin(server, prefix)
	got, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return writeWhole(path, want)
	case err != nil:
		return err
	case !bytes.Equal(got, want):
		return fmt.Errorf("the cache in %s was filled from %s, and is asked for %s: a cache is of one server and one prefix",
			dir, oneLine(got), oneLine(want))
	}
	return nil
}

// oneLine puts the lines of an origin on one, for an error.
func oneLine(origin []byte) string {
	return string(bytes.ReplaceAll(bytes.TrimSpace(origin), []byte("\n"), []byte(", ")))
}

// writeWhole writes a file that is there with all of data or not at all:
// under another name first, synced, and then renamed.
func writeWhole(path string, data []byte) error {
	tmp := path + ".new"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}

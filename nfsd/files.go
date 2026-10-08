//go:build linux

package nfsd

import (
	"container/list"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/amber-store/jaccard-nfs-nix-store/handles"
	"golang.org/x/sys/unix"
)

// openFiles keeps the materialized files that were read last open, so that
// a read is one system call and not three. When there are more than max,
// the one read longest ago is closed. It is safe for concurrent use.
type openFiles struct {
	mu    sync.Mutex
	max   int
	order *list.List // of *openFile, the one read last first
	byID  map[handles.ID]*list.Element
}

// openFile is one file that is kept open.
type openFile struct {
	id handles.ID
	f  *os.File
	// readers are the reads going on. A file that left the cache while
	// it was being read is closed by the last of them.
	readers int
	dropped bool
}

func newOpenFiles(max int) *openFiles {
	return &openFiles{max: max, order: list.New(), byID: map[handles.ID]*list.Element{}}
}

// readAt fills p from the file at path, the plain file of the node id,
// starting at off. Anything less than all of p is an error, and costs the
// file its place among the open ones: the next read opens it anew.
func (o *openFiles) readAt(id handles.ID, path string, p []byte, off int64) error {
	of, err := o.acquire(id, path)
	if err != nil {
		return err
	}
	n, err := of.f.ReadAt(p, off)
	if n == len(p) {
		o.release(of, false)
		return nil
	}
	o.release(of, true)
	if err == nil || errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// acquire returns the open file of the node id, opening the file at path
// if it is not among the open ones, and counts a reader on it.
func (o *openFiles) acquire(id handles.ID, path string) (*openFile, error) {
	o.mu.Lock()
	if el, ok := o.byID[id]; ok {
		o.order.MoveToFront(el)
		of := el.Value.(*openFile)
		of.readers++
		o.mu.Unlock()
		return of, nil
	}
	o.mu.Unlock()

	// The last name of the path is never followed: what is materialized
	// are regular files, and a link in their place is not one of them.
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}

	o.mu.Lock()
	if el, ok := o.byID[id]; ok {
		// Somebody else opened it meanwhile.
		o.order.MoveToFront(el)
		of := el.Value.(*openFile)
		of.readers++
		o.mu.Unlock()
		f.Close()
		return of, nil
	}
	of := &openFile{id: id, f: f, readers: 1}
	o.byID[id] = o.order.PushFront(of)
	var closing []*os.File
	for o.order.Len() > o.max {
		closing = o.dropLocked(o.order.Back(), closing)
	}
	o.mu.Unlock()
	for _, f := range closing {
		f.Close()
	}
	return of, nil
}

// release ends a read. With drop the file leaves the open ones.
func (o *openFiles) release(of *openFile, drop bool) {
	o.mu.Lock()
	var closing []*os.File
	if drop && !of.dropped {
		if el, ok := o.byID[of.id]; ok && el.Value.(*openFile) == of {
			// Counted as a reader still, so it is not closed here.
			closing = o.dropLocked(el, closing)
		}
	}
	of.readers--
	if of.dropped && of.readers == 0 {
		closing = append(closing, of.f)
	}
	o.mu.Unlock()
	for _, f := range closing {
		f.Close()
	}
}

// dropLocked takes a file out of the open ones and adds it to closing if
// nobody is reading it. The caller holds the lock.
func (o *openFiles) dropLocked(el *list.Element, closing []*os.File) []*os.File {
	of := o.order.Remove(el).(*openFile)
	delete(o.byID, of.id)
	of.dropped = true
	if of.readers == 0 {
		closing = append(closing, of.f)
	}
	return closing
}

// closeAll closes every file that is kept open. One that is being read is
// closed when the read ends.
func (o *openFiles) closeAll() {
	o.mu.Lock()
	var closing []*os.File
	for o.order.Len() > 0 {
		closing = o.dropLocked(o.order.Front(), closing)
	}
	o.mu.Unlock()
	for _, f := range closing {
		f.Close()
	}
}

// len returns the number of files that are kept open.
func (o *openFiles) len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.order.Len()
}

// Package handles gives every path of the served tree the 16 bytes by which
// an NFS client names it, and keeps the table that leads from those bytes
// back to the path.
//
// The handle of the root is sixteen zero bytes. The handle of any other
// node is the first sixteen bytes of the BLAKE3 hash of its parent's
// handle followed by its name. A path therefore has one handle, the same in
// every run of the sidecar, and two paths never share one, also not two
// directories of the same content: the kernel does not take one directory
// in two places.
//
// A hash cannot be turned back into a path, so the table remembers, for
// every handle it gave out, the parent's handle and the name. It is kept in
// a file (package reclog) and read again at start: a client that held a
// handle before the sidecar was restarted finds it still means the same
// node.
package handles

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/amber-store/jaccard-nfs-nix-store/reclog"
	"github.com/zeebo/blake3"
)

// ID is the handle of a node.
type ID [16]byte

// Root is the handle of the root directory.
var Root ID

// maxDepth bounds the walk from a node to the root. No path is that deep; a
// table that says otherwise is not to be followed for ever.
const maxDepth = 4096

// flushEvery is how often the table hands what it was given to its file.
// It is a variable for the tests.
var flushEvery = time.Second

// Child returns the handle of the entry name in the directory parent.
func Child(parent ID, name string) ID {
	buf := make([]byte, 0, len(parent)+len(name))
	buf = append(buf, parent[:]...)
	buf = append(buf, name...)
	sum := blake3.Sum256(buf)
	return ID(sum[:len(ID{})])
}

// Inode returns the inode number of the node: the first eight bytes of its
// handle.
func (id ID) Inode() uint64 {
	return binary.LittleEndian.Uint64(id[:8])
}

// node is what the table knows of a handle.
type node struct {
	parent ID
	name   string
}

// Table maps the handles that were given out to their paths. It is safe for
// concurrent use.
type Table struct {
	mu    sync.RWMutex
	nodes map[ID]node
	log   *reclog.Log // nil once closed
	// err is the first failure to record a node. The table goes on
	// without the file; Flush and Close report it.
	err error

	stop chan struct{}
	done chan struct{}
}

// Open reads the table from the file at path, which is created if it is
// not there, and returns it ready to be added to. What is added reaches the
// file every second, and at Flush and Close.
func Open(path string) (*Table, error) {
	log, recs, err := reclog.Open(path)
	if err != nil {
		return nil, fmt.Errorf("handles: %w", err)
	}
	t := &Table{
		nodes: make(map[ID]node, len(recs)),
		log:   log,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	for i, rec := range recs {
		if len(rec) < len(ID{}) {
			log.Close()
			return nil, fmt.Errorf("handles: %s: record %d is %d bytes, shorter than a handle", path, i, len(rec))
		}
		parent := ID(rec[:len(ID{})])
		name := string(rec[len(ID{}):])
		t.nodes[Child(parent, name)] = node{parent: parent, name: name}
	}
	go t.flusher(flushEvery)
	return t, nil
}

// flusher hands the table to its file at every interval until it is closed.
func (t *Table) flusher(every time.Duration) {
	defer close(t.done)
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-tick.C:
			t.Flush()
		}
	}
}

// Add returns the handle of the entry name in the directory parent and
// remembers it. A node the table has is not recorded again.
func (t *Table) Add(parent ID, name string) ID {
	id := Child(parent, name)
	t.mu.RLock()
	_, known := t.nodes[id]
	t.mu.RUnlock()
	if known {
		return id
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if _, known := t.nodes[id]; known {
		return id
	}
	t.nodes[id] = node{parent: parent, name: name}
	if t.log != nil {
		rec := make([]byte, 0, len(parent)+len(name))
		rec = append(rec, parent[:]...)
		rec = append(rec, name...)
		if err := t.log.Append(rec); err != nil && t.err == nil {
			t.err = fmt.Errorf("handles: recording a node: %w", err)
		}
	}
	return id
}

// Path returns the names that lead from the root to the node id, and
// whether the table knows the node and every directory above it. The path
// of the root is empty.
func (t *Table) Path(id ID) ([]string, bool) {
	if id == Root {
		return nil, true
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	var up []string
	for id != Root {
		n, ok := t.nodes[id]
		if !ok || len(up) == maxDepth {
			return nil, false
		}
		up = append(up, n.name)
		id = n.parent
	}
	// up was gathered from the node to the root.
	for i, j := 0, len(up)-1; i < j; i, j = i+1, j-1 {
		up[i], up[j] = up[j], up[i]
	}
	return up, true
}

// Len returns the number of nodes the table knows.
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.nodes)
}

// Flush hands what was added to the file. It returns the first failure to
// record a node, if there was one.
func (t *Table) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.log == nil {
		return t.err
	}
	return errors.Join(t.err, t.log.Flush())
}

// Close hands what was added to the file and closes it. The table still
// answers afterwards, and still takes nodes, which it then only holds in
// memory. Closing twice is no error.
func (t *Table) Close() error {
	t.mu.Lock()
	log := t.log
	t.log = nil
	t.mu.Unlock()
	if log == nil {
		return nil
	}
	close(t.stop)
	<-t.done
	return errors.Join(t.err, log.Close())
}

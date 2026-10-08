// Package materialize writes the trees of references out as plain files.
//
// A reference is a name and the root of a tree of Amber-Store Core objects
// in a packstore. Its files can be read from the objects, a blob at a time;
// a plain file answers a read at an offset with less work. A Store writes
// the references it is asked for in the background, a few at a time, under
// the directory it owns:
//
//	partial/<name>   a tree that is being written
//	done/<name>      a tree that is whole and on disk
//
// A tree is built under partial, the file system is synced, and the tree is
// renamed into done: the rename is the record. What is under partial when a
// Store is opened is what an earlier run left half written, and is removed.
//
// Only the content is written: directories with mode 0755, regular files
// with mode 0644. Owners, modes, times and extended attributes are not
// reproduced, and symbolic links and entries of every other type are left
// out. Whoever serves the reference takes those from the objects.
package materialize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// defaultJobs is the number of references written at once when Options
// does not say.
const defaultJobs = 2

// The modes of everything that is written.
const (
	dirMode  = 0o755
	fileMode = 0o644
)

// Options are the settings of a Store.
type Options struct {
	// Jobs is the number of references written at once. Zero means 2.
	Jobs int
	// Log is told of every reference that is done or has failed. Nil means
	// slog.Default().
	Log *slog.Logger
}

// state is how far a reference has come in this run. A name without a
// state has not been asked for.
type state uint8

const (
	queued  state = iota + 1 // waiting for a worker
	writing                  // a worker is writing it under partial
	done                     // its tree is under done: materialized
	failed                   // it could not be written, and is not tried again
)

// job is one reference to write.
type job struct {
	name string
	root key.Key
}

// Store writes references out under one directory and knows which of them
// are whole. It is safe for use from many goroutines.
type Store struct {
	dir string
	get func(key.Key) ([]byte, error)
	log *slog.Logger

	// stopped is canceled by Close: the workers give up what they are
	// writing where they next look.
	stopped context.Context
	stop    context.CancelFunc
	workers sync.WaitGroup

	mu sync.Mutex
	// changed is signaled when a job is queued, when one ends and when the
	// Store is closed: what the workers and Wait wait for.
	changed *sync.Cond
	states  map[string]state
	queue   []job // in the order of Queue
	active  int   // jobs a worker has taken and not ended
	closed  bool
}

// Open returns a Store that writes under dir, which it creates if need be,
// reading objects with get. What an earlier run left half written is
// removed, and the references it wrote whole are known as done. Close
// stops the workers Open starts.
func Open(dir string, get func(key.Key) ([]byte, error), opts Options) (*Store, error) {
	if dir == "" {
		return nil, errors.New("materialize: no directory")
	}
	if get == nil {
		return nil, errors.New("materialize: no getter")
	}
	jobs := opts.Jobs
	if jobs < 1 {
		jobs = defaultJobs
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	s := &Store{dir: dir, get: get, log: log, states: map[string]state{}}
	s.changed = sync.NewCond(&s.mu)
	for _, sub := range []string{s.doneDir(), s.partialDir()} {
		if err := os.MkdirAll(sub, dirMode); err != nil {
			return nil, fmt.Errorf("materialize: %w", err)
		}
	}
	// A tree under partial is not whole, or not known to be on disk.
	left, err := os.ReadDir(s.partialDir())
	if err != nil {
		return nil, fmt.Errorf("materialize: %w", err)
	}
	for _, e := range left {
		if err := os.RemoveAll(filepath.Join(s.partialDir(), e.Name())); err != nil {
			return nil, fmt.Errorf("materialize: removing what was left half written: %w", err)
		}
	}
	whole, err := os.ReadDir(s.doneDir())
	if err != nil {
		return nil, fmt.Errorf("materialize: %w", err)
	}
	for _, e := range whole {
		s.states[e.Name()] = done
	}

	s.stopped, s.stop = context.WithCancel(context.Background())
	for range jobs {
		s.workers.Go(s.work)
	}
	return s, nil
}

func (s *Store) doneDir() string    { return filepath.Join(s.dir, "done") }
func (s *Store) partialDir() string { return filepath.Join(s.dir, "partial") }

// Queue asks for the reference name, whose tree is at root, to be written
// out, and returns at once. A name is taken once: nothing happens when the
// reference is materialized, queued, being written or has failed in this
// run, or when the Store is closed.
func (s *Store) Queue(name string, root key.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.states[name] != 0 {
		return
	}
	s.states[name] = queued
	s.queue = append(s.queue, job{name: name, root: root})
	s.changed.Broadcast()
}

// Done reports whether the reference name is materialized: its tree is
// whole, on disk, and under done.
func (s *Store) Done(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[name] == done
}

// Path returns where the plain file is of the file at rel, the names of the
// path below the root of the reference name (none when the reference is
// itself one file), and whether the reference is materialized. Only then
// is anything at the path. Neither is looked up on the disk.
func (s *Store) Path(name string, rel []string) (string, bool) {
	elems := make([]string, 0, 2+len(rel))
	elems = append(elems, s.doneDir(), name)
	elems = append(elems, rel...)
	return filepath.Join(elems...), s.Done(name)
}

// Wait blocks until nothing is queued and nothing is being written.
func (s *Store) Wait() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) > 0 || s.active > 0 {
		s.changed.Wait()
	}
}

// Close stops the writing where it is and waits for the workers. A
// reference in the middle is abandoned and its partial tree removed; what
// was queued is forgotten. A getter that is waiting has to return before
// Close does. Close may be called more than once.
func (s *Store) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		for _, j := range s.queue {
			delete(s.states, j.name)
		}
		s.queue = nil
		s.stop()
		s.changed.Broadcast()
	}
	s.mu.Unlock()
	s.workers.Wait()
	return nil
}

// work writes the queued references one after another until the Store is
// closed.
func (s *Store) work() {
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.closed {
			s.changed.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		j := s.queue[0]
		s.queue = s.queue[1:]
		if len(s.queue) == 0 {
			s.queue = nil // lets go of the array the jobs were in
		}
		s.states[j.name] = writing
		s.active++
		s.mu.Unlock()

		err := s.materialize(j)

		s.mu.Lock()
		s.active--
		switch {
		case err == nil:
			s.states[j.name] = done
		case s.stopped.Err() != nil:
			delete(s.states, j.name) // abandoned, not failed
		default:
			s.states[j.name] = failed
		}
		s.changed.Broadcast()
		s.mu.Unlock()
	}
}

// materialize writes the reference of j and logs how it went.
func (s *Store) materialize(j job) error {
	start := time.Now()
	w := &writer{stopped: s.stopped, get: s.get}
	err := s.write(w, j)
	switch {
	case err == nil:
		s.log.Info("reference materialized", "name", j.name, "files", w.files, "bytes", w.bytes, "took", time.Since(start))
	case s.stopped.Err() != nil:
		s.log.Debug("materializing abandoned", "name", j.name, "err", err)
	default:
		s.log.Error("materializing failed", "name", j.name, "root", j.root.String(), "err", err)
	}
	return err
}

// write builds the tree of j under partial and moves it to done. When it
// fails, nothing of the reference is left in either place.
func (s *Store) write(w *writer, j job) (err error) {
	// Before any path is made of it: a name that is not one component
	// would lead out of the directory, for the removal below as well.
	if !validName(j.name) {
		return fmt.Errorf("reference name %q is not a file name", j.name)
	}
	partial := filepath.Join(s.partialDir(), j.name)
	whole := filepath.Join(s.doneDir(), j.name)
	defer func() {
		if err == nil {
			return
		}
		if rerr := os.RemoveAll(partial); rerr != nil {
			err = errors.Join(err, fmt.Errorf("removing the partial tree: %w", rerr))
		}
	}()

	switch t := j.root.Type(); t {
	case key.DirLeaf, key.DirNode, key.Commit:
		if err := makeDir(partial); err != nil {
			return err
		}
		if err := w.dir(partial, j.root); err != nil {
			return err
		}
	case key.Blob, key.FileNode:
		if err := w.file(partial, j.root); err != nil {
			return err
		}
	default:
		return fmt.Errorf("root %s is a %v: neither a tree nor the content of a file", j.root, t)
	}
	// The getter may have answered after Close was called.
	if err := w.stopped.Err(); err != nil {
		return err
	}
	if err := syncTree(partial); err != nil {
		return err
	}
	if err := os.Rename(partial, whole); err != nil {
		return err
	}
	if err := syncDir(s.doneDir()); err != nil {
		// The rename may not be on disk, so the tree does not count, and
		// is not left where the next run would take it for whole.
		return errors.Join(err, os.RemoveAll(whole))
	}
	return nil
}

// writer writes the tree of one reference and counts what it writes.
type writer struct {
	stopped context.Context
	get     func(key.Key) ([]byte, error)
	files   int   // regular files written
	bytes   int64 // and their content
}

// fetch is the getter the tree is read with: the Store's, until Close.
// Nothing core reads with knows of a context, so this is where a stop
// reaches a walk and a file in the middle.
func (w *writer) fetch(k key.Key) ([]byte, error) {
	if err := w.stopped.Err(); err != nil {
		return nil, err
	}
	data, err := w.get(k)
	if err != nil {
		return nil, err
	}
	// The getter may have waited a long time.
	if err := w.stopped.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// dir writes the entries of the directory at k into path, which is there,
// and those of every directory below it.
func (w *writer) dir(path string, k key.Key) error {
	entries, err := fstree.CollectEntries(k, w.fetch)
	if err != nil {
		return fmt.Errorf("reading the directory for %s: %w", path, err)
	}
	for _, e := range entries {
		if err := w.stopped.Err(); err != nil {
			return err
		}
		// Every name, also that of an entry that is not written: a tree
		// with such a name in it is not one to trust further.
		if !validName(string(e.Name)) {
			return fmt.Errorf("entry name %q in %s is not a file name", e.Name, path)
		}
		child := filepath.Join(path, string(e.Name))
		switch e.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			sub, err := key.Parse(e.ContentKey)
			if err != nil {
				return fmt.Errorf("directory %s: %w", child, err)
			}
			if err := makeDir(child); err != nil {
				return err
			}
			if err := w.dir(child, sub); err != nil {
				return err
			}
		case unix.S_IFREG:
			content, err := key.Parse(e.ContentKey)
			if err != nil {
				return fmt.Errorf("file %s: %w", child, err)
			}
			if err := w.file(child, content); err != nil {
				return err
			}
		}
	}
	return nil
}

// file writes the content at k to a new file at path.
func (w *writer) file(path string, k key.Key) (err error) {
	// A new file and nothing else: names are unique in a directory, so
	// something at path already is a file system that folds two names
	// into one, and writing through it would mix two files.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	// The mode asked for is narrowed by the process's umask.
	if err := f.Chmod(fileMode); err != nil {
		return err
	}
	counted := &counter{to: f}
	if err := fstree.WriteContent(counted, k, w.fetch); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := syncFile(f); err != nil {
		return err
	}
	w.files++
	w.bytes += counted.n
	return nil
}

// makeDir makes the directory path, whose parent is there.
func makeDir(path string) error {
	if err := os.Mkdir(path, dirMode); err != nil {
		return err
	}
	// The mode asked for is narrowed by the process's umask.
	return os.Chmod(path, dirMode)
}

// syncDir makes the entries of the directory path durable: a rename into
// it is on disk when it returns.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("syncing %s: %w", path, err)
	}
	return d.Close()
}

// validName reports whether name can be the name of one file in a
// directory. Any bytes will do but those that would make it a path, or no
// name at all.
func validName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\x00")
}

// counter counts the bytes written through it.
type counter struct {
	to io.Writer
	n  int64
}

func (c *counter) Write(p []byte) (int, error) {
	n, err := c.to.Write(p)
	c.n += int64(n)
	return n, err
}

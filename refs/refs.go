// Package refs fetches references from a jaccard-store server the first
// time their names are looked up, and remembers which it has.
//
// A Store owns the cache directory's packstore and the file refs beside
// it, a reclog of pins: the names that were fetched and the roots they had.
// The file origin says which server and prefix the cache is of; it serves
// no other.
// Ensure answers a pinned name from memory. Any other name it pulls from
// the server into the packstore, and it pins the name once the objects are
// on disk, so that a pin never names objects that are not there. A pin is
// for good: nothing here asks the server about a name again, and nothing
// is ever removed.
//
// A pull belongs to the store and to no request. Lookups of one name share
// one pull, a caller that gives up does not end it, and whoever asks next
// finds it running or done. A pull that fails for any reason but a missing
// reference is tried again over a new connection until Options.PullTimeout
// has passed.
package refs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/jaccard-nfs-nix-store/reclog"
	"github.com/amber-store/jaccard-store/client"
)

// ErrNotFound is returned by Ensure for a name the server has no reference
// under, and for one that could not be the name of a reference.
var ErrNotFound = errors.New("refs: no such reference")

// errClosed is returned by Ensure on a store that was closed, and to those
// who waited for a pull that Close cut short.
var errClosed = errors.New("refs: the store is closed")

const (
	// dialTimeout is how long one call of Options.Dial may take.
	dialTimeout = 30 * time.Second
	// missingSweep is the number of names remembered as missing at which
	// those whose time is up are forgotten, so that looking up many names
	// that do not exist does not grow the memory of them without end.
	missingSweep = 1024
)

// Options configure a Store.
type Options struct {
	// Prefix is put before a name, as it is, to make the name of the
	// reference on the server.
	Prefix string
	// Server names the server the references come from: its endpoint ID.
	// It is not dialed with, which is Dial's part, but written to a new
	// cache beside the prefix: a cache that was filled from another
	// server or under another prefix is refused by Open.
	Server string
	// Dir is the cache directory: the packstore is in packstore/ under it
	// and the pins are in the file refs. It is created if it is missing.
	Dir string
	// Dial connects to the server. It is called when a connection is first
	// needed and again after one has failed. Its context ends after 30
	// seconds or when the store is closed.
	Dial func(ctx context.Context) (Conn, error)
	// PullTimeout is how long a pull that fails is tried again for,
	// counted from its first failure. Zero means 2 minutes.
	PullTimeout time.Duration
	// AttemptTimeout is how long one attempt at a pull may take. One that
	// has neither ended nor failed by then is ended and counts as failed:
	// a transfer that hangs does not keep its name from being fetched for
	// ever. Zero means 1 hour.
	AttemptTimeout time.Duration
	// PullJobs is how many pulls run at once. Zero means 4.
	PullJobs int
	// MissingFor is how long a name the server had no reference under is
	// taken to be missing without asking again. Zero means 5 seconds.
	MissingFor time.Duration
	// RetryWait is the pause before a pull that failed is tried a second
	// time; it doubles with every further attempt, up to RetryMax. Zero
	// means 1 second, and 15 seconds for RetryMax.
	RetryWait time.Duration
	RetryMax  time.Duration
	// OnPin, when it is set, is called once for every new pin, after the
	// pin is on disk and Pinned reports it, and before those who waited
	// for it are answered. It is not called for the pins Open reads.
	OnPin func(name string, root key.Key)
	// Log receives a line for every fetch and every failure. Nil means
	// slog.Default().
	Log *slog.Logger
}

// Store is the local packstore, the pins, and the connection to the server
// that both are filled over. It is safe for concurrent use.
type Store struct {
	opts    Options
	log     *slog.Logger
	objects *packstore.Store
	pinLog  *reclog.Log

	// ctx is what pulls and dials run under. It ends at Close.
	ctx    context.Context
	cancel context.CancelFunc
	// pulls counts the fetches that are running, for Close to wait for.
	pulls sync.WaitGroup
	// jobs has a slot for every pull that may run at once.
	jobs chan struct{}

	// pinMu is held while a pin is written to the file and then added to
	// pins, so that the two have one order. It is taken before mu.
	pinMu sync.Mutex
	// dialMu is held while the connection is dialed, so that pulls that
	// need one at the same moment wait for one dial.
	dialMu sync.Mutex

	mu      sync.Mutex
	closed  bool
	pins    pinSet
	fetches map[string]*fetch    // the names being fetched
	missing map[string]time.Time // when a name was found missing
	conn    Conn                 // nil until dialed and after a failure
	connGen uint64               // counts the connections dialed: which one conn is
}

// fetch is the pull of one name, shared by everyone who asks for the name
// while it runs.
type fetch struct {
	// done is closed when the fetch has ended; root and err are set by
	// then.
	done chan struct{}
	root key.Key
	err  error
}

// Open opens the store in opts.Dir, creating what is missing of it, and
// reads the pins. Nothing is asked of the server before the first Ensure
// of a name that is not pinned.
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, errors.New("refs: no cache directory")
	}
	if opts.Dial == nil {
		return nil, errors.New("refs: no way to dial the server")
	}
	if opts.PullTimeout <= 0 {
		opts.PullTimeout = 2 * time.Minute
	}
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = time.Hour
	}
	if opts.PullJobs <= 0 {
		opts.PullJobs = 4
	}
	if opts.MissingFor <= 0 {
		opts.MissingFor = 5 * time.Second
	}
	if opts.RetryWait <= 0 {
		opts.RetryWait = time.Second
	}
	if opts.RetryMax <= 0 {
		opts.RetryMax = 15 * time.Second
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("refs: %w", err)
	}
	if err := claim(opts.Dir, opts.Server, opts.Prefix); err != nil {
		return nil, fmt.Errorf("refs: %w", err)
	}
	// Every write is not synced by itself: the store is synced once after
	// a pull, before the pin that names its objects is written.
	objects, err := packstore.Open(filepath.Join(opts.Dir, "packstore"), packstore.WithSync(false))
	if err != nil {
		return nil, fmt.Errorf("refs: opening the packstore: %w", err)
	}
	path := filepath.Join(opts.Dir, "refs")
	pinLog, recs, err := reclog.Open(path)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("refs: opening the pins: %w", err), objects.Close())
	}
	var pins pinSet
	for i, rec := range recs {
		pin, err := decodePin(rec)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("refs: %s: record %d: %w", path, i, err), pinLog.Close(), objects.Close())
		}
		pins.add(pin)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Store{
		opts:    opts,
		log:     log,
		objects: objects,
		pinLog:  pinLog,
		ctx:     ctx,
		cancel:  cancel,
		jobs:    make(chan struct{}, opts.PullJobs),
		pins:    pins,
		fetches: make(map[string]*fetch),
		missing: make(map[string]time.Time),
	}, nil
}

// Ensure returns the root of the reference name, fetching it from the
// server unless it is pinned. A name the server has no reference under is
// ErrNotFound, and so is one that could not name a reference; any other
// failure to fetch is an error that is not.
//
// When ctx ends Ensure returns its error. The pull it waited for goes on,
// and pins the name if it succeeds.
func (s *Store) Ensure(ctx context.Context, name string) (key.Key, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return key.Key{}, errClosed
	}
	if root, ok := s.pins.byName[name]; ok {
		s.mu.Unlock()
		return root, nil
	}
	if !s.nameable(name) {
		s.mu.Unlock()
		return key.Key{}, fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	if at, ok := s.missing[name]; ok {
		if time.Since(at) < s.opts.MissingFor {
			s.mu.Unlock()
			return key.Key{}, fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		delete(s.missing, name)
	}
	f := s.fetches[name]
	if f == nil {
		f = &fetch{done: make(chan struct{})}
		s.fetches[name] = f
		// Under mu, as closed is: Close does not start to wait while a
		// fetch is being added.
		s.pulls.Add(1)
		go s.run(name, f)
	}
	s.mu.Unlock()

	select {
	case <-f.done:
		return f.root, f.err
	case <-ctx.Done():
		return key.Key{}, ctx.Err()
	}
}

// nameable reports whether name could be a reference under the prefix and
// an entry of a directory: the server is not asked about one that could
// not.
func (s *Store) nameable(name string) bool {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return false
	}
	return reference.ValidateName(s.opts.Prefix+name) == nil
}

// run fetches name and pins it, and answers everyone who waits on f.
func (s *Store) run(name string, f *fetch) {
	defer s.pulls.Done()
	root, err := s.pull(name)
	pinned := false
	if err == nil {
		// Held until the pin is in memory as well, which is below.
		s.pinMu.Lock()
		if err = s.writePin(Pin{Name: name, Root: root}); err != nil {
			s.pinMu.Unlock()
			err = fmt.Errorf("refs: pinning %q: %w", name, err)
			s.log.Error("pinning failed", "name", name, "error", err)
		} else {
			pinned = true
		}
	}

	// The fetch leaves the table in the step in which what it found
	// arrives: nobody finds neither.
	s.mu.Lock()
	delete(s.fetches, name)
	switch {
	case pinned:
		s.pins.add(Pin{Name: name, Root: root})
	case errors.Is(err, ErrNotFound):
		s.rememberMissing(name)
	}
	s.mu.Unlock()

	if pinned {
		s.pinMu.Unlock()
		if s.opts.OnPin != nil {
			s.opts.OnPin(name, root)
		}
	}
	f.root, f.err = root, err
	close(f.done)
}

// rememberMissing notes that name was found missing now. The caller holds
// mu.
func (s *Store) rememberMissing(name string) {
	now := time.Now()
	if len(s.missing) >= missingSweep {
		for n, at := range s.missing {
			if now.Sub(at) >= s.opts.MissingFor {
				delete(s.missing, n)
			}
		}
	}
	s.missing[name] = now
}

// writePin puts a pin on disk: the objects first, so that the pin never
// names objects that are not there. The caller holds pinMu.
func (s *Store) writePin(pin Pin) error {
	if err := s.objects.Sync(); err != nil {
		return fmt.Errorf("syncing the packstore: %w", err)
	}
	if err := s.pinLog.Append(pin.encode()); err != nil {
		return err
	}
	return s.pinLog.Sync()
}

// pull pulls the reference of name into the packstore and returns its
// root. A pull that fails is tried again, after RetryWait and then twice
// as long each time up to RetryMax, until PullTimeout has passed since the
// first failure. A missing reference is ErrNotFound and is not tried again.
func (s *Store) pull(name string) (key.Key, error) {
	began := time.Now()
	var failedAt time.Time // of the first failure
	wait := s.opts.RetryWait
	for attempt := 1; ; attempt++ {
		res, err := s.attempt(name)
		if err == nil {
			s.log.Info("fetched", "name", name, "root", res.Root,
				"packs", res.Packs, "objects", res.Objects, "bytes", res.Bytes,
				"took", time.Since(began))
			return res.Root, nil
		}
		if errors.Is(err, client.ErrNotFound) {
			return key.Key{}, fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		if s.ctx.Err() != nil {
			return key.Key{}, fmt.Errorf("refs: fetching %q: %w", name, errClosed)
		}
		now := time.Now()
		if failedAt.IsZero() {
			failedAt = now
		}
		// An attempt that would begin after the time is up is not waited
		// for.
		if now.Add(wait).Sub(failedAt) >= s.opts.PullTimeout {
			s.log.Error("fetch given up", "name", name, "attempts", attempt,
				"took", time.Since(began), "error", err)
			return key.Key{}, fmt.Errorf("refs: fetching %q: %w", name, err)
		}
		s.log.Warn("fetch failed, will be tried again", "name", name, "attempt", attempt,
			"in", wait, "error", err)
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-s.ctx.Done():
			timer.Stop()
			return key.Key{}, fmt.Errorf("refs: fetching %q: %w", name, errClosed)
		}
		wait = min(2*wait, s.opts.RetryMax)
	}
}

// attempt pulls the reference of name once, when one of the jobs is free.
// A failure costs the connection it happened on.
func (s *Store) attempt(name string) (client.PullResult, error) {
	select {
	case s.jobs <- struct{}{}:
	case <-s.ctx.Done():
		return client.PullResult{}, errClosed
	}
	defer func() { <-s.jobs }()

	conn, gen, err := s.connect()
	if err != nil {
		return client.PullResult{}, fmt.Errorf("dialing the server: %w", err)
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.opts.AttemptTimeout)
	defer cancel()
	res, err := conn.Pull(ctx, s.objects, s.opts.Prefix+name, client.PullOptions{})
	// A reference that is not there is an answer, and the connection it
	// came over is sound.
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		s.drop(gen)
	}
	return res, err
}

// connect returns the connection to the server, dialing it if there is
// none, and which of the connections dialed it is.
func (s *Store) connect() (Conn, uint64, error) {
	s.dialMu.Lock()
	defer s.dialMu.Unlock()
	s.mu.Lock()
	conn, gen := s.conn, s.connGen
	s.mu.Unlock()
	if conn != nil {
		return conn, gen, nil
	}

	ctx, cancel := context.WithTimeout(s.ctx, dialTimeout)
	defer cancel()
	conn, err := s.opts.Dial(ctx)
	if err != nil {
		return nil, 0, err
	}
	if conn == nil {
		return nil, 0, errors.New("no connection and no error")
	}
	s.mu.Lock()
	s.connGen++
	s.conn, gen = conn, s.connGen
	s.mu.Unlock()
	return conn, gen, nil
}

// drop closes the connection a pull failed on, unless it is gone already:
// of the pulls that fail on one connection the first drops it, and none of
// them closes the one that was dialed since.
func (s *Store) drop(gen uint64) {
	s.mu.Lock()
	var conn Conn
	if s.conn != nil && s.connGen == gen {
		conn, s.conn = s.conn, nil
	}
	s.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

// Pinned returns the root of name if name was fetched.
func (s *Store) Pinned(name string) (key.Key, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	root, ok := s.pins.byName[name]
	return root, ok
}

// Pins returns the pins in the order they were made. The slice is the
// caller's.
func (s *Store) Pins() []Pin {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Pin(nil), s.pins.list...)
}

// Count returns the number of pins.
func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pins.list)
}

// Get returns the object stored under k in the packstore.
func (s *Store) Get(k key.Key) ([]byte, error) {
	return s.objects.Get(k)
}

// Close ends the pulls that are running, waits for them to return, and
// closes the connection, the pin file and the packstore. Whoever waits in
// Ensure is answered with an error, unless the pull was done in time.
// Closing a store twice is no error.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	s.cancel()
	s.pulls.Wait()

	s.mu.Lock()
	conn := s.conn
	s.conn = nil
	s.mu.Unlock()
	var err error
	if conn != nil {
		if cerr := conn.Close(); cerr != nil {
			err = fmt.Errorf("refs: closing the connection: %w", cerr)
		}
	}
	return errors.Join(err, s.pinLog.Close(), s.objects.Close())
}

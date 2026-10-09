package refs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/jaccard-store/client"
)

// quiet drops what the store logs.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// errBoom is what the fake fails with when it is told to.
var errBoom = errors.New("boom")

// source is a tree in a packstore of its own: what the fake server has
// under a name.
type source struct {
	objects *packstore.Store
	root    key.Key
	keys    []key.Key
}

// newSource imports a directory with the given files into a new packstore.
func newSource(t *testing.T, files map[string]string) *source {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	objects, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { objects.Close() })
	root, _, err := ingest.Dir(objects, dir, ingest.Opts{NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := fstree.ReachableKeys(root, objects.Get)
	if err != nil {
		t.Fatal(err)
	}
	return &source{objects: objects, root: root, keys: keys}
}

// fake is a server: the references it has, and what its connections are
// to do and have done.
type fake struct {
	mu   sync.Mutex
	refs map[string]*source
	// pullErrs and dialErrs are failures still to be handed out, one per
	// call; always fails every pull while it is set.
	pullErrs []error
	dialErrs []error
	always   error
	// hold, when it is set, keeps every pull until it is closed or the
	// pull's context ends.
	hold chan struct{}
	// delay is how long a pull takes at least.
	delay time.Duration
	// started is sent the name of every pull that begins, while there is
	// room.
	started chan string

	calls   int
	names   []string
	running int
	most    int
	dials   int
	conns   []*fakeConn
}

func newFake() *fake {
	return &fake{refs: map[string]*source{}, started: make(chan string, 64)}
}

// fakeConn is one connection the fake handed out.
type fakeConn struct {
	f      *fake
	mu     sync.Mutex
	closed bool
}

func (f *fake) dial(ctx context.Context) (Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dials++
	if len(f.dialErrs) > 0 {
		err := f.dialErrs[0]
		f.dialErrs = f.dialErrs[1:]
		return nil, err
	}
	c := &fakeConn{f: f}
	f.conns = append(f.conns, c)
	return c, nil
}

func (c *fakeConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *fakeConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeConn) Pull(ctx context.Context, objects *packstore.Store, name string, _ client.PullOptions) (client.PullResult, error) {
	f := c.f
	f.mu.Lock()
	f.calls++
	f.names = append(f.names, name)
	f.running++
	f.most = max(f.most, f.running)
	err := f.always
	if err == nil && len(f.pullErrs) > 0 {
		err = f.pullErrs[0]
		f.pullErrs = f.pullErrs[1:]
	}
	hold, delay, src := f.hold, f.delay, f.refs[name]
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.running--
		f.mu.Unlock()
	}()
	select {
	case f.started <- name:
	default:
	}
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return client.PullResult{}, ctx.Err()
		}
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	if err != nil {
		return client.PullResult{}, err
	}
	if src == nil {
		return client.PullResult{}, fmt.Errorf("%w: %q", client.ErrNotFound, name)
	}
	res := client.PullResult{Root: src.root, Packs: 1}
	for _, k := range src.keys {
		data, err := src.objects.Get(k)
		if err != nil {
			return client.PullResult{}, err
		}
		if err := objects.PutVerified(k, data); err != nil {
			return client.PullResult{}, err
		}
		res.Objects++
		res.Bytes += uint64(len(data))
	}
	return res, nil
}

// count returns how many pulls and dials the fake has seen.
func (f *fake) count() (calls, dials int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.dials
}

// options are the options of a store over f in dir, with waits short
// enough for a test.
func options(f *fake, dir string) Options {
	return Options{
		Dir:         dir,
		Dial:        f.dial,
		PullTimeout: 10 * time.Second,
		MissingFor:  10 * time.Second,
		RetryWait:   5 * time.Millisecond,
		RetryMax:    20 * time.Millisecond,
		Log:         quiet,
	}
}

// open opens a store over f in a new directory and closes it with the test.
func open(t *testing.T, f *fake, mutate func(*Options)) *Store {
	t.Helper()
	opts := options(f, filepath.Join(t.TempDir(), "cache"))
	if mutate != nil {
		mutate(&opts)
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// waitStarted waits for a pull to begin and returns its name.
func waitStarted(t *testing.T, f *fake) string {
	t.Helper()
	select {
	case name := <-f.started:
		return name
	case <-time.After(10 * time.Second):
		t.Fatal("no pull began")
		return ""
	}
}

func TestEnsurePullsAndPins(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha", "d/b": "beta"})
	f.refs["one"] = src
	s := open(t, f, nil)

	if _, ok := s.Pinned("one"); ok {
		t.Fatal("pinned before it was fetched")
	}
	root, err := s.Ensure(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	if root != src.root {
		t.Fatalf("root %s, want %s", root, src.root)
	}
	if got, ok := s.Pinned("one"); !ok || got != src.root {
		t.Fatalf("Pinned = %s, %v", got, ok)
	}
	if _, err := s.Get(root); err != nil {
		t.Fatalf("the root is not in the store: %v", err)
	}

	// A pinned name asks nobody.
	if _, err := s.Ensure(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	if calls, dials := f.count(); calls != 1 || dials != 1 {
		t.Fatalf("%d pulls and %d dials, want one of each", calls, dials)
	}
}

func TestEnsureSharesOnePull(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["one"] = src
	f.hold = make(chan struct{})
	s := open(t, f, nil)

	type answer struct {
		root key.Key
		err  error
	}
	answers := make(chan answer, 2)
	ask := func() {
		root, err := s.Ensure(context.Background(), "one")
		answers <- answer{root, err}
	}
	go ask()
	waitStarted(t, f)
	go ask()
	// The second has nothing to show for itself until it is answered; give
	// it the time to get to the pull that is running.
	time.Sleep(50 * time.Millisecond)
	close(f.hold)
	for range 2 {
		a := <-answers
		if a.err != nil || a.root != src.root {
			t.Fatalf("answer %s, %v", a.root, a.err)
		}
	}
	if calls, _ := f.count(); calls != 1 {
		t.Fatalf("%d pulls, want 1", calls)
	}
}

func TestEnsureNotFound(t *testing.T) {
	f := newFake()
	s := open(t, f, func(o *Options) { o.MissingFor = 300 * time.Millisecond })

	_, err := s.Ensure(context.Background(), "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	first := time.Now()
	if _, ok := s.Pinned("nope"); ok {
		t.Fatal("a missing name is pinned")
	}
	_, err = s.Ensure(context.Background(), "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if calls, _ := f.count(); calls != 1 && time.Since(first) < 300*time.Millisecond {
		t.Fatalf("%d pulls within MissingFor, want 1", calls)
	}
	before, _ := f.count()

	time.Sleep(350 * time.Millisecond)
	_, err = s.Ensure(context.Background(), "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if calls, _ := f.count(); calls != before+1 {
		t.Fatalf("%d pulls after MissingFor, want %d", calls, before+1)
	}
}

func TestEnsureNamesThatAreNone(t *testing.T) {
	f := newFake()
	s := open(t, f, func(o *Options) { o.Prefix = "pfx-" })

	names := []string{
		"", ".", "..", "a/b", "/", "a\x00b",
		"a@b",      // no reference name has an '@'
		"a\nb",     // nor a control character
		"\xff\xfe", // nor bytes that are no UTF-8
		strings.Repeat("n", reference.MaxNameLen-2), // too long with the prefix only
	}
	for _, name := range names {
		if _, err := s.Ensure(context.Background(), name); !errors.Is(err, ErrNotFound) {
			t.Errorf("Ensure(%q) = %v, want ErrNotFound", name, err)
		}
	}
	if calls, dials := f.count(); calls != 0 || dials != 0 {
		t.Fatalf("%d pulls and %d dials, want none", calls, dials)
	}
}

func TestEnsurePutsThePrefixBefore(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["nix/store/abc"] = src
	s := open(t, f, func(o *Options) { o.Prefix = "nix/store/" })

	root, err := s.Ensure(context.Background(), "abc")
	if err != nil {
		t.Fatal(err)
	}
	if root != src.root {
		t.Fatalf("root %s, want %s", root, src.root)
	}
	f.mu.Lock()
	names := fmt.Sprint(f.names)
	f.mu.Unlock()
	if names != "[nix/store/abc]" {
		t.Fatalf("the fake was asked for %s", names)
	}
	// The pin is under the name that was asked for.
	if _, ok := s.Pinned("abc"); !ok {
		t.Fatal("abc is not pinned")
	}
	if _, ok := s.Pinned("nix/store/abc"); ok {
		t.Fatal("the name with its prefix is pinned")
	}
}

func TestEnsureRetriesAFailedPull(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["one"] = src
	f.pullErrs = []error{errBoom}
	s := open(t, f, nil)

	root, err := s.Ensure(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	if root != src.root {
		t.Fatalf("root %s, want %s", root, src.root)
	}
	if calls, dials := f.count(); calls != 2 || dials != 2 {
		t.Fatalf("%d pulls and %d dials, want two of each", calls, dials)
	}
	if !f.conns[0].isClosed() {
		t.Fatal("the connection that failed was not closed")
	}
	if f.conns[1].isClosed() {
		t.Fatal("the new connection was closed")
	}
}

func TestEnsureRetriesAFailedDial(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["one"] = src
	f.dialErrs = []error{errBoom}
	s := open(t, f, nil)

	root, err := s.Ensure(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	if root != src.root {
		t.Fatalf("root %s, want %s", root, src.root)
	}
	if calls, dials := f.count(); calls != 1 || dials != 2 {
		t.Fatalf("%d pulls and %d dials, want 1 and 2", calls, dials)
	}
}

func TestEnsureGivesUpAfterPullTimeout(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["one"] = src
	f.always = errBoom
	s := open(t, f, func(o *Options) { o.PullTimeout = 60 * time.Millisecond })

	began := time.Now()
	_, err := s.Ensure(context.Background(), "one")
	if err == nil {
		t.Fatal("no error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, which is ErrNotFound", err)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, which does not wrap the failure", err)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Fatalf("gave up after %v", took)
	}
	if calls, _ := f.count(); calls < 2 {
		t.Fatalf("%d pulls, want it tried again", calls)
	}
	if _, ok := s.Pinned("one"); ok {
		t.Fatal("pinned after a failure")
	}

	// Nothing is remembered: the next one starts over.
	f.mu.Lock()
	f.always = nil
	f.mu.Unlock()
	root, err := s.Ensure(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	if root != src.root {
		t.Fatalf("root %s, want %s", root, src.root)
	}
}

func TestEnsurePullJobs(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	const n = 20
	for i := range n {
		f.refs[fmt.Sprintf("ref-%02d", i)] = src
	}
	f.delay = 10 * time.Millisecond
	s := open(t, f, func(o *Options) { o.PullJobs = 4 })

	errs := make(chan error, n)
	for i := range n {
		go func() {
			root, err := s.Ensure(context.Background(), fmt.Sprintf("ref-%02d", i))
			if err == nil && root != src.root {
				err = fmt.Errorf("root %s, want %s", root, src.root)
			}
			errs <- err
		}()
	}
	for range n {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
	f.mu.Lock()
	calls, most := f.calls, f.most
	f.mu.Unlock()
	if calls != n {
		t.Errorf("%d pulls, want %d", calls, n)
	}
	if most > 4 {
		t.Errorf("%d pulls ran at once, want 4 at most", most)
	}
	if most < 2 {
		t.Errorf("%d pulls ran at once, want them to run together", most)
	}
	if got := len(s.Pins()); got != n {
		t.Errorf("%d pins, want %d", got, n)
	}
}

func TestEnsureContextEnds(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["one"] = src
	f.hold = make(chan struct{})
	s := open(t, f, nil)

	ctx, cancel := context.WithCancel(context.Background())
	answered := make(chan error, 1)
	go func() {
		_, err := s.Ensure(ctx, "one")
		answered <- err
	}()
	waitStarted(t, f)
	cancel()
	select {
	case err := <-answered:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Ensure did not return when its context ended")
	}
	if _, ok := s.Pinned("one"); ok {
		t.Fatal("pinned while the pull is held")
	}

	// The pull went on, and pins when it is let go.
	close(f.hold)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if root, ok := s.Pinned("one"); ok {
			if root != src.root {
				t.Fatalf("root %s, want %s", root, src.root)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the pull did not pin")
		}
		time.Sleep(time.Millisecond)
	}
	if calls, _ := f.count(); calls != 1 {
		t.Fatalf("%d pulls, want 1", calls)
	}
}

func TestPinsSurviveReopen(t *testing.T) {
	f := newFake()
	names := []string{"zeta", "alpha", "mid"}
	roots := map[string]key.Key{}
	for _, name := range names {
		src := newSource(t, map[string]string{"name": name})
		f.refs[name] = src
		roots[name] = src.root
	}
	dir := filepath.Join(t.TempDir(), "cache")
	s, err := Open(options(f, dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err := s.Ensure(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	again := newFake()
	s, err = Open(options(again, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	pins := s.Pins()
	if len(pins) != len(names) {
		t.Fatalf("%d pins after the reopen, want %d", len(pins), len(names))
	}
	for i, name := range names {
		if pins[i].Name != name || pins[i].Root != roots[name] {
			t.Errorf("pin %d is %q %s, want %q %s", i, pins[i].Name, pins[i].Root, name, roots[name])
		}
		root, err := s.Ensure(context.Background(), name)
		if err != nil || root != roots[name] {
			t.Errorf("Ensure(%q) = %s, %v", name, root, err)
		}
		if _, err := s.Get(root); err != nil {
			t.Errorf("the root of %q is not in the store: %v", name, err)
		}
	}
	if calls, dials := again.count(); calls != 0 || dials != 0 {
		t.Fatalf("%d pulls and %d dials after the reopen, want none", calls, dials)
	}
}

func TestOpenRefusesABadPin(t *testing.T) {
	for name, rec := range map[string][]byte{
		"short":   make([]byte, 32),
		"bad key": append(bytes.Repeat([]byte{0xff}, 32), "name"...),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake()
			f.refs["one"] = newSource(t, map[string]string{"a": "alpha"})
			dir := filepath.Join(t.TempDir(), "cache")
			s, err := Open(options(f, dir))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Ensure(context.Background(), "one"); err != nil {
				t.Fatal(err)
			}
			if err := s.pinLog.Append(rec); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if s, err := Open(options(f, dir)); err == nil {
				s.Close()
				t.Fatal("Open took a record that is no pin")
			}
		})
	}
}

func TestOnPin(t *testing.T) {
	f := newFake()
	a := newSource(t, map[string]string{"a": "alpha"})
	b := newSource(t, map[string]string{"b": "beta"})
	f.refs["a"], f.refs["b"] = a, b

	type call struct {
		name   string
		root   key.Key
		pinned bool
	}
	var (
		mu    sync.Mutex
		calls []call
		s     *Store
	)
	s = open(t, f, func(o *Options) {
		o.OnPin = func(name string, root key.Key) {
			got, ok := s.Pinned(name)
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, call{name, root, ok && got == root})
		}
	})
	for _, name := range []string{"a", "b", "a", "nope", "b"} {
		s.Ensure(context.Background(), name)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []call{{"a", a.root, true}, {"b", b.root, true}}
	if len(calls) != len(want) {
		t.Fatalf("OnPin was called %d times, want %d: %v", len(calls), len(want), calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Errorf("call %d is %v, want %v", i, calls[i], want[i])
		}
	}
}

func TestPinsInOrder(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	names := []string{"m", "z", "a", "k", "b"}
	for _, name := range names {
		f.refs[name] = src
	}
	s := open(t, f, nil)
	for _, name := range names {
		if _, err := s.Ensure(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	pins := s.Pins()
	if len(pins) != len(names) {
		t.Fatalf("%d pins, want %d", len(pins), len(names))
	}
	for i, name := range names {
		if pins[i].Name != name || pins[i].Root != src.root {
			t.Errorf("pin %d is %q, want %q", i, pins[i].Name, name)
		}
	}
	// What Pins returns is the caller's.
	pins[0].Name = "changed"
	if s.Pins()[0].Name != names[0] {
		t.Fatal("Pins handed out the store's own slice")
	}
}

func TestClose(t *testing.T) {
	f := newFake()
	f.refs["one"] = newSource(t, map[string]string{"a": "alpha"})
	f.hold = make(chan struct{})
	s := open(t, f, nil)

	answered := make(chan error, 1)
	go func() {
		_, err := s.Ensure(context.Background(), "one")
		answered <- err
	}()
	waitStarted(t, f)

	// The fake lets go of the pull when its context ends, which Close
	// sees to.
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	select {
	case err := <-answered:
		if err == nil {
			t.Fatal("the Ensure that Close cut short has no error")
		}
		if errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, which is ErrNotFound", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Ensure did not return after Close")
	}
	if f.running != 0 {
		t.Fatalf("%d pulls still run after Close", f.running)
	}
	if !f.conns[0].isClosed() {
		t.Fatal("Close left the connection open")
	}
	if _, err := s.Ensure(context.Background(), "one"); err == nil {
		t.Fatal("Ensure after Close has no error")
	} else if errors.Is(err, ErrNotFound) {
		t.Fatalf("Ensure after Close: %v, which is ErrNotFound", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
}

func TestCount(t *testing.T) {
	f := newFake()
	f.refs["one"] = newSource(t, map[string]string{"a": "alpha"})
	f.refs["two"] = newSource(t, map[string]string{"b": "beta"})
	dir := filepath.Join(t.TempDir(), "cache")
	s, err := Open(options(f, dir))
	if err != nil {
		t.Fatal(err)
	}
	if n := s.Count(); n != 0 {
		t.Fatalf("Count = %d in a new store", n)
	}
	for _, name := range []string{"one", "two", "one", "missing"} {
		s.Ensure(context.Background(), name)
	}
	if n := s.Count(); n != 2 {
		t.Fatalf("Count = %d, want the 2 that were fetched", n)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(options(f, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if n := s.Count(); n != 2 {
		t.Fatalf("Count = %d after reopening, want 2", n)
	}
}

// A pull that neither ends nor fails would keep its name from ever being
// fetched: it is ended when its time is up, and counts as one that failed.
func TestAnAttemptThatHangsIsEnded(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["one"] = src
	f.hold = make(chan struct{}) // never let go: every pull hangs
	s := open(t, f, func(o *Options) {
		o.AttemptTimeout = 20 * time.Millisecond
		o.PullTimeout = 150 * time.Millisecond
	})

	_, err := s.Ensure(context.Background(), "one")
	if err == nil {
		t.Fatal("no error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, which is ErrNotFound", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, which does not say the time was up", err)
	}
	if calls, _ := f.count(); calls < 2 {
		t.Fatalf("%d pulls, want it tried again", calls)
	}

	// The server answers again: so does the store.
	f.mu.Lock()
	f.hold = nil
	f.mu.Unlock()
	root, err := s.Ensure(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	if root != src.root {
		t.Fatalf("root %s, want %s", root, src.root)
	}
}

// A name keeps the root it was first fetched with, so a cache filled from
// one server and prefix has nothing to say about another: it is refused,
// where it would otherwise serve the names of the one as those of the
// other.
func TestACacheIsOfOneServerAndPrefix(t *testing.T) {
	f := newFake()
	f.refs["store/one"] = newSource(t, map[string]string{"a": "alpha"})
	dir := filepath.Join(t.TempDir(), "cache")
	with := func(server, prefix string) Options {
		o := options(f, dir)
		o.Server, o.Prefix = server, prefix
		return o
	}

	s, err := Open(with("server-a", "store/"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ensure(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The same again is the same cache.
	s, err = Open(with("server-a", "store/"))
	if err != nil {
		t.Fatalf("the cache opened again as it was made: %v", err)
	}
	if n := s.Count(); n != 1 {
		t.Fatalf("Count = %d, want the pin that was made", n)
	}
	s.Close()

	for what, o := range map[string]Options{
		"another prefix":           with("server-a", "other/"),
		"a prefix that is longer":  with("server-a", "store/x"),
		"no prefix":                with("server-a", ""),
		"another server":           with("server-b", "store/"),
		"a server that is unnamed": with("", "store/"),
	} {
		s, err := Open(o)
		if err == nil {
			s.Close()
			t.Errorf("%s: the cache was opened", what)
			continue
		}
		// The error says what the cache is of and what was asked for.
		for _, part := range []string{"server-a", "store/", o.Prefix} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("%s: %q does not name %q", what, err, part)
			}
		}
	}

	// A refusal changed nothing.
	s, err = Open(with("server-a", "store/"))
	if err != nil {
		t.Fatalf("the cache after it was refused to others: %v", err)
	}
	s.Close()
}

// What a prefix or a server is called is written as it is: a prefix with a
// line feed in it is not another cache's first line.
func TestTheOriginOfACacheIsNotMistaken(t *testing.T) {
	f := newFake()
	dir := filepath.Join(t.TempDir(), "cache")
	o := options(f, dir)
	o.Server, o.Prefix = "a", "b\nprefix \"c\""
	s, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	o.Server, o.Prefix = "a\"\nprefix \"b", "c"
	if s, err := Open(o); err == nil {
		s.Close()
		t.Fatal("two servers and prefixes were taken for one")
	}
}

// held returns a fake whose pulls wait until they are let go or their
// context ends, with the references given.
func held(t *testing.T, names ...string) (*fake, chan struct{}) {
	t.Helper()
	f := newFake()
	for _, name := range names {
		f.refs[name] = newSource(t, map[string]string{"file": name})
	}
	f.hold = make(chan struct{})
	return f, f.hold
}

// eventually waits for a condition that another goroutine brings about.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("it never came about that %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *fake) runningNow() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.running
}

func TestPreloadPullsAndPins(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["one"] = src
	pinned := 0
	s := open(t, f, func(o *Options) { o.OnPin = func(string, key.Key) { pinned++ } })

	root, err := s.Preload(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	if root != src.root {
		t.Fatalf("root %s, want %s", root, src.root)
	}
	if got, ok := s.Pinned("one"); !ok || got != src.root || pinned != 1 {
		t.Fatalf("Pinned = %s, %v; OnPin called %d times", got, ok, pinned)
	}
	// What is pinned is answered at once, to a preload as to a lookup.
	if _, err := s.Preload(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Ensure(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	if calls, _ := f.count(); calls != 1 {
		t.Fatalf("%d pulls, want 1", calls)
	}

	if _, err := s.Preload(context.Background(), "nothing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a reference the server does not have: %v", err)
	}
}

// The pulls of a preload do not wait for one of the PullJobs, which are
// the lookups': whoever preloads says how many of its own run at once.
func TestPreloadIsNotOneOfThePullJobs(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e", "f"}
	f, hold := held(t, append(names, "looked-up-1", "looked-up-2")...)
	s := open(t, f, func(o *Options) { o.PullJobs = 1 })

	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Preload(context.Background(), name); err != nil {
				t.Errorf("preload of %q: %v", name, err)
			}
		}()
	}
	eventually(t, "all six preloads pull at once", func() bool { return f.runningNow() == len(names) })

	// The lookups still have their one job, and only that one.
	for _, name := range []string{"looked-up-1", "looked-up-2"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Ensure(context.Background(), name); err != nil {
				t.Errorf("lookup of %q: %v", name, err)
			}
		}()
	}
	eventually(t, "a lookup pulls beside them", func() bool { return f.runningNow() == len(names)+1 })
	time.Sleep(50 * time.Millisecond)
	if n := f.runningNow(); n != len(names)+1 {
		t.Fatalf("%d pulls at once, want the six preloads and one lookup", n)
	}
	close(hold)
	wg.Wait()
}

// A preload that is given up ends its pull: nobody else wanted it.
func TestAPreloadThatIsGivenUpEndsItsPull(t *testing.T) {
	f, hold := held(t, "one")
	s := open(t, f, nil)

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() {
		_, err := s.Preload(ctx, "one")
		got <- err
	}()
	waitStarted(t, f)
	cancel()
	if err := <-got; !errors.Is(err, context.Canceled) {
		t.Fatalf("the preload that was given up: %v", err)
	}
	eventually(t, "the pull ended", func() bool { return f.runningNow() == 0 })
	if _, ok := s.Pinned("one"); ok {
		t.Fatal("a reference whose pull was ended is pinned")
	}
	// The pull was ended, not the connection it ran over.
	if f.conns[0].isClosed() {
		t.Fatal("ending a pull closed the connection")
	}

	// It is fetched when it is asked for again, over the same connection.
	close(hold)
	if _, err := s.Ensure(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	if calls, dials := f.count(); calls != 2 || dials != 1 {
		t.Fatalf("%d pulls and %d dials, want 2 and 1", calls, dials)
	}
}

// A lookup that waits for the same reference keeps the pull going, whether
// it came before the preload or after: it is the kernel that waits.
func TestALookupKeepsThePullOfAPreloadGoing(t *testing.T) {
	for _, order := range []string{"the lookup first", "the preload first"} {
		t.Run(order, func(t *testing.T) {
			f, hold := held(t, "one")
			s := open(t, f, nil)

			ctx, cancel := context.WithCancel(context.Background())
			preloaded, looked := make(chan error, 1), make(chan error, 1)
			preload := func() {
				go func() {
					_, err := s.Preload(ctx, "one")
					preloaded <- err
				}()
			}
			lookup := func() {
				go func() {
					_, err := s.Ensure(context.Background(), "one")
					looked <- err
				}()
			}
			if order == "the lookup first" {
				lookup()
				waitStarted(t, f)
				preload()
			} else {
				preload()
				waitStarted(t, f)
				lookup()
			}
			// Both wait for the one pull.
			time.Sleep(50 * time.Millisecond)
			cancel()
			if err := <-preloaded; !errors.Is(err, context.Canceled) {
				t.Fatalf("the preload that was given up: %v", err)
			}
			time.Sleep(50 * time.Millisecond)
			if n := f.runningNow(); n != 1 {
				t.Fatalf("%d pulls running after the preload was given up, want the one the lookup waits for", n)
			}
			close(hold)
			if err := <-looked; err != nil {
				t.Fatalf("the lookup: %v", err)
			}
			if _, ok := s.Pinned("one"); !ok {
				t.Fatal("not pinned")
			}
			if calls, _ := f.count(); calls != 1 {
				t.Fatalf("%d pulls, want the one", calls)
			}
		})
	}
}

// A lookup whose own request ended still counts: the kernel asks again,
// and is to find the pull running or done.
func TestALookupThatGaveUpStillKeepsThePull(t *testing.T) {
	f, hold := held(t, "one")
	s := open(t, f, nil)

	lookupCtx, giveUp := context.WithCancel(context.Background())
	looked := make(chan error, 1)
	go func() {
		_, err := s.Ensure(lookupCtx, "one")
		looked <- err
	}()
	waitStarted(t, f)
	giveUp()
	<-looked

	ctx, cancel := context.WithCancel(context.Background())
	preloaded := make(chan error, 1)
	go func() {
		_, err := s.Preload(ctx, "one")
		preloaded <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-preloaded
	time.Sleep(50 * time.Millisecond)
	if n := f.runningNow(); n != 1 {
		t.Fatalf("%d pulls running, want the one a lookup asked for", n)
	}
	close(hold)
	eventually(t, "the reference is pinned", func() bool { _, ok := s.Pinned("one"); return ok })
}

// Two preloads of one reference share its pull, which ends when the last
// of them is given up.
func TestThePullEndsWithTheLastPreload(t *testing.T) {
	f, _ := held(t, "one")
	s := open(t, f, nil)

	firstCtx, giveUpFirst := context.WithCancel(context.Background())
	secondCtx, giveUpSecond := context.WithCancel(context.Background())
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := s.Preload(firstCtx, "one"); first <- err }()
	waitStarted(t, f)
	go func() { _, err := s.Preload(secondCtx, "one"); second <- err }()
	time.Sleep(50 * time.Millisecond)

	giveUpFirst()
	<-first
	time.Sleep(50 * time.Millisecond)
	if n := f.runningNow(); n != 1 {
		t.Fatalf("%d pulls running after one of two preloads was given up, want 1", n)
	}
	giveUpSecond()
	<-second
	eventually(t, "the pull ended", func() bool { return f.runningNow() == 0 })
	if calls, _ := f.count(); calls != 1 {
		t.Fatalf("%d pulls, want the one they shared", calls)
	}
}

// Whoever asks for a reference just after its pull was ended gets a pull
// of its own, and not the end of the other.
func TestAskingRightAfterAPullWasEnded(t *testing.T) {
	f := newFake()
	src := newSource(t, map[string]string{"a": "alpha"})
	f.refs["one"] = src
	f.delay = 20 * time.Millisecond
	s := open(t, f, nil)

	for range 30 {
		ctx, cancel := context.WithCancel(context.Background())
		go s.Preload(ctx, "one")
		time.Sleep(time.Millisecond)
		cancel()
		if _, ok := s.Pinned("one"); ok {
			break // the pull was done before it could be ended
		}
		root, err := s.Ensure(context.Background(), "one")
		if err != nil {
			t.Fatalf("the lookup after a pull that was ended: %v", err)
		}
		if root != src.root {
			t.Fatalf("root %s", root)
		}
		break
	}
	if n := s.Count(); n != 1 {
		t.Fatalf("Count = %d, want the one pin", n)
	}
}

func TestCloseEndsThePullsOfPreloads(t *testing.T) {
	f, _ := held(t, "one", "two")
	s := open(t, f, nil)
	errs := make(chan error, 2)
	for _, name := range []string{"one", "two"} {
		go func() { _, err := s.Preload(context.Background(), name); errs <- err }()
	}
	eventually(t, "both pull", func() bool { return f.runningNow() == 2 })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-errs; err == nil || errors.Is(err, ErrNotFound) {
			t.Fatalf("a preload cut short by Close: %v", err)
		}
	}
}

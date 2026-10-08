package materialize

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"golang.org/x/sys/unix"
)

// newObjects opens a packstore that lives as long as the test.
func newObjects(t *testing.T) *packstore.Store {
	t.Helper()
	objects, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"), packstore.WithSync(false))
	if err != nil {
		t.Fatalf("opening the packstore: %v", err)
	}
	t.Cleanup(func() { objects.Close() })
	return objects
}

// importPath builds the tree of the directory, or the content of the file,
// at path and writes its objects to objects. It returns the root.
func importPath(t *testing.T, objects *packstore.Store, path string) key.Key {
	t.Helper()
	built, root, err := ingest.Objects(path, ingest.Opts{NoIgnore: true})
	if err != nil {
		t.Fatalf("importing %s: %v", path, err)
	}
	_, err = objects.WriteParallel(func(yield func(packstore.Object, error) bool) {
		for o, err := range built {
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, err) || err != nil {
				return
			}
		}
	}, packstore.WriteOpts{})
	if err != nil {
		t.Fatalf("importing %s: %v", path, err)
	}
	return *root
}

// put stores one object built by hand.
func put(t *testing.T, objects *packstore.Store, o fstree.Object, err error) key.Key {
	t.Helper()
	if err != nil {
		t.Fatalf("encoding an object: %v", err)
	}
	if err := objects.Put(o.Key, o.Bytes); err != nil {
		t.Fatalf("storing %s: %v", o.Key, err)
	}
	return o.Key
}

// writeFiles writes files, given as path below dir to content, and the
// directories they are in.
func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for rel, content := range files {
		path := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// importFiles imports a directory made of files and returns its root.
func importFiles(t *testing.T, objects *packstore.Store, files map[string]string) key.Key {
	t.Helper()
	src := filepath.Join(t.TempDir(), "src")
	writeFiles(t, src, files)
	return importPath(t, objects, src)
}

// getter is the getter a test hands to Open: the packstore's, which counts
// its calls, and can be held or made to fail on a key.
type getter struct {
	objects *packstore.Store

	mu       sync.Mutex
	calls    map[key.Key]int
	order    []key.Key
	held     map[key.Key]chan struct{}
	failing  map[key.Key]error
	running  int
	mostEver int

	// entered is told the key of every call that is held, before it waits.
	entered chan key.Key
}

func newGetter(objects *packstore.Store) *getter {
	return &getter{
		objects: objects,
		calls:   map[key.Key]int{},
		held:    map[key.Key]chan struct{}{},
		failing: map[key.Key]error{},
		entered: make(chan key.Key, 64),
	}
}

// hold makes every get of k wait until the function it returns is called.
func (g *getter) hold(k key.Key) (release func()) {
	gate := make(chan struct{})
	g.mu.Lock()
	g.held[k] = gate
	g.mu.Unlock()
	return sync.OnceFunc(func() { close(gate) })
}

// fail makes every get of k return err.
func (g *getter) fail(k key.Key, err error) {
	g.mu.Lock()
	g.failing[k] = err
	g.mu.Unlock()
}

func (g *getter) get(k key.Key) ([]byte, error) {
	g.mu.Lock()
	g.calls[k]++
	g.order = append(g.order, k)
	g.running++
	g.mostEver = max(g.mostEver, g.running)
	gate, err := g.held[k], g.failing[k]
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.running--
		g.mu.Unlock()
	}()
	if gate != nil {
		g.entered <- k
		<-gate
	}
	if err != nil {
		return nil, err
	}
	return g.objects.Get(k)
}

func (g *getter) count(k key.Key) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[k]
}

func (g *getter) total() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.order)
}

// asked returns the keys asked for so far, in order.
func (g *getter) asked() []key.Key {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]key.Key(nil), g.order...)
}

// waitEntered waits until a held get has begun and returns its key.
func (g *getter) waitEntered(t *testing.T) key.Key {
	t.Helper()
	select {
	case k := <-g.entered:
		return k
	case <-time.After(10 * time.Second):
		t.Fatal("no held get began")
		return key.Key{}
	}
}

func quiet() *slog.Logger { return slog.New(slog.DiscardHandler) }

// open opens a Store over dir that is closed when the test ends.
func open(t *testing.T, dir string, get func(key.Key) ([]byte, error), jobs int) *Store {
	t.Helper()
	s, err := Open(dir, get, Options{Jobs: jobs, Log: quiet()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// names returns the names of what is in dir.
func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// wantEmpty fails the test when anything is in dir.
func wantEmpty(t *testing.T, dir string) {
	t.Helper()
	if got := names(t, dir); len(got) != 0 {
		t.Errorf("%s holds %q, want nothing", dir, got)
	}
}

// sameTree fails the test unless got holds the directories and regular
// files of src, with the same content, and nothing else.
func sameTree(t *testing.T, src, got string) {
	t.Helper()
	want := map[string]bool{}
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		other := filepath.Join(got, rel)
		switch {
		case d.IsDir():
			want[rel] = true
			info, err := os.Lstat(other)
			if err != nil || !info.IsDir() {
				t.Errorf("%q: want a directory, got %v (%v)", rel, info, err)
			}
		case d.Type().IsRegular():
			want[rel] = true
			a, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			info, err := os.Lstat(other)
			if err != nil || !info.Mode().IsRegular() {
				t.Errorf("%q: want a regular file, got %v (%v)", rel, info, err)
				return nil
			}
			b, err := os.ReadFile(other)
			if err != nil {
				t.Errorf("%q: %v", rel, err)
			} else if !bytes.Equal(a, b) {
				t.Errorf("%q: content differs (%d bytes, want %d)", rel, len(b), len(a))
			}
		default:
			if _, err := os.Lstat(other); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("%q is a %v in the source and should not be written (%v)", rel, d.Type(), err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(got, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if rel, _ := filepath.Rel(got, path); !want[rel] {
			t.Errorf("%q was written and is not in the source", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTreeEqualsSource(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	big := make([]byte, 9<<20)
	rand.NewChaCha8([32]byte{1, 2, 3}).Read(big)
	writeFiles(t, src, map[string]string{
		"top":                  "top\n",
		"empty":                "",
		"big":                  string(big),
		"a/b/c/deep":           "deep\n",
		"a/b/sibling":          "sibling\n",
		"a/name with\n spaces": "odd\n",
		"lib/libfoo.so":        "\x7fELF",
	})
	for _, dir := range []string{"hollow", "a/b/hollow"} {
		if err := os.MkdirAll(filepath.Join(src, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// What the result does not reproduce: modes, links and other types.
	if err := os.Chmod(filepath.Join(src, "lib/libfoo.so"), 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(src, "top"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(src, "hollow"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("top", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/nowhere/at/all", filepath.Join(src, "a/dangling")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(src, "a/fifo"), 0o644); err != nil {
		t.Fatal(err)
	}

	objects := newObjects(t)
	root := importPath(t, objects, src)
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, objects.Get, 0)
	s.Queue("ref", root)
	s.Wait()
	if !s.Done("ref") {
		t.Fatal("the reference is not done")
	}
	out := filepath.Join(dir, "done", "ref")
	sameTree(t, src, out)
	wantEmpty(t, filepath.Join(dir, "partial"))

	// Modes are the two the package writes, whatever the source had.
	err := filepath.WalkDir(out, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		want := fs.FileMode(0o644)
		if d.IsDir() {
			want = fs.ModeDir | 0o755
		}
		if info.Mode() != want {
			t.Errorf("%q has mode %v, want %v", path, info.Mode(), want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestModesUnderNarrowUmask(t *testing.T) {
	// The umask is the process's: this test must not run beside others.
	old := unix.Umask(0o077)
	defer unix.Umask(old)

	objects := newObjects(t)
	root := importFiles(t, objects, map[string]string{"d/f": "f\n"})
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, objects.Get, 0)
	s.Queue("ref", root)
	s.Wait()
	for rel, want := range map[string]fs.FileMode{
		"":    fs.ModeDir | 0o755,
		"d":   fs.ModeDir | 0o755,
		"d/f": 0o644,
	} {
		info, err := os.Lstat(filepath.Join(dir, "done", "ref", rel))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode() != want {
			t.Errorf("%q has mode %v, want %v", rel, info.Mode(), want)
		}
	}
}

func TestSingleFile(t *testing.T) {
	for name, content := range map[string]string{
		"small": "one file\n",
		"empty": "",
		"large": strings.Repeat("0123456789abcdef", 3<<16),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
				t.Fatal(err)
			}
			objects := newObjects(t)
			root := importPath(t, objects, path)
			if typ := root.Type(); typ != key.Blob && typ != key.FileNode {
				t.Fatalf("the root of a file is a %v", typ)
			}
			dir := filepath.Join(t.TempDir(), "files")
			s := open(t, dir, objects.Get, 0)
			s.Queue("ref", root)
			s.Wait()

			out := filepath.Join(dir, "done", "ref")
			info, err := os.Lstat(out)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode() != 0o644 {
				t.Errorf("mode %v, want a regular file of 0644", info.Mode())
			}
			got, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != content {
				t.Errorf("content differs (%d bytes, want %d)", len(got), len(content))
			}
			if p, ok := s.Path("ref", nil); p != out || !ok {
				t.Errorf("Path = %q, %v, want %q, true", p, ok, out)
			}
		})
	}
}

func TestEmptyDirectory(t *testing.T) {
	objects := newObjects(t)
	root := importFiles(t, objects, nil)
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, objects.Get, 0)
	s.Queue("ref", root)
	s.Wait()
	if !s.Done("ref") {
		t.Fatal("the reference is not done")
	}
	out := filepath.Join(dir, "done", "ref")
	info, err := os.Lstat(out)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode() != fs.ModeDir|0o755 {
		t.Errorf("mode %v, want a directory of 0755", info.Mode())
	}
	wantEmpty(t, out)
}

func TestCommitRoot(t *testing.T) {
	objects := newObjects(t)
	tree := importFiles(t, objects, map[string]string{"d/f": "f\n"})
	k, data, err := commit.Commit{Tree: tree, Message: "one"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(k, data); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, objects.Get, 0)
	s.Queue("ref", k)
	s.Wait()
	got, err := os.ReadFile(filepath.Join(dir, "done", "ref", "d", "f"))
	if err != nil || string(got) != "f\n" {
		t.Errorf("d/f = %q, %v", got, err)
	}
}

// lastBlob materializes root once with a Store of its own and returns the
// blob the getter was asked for last.
func lastBlob(t *testing.T, objects *packstore.Store, root key.Key) key.Key {
	t.Helper()
	g := newGetter(objects)
	s := open(t, filepath.Join(t.TempDir(), "files"), g.get, 1)
	s.Queue("probe", root)
	s.Wait()
	if !s.Done("probe") {
		t.Fatal("the probe was not written")
	}
	asked := g.asked()
	for i := len(asked) - 1; i >= 0; i-- {
		if asked[i].Type() == key.Blob {
			if g.count(asked[i]) != 1 {
				t.Fatalf("the last blob was asked for %d times", g.count(asked[i]))
			}
			return asked[i]
		}
	}
	t.Fatal("no blob was asked for")
	return key.Key{}
}

var threeFiles = map[string]string{
	"a/first":  "first\n",
	"b/second": "second\n",
	"c/third":  "third\n",
}

func TestDoneOnlyWhenWhole(t *testing.T) {
	objects := newObjects(t)
	root := importFiles(t, objects, threeFiles)
	last := lastBlob(t, objects, root)

	g := newGetter(objects)
	release := g.hold(last)
	defer release()
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, g.get, 0)
	want := filepath.Join(dir, "done", "ref", "a", "first")

	if p, ok := s.Path("ref", []string{"a", "first"}); p != want || ok {
		t.Errorf("before Queue: Path = %q, %v, want %q, false", p, ok, want)
	}
	s.Queue("ref", root)
	if k := g.waitEntered(t); k != last {
		t.Fatalf("held on %s, want %s", k, last)
	}
	if s.Done("ref") {
		t.Error("Done is true while the last blob is held")
	}
	if p, ok := s.Path("ref", []string{"a", "first"}); p != want || ok {
		t.Errorf("while held: Path = %q, %v, want %q, false", p, ok, want)
	}
	wantEmpty(t, filepath.Join(dir, "done"))
	if got := names(t, filepath.Join(dir, "partial")); len(got) != 1 || got[0] != "ref" {
		t.Errorf("partial holds %q while the reference is written, want it alone", got)
	}

	release()
	s.Wait()
	if !s.Done("ref") {
		t.Error("Done is false after Wait")
	}
	if p, ok := s.Path("ref", []string{"a", "first"}); p != want || !ok {
		t.Errorf("after Wait: Path = %q, %v, want %q, true", p, ok, want)
	}
	if got, err := os.ReadFile(want); err != nil || string(got) != "first\n" {
		t.Errorf("%s = %q, %v", want, got, err)
	}
	wantEmpty(t, filepath.Join(dir, "partial"))
}

func TestPathJoinsRel(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, func(key.Key) ([]byte, error) { return nil, errors.New("not asked") }, 0)
	for _, c := range []struct {
		rel  []string
		want string
	}{
		{nil, filepath.Join(dir, "done", "ref")},
		{[]string{}, filepath.Join(dir, "done", "ref")},
		{[]string{"f"}, filepath.Join(dir, "done", "ref", "f")},
		{[]string{"a", "b c", "d"}, filepath.Join(dir, "done", "ref", "a", "b c", "d")},
	} {
		if p, ok := s.Path("ref", c.rel); p != c.want || ok {
			t.Errorf("Path(ref, %q) = %q, %v, want %q, false", c.rel, p, ok, c.want)
		}
	}
	if s.Done("ref") {
		t.Error("Done is true for a name never queued")
	}
}

func TestQueueTwiceWritesOnce(t *testing.T) {
	objects := newObjects(t)
	root := importFiles(t, objects, threeFiles)
	g := newGetter(objects)
	release := g.hold(root)
	defer release()
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, g.get, 2)

	s.Queue("ref", root)
	g.waitEntered(t)
	s.Queue("ref", root) // being written
	release()
	s.Wait()
	s.Queue("ref", root) // done
	s.Wait()
	if n := g.count(root); n != 1 {
		t.Errorf("the root was asked for %d times, want 1", n)
	}

	// Queued and not begun: one worker, held on another reference.
	other := importFiles(t, objects, map[string]string{"x": "x\n"})
	third := importFiles(t, objects, map[string]string{"y": "y\n"})
	g = newGetter(objects)
	release = g.hold(other)
	defer release()
	s = open(t, filepath.Join(t.TempDir(), "files"), g.get, 1)
	s.Queue("other", other)
	g.waitEntered(t)
	s.Queue("third", third)
	s.Queue("third", third)
	release()
	s.Wait()
	if n := g.count(third); n != 1 {
		t.Errorf("the queued root was asked for %d times, want 1", n)
	}
	if !s.Done("other") || !s.Done("third") {
		t.Error("not both references are done")
	}
}

func TestAtMostJobsAtOnce(t *testing.T) {
	objects := newObjects(t)
	g := newGetter(objects)
	var roots []key.Key
	var releases []func()
	for _, name := range []string{"w", "x", "y", "z"} {
		root := importFiles(t, objects, map[string]string{name: name, "sub/" + name: name + name})
		roots = append(roots, root)
		release := g.hold(root)
		defer release()
		releases = append(releases, release)
	}
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, g.get, 2)
	for i, root := range roots {
		s.Queue(string(rune('0'+i)), root)
	}

	// The first two queued are the two that begin, in either order.
	first := map[key.Key]bool{g.waitEntered(t): true, g.waitEntered(t): true}
	if !first[roots[0]] || !first[roots[1]] {
		t.Errorf("the two begun are not the first two queued")
	}
	select {
	case k := <-g.entered:
		t.Fatalf("a third reference (%s) is being written with two jobs", k)
	case <-time.After(100 * time.Millisecond):
	}
	// One let go makes room for exactly the next in the queue.
	releases[0]()
	if k := g.waitEntered(t); k != roots[2] {
		t.Errorf("the third begun is not the third queued")
	}
	select {
	case k := <-g.entered:
		t.Fatalf("a third reference (%s) is being written with two jobs", k)
	case <-time.After(100 * time.Millisecond):
	}
	for _, release := range releases {
		release()
	}
	s.Wait()
	for i := range roots {
		if !s.Done(string(rune('0' + i))) {
			t.Errorf("reference %d is not done", i)
		}
	}
	g.mu.Lock()
	most := g.mostEver
	g.mu.Unlock()
	if most > 2 {
		t.Errorf("%d gets ran at once, want no more than 2", most)
	}
}

func TestWrittenInQueueOrder(t *testing.T) {
	objects := newObjects(t)
	g := newGetter(objects)
	var roots []key.Key
	for _, name := range []string{"w", "x", "y", "z"} {
		roots = append(roots, importFiles(t, objects, map[string]string{name: name}))
	}
	release := g.hold(roots[0])
	defer release()
	s := open(t, filepath.Join(t.TempDir(), "files"), g.get, 1)
	s.Queue("0", roots[0])
	g.waitEntered(t)
	// Not in the order of their names.
	s.Queue("9", roots[1])
	s.Queue("5", roots[2])
	s.Queue("7", roots[3])
	release()
	s.Wait()

	var got []key.Key
	for _, k := range g.asked() {
		if k.Type() != key.Blob {
			got = append(got, k)
		}
	}
	if len(got) != len(roots) {
		t.Fatalf("%d trees were asked for, want %d", len(got), len(roots))
	}
	for i := range roots {
		if got[i] != roots[i] {
			t.Errorf("tree %d written is not the one queued in that place", i)
		}
	}
}

func TestOpenEmptiesPartial(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "files")
	writeFiles(t, filepath.Join(dir, "partial"), map[string]string{
		"half/a/b": "half",
		"file":     "half",
	})
	objects := newObjects(t)
	s := open(t, dir, objects.Get, 0)
	wantEmpty(t, filepath.Join(dir, "partial"))
	if s.Done("half") || s.Done("file") {
		t.Error("what was under partial is done")
	}

	// A name left half written is written whole when queued.
	root := importFiles(t, objects, threeFiles)
	s.Queue("half", root)
	s.Wait()
	if !s.Done("half") {
		t.Error("the reference is not done")
	}
}

func TestOpenKnowsDone(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "files")
	objects := newObjects(t)
	root := importFiles(t, objects, threeFiles)

	first, err := Open(dir, objects.Get, Options{Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	first.Queue("tree", root)
	first.Wait()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// A reference that is one file is known by its file.
	if err := os.WriteFile(filepath.Join(dir, "done", "single"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	g := newGetter(objects)
	s := open(t, dir, g.get, 0)
	if !s.Done("tree") || !s.Done("single") {
		t.Error("what is under done is not done after Open")
	}
	if s.Done("other") {
		t.Error("a name that is not under done is done")
	}
	want := filepath.Join(dir, "done", "tree", "a", "first")
	if p, ok := s.Path("tree", []string{"a", "first"}); p != want || !ok {
		t.Errorf("Path = %q, %v, want %q, true", p, ok, want)
	}
	s.Queue("tree", root)
	s.Queue("single", root)
	s.Wait()
	if n := g.total(); n != 0 {
		t.Errorf("the getter was called %d times for references that are done", n)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "done", "single")); err != nil || string(got) != "x" {
		t.Errorf("the file under done was touched: %q, %v", got, err)
	}
}

func TestFailingGetter(t *testing.T) {
	objects := newObjects(t)
	root := importFiles(t, objects, threeFiles)
	last := lastBlob(t, objects, root)

	for name, bad := range map[string]key.Key{"root": root, "last blob": last} {
		t.Run(name, func(t *testing.T) {
			g := newGetter(objects)
			g.fail(bad, errors.New("the disk is gone"))
			dir := filepath.Join(t.TempDir(), "files")
			var logged bytes.Buffer
			s, err := Open(dir, g.get, Options{Log: slog.New(slog.NewTextHandler(&logged, nil))})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			s.Queue("ref", root)
			s.Wait()
			if s.Done("ref") {
				t.Error("Done is true after a failure")
			}
			if _, ok := s.Path("ref", nil); ok {
				t.Error("Path says the reference is materialized")
			}
			wantEmpty(t, filepath.Join(dir, "done"))
			wantEmpty(t, filepath.Join(dir, "partial"))
			if out := logged.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "the disk is gone") || !strings.Contains(out, "name=ref") {
				t.Errorf("the failure was not logged as an error: %q", out)
			}

			calls := g.total()
			s.Queue("ref", root)
			s.Wait()
			if n := g.total(); n != calls {
				t.Errorf("a second Queue called the getter %d more times", n-calls)
			}
			if s.Done("ref") {
				t.Error("Done is true after a second Queue")
			}

			// One failure is not the worker's end.
			good := importFiles(t, objects, map[string]string{"x": "x\n"})
			s.Queue("good", good)
			s.Wait()
			if !s.Done("good") {
				t.Error("a reference queued after a failure is not done")
			}
		})
	}
}

func TestCloseInTheMiddle(t *testing.T) {
	objects := newObjects(t)
	root := importFiles(t, objects, threeFiles)
	last := lastBlob(t, objects, root)
	waiting := importFiles(t, objects, map[string]string{"x": "x\n"})

	g := newGetter(objects)
	release := g.hold(last)
	defer release()
	dir := filepath.Join(t.TempDir(), "files")
	s, err := Open(dir, g.get, Options{Jobs: 1, Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	s.Queue("ref", root)
	s.Queue("waiting", waiting)
	g.waitEntered(t)

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	// Close waits for the worker, which waits for the getter: it is let go
	// when Close has had every chance to begin.
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) while the getter was held", err)
	case <-time.After(100 * time.Millisecond):
	}
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}

	// The getter gave the last blob, and the reference is abandoned all
	// the same.
	if s.Done("ref") || s.Done("waiting") {
		t.Error("a reference is done after Close in the middle")
	}
	wantEmpty(t, filepath.Join(dir, "done"))
	wantEmpty(t, filepath.Join(dir, "partial"))
	if n := g.count(waiting); n != 0 {
		t.Errorf("the reference that was queued was begun after Close")
	}

	s.Wait() // nothing is queued, nothing is written
	calls := g.total()
	s.Queue("after", waiting)
	s.Wait()
	if g.total() != calls || s.Done("after") {
		t.Error("Queue after Close did something")
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close a second time: %v", err)
	}
	wantEmpty(t, filepath.Join(dir, "done"))
}

func TestCloseIdle(t *testing.T) {
	objects := newObjects(t)
	root := importFiles(t, objects, threeFiles)
	dir := filepath.Join(t.TempDir(), "files")
	s, err := Open(dir, objects.Get, Options{Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	s.Queue("ref", root)
	s.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// What was written stays, and stays known.
	if !s.Done("ref") {
		t.Error("a reference written before Close is not done after it")
	}
	sameNames := names(t, filepath.Join(dir, "done"))
	if len(sameNames) != 1 || sameNames[0] != "ref" {
		t.Errorf("done holds %q", sameNames)
	}
}

func TestRefusedEntryNames(t *testing.T) {
	objects := newObjects(t)
	blob := putBlob(t, objects, []byte("content\n"))
	inner := importFiles(t, objects, map[string]string{"f": "f\n"})

	for what, name := range map[string]string{
		"slash":         "a/b",
		"leading slash": "/etc",
		"dot":           ".",
		"dot dot":       "..",
		"empty":         "",
		"nul":           "a\x00b",
	} {
		for kind, entry := range map[string]fstree.Entry{
			"file":      {Name: []byte(name), Mode: unix.S_IFREG | 0o644, ContentKey: blob[:]},
			"directory": {Name: []byte(name), Mode: unix.S_IFDIR | 0o755, ContentKey: inner[:]},
			// Not written, and its name is refused all the same.
			"link": {Name: []byte(name), Mode: unix.S_IFLNK | 0o777, LinkTarget: []byte("x")},
		} {
			t.Run(what+" "+kind, func(t *testing.T) {
				// A good entry before the bad one, so that something is
				// there to remove. "!" sorts before every name above but
				// the empty one.
				entries := []fstree.Entry{entry}
				good := fstree.Entry{Name: []byte("!"), Mode: unix.S_IFREG | 0o644, ContentKey: blob[:]}
				if name == "" {
					entries = append(entries, good)
				} else {
					entries = append([]fstree.Entry{good}, entries...)
				}
				leaf, err := fstree.EncodeDirLeaf(entries)
				root := put(t, objects, leaf, err)
				// The bad name one level down as well.
				outer, err := fstree.EncodeDirLeaf([]fstree.Entry{
					{Name: []byte("sub"), Mode: unix.S_IFDIR | 0o755, ContentKey: root[:]},
				})
				nested := put(t, objects, outer, err)

				base := t.TempDir()
				dir := filepath.Join(base, "cache", "files")
				s := open(t, dir, objects.Get, 0)
				s.Queue("top", root)
				s.Queue("nested", nested)
				s.Wait()
				if s.Done("top") || s.Done("nested") {
					t.Error("a reference with a refused name is done")
				}
				wantEmpty(t, filepath.Join(dir, "done"))
				wantEmpty(t, filepath.Join(dir, "partial"))
				// Nothing was written above the directory either.
				if got := names(t, filepath.Join(base, "cache")); len(got) != 1 {
					t.Errorf("the cache directory holds %q", got)
				}
				if got := names(t, dir); len(got) != 2 {
					t.Errorf("the files directory holds %q", got)
				}
			})
		}
	}
}

// putBlob stores data as a blob and returns its key.
func putBlob(t *testing.T, objects *packstore.Store, data []byte) key.Key {
	t.Helper()
	o, err := fstree.EncodeBlob(data)
	return put(t, objects, o, err)
}

func TestRefusedReferenceNames(t *testing.T) {
	objects := newObjects(t)
	tree := importFiles(t, objects, map[string]string{"f": "f\n"})
	file := putBlob(t, objects, []byte("content\n"))

	base := t.TempDir()
	dir := filepath.Join(base, "cache", "files")
	s := open(t, dir, objects.Get, 0)
	bad := []string{"", ".", "..", "a/b", "../escaped", "../../escaped", "/escaped", "a\x00b"}
	for _, name := range bad {
		s.Queue(name, tree)
		s.Wait()
		if s.Done(name) {
			t.Errorf("%q is done", name)
		}
	}
	// The same names again for a file: each has failed, and nothing is
	// tried.
	for _, name := range bad {
		s.Queue(name, file)
	}
	s.Wait()

	// And for a file on a Store that has not seen them.
	s2 := open(t, filepath.Join(base, "cache2", "files"), objects.Get, 0)
	for _, name := range bad {
		s2.Queue(name, file)
		s2.Wait()
		if s2.Done(name) {
			t.Errorf("%q is done", name)
		}
	}

	for _, d := range []string{dir, filepath.Join(base, "cache2", "files")} {
		wantEmpty(t, filepath.Join(d, "done"))
		wantEmpty(t, filepath.Join(d, "partial"))
		if got := names(t, d); len(got) != 2 {
			t.Errorf("%s holds %q", d, got)
		}
	}
	if got := names(t, base); len(got) != 2 {
		t.Errorf("something escaped: %q", got)
	}
	if got := names(t, filepath.Join(base, "cache")); len(got) != 1 {
		t.Errorf("something escaped: %q", got)
	}
}

func TestRootOfAnotherType(t *testing.T) {
	objects := newObjects(t)
	o, err := fstree.EncodeXattrSet(map[string][]byte{"user.x": []byte("y")})
	root := put(t, objects, o, err)
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, objects.Get, 0)
	s.Queue("ref", root)
	s.Wait()
	if s.Done("ref") {
		t.Error("a set of extended attributes was materialized")
	}
	wantEmpty(t, filepath.Join(dir, "done"))
	wantEmpty(t, filepath.Join(dir, "partial"))
}

func TestLogsWhatWasWritten(t *testing.T) {
	objects := newObjects(t)
	root := importFiles(t, objects, threeFiles)
	var logged bytes.Buffer
	s, err := Open(filepath.Join(t.TempDir(), "files"), objects.Get,
		Options{Log: slog.New(slog.NewTextHandler(&logged, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Queue("ref", root)
	s.Wait()
	out := logged.String()
	// first, second and third, each with its newline.
	for _, want := range []string{"level=INFO", "name=ref", "files=3", "bytes=19", "took="} {
		if !strings.Contains(out, want) {
			t.Errorf("the log lacks %q: %q", want, out)
		}
	}
}

func TestOpenRefuses(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "files"), nil, Options{}); err == nil {
		t.Error("Open took a nil getter")
	}
	if _, err := Open("", func(key.Key) ([]byte, error) { return nil, nil }, Options{}); err == nil {
		t.Error("Open took an empty directory name")
	}
	// A file where the directory should be.
	path := filepath.Join(t.TempDir(), "files")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, func(key.Key) ([]byte, error) { return nil, nil }, Options{}); err == nil {
		t.Error("Open took a file for its directory")
	}
}

func TestManyGoroutines(t *testing.T) {
	objects := newObjects(t)
	var roots []key.Key
	for i := range 8 {
		roots = append(roots, importFiles(t, objects, map[string]string{
			"f":   strings.Repeat("x", i+1),
			"d/g": strings.Repeat("y", i+1),
		}))
	}
	dir := filepath.Join(t.TempDir(), "files")
	s := open(t, dir, objects.Get, 3)
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i, root := range roots {
				name := string(rune('a' + i))
				s.Queue(name, root)
				s.Done(name)
				s.Path(name, []string{"f"})
				if g%4 == 0 {
					s.Wait()
				}
			}
		})
	}
	wg.Wait()
	s.Wait()
	for i := range roots {
		name := string(rune('a' + i))
		p, ok := s.Path(name, []string{"d", "g"})
		if !ok {
			t.Errorf("%q is not done", name)
			continue
		}
		if got, err := os.ReadFile(p); err != nil || len(got) != i+1 {
			t.Errorf("%s: %d bytes, %v", p, len(got), err)
		}
	}
	wantEmpty(t, filepath.Join(dir, "partial"))
}

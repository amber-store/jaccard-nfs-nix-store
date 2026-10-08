package tree

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/amber-store/core/commit"
	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"golang.org/x/sys/unix"
)

// newStore opens a packstore that lives as long as the test.
func newStore(t *testing.T) *packstore.Store {
	t.Helper()
	st, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"), packstore.WithSync(false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// importPath builds the tree of path, a directory or a single file, writes
// its objects to st and returns the root, as jaccard-store's importDir does.
func importPath(t *testing.T, st *packstore.Store, path string) key.Key {
	t.Helper()
	built, root, err := ingest.Objects(path, ingest.Opts{NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.WriteParallel(func(yield func(packstore.Object, error) bool) {
		for o, err := range built {
			if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, err) || err != nil {
				return
			}
		}
	}, packstore.WriteOpts{})
	if err != nil {
		t.Fatal(err)
	}
	return *root
}

// counted wraps a getter and counts the calls made through it.
func counted(get Getter) (Getter, *atomic.Int64) {
	var n atomic.Int64
	return func(k key.Key) ([]byte, error) {
		n.Add(1)
		return get(k)
	}, &n
}

// randomBytes returns n bytes that are the same in every run.
func randomBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

func write(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	// The mode asked for, whatever the umask is.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

const bigSize = 9 << 20

// source is a directory written for a test, and what was put into it.
type source struct {
	dir     string
	big     []byte // the content of "big"
	fifo    bool   // "fifo" is there
	notUTF8 bool   // the name notUTF8Name is there
}

const (
	untidyName  = "a name\nof two lines"
	notUTF8Name = "caf\xe9\xff"
)

// writeSource writes the directory the tests read:
//
//	big          9 MiB of random bytes
//	empty        a file of no bytes
//	emptydir/    a directory of nothing
//	fifo         a FIFO
//	link         a link to sub/inner
//	script       an executable file
//	small        a file of one blob
//	sub/inner    a file in a directory
//	and a file whose name has a space and a newline in it, and one whose
//	name is not UTF-8 where the file system takes it.
func writeSource(t *testing.T) source {
	t.Helper()
	src := source{dir: filepath.Join(t.TempDir(), "src"), big: randomBytes(bigSize, 1)}
	for _, d := range []string{"sub", "emptydir"} {
		if err := os.MkdirAll(filepath.Join(src.dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write(t, filepath.Join(src.dir, "big"), src.big, 0o644)
	write(t, filepath.Join(src.dir, "empty"), nil, 0o644)
	write(t, filepath.Join(src.dir, "script"), []byte("#!/bin/sh\necho hello\n"), 0o755)
	write(t, filepath.Join(src.dir, "small"), []byte("a small file\n"), 0o644)
	write(t, filepath.Join(src.dir, "sub", "inner"), []byte("inside\n"), 0o600)
	write(t, filepath.Join(src.dir, untidyName), []byte("untidy\n"), 0o644)
	if err := os.Symlink("sub/inner", filepath.Join(src.dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(src.dir, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	src.fifo = true
	// APFS, for one, refuses a name that is not UTF-8.
	if err := os.WriteFile(filepath.Join(src.dir, notUTF8Name), []byte("bytes\n"), 0o644); err == nil {
		src.notUTF8 = true
	}
	// A time that is not now, and has nanoseconds where the file system
	// keeps them.
	when := time.Unix(1_600_000_000, 123_456_789)
	if err := os.Chtimes(filepath.Join(src.dir, "script"), when, when); err != nil {
		t.Fatal(err)
	}
	return src
}

// world is a source imported into a store, and a reader over it.
type world struct {
	source
	store *packstore.Store
	root  key.Key
	r     *Reader
	calls *atomic.Int64
}

func newWorld(t *testing.T, opts Options) *world {
	t.Helper()
	src := writeSource(t)
	st := newStore(t)
	root := importPath(t, st, src.dir)
	get, calls := counted(st.Get)
	return &world{source: src, store: st, root: root, r: New(get, opts), calls: calls}
}

// served reports whether an entry of a source directory is one List gives.
func served(e os.DirEntry) bool {
	return e.IsDir() || e.Type().IsRegular() || e.Type()&os.ModeSymlink != 0
}

// checkNode holds a node against the entry of the source it was made of.
func checkNode(t *testing.T, path string, n Node) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	if n.UID != st.Uid || n.GID != st.Gid {
		t.Errorf("%q: owner %d:%d, want %d:%d", path, n.UID, n.GID, st.Uid, st.Gid)
	}
	if !n.Mtime.Equal(info.ModTime()) {
		t.Errorf("%q: mtime %v, want %v", path, n.Mtime, info.ModTime())
	}
	if want := info.Mode().Perm()&0o111 != 0; n.Exec != want {
		t.Errorf("%q: exec %v, want %v", path, n.Exec, want)
	}
	switch {
	case info.IsDir():
		if n.Kind != Dir || n.Size != 0 {
			t.Errorf("%q: kind %v size %d, want a directory of size 0", path, n.Kind, n.Size)
		}
		if ty := n.Key.Type(); ty != key.DirLeaf && ty != key.DirNode {
			t.Errorf("%q: key of type %v", path, ty)
		}
	case info.Mode().IsRegular():
		if n.Kind != File || n.Size != uint64(info.Size()) {
			t.Errorf("%q: kind %v size %d, want a file of size %d", path, n.Kind, n.Size, info.Size())
		}
		if n.Key.Length() != n.Size {
			t.Errorf("%q: key of length %d, size %d", path, n.Key.Length(), n.Size)
		}
	case info.Mode()&os.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			t.Fatal(err)
		}
		if n.Kind != Symlink || n.Target != target || n.Size != uint64(len(target)) {
			t.Errorf("%q: kind %v target %q size %d, want a link to %q", path, n.Kind, n.Target, n.Size, target)
		}
	default:
		t.Errorf("%q: a node for an entry that is not served", path)
	}
}

func TestRootDirectory(t *testing.T) {
	w := newWorld(t, Options{})
	n, err := w.r.Root(w.root)
	if err != nil {
		t.Fatal(err)
	}
	want := Node{Kind: Dir, Key: w.root, Mtime: time.Unix(1, 0)}
	if n != want {
		t.Errorf("root %+v, want %+v", n, want)
	}
}

func TestRootFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "single")
	data := []byte("one file and nothing else\n")
	write(t, path, data, 0o644)
	st := newStore(t)
	root := importPath(t, st, path)
	r := New(st.Get, Options{})

	n, err := r.Root(root)
	if err != nil {
		t.Fatal(err)
	}
	want := Node{Kind: File, Key: root, Size: uint64(len(data)), Exec: true, Mtime: time.Unix(1, 0)}
	if n != want {
		t.Errorf("root %+v, want %+v", n, want)
	}
	got := make([]byte, len(data))
	if _, err := r.ReadAt(n.Key, got, 0); err != nil || !bytes.Equal(got, data) {
		t.Errorf("read %q, %v", got, err)
	}
}

func TestRootCommit(t *testing.T) {
	w := newWorld(t, Options{})
	who := commit.Identity{Name: "A", Email: "a@example.org", When: 1}
	ck, data, err := commit.Commit{Tree: w.root, Author: who, Committer: who, Message: "a commit\n"}.Object()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.store.Put(ck, data); err != nil {
		t.Fatal(err)
	}
	n, err := w.r.Root(ck)
	if err != nil {
		t.Fatal(err)
	}
	if n.Kind != Dir || n.Key != w.root {
		t.Errorf("root of a commit %+v, want the directory %s", n, w.root)
	}
	// A commit is taken wherever a directory is.
	entries, err := w.r.List(ck)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := w.r.List(w.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(direct) {
		t.Errorf("%d entries through the commit, %d in its tree", len(entries), len(direct))
	}
}

func TestRootOfOtherType(t *testing.T) {
	r := New(func(key.Key) ([]byte, error) { return nil, errors.New("not asked") }, Options{})
	k, err := key.New(key.XattrSet, 3, []byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Root(k); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("root of an xattr set: %v", err)
	}
}

func TestLookup(t *testing.T) {
	w := newWorld(t, Options{})
	for _, name := range []string{"small", "script", "empty", "big", "sub", "emptydir", "link", untidyName} {
		n, err := w.r.Lookup(w.root, name)
		if err != nil {
			t.Errorf("%q: %v", name, err)
			continue
		}
		checkNode(t, filepath.Join(w.dir, name), n)
	}

	script, err := w.r.Lookup(w.root, "script")
	if err != nil {
		t.Fatal(err)
	}
	if !script.Exec {
		t.Error("script is not executable")
	}
	if want := time.Unix(1_600_000_000, 123_456_789); !script.Mtime.Equal(want) {
		// Held against the source above; this is the time it was given.
		t.Logf("script mtime %v, the file system kept less than %v", script.Mtime, want)
	}
	small, err := w.r.Lookup(w.root, "small")
	if err != nil {
		t.Fatal(err)
	}
	if small.Exec {
		t.Error("small is executable")
	}

	sub, err := w.r.Lookup(w.root, "sub")
	if err != nil {
		t.Fatal(err)
	}
	inner, err := w.r.Lookup(sub.Key, "inner")
	if err != nil {
		t.Fatal(err)
	}
	checkNode(t, filepath.Join(w.dir, "sub", "inner"), inner)

	for _, name := range []string{"missing", "", "smal", "small ", "zzz", "\x00"} {
		if _, err := w.r.Lookup(w.root, name); !errors.Is(err, ErrNotFound) {
			t.Errorf("%q: %v, want ErrNotFound", name, err)
		}
	}
}

func TestList(t *testing.T) {
	w := newWorld(t, Options{})
	for _, rel := range []string{".", "sub", "emptydir"} {
		dir := w.root
		if rel != "." {
			n, err := w.r.Lookup(w.root, rel)
			if err != nil {
				t.Fatal(err)
			}
			dir = n.Key
		}
		entries, err := w.r.List(dir)
		if err != nil {
			t.Fatal(err)
		}
		source, err := os.ReadDir(filepath.Join(w.dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for _, e := range source {
			if served(e) {
				want = append(want, e.Name())
			}
		}
		if len(entries) != len(want) {
			t.Fatalf("%s: %d entries, want %d", rel, len(entries), len(want))
		}
		for i, e := range entries {
			if e.Name != want[i] {
				t.Errorf("%s: entry %d is %q, want %q", rel, i, e.Name, want[i])
			}
			checkNode(t, filepath.Join(w.dir, rel, e.Name), e.Node)
		}
	}

	empty, err := w.r.Lookup(w.root, "emptydir")
	if err != nil {
		t.Fatal(err)
	}
	if entries, err := w.r.List(empty.Key); err != nil || len(entries) != 0 {
		t.Errorf("empty directory: %d entries, %v", len(entries), err)
	}
}

func TestListOfAFile(t *testing.T) {
	w := newWorld(t, Options{})
	small, err := w.r.Lookup(w.root, "small")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.r.List(small.Key); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("list of a file: %v", err)
	}
}

func TestUntidyNames(t *testing.T) {
	w := newWorld(t, Options{})
	names := []string{untidyName}
	if w.notUTF8 {
		names = append(names, notUTF8Name)
	} else {
		t.Log("the file system does not take a name that is not UTF-8: that case is skipped")
	}
	entries, err := w.r.List(w.root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		listed := false
		for _, e := range entries {
			listed = listed || e.Name == name
		}
		if !listed {
			t.Errorf("%q is not listed", name)
		}
		n, err := w.r.Lookup(w.root, name)
		if err != nil {
			t.Errorf("%q: %v", name, err)
			continue
		}
		want, err := os.ReadFile(filepath.Join(w.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, n.Size)
		if _, err := w.r.ReadAt(n.Key, got, 0); err != nil || !bytes.Equal(got, want) {
			t.Errorf("%q: read %q, %v", name, got, err)
		}
	}
}

func TestManyFiles(t *testing.T) {
	const count = 3000
	dir := filepath.Join(t.TempDir(), "many")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		// Names of several lengths, so that their order is not that of
		// their numbers.
		write(t, filepath.Join(dir, fmt.Sprintf("file-%d", i*7)), []byte(fmt.Sprint(i)), 0o644)
	}
	st := newStore(t)
	root := importPath(t, st, dir)
	r := New(st.Get, Options{})

	entries, err := r.List(root)
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != count || len(source) != count {
		t.Fatalf("%d entries, %d in the source, want %d", len(entries), len(source), count)
	}
	for i, e := range entries {
		if e.Name != source[i].Name() {
			t.Fatalf("entry %d is %q, want %q", i, e.Name, source[i].Name())
		}
		n, err := r.Lookup(root, e.Name)
		if err != nil {
			t.Fatalf("%q: %v", e.Name, err)
		}
		if n != e.Node {
			t.Fatalf("%q: looked up %+v, listed %+v", e.Name, n, e.Node)
		}
	}
	if _, err := r.Lookup(root, "file-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a missing name among many: %v", err)
	}
}

// firstLeaf returns the number of bytes the first blob of a file holds.
func firstLeaf(t *testing.T, get Getter, content key.Key) int64 {
	t.Helper()
	for content.Type() == key.FileNode {
		data, err := get(content)
		if err != nil {
			t.Fatal(err)
		}
		children, err := fstree.DecodeFileNode(data)
		if err != nil {
			t.Fatal(err)
		}
		content = children[0]
	}
	return int64(content.Length())
}

// checkRead reads n bytes at off and holds the answer against data, the
// whole content of the file, by the rules of io.ReaderAt.
func checkRead(t *testing.T, r *Reader, content key.Key, data []byte, off int64, n int) {
	t.Helper()
	p := make([]byte, n)
	got, err := r.ReadAt(content, p, off)
	want := data[min(off, int64(len(data))):]
	if len(want) > n {
		want = want[:n]
	}
	if got != len(want) || !bytes.Equal(p[:got], want) {
		t.Errorf("read of %d at %d: %d bytes, want %d, equal: %v", n, off, got, len(want), bytes.Equal(p[:got], want))
	}
	switch {
	case len(want) < n && err != io.EOF:
		t.Errorf("read of %d at %d: %v, want io.EOF", n, off, err)
	case len(want) == n && err != nil:
		t.Errorf("read of %d at %d: %v", n, off, err)
	}
}

func TestReadAt(t *testing.T) {
	w := newWorld(t, Options{})
	big, err := w.r.Lookup(w.root, "big")
	if err != nil {
		t.Fatal(err)
	}
	if big.Key.Type() != key.FileNode {
		t.Fatalf("a file of 9 MiB is a %v", big.Key.Type())
	}
	edge := firstLeaf(t, w.store.Get, big.Key)
	if edge <= 100 || edge >= bigSize {
		t.Fatalf("first leaf of %d bytes", edge)
	}

	checkRead(t, w.r, big.Key, w.big, 0, 4096)
	checkRead(t, w.r, big.Key, w.big, 0, 1)
	checkRead(t, w.r, big.Key, w.big, bigSize/2+13, 65536)
	checkRead(t, w.r, big.Key, w.big, edge-100, 200)
	checkRead(t, w.r, big.Key, w.big, edge-1, 1)
	checkRead(t, w.r, big.Key, w.big, edge, 1)
	checkRead(t, w.r, big.Key, w.big, edge-1, 2)
	checkRead(t, w.r, big.Key, w.big, bigSize-100, 100) // to the end exactly
	checkRead(t, w.r, big.Key, w.big, bigSize-100, 300) // over the end
	checkRead(t, w.r, big.Key, w.big, bigSize, 10)      // at the end
	checkRead(t, w.r, big.Key, w.big, bigSize+5000, 10) // past the end
	checkRead(t, w.r, big.Key, w.big, 0, bigSize)       // all of it
	checkRead(t, w.r, big.Key, w.big, 0, bigSize+1)
	// The whole file in pieces that fall on no boundary.
	for off := int64(0); off < bigSize; off += 1_000_003 {
		checkRead(t, w.r, big.Key, w.big, off, 1_000_003)
	}

	if _, err := w.r.ReadAt(big.Key, make([]byte, 10), -1); err == nil || err == io.EOF {
		t.Errorf("read at a negative offset: %v", err)
	}
	if n, err := w.r.ReadAt(big.Key, nil, 0); n != 0 || err != nil {
		t.Errorf("read of nothing: %d, %v", n, err)
	}

	for _, name := range []string{"empty", "small"} {
		n, err := w.r.Lookup(w.root, name)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(w.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		size := int64(len(data))
		checkRead(t, w.r, n.Key, data, 0, len(data))
		checkRead(t, w.r, n.Key, data, 0, len(data)+10)
		checkRead(t, w.r, n.Key, data, size/2, 4)
		checkRead(t, w.r, n.Key, data, size, 10)
		checkRead(t, w.r, n.Key, data, size+7, 10)
	}
	small, err := w.r.Lookup(w.root, "small")
	if err != nil {
		t.Fatal(err)
	}
	if small.Key.Type() != key.Blob {
		t.Errorf("a small file is a %v", small.Key.Type())
	}
}

func TestReadAtOfADirectory(t *testing.T) {
	w := newWorld(t, Options{})
	if _, err := w.r.ReadAt(w.root, make([]byte, 10), 0); err == nil || err == io.EOF {
		t.Errorf("read of a directory: %v", err)
	}
}

func TestOtherTypes(t *testing.T) {
	w := newWorld(t, Options{})
	if !w.fifo {
		t.Fatal("the source has no FIFO")
	}
	// The FIFO is in the directory as core reads it.
	if _, err := fstree.LookupEntry(w.root, []byte("fifo"), w.store.Get); err != nil {
		t.Fatalf("the FIFO was not imported: %v", err)
	}
	entries, err := w.r.List(w.root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name == "fifo" {
			t.Error("the FIFO is listed")
		}
	}
	if _, err := w.r.Lookup(w.root, "fifo"); !errors.Is(err, ErrNotFound) {
		t.Errorf("the FIFO by name: %v, want ErrNotFound", err)
	}
}

// TestHandBuilt reads a directory whose entries no file system would give:
// a device, a socket, and owners beyond 32 bits.
func TestHandBuilt(t *testing.T) {
	st := newStore(t)
	blob, err := fstree.EncodeBlob([]byte("content"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(blob.Key, blob.Bytes); err != nil {
		t.Fatal(err)
	}
	leaf, err := fstree.EncodeDirLeaf([]fstree.Entry{
		{Name: []byte("device"), Mode: unix.S_IFCHR | 0o644, Rdev: []uint64{1, 3}},
		{Name: []byte("file"), Mode: unix.S_IFREG | 0o010, UID: 1 << 40, GID: 1<<32 - 1, Mtime: -5, ContentKey: blob.Key[:]},
		{Name: []byte("socket"), Mode: unix.S_IFSOCK | 0o755},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(leaf.Key, leaf.Bytes); err != nil {
		t.Fatal(err)
	}
	r := New(st.Get, Options{})

	entries, err := r.List(leaf.Key)
	if err != nil {
		t.Fatal(err)
	}
	want := Node{Kind: File, Key: blob.Key, Size: 7, Exec: true, UID: 1<<32 - 1, GID: 1<<32 - 1, Mtime: time.Unix(0, -5)}
	if len(entries) != 1 || entries[0].Name != "file" || entries[0].Node != want {
		t.Errorf("listed %+v, want the one file %+v", entries, want)
	}
	for _, name := range []string{"device", "socket"} {
		if _, err := r.Lookup(leaf.Key, name); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s by name: %v, want ErrNotFound", name, err)
		}
	}
}

func TestMissingObject(t *testing.T) {
	w := newWorld(t, Options{})
	// A reader over a store that has nothing.
	r := New(newStore(t).Get, Options{})
	big, err := w.r.Lookup(w.root, "big")
	if err != nil {
		t.Fatal(err)
	}
	small, err := w.r.Lookup(w.root, "small")
	if err != nil {
		t.Fatal(err)
	}

	check := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, packstore.ErrNotFound) {
			t.Errorf("%s: %v, want what the getter returned", what, err)
		}
		if errors.Is(err, ErrNotFound) {
			t.Errorf("%s: %v is ErrNotFound", what, err)
		}
	}
	_, err = r.List(w.root)
	check("list", err)
	_, err = r.Lookup(w.root, "small")
	check("lookup", err)
	_, err = r.ReadAt(big.Key, make([]byte, 10), 0)
	check("read of a file node", err)
	_, err = r.ReadAt(small.Key, make([]byte, 10), 0)
	check("read of a blob", err)
}

func TestConcurrent(t *testing.T) {
	// Caches small enough that the goroutines push each other's entries
	// out.
	w := newWorld(t, Options{Dirs: 2, Files: 1, BlobBytes: 3 << 20})
	names := []string{"big", "small", "empty", "script"}
	content := map[string][]byte{}
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(w.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		content[name] = data
	}
	listed, err := w.r.List(w.root)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 7))
			for i := range 200 {
				name := names[(g+i)%len(names)]
				n, err := w.r.Lookup(w.root, name)
				if err != nil {
					t.Errorf("%q: %v", name, err)
					return
				}
				data := content[name]
				off := rng.Int64N(int64(len(data)) + 1)
				checkRead(t, w.r, n.Key, data, off, 1+rng.IntN(200_000))

				entries, err := w.r.List(w.root)
				if err != nil || len(entries) != len(listed) {
					t.Errorf("list: %d entries, %v", len(entries), err)
					return
				}
				for _, dir := range []string{"sub", "emptydir"} {
					d, err := w.r.Lookup(w.root, dir)
					if err != nil {
						t.Errorf("%q: %v", dir, err)
						return
					}
					if _, err := w.r.List(d.Key); err != nil {
						t.Errorf("list of %q: %v", dir, err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestBlobIsReadOnce(t *testing.T) {
	w := newWorld(t, Options{})
	for _, name := range []string{"small", "big"} {
		n, err := w.r.Lookup(w.root, name)
		if err != nil {
			t.Fatal(err)
		}
		p := make([]byte, 10)
		if _, err := w.r.ReadAt(n.Key, p, 0); err != nil {
			t.Fatal(err)
		}
		before := w.calls.Load()
		if before == 0 {
			t.Fatal("nothing was asked of the getter")
		}
		for off := range int64(3) {
			if _, err := w.r.ReadAt(n.Key, p, off); err != nil {
				t.Fatal(err)
			}
		}
		if after := w.calls.Load(); after != before {
			t.Errorf("%s: %d calls of the getter for reads within a blob already read", name, after-before)
		}
		// The directory is not read again either.
		if _, err := w.r.Lookup(w.root, name); err != nil {
			t.Fatal(err)
		}
		if after := w.calls.Load(); after != before {
			t.Errorf("%s: %d calls of the getter for a directory already read", name, after-before)
		}
	}
}

func TestBlobOverBudget(t *testing.T) {
	// A budget no blob fits in: every read asks the getter, and is
	// answered all the same.
	w := newWorld(t, Options{BlobBytes: 1})
	small, err := w.r.Lookup(w.root, "small")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(w.dir, "small"))
	if err != nil {
		t.Fatal(err)
	}
	checkRead(t, w.r, small.Key, data, 0, len(data))
	before := w.calls.Load()
	checkRead(t, w.r, small.Key, data, 0, len(data))
	if after := w.calls.Load(); after != before+1 {
		t.Errorf("%d calls of the getter for a blob that is not kept, want 1", after-before)
	}
}

func TestBlobBudget(t *testing.T) {
	// Room for about two leaves of the big file: reading it through and
	// then its start again asks the getter again.
	w := newWorld(t, Options{BlobBytes: 2 << 20})
	big, err := w.r.Lookup(w.root, "big")
	if err != nil {
		t.Fatal(err)
	}
	checkRead(t, w.r, big.Key, w.big, 0, bigSize)
	if used := w.r.blobs.size(); used > 2<<20 {
		t.Errorf("%d bytes of blobs kept, the budget is %d", used, 2<<20)
	}
	before := w.calls.Load()
	checkRead(t, w.r, big.Key, w.big, 0, 10)
	if after := w.calls.Load(); after != before+1 {
		t.Errorf("%d calls of the getter for a blob pushed out, want 1", after-before)
	}
}

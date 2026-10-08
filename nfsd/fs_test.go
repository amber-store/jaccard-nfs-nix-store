//go:build linux

package nfsd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-nfs-nix-store/handles"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
	"github.com/amber-store/jaccard-nfs-nix-store/tree"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"golang.org/x/sys/unix"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// everything asks for every attribute.
const everything = ^virtual.AttributesMask(0)

var ctx = context.Background()

// fakeRefs is the fetching, with the references a test gave it: Ensure pins
// what it has, counts what it was asked, and fails where it was told to.
type fakeRefs struct {
	mu    sync.Mutex
	has   map[string]key.Key
	fail  map[string]error
	pins  []refs.Pin
	asked map[string]int
}

func (f *fakeRefs) Ensure(_ context.Context, name string) (key.Key, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked[name]++
	if err := f.fail[name]; err != nil {
		return key.Key{}, err
	}
	root, ok := f.has[name]
	if !ok {
		return key.Key{}, fmt.Errorf("%w: %q", refs.ErrNotFound, name)
	}
	if !slices.ContainsFunc(f.pins, func(p refs.Pin) bool { return p.Name == name }) {
		f.pins = append(f.pins, refs.Pin{Name: name, Root: root})
	}
	return root, nil
}

func (f *fakeRefs) Pinned(name string) (key.Key, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.pins {
		if p.Name == name {
			return p.Root, true
		}
	}
	return key.Key{}, false
}

func (f *fakeRefs) Pins() []refs.Pin {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.pins)
}

func (f *fakeRefs) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pins)
}

func (f *fakeRefs) askedFor(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.asked[name]
}

// fakeFiles says which references are materialized, and where.
type fakeFiles struct {
	mu   sync.Mutex
	done map[string]string // the name of a reference, and the tree of its files
}

func (f *fakeFiles) Path(name string, rel []string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dir, ok := f.done[name]
	return filepath.Join(append([]string{dir}, rel...)...), ok
}

func (f *fakeFiles) materialized(name, dir string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.done[name] = dir
}

// world is a file system over two references: "hello", a directory, and
// "single", one file. src is where they were imported from.
type world struct {
	fs      *FS
	refs    *fakeRefs
	files   *fakeFiles
	objects *packstore.Store
	src     string
	data    []byte // the content of hello/lib/data.bin
	table   string // the file of the handle table
}

const dataSize = 3<<20 + 12345

func randomBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

func writeFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func importPath(t *testing.T, st *packstore.Store, path string) key.Key {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.IsDir() {
		root, _, err := ingest.Dir(st, path, ingest.Opts{NoIgnore: true})
		if err != nil {
			t.Fatal(err)
		}
		return root
	}
	built, root, err := ingest.Objects(path, ingest.Opts{NoIgnore: true})
	if err != nil {
		t.Fatal(err)
	}
	for o, err := range built {
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	return *root
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{src: t.TempDir(), data: randomBytes(dataSize, 1)}
	hello := filepath.Join(w.src, "hello")
	writeFile(t, filepath.Join(hello, "bin", "run.sh"), []byte("#!/bin/sh\necho hi\n"), 0o755)
	writeFile(t, filepath.Join(hello, "lib", "data.bin"), w.data, 0o644)
	writeFile(t, filepath.Join(hello, "lib", "empty"), nil, 0o644)
	if err := os.Symlink("bin/run.sh", filepath.Join(hello, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("./bin//run.sh", filepath.Join(hello, "untidy")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(hello, "emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range 300 {
		writeFile(t, filepath.Join(hello, "many", fmt.Sprintf("f%04d", i)), fmt.Appendf(nil, "%d\n", i), 0o644)
	}
	if err := unix.Mkfifo(filepath.Join(hello, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(w.src, "single"), []byte("one file\n"), 0o644)

	var err error
	if w.objects, err = packstore.Open(filepath.Join(t.TempDir(), "packstore"), packstore.WithSync(false)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.objects.Close() })
	w.refs = &fakeRefs{
		has: map[string]key.Key{
			"hello":  importPath(t, w.objects, hello),
			"single": importPath(t, w.objects, filepath.Join(w.src, "single")),
		},
		fail:  map[string]error{"broken": errors.New("the server is away")},
		asked: map[string]int{},
	}
	w.files = &fakeFiles{done: map[string]string{}}
	w.table = filepath.Join(t.TempDir(), "handles")
	w.fs = w.open(t)
	return w
}

// open returns a file system over the world's references and its table,
// as a new run of the sidecar would have it.
func (w *world) open(t *testing.T) *FS {
	t.Helper()
	table, err := handles.Open(w.table)
	if err != nil {
		t.Fatal(err)
	}
	fs := New(Config{
		Refs:    w.refs,
		Tree:    tree.New(w.objects.Get, tree.Options{}),
		Handles: table,
		Files:   w.files,
		Log:     quiet,
	})
	t.Cleanup(func() {
		fs.Close()
		table.Close()
	})
	return fs
}

func component(name string) path.Component { return path.MustNewComponent(name) }

// lookup looks name up in dir and fails the test unless that works.
func lookup(t *testing.T, dir virtual.Directory, name string) (virtual.DirectoryChild, *virtual.Attributes) {
	t.Helper()
	var a virtual.Attributes
	child, s := dir.VirtualLookup(ctx, component(name), everything, &a)
	if s != virtual.StatusOK {
		t.Fatalf("lookup of %q: status %v", name, s)
	}
	return child, &a
}

// walk looks the names up one after another, from the root.
func (w *world) walk(t *testing.T, names ...string) (virtual.DirectoryChild, *virtual.Attributes) {
	t.Helper()
	child := virtual.DirectoryChild{}.FromDirectory(w.fs.Root())
	var a *virtual.Attributes
	for _, name := range names {
		dir, _ := child.GetPair()
		if dir == nil {
			t.Fatalf("%q is looked up in something that is no directory", name)
		}
		child, a = lookup(t, dir, name)
	}
	return child, a
}

func (w *world) dir(t *testing.T, names ...string) virtual.Directory {
	t.Helper()
	child, _ := w.walk(t, names...)
	dir, _ := child.GetPair()
	if dir == nil {
		t.Fatalf("%q is no directory", names)
	}
	return dir
}

func (w *world) leaf(t *testing.T, names ...string) virtual.Leaf {
	t.Helper()
	child, _ := w.walk(t, names...)
	_, leaf := child.GetPair()
	if leaf == nil {
		t.Fatalf("%q is no leaf", names)
	}
	return leaf
}

// reporter takes the entries of a listing, as many as fit.
type reporter struct {
	room    int
	names   []string
	cookies []uint64
	attrs   []virtual.Attributes
}

func (r *reporter) ReportEntry(nextCookie uint64, name path.Component, _ virtual.DirectoryChild, a *virtual.Attributes) bool {
	if len(r.names) == r.room {
		return false
	}
	r.names = append(r.names, name.String())
	r.cookies = append(r.cookies, nextCookie)
	r.attrs = append(r.attrs, *a)
	return true
}

// listing reads the whole of dir, room entries to an answer, as a client does
// that continues from the cookie of the last entry it got.
func listing(t *testing.T, dir virtual.Directory, room int) []string {
	t.Helper()
	var names []string
	cookie := uint64(0)
	for {
		r := &reporter{room: room}
		if s := dir.VirtualReadDir(ctx, cookie, everything, r); s != virtual.StatusOK {
			t.Fatalf("listing from cookie %d: status %v", cookie, s)
		}
		if len(r.names) == 0 {
			return names
		}
		names = append(names, r.names...)
		cookie = r.cookies[len(r.cookies)-1]
	}
}

// read reads size bytes of leaf from off, chunk bytes at a time.
func read(t *testing.T, leaf virtual.Leaf, off, size uint64, chunk int) []byte {
	t.Helper()
	var out []byte
	for uint64(len(out)) < size {
		buf := make([]byte, min(uint64(chunk), size-uint64(len(out))))
		n, eof, s := leaf.VirtualRead(ctx, buf, off+uint64(len(out)))
		if s != virtual.StatusOK {
			t.Fatalf("read at %d: status %v", off+uint64(len(out)), s)
		}
		out = append(out, buf[:n]...)
		if eof || n == 0 {
			break
		}
	}
	return out
}

func permissions(t *testing.T, a *virtual.Attributes) virtual.Permissions {
	t.Helper()
	p, ok := a.GetPermissions()
	if !ok {
		t.Fatal("no permissions")
	}
	return p
}

func size(t *testing.T, a *virtual.Attributes) uint64 {
	t.Helper()
	n, ok := a.GetSizeBytes()
	if !ok {
		t.Fatal("no size")
	}
	return n
}

func target(t *testing.T, a *virtual.Attributes) string {
	t.Helper()
	parser, ok := a.GetSymlinkTarget()
	if !ok {
		t.Fatal("no link target")
	}
	builder, walker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(parser, walker); err != nil {
		t.Fatal(err)
	}
	return builder.GetUNIXString()
}

const readExecute = virtual.PermissionsRead | virtual.PermissionsExecute

func TestALookupInTheRootFetches(t *testing.T) {
	w := newWorld(t)
	root := w.fs.Root()

	child, a := lookup(t, root, "hello")
	if dir, _ := child.GetPair(); dir == nil {
		t.Fatal("a reference that is a directory came back as a leaf")
	}
	if n := w.refs.askedFor("hello"); n != 1 {
		t.Fatalf("the reference was asked for %d times, want once", n)
	}
	// What the root key of a reference does not record is what a Nix store
	// gives its paths.
	if a.GetFileType() != filesystem.FileTypeDirectory {
		t.Errorf("type %v", a.GetFileType())
	}
	if p := permissions(t, a); p != readExecute {
		t.Errorf("permissions %v, want r-x", p)
	}
	if uid, _ := a.GetOwnerUserID(); uid != 0 {
		t.Errorf("owner %d, want 0", uid)
	}
	if gid, _ := a.GetOwnerGroupID(); gid != 0 {
		t.Errorf("group %d, want 0", gid)
	}
	if mtime, _ := a.GetLastDataModificationTime(); !mtime.Equal(time.Unix(1, 0)) {
		t.Errorf("modified %v, want one second after the epoch", mtime)
	}
	want := handles.Child(handles.Root, "hello")
	if !bytes.Equal(a.GetFileHandle(), want[:]) {
		t.Errorf("handle %x, want %x", a.GetFileHandle(), want)
	}
	if a.GetInodeNumber() != want.Inode() {
		t.Errorf("inode %d, want %d", a.GetInodeNumber(), want.Inode())
	}
	if a.GetLinkCount() != 1 {
		t.Errorf("links %d, want 1", a.GetLinkCount())
	}

	var out virtual.Attributes
	if _, s := root.VirtualLookup(ctx, component("nothing"), everything, &out); s != virtual.StatusErrNoEnt {
		t.Errorf("a name the server does not have: status %v, want no such entry", s)
	}
	if _, s := root.VirtualLookup(ctx, component("broken"), everything, &out); s != virtual.StatusErrIO {
		t.Errorf("a name that cannot be fetched: status %v, want I/O error", s)
	}
}

func TestOpeningANameInTheRootFetches(t *testing.T) {
	w := newWorld(t)
	root := w.fs.Root()
	existing := &virtual.OpenExistingOptions{}
	open := func(name string, create *virtual.Attributes, existing *virtual.OpenExistingOptions) (virtual.Leaf, virtual.Status, *virtual.Attributes) {
		var a virtual.Attributes
		leaf, _, _, s := root.VirtualOpenChild(ctx, component(name), virtual.ShareMaskRead, create, existing, everything, &a)
		return leaf, s, &a
	}

	// A file is opened by its name without having been looked up: the
	// open is what fetches it.
	leaf, s, a := open("single", nil, existing)
	if s != virtual.StatusOK || leaf == nil {
		t.Fatalf("open of a reference that is a file: status %v", s)
	}
	if n := w.refs.askedFor("single"); n != 1 {
		t.Fatalf("the reference was asked for %d times, want once", n)
	}
	if got := size(t, a); got != uint64(len("one file\n")) {
		t.Errorf("size %d", got)
	}
	if got := read(t, leaf, 0, 100, 100); string(got) != "one file\n" {
		t.Errorf("content %q", got)
	}

	if _, s, _ := open("hello", nil, existing); s != virtual.StatusErrIsDir {
		t.Errorf("open of a directory: status %v, want is a directory", s)
	}
	if _, s, _ := open("nothing", nil, existing); s != virtual.StatusErrNoEnt {
		t.Errorf("open of a missing name: status %v, want no such entry", s)
	}
	if _, s, _ := open("nothing", &virtual.Attributes{}, existing); s != virtual.StatusErrROFS {
		t.Errorf("creating a file: status %v, want read-only", s)
	}
	if _, s, _ := open("single", &virtual.Attributes{}, nil); s != virtual.StatusErrExist {
		t.Errorf("creating a file that is there: status %v, want exists", s)
	}
	if _, s, _ := open("broken", nil, existing); s != virtual.StatusErrIO {
		t.Errorf("open of a name that cannot be fetched: status %v, want I/O error", s)
	}
}

func TestTheRootListsWhatWasFetched(t *testing.T) {
	w := newWorld(t)
	root := w.fs.Root()
	changeID := func() uint64 {
		var a virtual.Attributes
		root.VirtualGetAttributes(ctx, everything, &a)
		return a.GetChangeID()
	}

	if names := listing(t, root, 10); len(names) != 0 {
		t.Fatalf("a root nothing was fetched into lists %q", names)
	}
	before := changeID()

	lookup(t, root, "single")
	lookup(t, root, "hello")
	// In the order they were fetched, which is the order a continued
	// listing can rely on.
	if names := listing(t, root, 1); !slices.Equal(names, []string{"single", "hello"}) {
		t.Fatalf("the root lists %q", names)
	}
	if after := changeID(); after == before {
		t.Fatal("the change ID of the root is what it was before two references were fetched")
	}
	// Listing fetched nothing.
	if n := w.refs.askedFor("hello") + w.refs.askedFor("single"); n != 2 {
		t.Fatalf("%d fetches, want the two lookups", n)
	}

	// What a listing reports of an entry is what a lookup does.
	r := &reporter{room: 10}
	root.VirtualReadDir(ctx, 0, everything, r)
	_, a := lookup(t, root, "hello")
	if !bytes.Equal(r.attrs[1].GetFileHandle(), a.GetFileHandle()) {
		t.Error("the listing and the lookup give one name two handles")
	}
	if r.attrs[1].GetFileType() != filesystem.FileTypeDirectory || r.attrs[0].GetFileType() != filesystem.FileTypeRegularFile {
		t.Errorf("types %v and %v", r.attrs[0].GetFileType(), r.attrs[1].GetFileType())
	}
}

func TestTheAttributesOfTheRoot(t *testing.T) {
	w := newWorld(t)
	var a virtual.Attributes
	w.fs.Root().VirtualGetAttributes(ctx, everything, &a)
	if a.GetFileType() != filesystem.FileTypeDirectory {
		t.Errorf("type %v", a.GetFileType())
	}
	if p := permissions(t, &a); p != readExecute {
		t.Errorf("permissions %v, want r-x", p)
	}
	if !bytes.Equal(a.GetFileHandle(), handles.Root[:]) {
		t.Errorf("handle %x", a.GetFileHandle())
	}
}

func TestInsideAReference(t *testing.T) {
	w := newWorld(t)

	_, script := w.walk(t, "hello", "bin", "run.sh")
	if script.GetFileType() != filesystem.FileTypeRegularFile {
		t.Errorf("run.sh: type %v", script.GetFileType())
	}
	if p := permissions(t, script); p != readExecute {
		t.Errorf("run.sh: permissions %v, want r-x", p)
	}
	_, data := w.walk(t, "hello", "lib", "data.bin")
	if p := permissions(t, data); p != virtual.PermissionsRead {
		t.Errorf("data.bin: permissions %v, want r--", p)
	}
	if got := size(t, data); got != dataSize {
		t.Errorf("data.bin: size %d, want %d", got, dataSize)
	}
	// Owner and time are the ones recorded.
	info, err := os.Lstat(filepath.Join(w.src, "hello", "lib", "data.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if uid, _ := data.GetOwnerUserID(); uid != uint32(os.Getuid()) {
		t.Errorf("data.bin: owner %d, want %d", uid, os.Getuid())
	}
	if mtime, _ := data.GetLastDataModificationTime(); !mtime.Equal(info.ModTime()) {
		t.Errorf("data.bin: modified %v, want %v", mtime, info.ModTime())
	}
	want := handles.Child(handles.Child(handles.Child(handles.Root, "hello"), "lib"), "data.bin")
	if !bytes.Equal(data.GetFileHandle(), want[:]) {
		t.Errorf("data.bin: handle %x, want %x", data.GetFileHandle(), want)
	}

	_, lib := w.walk(t, "hello", "lib")
	if p := permissions(t, lib); p != readExecute {
		t.Errorf("lib: permissions %v, want r-x", p)
	}

	_, link := w.walk(t, "hello", "link")
	if link.GetFileType() != filesystem.FileTypeSymlink {
		t.Errorf("link: type %v", link.GetFileType())
	}
	if got := target(t, link); got != "bin/run.sh" {
		t.Errorf("link: target %q", got)
	}
	// Buildbarn answers a readlink with the target in a form of its own,
	// and says the size of that when it is told none. Told the length of
	// what was recorded, it would say a size the link does not have.
	_, untidy := w.walk(t, "hello", "untidy")
	if got := target(t, untidy); got != "bin/run.sh" {
		t.Errorf("a link to ./bin//run.sh: target %q as Buildbarn gives it", got)
	}
	for name, a := range map[string]*virtual.Attributes{"link": link, "untidy": untidy} {
		if n, ok := a.GetSizeBytes(); ok {
			t.Errorf("%s: the size is given as %d, and is to be left to the server", name, n)
		}
	}
	// The target is there whenever the attributes are, whatever was asked
	// for: it is what the size is worked out from.
	var sizeOnly virtual.Attributes
	w.leaf(t, "hello", "link").VirtualGetAttributes(ctx, virtual.AttributesMaskSizeBytes, &sizeOnly)
	if _, ok := sizeOnly.GetSymlinkTarget(); !ok {
		t.Error("link: asked for its size alone, it has no target to work the size out from")
	}

	var out virtual.Attributes
	hello := w.dir(t, "hello")
	if _, s := hello.VirtualLookup(ctx, component("nothing"), everything, &out); s != virtual.StatusErrNoEnt {
		t.Errorf("a missing name: status %v", s)
	}
	// A FIFO is in the tree and is not served.
	if _, s := hello.VirtualLookup(ctx, component("fifo"), everything, &out); s != virtual.StatusErrNoEnt {
		t.Errorf("a FIFO: status %v, want no such entry", s)
	}

	// Only the lookup of the reference itself asked the fetching.
	if n := w.refs.askedFor("hello"); n != 7 {
		t.Errorf("the reference was asked for %d times, want once for each of the 7 walks", n)
	}
}

func TestListingADirectoryInPieces(t *testing.T) {
	w := newWorld(t)

	if got, want := listing(t, w.dir(t, "hello"), 2), []string{"bin", "emptydir", "lib", "link", "many", "untidy"}; !slices.Equal(got, want) {
		t.Errorf("hello lists %q, want %q", got, want)
	}
	if got := listing(t, w.dir(t, "hello", "emptydir"), 5); len(got) != 0 {
		t.Errorf("an empty directory lists %q", got)
	}

	var want []string
	for i := range 300 {
		want = append(want, fmt.Sprintf("f%04d", i))
	}
	many := w.dir(t, "hello", "many")
	// Whatever fits in an answer: nothing is skipped and nothing repeated.
	for _, room := range []int{1, 7, 300, 1000} {
		if got := listing(t, many, room); !slices.Equal(got, want) {
			t.Errorf("%d entries to an answer: %d names, want %d; first %q", room, len(got), len(want), got[:min(3, len(got))])
		}
	}
	// A cookie past the end is the end.
	r := &reporter{room: 10}
	if s := many.VirtualReadDir(ctx, 5000, everything, r); s != virtual.StatusOK || len(r.names) != 0 {
		t.Errorf("a cookie past the end: status %v, %d entries", s, len(r.names))
	}
}

func TestAnEntryNoClientCouldNameIsLeftOut(t *testing.T) {
	w := newWorld(t)
	blob, err := fstree.EncodeBlob([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	entry := func(name string) fstree.Entry {
		return fstree.Entry{Name: []byte(name), Mode: unix.S_IFREG | 0o644, ContentKey: blob.Key[:]}
	}
	// Core's objects hold any bytes as a name. One with a slash in it is
	// no name of a file.
	odd, err := fstree.EncodeDirLeaf([]fstree.Entry{entry("a/b"), entry("fine")})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []fstree.Object{blob, odd} {
		if err := w.objects.Put(o.Key, o.Bytes); err != nil {
			t.Fatal(err)
		}
	}
	w.refs.mu.Lock()
	w.refs.has["odd"] = odd.Key
	w.refs.mu.Unlock()

	if got := listing(t, w.dir(t, "odd"), 10); !slices.Equal(got, []string{"fine"}) {
		t.Errorf("the directory lists %q, want only the name that is one", got)
	}
}

func TestReadingFromTheObjects(t *testing.T) {
	w := newWorld(t)
	data := w.leaf(t, "hello", "lib", "data.bin")

	if got := read(t, data, 0, dataSize, 256<<10); !bytes.Equal(got, w.data) {
		t.Fatalf("%d bytes that are not the file's", len(got))
	}
	if got := read(t, data, 1<<20+7, 100_000, 100_000); !bytes.Equal(got, w.data[1<<20+7:][:100_000]) {
		t.Error("a read in the middle is not the file's bytes")
	}

	// The end of the file is said, at it and before a read that reaches it.
	buf := make([]byte, 100)
	n, eof, s := data.VirtualRead(ctx, buf, dataSize-40)
	if s != virtual.StatusOK || n != 40 || !eof || !bytes.Equal(buf[:40], w.data[dataSize-40:]) {
		t.Errorf("a read across the end: %d bytes, eof %v, status %v", n, eof, s)
	}
	n, eof, s = data.VirtualRead(ctx, buf, dataSize)
	if s != virtual.StatusOK || n != 0 || !eof {
		t.Errorf("a read at the end: %d bytes, eof %v, status %v", n, eof, s)
	}
	n, eof, s = data.VirtualRead(ctx, buf, dataSize+1000)
	if s != virtual.StatusOK || n != 0 || !eof {
		t.Errorf("a read past the end: %d bytes, eof %v, status %v", n, eof, s)
	}
	n, eof, s = data.VirtualRead(ctx, buf[:10], 0)
	if s != virtual.StatusOK || n != 10 || eof {
		t.Errorf("a read at the start: %d bytes, eof %v, status %v", n, eof, s)
	}

	empty := w.leaf(t, "hello", "lib", "empty")
	n, eof, s = empty.VirtualRead(ctx, buf, 0)
	if s != virtual.StatusOK || n != 0 || !eof {
		t.Errorf("a read of an empty file: %d bytes, eof %v, status %v", n, eof, s)
	}
}

// copyTree copies the regular files under src to dst, as a reference is
// materialized.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		switch {
		case d.IsDir():
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		case d.Type().IsRegular():
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(dst, rel), data, 0o644)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadingFromTheFilesOnceMaterialized(t *testing.T) {
	w := newWorld(t)
	data := w.leaf(t, "hello", "lib", "data.bin")
	half := uint64(dataSize / 2)

	first := read(t, data, 0, half, 1<<20)

	// The reference is materialized while the file is being read. The
	// tree on disk holds other bytes than the objects, which a real one
	// never does: it is how the test sees where a read came from.
	onDisk := filepath.Join(t.TempDir(), "hello")
	copyTree(t, filepath.Join(w.src, "hello"), onDisk)
	altered := bytes.ToUpper(bytes.Repeat([]byte("materialized "), dataSize/13+1))[:dataSize]
	writeFile(t, filepath.Join(onDisk, "lib", "data.bin"), altered, 0o644)
	w.files.materialized("hello", onDisk)

	second := read(t, data, half, dataSize-half, 1<<20)
	if !bytes.Equal(first, w.data[:half]) {
		t.Error("the first half, read before, is not the objects' bytes")
	}
	if !bytes.Equal(second, altered[half:]) {
		t.Error("the second half, read after, is not the file's bytes")
	}

	// The end of the file is said from the file as from the objects.
	buf := make([]byte, 100)
	n, eof, s := data.VirtualRead(ctx, buf, dataSize-40)
	if s != virtual.StatusOK || n != 40 || !eof || !bytes.Equal(buf[:40], altered[dataSize-40:]) {
		t.Errorf("a read across the end: %d bytes, eof %v, status %v", n, eof, s)
	}

	// A file that is looked up after is read from the tree as well.
	script := w.leaf(t, "hello", "bin", "run.sh")
	if got := read(t, script, 0, 100, 100); string(got) != "#!/bin/sh\necho hi\n" {
		t.Errorf("run.sh: %q", got)
	}
}

func TestAFileThatCannotAnswerIsReadFromTheObjects(t *testing.T) {
	w := newWorld(t)
	data := w.leaf(t, "hello", "lib", "data.bin")

	// The tree is said to be whole, and the file is not in it.
	onDisk := filepath.Join(t.TempDir(), "hello")
	if err := os.MkdirAll(filepath.Join(onDisk, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}
	w.files.materialized("hello", onDisk)
	if got := read(t, data, 0, dataSize, 1<<20); !bytes.Equal(got, w.data) {
		t.Fatal("with the file gone, the read is not the objects' bytes")
	}

	// The file is there and shorter than the content.
	writeFile(t, filepath.Join(onDisk, "lib", "data.bin"), w.data[:1000], 0o644)
	if got := read(t, data, 0, dataSize, 1<<20); !bytes.Equal(got, w.data) {
		t.Fatal("with the file cut short, the read is not the objects' bytes")
	}

	// The file is whole again: it is read.
	whole := bytes.Repeat([]byte{'z'}, dataSize)
	writeFile(t, filepath.Join(onDisk, "lib", "data.bin"), whole, 0o644)
	if got := read(t, data, 0, dataSize, 1<<20); !bytes.Equal(got, whole) {
		t.Fatal("with the file back, the read is not the file's bytes")
	}
}

func TestAReferenceThatIsOneFile(t *testing.T) {
	w := newWorld(t)
	child, a := w.walk(t, "single")
	_, leaf := child.GetPair()
	if leaf == nil {
		t.Fatal("a reference that is a file came back as a directory")
	}
	if a.GetFileType() != filesystem.FileTypeRegularFile {
		t.Errorf("type %v", a.GetFileType())
	}
	// Nothing says whether it is executable, so it is.
	if p := permissions(t, a); p != readExecute {
		t.Errorf("permissions %v, want r-x", p)
	}
	if got := read(t, leaf, 0, 100, 100); string(got) != "one file\n" {
		t.Errorf("content %q", got)
	}

	// Materialized, it is one file and no tree.
	file := filepath.Join(t.TempDir(), "single")
	writeFile(t, file, []byte("ONE FILE\n"), 0o644)
	w.files.materialized("single", file)
	if got := read(t, leaf, 0, 100, 100); string(got) != "ONE FILE\n" {
		t.Errorf("content %q once materialized", got)
	}
}

func TestNothingCanBeChanged(t *testing.T) {
	w := newWorld(t)
	var out virtual.Attributes
	name := component("new")
	for what, dir := range map[string]virtual.Directory{"the root": w.fs.Root(), "a directory": w.dir(t, "hello", "lib")} {
		if _, _, s := dir.VirtualMkdir(ctx, name, &virtual.Attributes{}, everything, &out); s != virtual.StatusErrROFS {
			t.Errorf("mkdir in %s: status %v", what, s)
		}
		if _, _, s := dir.VirtualMknod(ctx, name, &virtual.Attributes{}, everything, &out); s != virtual.StatusErrROFS {
			t.Errorf("mknod in %s: status %v", what, s)
		}
		if _, s := dir.VirtualRemove(ctx, name, true, true); s != virtual.StatusErrROFS {
			t.Errorf("remove in %s: status %v", what, s)
		}
		if _, _, s := dir.VirtualRename(ctx, name, dir, name); s != virtual.StatusErrROFS {
			t.Errorf("rename in %s: status %v", what, s)
		}
		if _, s := dir.VirtualLink(ctx, name, w.leaf(t, "single"), everything, &out); s != virtual.StatusErrROFS {
			t.Errorf("link in %s: status %v", what, s)
		}
		if s := dir.VirtualSetAttributes(ctx, &virtual.Attributes{}, everything, &out); s != virtual.StatusErrROFS {
			t.Errorf("setattr of %s: status %v", what, s)
		}
		if _, _, _, s := dir.VirtualOpenChild(ctx, name, virtual.ShareMaskWrite, &virtual.Attributes{}, nil, everything, &out); s != virtual.StatusErrROFS {
			t.Errorf("create in %s: status %v", what, s)
		}
	}

	leaf := w.leaf(t, "hello", "lib", "data.bin")
	if _, s := leaf.VirtualWrite(ctx, []byte("x"), 0); s != virtual.StatusErrROFS {
		t.Errorf("write: status %v", s)
	}
	if s := leaf.VirtualAllocate(ctx, 0, 10); s != virtual.StatusErrROFS {
		t.Errorf("allocate: status %v", s)
	}
	if s := leaf.VirtualSetAttributes(ctx, &virtual.Attributes{}, everything, &out); s != virtual.StatusErrROFS {
		t.Errorf("setattr of a file: status %v", s)
	}
	if s := leaf.VirtualOpenSelf(ctx, virtual.ShareMaskWrite, &virtual.OpenExistingOptions{}, everything, &out); s != virtual.StatusErrROFS {
		t.Errorf("open for writing: status %v", s)
	}
	if s := leaf.VirtualOpenSelf(ctx, virtual.ShareMaskRead, &virtual.OpenExistingOptions{Truncate: true}, everything, &out); s != virtual.StatusErrROFS {
		t.Errorf("open with truncation: status %v", s)
	}
	if s := leaf.VirtualOpenSelf(ctx, virtual.ShareMaskRead, &virtual.OpenExistingOptions{}, everything, &out); s != virtual.StatusOK {
		t.Errorf("open for reading: status %v", s)
	}
	leaf.VirtualClose(virtual.ShareMaskRead)

	link := w.leaf(t, "hello", "link")
	if s := link.VirtualOpenSelf(ctx, virtual.ShareMaskRead, &virtual.OpenExistingOptions{}, everything, &out); s != virtual.StatusErrSymlink {
		t.Errorf("open of a link: status %v, want symbolic link", s)
	}
	if _, _, s := link.VirtualRead(ctx, make([]byte, 10), 0); s == virtual.StatusOK {
		t.Error("a link was read as a file")
	}
}

func resolve(fs *FS, handle []byte) (virtual.DirectoryChild, virtual.Status) {
	return fs.Resolve(bytes.NewReader(handle))
}

func TestAHandleIsResolved(t *testing.T) {
	w := newWorld(t)
	_, a := w.walk(t, "hello", "lib", "data.bin")
	handle := slices.Clone(a.GetFileHandle())

	child, s := resolve(w.fs, handle)
	if s != virtual.StatusOK {
		t.Fatalf("status %v", s)
	}
	_, leaf := child.GetPair()
	if leaf == nil {
		t.Fatal("the handle of a file is resolved to a directory")
	}
	var again virtual.Attributes
	leaf.VirtualGetAttributes(ctx, everything, &again)
	if !bytes.Equal(again.GetFileHandle(), handle) || size(t, &again) != dataSize {
		t.Error("what the handle is resolved to is not the file it was given for")
	}
	if got := read(t, leaf, 100, 1000, 1000); !bytes.Equal(got, w.data[100:1100]) {
		t.Error("what the handle is resolved to does not read as the file")
	}

	root, s := resolve(w.fs, handles.Root[:])
	if dir, _ := root.GetPair(); s != virtual.StatusOK || dir == nil {
		t.Errorf("the handle of the root: status %v", s)
	}

	for what, c := range map[string]struct {
		handle []byte
		want   virtual.Status
	}{
		"a handle nobody was given":      {bytes.Repeat([]byte{7}, 16), virtual.StatusErrStale},
		"a handle that is too short":     {handle[:15], virtual.StatusErrBadHandle},
		"a handle that is too long":      {append(slices.Clone(handle), 0), virtual.StatusErrBadHandle},
		"no handle at all":               {nil, virtual.StatusErrBadHandle},
		"a handle of the handle's shape": {handle, virtual.StatusOK},
	} {
		if _, s := resolve(w.fs, c.handle); s != c.want {
			t.Errorf("%s: status %v, want %v", what, s, c.want)
		}
	}
}

func TestHandlesOutliveTheProcess(t *testing.T) {
	w := newWorld(t)
	_, file := w.walk(t, "hello", "lib", "data.bin")
	_, dir := w.walk(t, "hello", "many")
	fileHandle, dirHandle := slices.Clone(file.GetFileHandle()), slices.Clone(dir.GetFileHandle())
	// Handles that a listing gave out, and no lookup.
	r := &reporter{room: 3}
	w.dir(t, "hello", "many").VirtualReadDir(ctx, 0, everything, r)
	listed := slices.Clone(r.attrs[2].GetFileHandle())
	asked := w.refs.askedFor("hello")

	// The sidecar is restarted: the table is read back from its file, and
	// the pins are what they were.
	w.fs.Close()
	w.fs.handles.Close()
	again := w.open(t)

	child, s := resolve(again, fileHandle)
	if s != virtual.StatusOK {
		t.Fatalf("the handle of a file after a restart: status %v", s)
	}
	_, leaf := child.GetPair()
	if got := read(t, leaf, 0, dataSize, 1<<20); !bytes.Equal(got, w.data) {
		t.Error("the file a handle is resolved to after a restart is not the file")
	}
	child, s = resolve(again, dirHandle)
	if d, _ := child.GetPair(); s != virtual.StatusOK || d == nil {
		t.Fatalf("the handle of a directory after a restart: status %v", s)
	}
	child, s = resolve(again, listed)
	if s != virtual.StatusOK {
		t.Fatalf("a handle from a listing after a restart: status %v", s)
	}
	_, leaf = child.GetPair()
	if got := read(t, leaf, 0, 10, 10); string(got) != "2\n" {
		t.Errorf("the file a listed handle is resolved to reads %q", got)
	}
	// None of that asked for the reference again: it is pinned.
	if n := w.refs.askedFor("hello"); n != asked {
		t.Errorf("resolving handles fetched: %d more", n-asked)
	}

	// A handle whose reference is not pinned leads nowhere.
	w.refs.mu.Lock()
	w.refs.pins = nil
	w.refs.mu.Unlock()
	if _, s := resolve(again, fileHandle); s != virtual.StatusErrStale {
		t.Errorf("a handle into a reference that is not pinned: status %v, want stale", s)
	}
}

func TestManyAtOnce(t *testing.T) {
	w := newWorld(t)
	onDisk := filepath.Join(t.TempDir(), "hello")
	copyTree(t, filepath.Join(w.src, "hello"), onDisk)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20 {
				child, _ := w.walk(t, "hello", "lib", "data.bin")
				_, leaf := child.GetPair()
				off := uint64((g*131 + i*977) % (dataSize - 5000))
				if got := read(t, leaf, off, 5000, 5000); !bytes.Equal(got, w.data[off:off+5000]) {
					t.Errorf("a read at %d is not the file's bytes", off)
					return
				}
				names := listing(t, w.dir(t, "hello", "many"), 50)
				if len(names) != 300 || !sort.StringsAreSorted(names) {
					t.Errorf("a listing of %d names", len(names))
					return
				}
				if g == 0 && i == 10 {
					// In the middle of it all the reference becomes whole.
					w.files.materialized("hello", onDisk)
				}
			}
		}()
	}
	wg.Wait()
}

// Package tree reads a filesystem tree out of the content-addressed objects
// of Amber-Store Core: it looks a name up in a directory, lists a directory
// and reads the content of a file by offset. It changes nothing. What it
// decodes it keeps, least recently used first out: directories, the
// positions of the blobs a file is made of, and the blobs last read.
package tree

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"golang.org/x/sys/unix"
)

// Getter fetches the bytes stored under a key. It is called from many
// goroutines at once, and what it returns is kept and must not be changed
// afterwards.
type Getter func(key.Key) ([]byte, error)

// Kind is what a node is.
type Kind int

const (
	Dir Kind = iota + 1
	File
	Symlink
)

func (k Kind) String() string {
	switch k {
	case Dir:
		return "directory"
	case File:
		return "file"
	case Symlink:
		return "symbolic link"
	}
	return fmt.Sprintf("kind %d", int(k))
}

// Node is a directory, a regular file or a symbolic link of a tree, with
// the attributes recorded for it.
type Node struct {
	Kind     Kind
	Key      key.Key // Dir: the directory's key. File: the content key.
	Target   string  // Symlink: where the link points, as recorded.
	Size     uint64  // File: bytes of content. Symlink: len(Target). Dir: 0.
	Exec     bool    // any execute bit of the recorded mode
	UID, GID uint32
	Mtime    time.Time
}

// Entry is a node under its name in a directory. The name is the bytes
// recorded, which need not be UTF-8.
type Entry struct {
	Name string
	Node
}

// Options are the sizes of a Reader's caches. A zero value means the
// default.
type Options struct {
	Dirs      int   // decoded directories kept, by default 4096
	Files     int   // files whose blob positions are kept, by default 1024
	BlobBytes int64 // bytes of blobs kept, by default 64 MiB
}

const (
	defaultDirs      = 4096
	defaultFiles     = 1024
	defaultBlobBytes = 64 << 20
)

// ErrNotFound means a directory has no entry of the name asked for, or one
// of a type that is not served. An object the getter does not have is not
// this error.
var ErrNotFound = errors.New("tree: no such entry")

// rootMtime is the modification time of a root: a reference's root key
// records none, and this is the one a Nix store gives its paths.
var rootMtime = time.Unix(1, 0)

// Reader reads trees through a Getter. It is safe for concurrent use.
//
// Two goroutines that miss the same object at the same moment both fetch
// it; the caches see to it that this is rare, not that it never happens.
type Reader struct {
	get    Getter
	dirs   *lru[key.Key, []Entry]
	leaves *lru[key.Key, []leaf]
	blobs  *lru[key.Key, []byte]
}

// New returns a Reader over the objects get fetches.
func New(get Getter, opts Options) *Reader {
	if opts.Dirs <= 0 {
		opts.Dirs = defaultDirs
	}
	if opts.Files <= 0 {
		opts.Files = defaultFiles
	}
	if opts.BlobBytes <= 0 {
		opts.BlobBytes = defaultBlobBytes
	}
	return &Reader{
		get:    get,
		dirs:   newLRU[key.Key, []Entry](int64(opts.Dirs)),
		leaves: newLRU[key.Key, []leaf](int64(opts.Files)),
		blobs:  newLRU[key.Key, []byte](opts.BlobBytes),
	}
}

// Root returns the node the root key of a reference stands for: a
// directory for a directory object or a commit, whose tree it is then, and
// a file for a blob or a file node. A root key records no owner, time or
// mode, so a root has those of a path in a Nix store, and a file is
// executable because nothing says whether it is.
func (r *Reader) Root(root key.Key) (Node, error) {
	switch root.Type() {
	case key.DirLeaf, key.DirNode, key.Commit:
		dir, err := fstree.DirOf(root, r.get)
		if err != nil {
			return Node{}, fmt.Errorf("root %s: %w", root, err)
		}
		return Node{Kind: Dir, Key: dir, Mtime: rootMtime}, nil
	case key.Blob, key.FileNode:
		return Node{Kind: File, Key: root, Size: root.Length(), Exec: true, Mtime: rootMtime}, nil
	default:
		return Node{}, fmt.Errorf("root %s: a %v is neither a directory nor a file", root, root.Type())
	}
}

// Lookup returns the entry of the directory dir that is called name. It is
// ErrNotFound when there is none, and when the entry is of a type that is
// not served.
func (r *Reader) Lookup(dir key.Key, name string) (Node, error) {
	entries, err := r.List(dir)
	if err != nil {
		return Node{}, err
	}
	// Core stores the entries sorted bytewise by name, which is how Go
	// orders strings.
	i := sort.Search(len(entries), func(i int) bool { return entries[i].Name >= name })
	if i == len(entries) || entries[i].Name != name {
		return Node{}, fmt.Errorf("%q in directory %s: %w", name, dir, ErrNotFound)
	}
	return entries[i].Node, nil
}

// List returns the directories, regular files and symbolic links of the
// directory dir, in the order they are stored, which is bytewise by name.
// Entries of any other type are left out. dir may be a commit, which
// stands for its tree.
//
// The slice is the one the cache holds: the caller must not change it.
func (r *Reader) List(dir key.Key) ([]Entry, error) {
	if entries, ok := r.dirs.get(dir); ok {
		return entries, nil
	}
	stored, err := fstree.CollectEntries(dir, r.get)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", dir, err)
	}
	entries := make([]Entry, 0, len(stored))
	for i := range stored {
		node, ok, err := nodeOf(&stored[i])
		if err != nil {
			return nil, fmt.Errorf("reading directory %s: entry %q: %w", dir, stored[i].Name, err)
		}
		if ok {
			entries = append(entries, Entry{Name: string(stored[i].Name), Node: node})
		}
	}
	r.dirs.add(dir, entries, 1)
	return entries, nil
}

// nodeOf returns the node a stored entry is served as. ok is false for an
// entry of a type that is not served.
func nodeOf(e *fstree.Entry) (node Node, ok bool, err error) {
	node = Node{
		Exec:  e.Mode&0o111 != 0,
		UID:   id32(e.UID),
		GID:   id32(e.GID),
		Mtime: time.Unix(0, e.Mtime),
	}
	switch e.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		node.Kind = Dir
		// The key may be a commit's. It stays what it is: core's readers
		// take a commit wherever they take a directory.
		if node.Key, err = key.Parse(e.ContentKey); err != nil {
			return Node{}, false, fmt.Errorf("content key: %w", err)
		}
	case unix.S_IFREG:
		node.Kind = File
		if node.Key, err = key.Parse(e.ContentKey); err != nil {
			return Node{}, false, fmt.Errorf("content key: %w", err)
		}
		// The length in a content key is the content's.
		node.Size = node.Key.Length()
	case unix.S_IFLNK:
		node.Kind = Symlink
		node.Target = string(e.LinkTarget)
		node.Size = uint64(len(e.LinkTarget))
	default:
		return Node{}, false, nil
	}
	return node, true, nil
}

// id32 returns a recorded owner or group as NFS can name it: core keeps 64
// bits, and what does not fit in 32 becomes the largest value that does.
func id32(id uint64) uint32 {
	if id > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(id)
}

// leaf is one blob of a file: its key, and the offset in the file of its
// first byte. The length in its key is the number of bytes it holds.
type leaf struct {
	key key.Key
	off int64
}

func (l leaf) end() int64 { return l.off + int64(l.key.Length()) }

// ReadAt reads the content of the file under the content key into p,
// starting at the offset off, as an io.ReaderAt does: it returns io.EOF
// when the file ends before p is full.
func (r *Reader) ReadAt(content key.Key, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("reading file %s: negative offset %d", content, off)
	}
	leaves, err := r.leavesOf(content)
	if err != nil {
		return 0, fmt.Errorf("reading file %s: %w", content, err)
	}
	// The first leaf that ends after off. A leaf of no bytes ends where it
	// begins and is never the one.
	i := sort.Search(len(leaves), func(i int) bool { return leaves[i].end() > off })
	n := 0
	for ; n < len(p) && i < len(leaves); i++ {
		data, err := r.blob(leaves[i].key)
		if err != nil {
			return n, fmt.Errorf("reading file %s: %w", content, err)
		}
		n += copy(p[n:], data[off+int64(n)-leaves[i].off:])
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// leavesOf returns the blobs the file under the content key is made of, in
// file order, each with its offset. The list of a file that has an index
// is worked out once and kept.
func (r *Reader) leavesOf(content key.Key) ([]leaf, error) {
	switch content.Type() {
	case key.Blob:
		if content.Length() == 0 {
			return nil, nil
		}
		return []leaf{{key: content}}, nil
	case key.FileNode:
	default:
		return nil, fmt.Errorf("a %v is not file content", content.Type())
	}
	if leaves, ok := r.leaves.get(content); ok {
		return leaves, nil
	}
	leaves, size, err := r.flatten(nil, 0, content)
	if err != nil {
		return nil, err
	}
	if uint64(size) != content.Length() {
		return nil, fmt.Errorf("the blobs hold %d bytes, the key says %d", size, content.Length())
	}
	r.leaves.add(content, leaves, 1)
	return leaves, nil
}

// flatten appends the blobs under the file node to leaves, the first of
// them at the offset off, and returns the list and the offset after the
// last. Blobs of no bytes are left out: no read needs them.
func (r *Reader) flatten(leaves []leaf, off int64, node key.Key) ([]leaf, int64, error) {
	data, err := r.get(node)
	if err != nil {
		return nil, 0, fmt.Errorf("file node %s: %w", node, err)
	}
	children, err := fstree.DecodeFileNode(data)
	if err != nil {
		return nil, 0, fmt.Errorf("file node %s: %w", node, err)
	}
	for _, child := range children {
		length := child.Length()
		if length > uint64(math.MaxInt64-off) {
			return nil, 0, fmt.Errorf("file node %s: the file is longer than an offset can say", node)
		}
		switch child.Type() {
		case key.Blob:
			if length > 0 {
				leaves = append(leaves, leaf{key: child, off: off})
			}
			off += int64(length)
		case key.FileNode:
			end := off + int64(length)
			if leaves, off, err = r.flatten(leaves, off, child); err != nil {
				return nil, 0, err
			}
			// A node that held more or less than its key says would
			// shift everything after it.
			if off != end {
				return nil, 0, fmt.Errorf("file node %s: the blobs end at %d, the key says %d", child, off, end)
			}
		default:
			return nil, 0, fmt.Errorf("file node %s: a child that is a %v", node, child.Type())
		}
	}
	return leaves, off, nil
}

// blob returns the bytes of a blob, from the cache when they are there.
func (r *Reader) blob(k key.Key) ([]byte, error) {
	if data, ok := r.blobs.get(k); ok {
		return data, nil
	}
	data, err := r.get(k)
	if err != nil {
		return nil, fmt.Errorf("blob %s: %w", k, err)
	}
	// The offsets were worked out from the lengths in the keys.
	if uint64(len(data)) != k.Length() {
		return nil, fmt.Errorf("blob %s: %d bytes, the key says %d", k, len(data), k.Length())
	}
	r.blobs.add(k, data, int64(len(data)))
	return data, nil
}

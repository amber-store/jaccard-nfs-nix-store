//go:build linux

package nfsd

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-nfs-nix-store/handles"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
	"github.com/amber-store/jaccard-nfs-nix-store/tree"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
)

// Refs is the fetching, as the file system uses it. *refs.Store is one.
type Refs interface {
	// Ensure returns the root of the reference name, fetching it unless
	// it is pinned. A name there is no reference for is refs.ErrNotFound.
	Ensure(ctx context.Context, name string) (key.Key, error)
	// Pinned returns the root of name if it was fetched, and asks nobody.
	Pinned(name string) (key.Key, bool)
	// Pins returns what was fetched, in the order it was.
	Pins() []refs.Pin
	// Count returns the number of pins.
	Count() int
}

// Files says where the plain file of a file is, once its reference is
// materialized. *materialize.Store does.
type Files interface {
	// Path returns the plain file of the file at the names rel below the
	// root of the reference name, and whether the reference is
	// materialized: only then is anything there.
	Path(name string, rel []string) (string, bool)
}

// defaultOpenFiles is how many materialized files are kept open.
const defaultOpenFiles = 256

// Config is what a file system is made of.
type Config struct {
	Refs    Refs
	Tree    *tree.Reader
	Handles *handles.Table
	Files   Files
	// OpenFiles is how many materialized files are kept open. Zero means
	// 256.
	OpenFiles int
	// Log receives what goes wrong. Nil means slog.Default().
	Log *slog.Logger
}

// FS is the served tree. Its nodes are made when they are asked for, by a
// lookup, a listing or a handle, and hold no state of their own: two nodes
// of one path are the same node to whoever uses them.
type FS struct {
	refs    Refs
	tree    *tree.Reader
	handles *handles.Table
	files   Files
	open    *openFiles
	log     *slog.Logger
	// unreadable holds the handles of the materialized files that a read
	// has failed on, so that each is logged once.
	unreadable sync.Map
}

// New returns the file system over what cfg names.
func New(cfg Config) *FS {
	if cfg.OpenFiles <= 0 {
		cfg.OpenFiles = defaultOpenFiles
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &FS{
		refs:    cfg.Refs,
		tree:    cfg.Tree,
		handles: cfg.Handles,
		files:   cfg.Files,
		open:    newOpenFiles(cfg.OpenFiles),
		log:     cfg.Log,
	}
}

// Root returns the root directory.
func (fs *FS) Root() virtual.Directory {
	return &root{fs: fs}
}

// Close closes the materialized files that are kept open.
func (fs *FS) Close() error {
	fs.open.closeAll()
	return nil
}

// place is where a node is: its handle, the reference it belongs to, and
// the names that lead to it from the reference's root, none for the
// reference itself.
type place struct {
	id  handles.ID
	ref string
	rel []string
}

// below returns the place of the entry name of the directory at p, and
// records its handle.
func (fs *FS) below(p place, name string) place {
	return place{
		id:  fs.handles.Add(p.id, name),
		ref: p.ref,
		rel: append(slices.Clip(p.rel), name),
	}
}

// inRoot returns the place of the reference name, and records its handle.
func (fs *FS) inRoot(name string) place {
	return place{id: fs.handles.Add(handles.Root, name), ref: name}
}

// node returns the node at a place as Buildbarn takes one.
func (fs *FS) node(at place, n tree.Node) virtual.DirectoryChild {
	if n.Kind == tree.Dir {
		return virtual.DirectoryChild{}.FromDirectory(&directory{fs: fs, at: at, node: n})
	}
	return virtual.DirectoryChild{}.FromLeaf(&leaf{fs: fs, at: at, node: n})
}

// Resolve returns the node a file handle names: it is the resolver of
// Buildbarn's pool of opened files. A handle the table does not know, or
// one into a reference that is not pinned, is stale: the client looks the
// path up again, and gets the same handle for it.
//
// Nothing is fetched here. A handle is only ever given out for something
// in a reference that is pinned, and the cache that holds the pins holds
// the table too.
func (fs *FS) Resolve(r io.ByteReader) (virtual.DirectoryChild, virtual.Status) {
	var id handles.ID
	for i := range id {
		b, err := r.ReadByte()
		if err != nil {
			return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
		}
		id[i] = b
	}
	if _, err := r.ReadByte(); err == nil {
		return virtual.DirectoryChild{}, virtual.StatusErrBadHandle
	}
	if id == handles.Root {
		return virtual.DirectoryChild{}.FromDirectory(fs.Root()), virtual.StatusOK
	}

	names, ok := fs.handles.Path(id)
	if !ok || len(names) == 0 {
		return virtual.DirectoryChild{}, virtual.StatusErrStale
	}
	rootKey, ok := fs.refs.Pinned(names[0])
	if !ok {
		return virtual.DirectoryChild{}, virtual.StatusErrStale
	}
	n, err := fs.tree.Root(rootKey)
	if err != nil {
		fs.log.Error("resolving a handle", "reference", names[0], "error", err)
		return virtual.DirectoryChild{}, virtual.StatusErrIO
	}
	for _, name := range names[1:] {
		if n.Kind != tree.Dir {
			return virtual.DirectoryChild{}, virtual.StatusErrStale
		}
		if n, err = fs.tree.Lookup(n.Key, name); err != nil {
			if errors.Is(err, tree.ErrNotFound) {
				return virtual.DirectoryChild{}, virtual.StatusErrStale
			}
			fs.log.Error("resolving a handle", "reference", names[0], "error", err)
			return virtual.DirectoryChild{}, virtual.StatusErrIO
		}
	}
	return fs.node(place{id: id, ref: names[0], rel: names[1:]}, n), virtual.StatusOK
}

// openChild is the second half of an open by name, for the root and for
// the directories alike: the entry was looked for, with the status s, and
// is the node n at the place at when it was found.
//
// An open may ask for a file to be created (createAttributes), to be
// opened if it is there (existingOptions), or both. Nothing is ever
// created here.
func (fs *FS) openChild(ctx context.Context, at place, n tree.Node, s virtual.Status, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	switch {
	case s == virtual.StatusErrNoEnt:
		return virtual.ReadOnlyDirectoryOpenChildDoesntExist(createAttributes)
	case s != virtual.StatusOK:
		return nil, 0, virtual.ChangeInfo{}, s
	case n.Kind == tree.Dir:
		return virtual.ReadOnlyDirectoryOpenChildWrongFileType(existingOptions, virtual.StatusErrIsDir)
	case existingOptions == nil:
		return nil, 0, virtual.ChangeInfo{}, virtual.StatusErrExist
	}
	l := &leaf{fs: fs, at: at, node: n}
	s = l.VirtualOpenSelf(ctx, shareAccess, existingOptions, requested, out)
	return l, existingOptions.ToAttributesMask(), virtual.ChangeInfo{}, s
}

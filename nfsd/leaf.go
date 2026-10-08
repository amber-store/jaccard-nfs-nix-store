//go:build linux

package nfsd

import (
	"context"
	"errors"
	"io"

	"github.com/amber-store/jaccard-nfs-nix-store/tree"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// leaf is a regular file or a symbolic link of a fetched reference.
type leaf struct {
	fs   *FS
	at   place
	node tree.Node
}

func (l *leaf) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, a *virtual.Attributes) {
	attributes(l.at.id, l.node, requested, a)
}

func (l *leaf) VirtualSetAttributes(ctx context.Context, in *virtual.Attributes, requested virtual.AttributesMask, out *virtual.Attributes) virtual.Status {
	return virtual.StatusErrROFS
}

func (l *leaf) VirtualApply(data any) bool { return false }

func (l *leaf) VirtualOpenNamedAttributes(ctx context.Context, createDirectory bool, requested virtual.AttributesMask, a *virtual.Attributes) (virtual.Directory, virtual.Status) {
	if createDirectory {
		return nil, virtual.StatusErrROFS
	}
	return nil, virtual.StatusErrNoEnt
}

func (l *leaf) VirtualAllocate(ctx context.Context, off, size uint64) virtual.Status {
	return virtual.StatusErrROFS
}

// VirtualSeek finds the next data or the next hole from an offset. A file
// here is data from its first byte to its last, and the hole after.
func (l *leaf) VirtualSeek(ctx context.Context, offset uint64, regionType filesystem.RegionType) (*uint64, virtual.Status) {
	if l.node.Kind != tree.File {
		return nil, virtual.StatusErrInval
	}
	if offset >= l.node.Size {
		return nil, virtual.StatusErrNXIO
	}
	if regionType == filesystem.Data {
		return &offset, virtual.StatusOK
	}
	end := l.node.Size
	return &end, virtual.StatusOK
}

func (l *leaf) VirtualOpenSelf(ctx context.Context, shareAccess virtual.ShareMask, options *virtual.OpenExistingOptions, requested virtual.AttributesMask, a *virtual.Attributes) virtual.Status {
	if l.node.Kind != tree.File {
		return virtual.StatusErrSymlink
	}
	if shareAccess&virtual.ShareMaskWrite != 0 || options.Truncate {
		return virtual.StatusErrROFS
	}
	attributes(l.at.id, l.node, requested, a)
	return virtual.StatusOK
}

// VirtualClose has nothing to undo: an open holds nothing.
func (l *leaf) VirtualClose(shareAccess virtual.ShareMask) {}

func (l *leaf) VirtualWrite(ctx context.Context, buf []byte, offset uint64) (int, virtual.Status) {
	return 0, virtual.StatusErrROFS
}

// VirtualRead reads from the plain file of a reference that is
// materialized, and from the objects otherwise. The two hold the same
// bytes, so a file may be read from one and then the other.
func (l *leaf) VirtualRead(ctx context.Context, buf []byte, offset uint64) (int, bool, virtual.Status) {
	if l.node.Kind != tree.File {
		return 0, false, virtual.StatusErrInval
	}
	buf, eof := virtual.BoundReadToFileSize(buf, offset, l.node.Size)
	if len(buf) == 0 {
		return 0, eof, virtual.StatusOK
	}
	if l.readFile(buf, offset) {
		return len(buf), eof, virtual.StatusOK
	}
	// buf ends where the file does at the latest, so all of it is there
	// to be read.
	if n, err := l.fs.tree.ReadAt(l.node.Key, buf, int64(offset)); n < len(buf) {
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		l.fs.log.Error("reading a file from the objects", "reference", l.at.ref, "file", l.at.rel, "offset", offset, "error", err)
		return 0, false, virtual.StatusErrIO
	}
	return len(buf), eof, virtual.StatusOK
}

// readFile fills buf from the plain file, and reports whether it did: not
// when the reference is not materialized, and not when the file cannot be
// read, which is logged the first time and leaves the read to the objects.
func (l *leaf) readFile(buf []byte, offset uint64) bool {
	path, ok := l.fs.files.Path(l.at.ref, l.at.rel)
	if !ok {
		return false
	}
	err := l.fs.open.readAt(l.at.id, path, buf, int64(offset))
	if err == nil {
		return true
	}
	if _, logged := l.fs.unreadable.LoadOrStore(l.at.id, struct{}{}); !logged {
		l.fs.log.Warn("a materialized file cannot be read, the objects are read instead",
			"reference", l.at.ref, "file", l.at.rel, "error", err)
	}
	return false
}

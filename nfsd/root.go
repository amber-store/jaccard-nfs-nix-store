//go:build linux

package nfsd

import (
	"context"
	"errors"

	"github.com/amber-store/jaccard-nfs-nix-store/handles"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
	"github.com/amber-store/jaccard-nfs-nix-store/tree"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// root is the root directory: a name in it is a reference, which is
// fetched when the name is first looked up or opened. A listing shows what
// has been fetched and fetches nothing.
type root struct {
	virtual.ReadOnlyDirectory
	fs *FS
}

func (r *root) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, a *virtual.Attributes) {
	identity(handles.Root, a)
	// The listing changes with every reference that is fetched, and by
	// nothing else: the client reads it again when this has moved.
	a.SetChangeID(uint64(r.fs.refs.Count()))
	a.SetFileType(filesystem.FileTypeDirectory)
	a.SetPermissions(virtual.PermissionsRead | virtual.PermissionsExecute)
	a.SetOwnerUserID(0)
	a.SetOwnerGroupID(0)
	a.SetLastDataModificationTime(rootModified)
	a.SetSizeBytes(0)
}

func (r *root) VirtualApply(data any) bool { return false }

// fetch returns the reference of a name, fetching it if need be. It is
// what both ways of naming something in the root go through, the lookup
// and the open: a client opens a file without looking it up first.
func (r *root) fetch(ctx context.Context, name string) (place, tree.Node, virtual.Status) {
	rootKey, err := r.fs.refs.Ensure(ctx, name)
	switch {
	case errors.Is(err, refs.ErrNotFound):
		return place{}, tree.Node{}, virtual.StatusErrNoEnt
	case err != nil:
		// The fetching has logged why.
		return place{}, tree.Node{}, virtual.StatusErrIO
	}
	n, err := r.fs.tree.Root(rootKey)
	if err != nil {
		r.fs.log.Error("reading a reference that was fetched", "name", name, "error", err)
		return place{}, tree.Node{}, virtual.StatusErrIO
	}
	return r.fs.inRoot(name), n, virtual.StatusOK
}

func (r *root) VirtualLookup(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
	at, n, s := r.fetch(ctx, name.String())
	if s != virtual.StatusOK {
		return virtual.DirectoryChild{}, s
	}
	attributes(at.id, n, requested, out)
	return r.fs.node(at, n), virtual.StatusOK
}

func (r *root) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	at, n, s := r.fetch(ctx, name.String())
	return r.fs.openChild(ctx, at, n, s, shareAccess, createAttributes, existingOptions, requested, out)
}

// VirtualReadDir reports the references that were fetched, in the order
// they were: the place in that order is the cookie, which stays what it is
// while references are added.
func (r *root) VirtualReadDir(ctx context.Context, firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	pins := r.fs.refs.Pins()
	for i := firstCookie; i < uint64(len(pins)); i++ {
		pin := pins[i]
		name, ok := path.NewComponent(pin.Name)
		if !ok {
			continue
		}
		n, err := r.fs.tree.Root(pin.Root)
		if err != nil {
			r.fs.log.Error("listing a reference that was fetched", "name", pin.Name, "error", err)
			continue
		}
		at := r.fs.inRoot(pin.Name)
		var a virtual.Attributes
		attributes(at.id, n, requested, &a)
		if !reporter.ReportEntry(i+1, name, r.fs.node(at, n), &a) {
			break
		}
	}
	return virtual.StatusOK
}

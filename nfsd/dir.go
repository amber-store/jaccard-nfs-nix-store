//go:build linux

package nfsd

import (
	"context"
	"errors"

	"github.com/amber-store/jaccard-nfs-nix-store/tree"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// directory is a directory of a fetched reference, the reference's own
// root among them. Everything in it is read from the objects.
type directory struct {
	virtual.ReadOnlyDirectory
	fs   *FS
	at   place
	node tree.Node
}

func (d *directory) VirtualGetAttributes(ctx context.Context, requested virtual.AttributesMask, a *virtual.Attributes) {
	attributes(d.at.id, d.node, requested, a)
}

func (d *directory) VirtualApply(data any) bool { return false }

// find returns the entry of a name.
func (d *directory) find(name string) (place, tree.Node, virtual.Status) {
	n, err := d.fs.tree.Lookup(d.node.Key, name)
	switch {
	case errors.Is(err, tree.ErrNotFound):
		return place{}, tree.Node{}, virtual.StatusErrNoEnt
	case err != nil:
		d.fs.log.Error("looking a name up", "reference", d.at.ref, "directory", d.at.rel, "name", name, "error", err)
		return place{}, tree.Node{}, virtual.StatusErrIO
	}
	return d.fs.below(d.at, name), n, virtual.StatusOK
}

func (d *directory) VirtualLookup(ctx context.Context, name path.Component, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.DirectoryChild, virtual.Status) {
	at, n, s := d.find(name.String())
	if s != virtual.StatusOK {
		return virtual.DirectoryChild{}, s
	}
	attributes(at.id, n, requested, out)
	return d.fs.node(at, n), virtual.StatusOK
}

func (d *directory) VirtualOpenChild(ctx context.Context, name path.Component, shareAccess virtual.ShareMask, createAttributes *virtual.Attributes, existingOptions *virtual.OpenExistingOptions, requested virtual.AttributesMask, out *virtual.Attributes) (virtual.Leaf, virtual.AttributesMask, virtual.ChangeInfo, virtual.Status) {
	at, n, s := d.find(name.String())
	return d.fs.openChild(ctx, at, n, s, shareAccess, createAttributes, existingOptions, requested, out)
}

// VirtualReadDir reports the entries in the order they are stored: the
// place in that order is the cookie, and a directory never changes.
func (d *directory) VirtualReadDir(ctx context.Context, firstCookie uint64, requested virtual.AttributesMask, reporter virtual.DirectoryEntryReporter) virtual.Status {
	entries, err := d.fs.tree.List(d.node.Key)
	if err != nil {
		d.fs.log.Error("listing a directory", "reference", d.at.ref, "directory", d.at.rel, "error", err)
		return virtual.StatusErrIO
	}
	for i := firstCookie; i < uint64(len(entries)); i++ {
		e := entries[i]
		// Core's objects hold any bytes as a name. One that no client
		// could ask for is left out.
		name, ok := path.NewComponent(e.Name)
		if !ok {
			continue
		}
		at := d.fs.below(d.at, e.Name)
		var a virtual.Attributes
		attributes(at.id, e.Node, requested, &a)
		if !reporter.ReportEntry(i+1, name, d.fs.node(at, e.Node), &a) {
			break
		}
	}
	return virtual.StatusOK
}

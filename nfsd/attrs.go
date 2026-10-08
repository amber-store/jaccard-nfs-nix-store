//go:build linux

package nfsd

import (
	"time"

	"github.com/amber-store/jaccard-nfs-nix-store/handles"
	"github.com/amber-store/jaccard-nfs-nix-store/tree"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// rootModified is the modification time of the root directory: that of a
// path in a Nix store. What changes when a reference is fetched is the
// root's change ID, which is what an NFSv4 client goes by.
var rootModified = time.Unix(1, 0)

// identity fills in what every node has of its handle. The handle is
// copied: Buildbarn keeps the slice it is given.
func identity(id handles.ID, a *virtual.Attributes) {
	a.SetFileHandle(id[:])
	a.SetInodeNumber(id.Inode())
	a.SetHasNamedAttributes(false)
	a.SetIsInNamedAttributeDirectory(false)
	// A node is one path, and has one name.
	a.SetLinkCount(1)
}

// attributes fills in the attributes of the node n whose handle is id.
//
// Buildbarn has one set of permission bits for owner, group and others,
// and nothing here is writable: a node is r--, or r-x when it is a
// directory or has an execute bit.
func attributes(id handles.ID, n tree.Node, requested virtual.AttributesMask, a *virtual.Attributes) {
	identity(id, a)
	// What a node is and holds never changes.
	a.SetChangeID(0)
	a.SetOwnerUserID(n.UID)
	a.SetOwnerGroupID(n.GID)
	a.SetLastDataModificationTime(n.Mtime)
	a.SetSizeBytes(n.Size)
	permissions := virtual.PermissionsRead
	if n.Exec || n.Kind == tree.Dir {
		permissions |= virtual.PermissionsExecute
	}
	a.SetPermissions(permissions)
	switch n.Kind {
	case tree.Dir:
		a.SetFileType(filesystem.FileTypeDirectory)
	case tree.Symlink:
		a.SetFileType(filesystem.FileTypeSymlink)
		if requested&virtual.AttributesMaskSymlinkTarget != 0 {
			a.SetSymlinkTarget(path.UNIXFormat.NewParser(n.Target))
		}
	default:
		a.SetFileType(filesystem.FileTypeRegularFile)
	}
}

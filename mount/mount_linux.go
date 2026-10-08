//go:build linux

package mount

import (
	"fmt"
	"net/netip"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Mount mounts the NFSv4 server at the address server on the directory
// target, read-only, with the options given, which are those of nfs(5).
//
// The kernel is called itself, without mount.nfs: it takes the server's
// address and port as options and needs nothing else to find it. The
// source is the root of what the server exports.
func Mount(target string, server netip.AddrPort, options string) error {
	data := fmt.Sprintf("addr=%s,port=%d", server.Addr(), server.Port())
	if options != "" {
		data += "," + options
	}
	if err := unix.Mount(":/", target, "nfs4", unix.MS_RDONLY|unix.MS_NOSUID|unix.MS_NODEV, data); err != nil {
		return fmt.Errorf("mounting the server at %s on %s with %q: %w", server, target, data, err)
	}
	return nil
}

// Unmount takes away what is mounted on target. It fails with an error
// that is unix.EBUSY while something there is in use, and one that is
// unix.EINVAL when nothing is mounted there.
func Unmount(target string) error {
	if err := unix.Unmount(target, 0); err != nil {
		return fmt.Errorf("unmounting %s: %w", target, err)
	}
	return nil
}

// Detach takes what is mounted on target out of the tree at once, also
// while it is in use. The file system itself is let go when the last of
// its users is gone, and talks to its server until then.
func Detach(target string) error {
	if err := unix.Unmount(target, unix.MNT_DETACH); err != nil {
		return fmt.Errorf("detaching %s: %w", target, err)
	}
	return nil
}

// Mounted returns the type of the file system that is mounted on target,
// and whether one is.
func Mounted(target string) (fstype string, mounted bool, err error) {
	abs, err := filepath.Abs(target)
	if err != nil {
		return "", false, err
	}
	// The table names a mount point by its real path.
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		abs = real
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", false, fmt.Errorf("mount: %w", err)
	}
	defer f.Close()
	return find(f, abs)
}

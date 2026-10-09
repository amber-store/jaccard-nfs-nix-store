// Package sidecar is the sidecar as one thing: the fetching, the
// materializing, the handles and the NFS server put together, the mount of
// that server on a directory, and the order in which it all ends. The
// command starts one and waits for a signal.
package sidecar

import (
	"errors"
	"log/slog"
	"time"

	"golang.org/x/sys/unix"
)

const (
	// lingerFor is how long, at the most, the server goes on after the
	// mount was taken away, for the kernel to be done with it; settleFor
	// is how long no client must have been connected for it to be.
	lingerFor = 5 * time.Second
	settleFor = 250 * time.Millisecond
	// adoptFor is how long a mount that an earlier run left is given to
	// reach the server before it is taken for dead, unless the
	// configuration says.
	adoptFor = 10 * time.Second
)

// leaver takes the mount away when the sidecar ends, while the server
// still answers: a mount whose server is gone holds up whoever unmounts
// it, and with that the end of the pod.
type leaver struct {
	unmount func(target string) error
	detach  func(target string) error
	log     *slog.Logger
}

// leave unmounts target, and detaches it at once when the unmount is
// refused because something there is in use.
//
// It does not wait for whoever uses the mount. A mount that the sidecar
// made reaches the node, through the propagation that carries it to the
// app containers, and nobody but the sidecar takes it away again: one
// that is killed with the mount in place leaves it on the node, where it
// keeps the pod's volume from being removed. The kubelet may give a
// sidecar as little as two seconds between telling it to stop and
// killing it, and does so just when the mount is in use, because the app
// is being killed in the same moment. Detached, the mount is gone from
// every tree at once; the file system behind it lives until its last
// user is gone.
//
// Either way the mount is then gone from the tree and the file system
// behind it may not be: whoever stops the server has to wait for the
// kernel to let go of it first (nfsd.Server.WaitIdle).
func (l leaver) leave(target string) error {
	err := l.unmount(target)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, unix.EINVAL):
		// Nothing is mounted there: somebody took it away already.
		return nil
	case errors.Is(err, unix.EBUSY):
		l.log.Warn("the mount is in use: detaching it", "mount", target)
		return l.detach(target)
	}
	return err
}

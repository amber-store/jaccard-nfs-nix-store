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
	// busyFor is how long an unmount that is refused as busy is tried
	// again, every busyEvery.
	busyFor   = 2 * time.Second
	busyEvery = 100 * time.Millisecond
	// lingerFor is how long, at the most, the server goes on after the
	// mount was taken away, for the kernel to be done with it; settleFor
	// is how long no client must have been connected for it to be.
	lingerFor = 5 * time.Second
	settleFor = 250 * time.Millisecond
)

// leaver takes the mount away when the sidecar ends, while the server
// still answers: a mount whose server is gone holds up whoever unmounts
// it, and with that the end of the pod.
type leaver struct {
	unmount func(target string) error
	detach  func(target string) error
	sleep   func(time.Duration)
	log     *slog.Logger
}

// leave unmounts target. An unmount that is refused because something
// there is in use is tried again for busyFor; after that the mount is
// detached.
//
// Either way the mount is then gone from the tree and the file system
// behind it may not be: whoever stops the server has to wait for the
// kernel to let go of it first (nfsd.Server.WaitIdle).
func (l leaver) leave(target string) error {
	for waited := time.Duration(0); ; waited += busyEvery {
		err := l.unmount(target)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, unix.EINVAL):
			// Nothing is mounted there: somebody took it away already.
			return nil
		case !errors.Is(err, unix.EBUSY):
			return err
		case waited >= busyFor:
			l.log.Warn("the mount is still in use: detaching it", "mount", target)
			return l.detach(target)
		}
		l.sleep(busyEvery)
	}
}

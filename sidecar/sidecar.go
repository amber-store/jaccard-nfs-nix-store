//go:build linux

package sidecar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-nfs-nix-store/handles"
	"github.com/amber-store/jaccard-nfs-nix-store/materialize"
	"github.com/amber-store/jaccard-nfs-nix-store/mount"
	"github.com/amber-store/jaccard-nfs-nix-store/nfsd"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
	"github.com/amber-store/jaccard-nfs-nix-store/tree"
)

// Config is what a sidecar is started with. Zero values of the numbers
// and durations are the defaults of the packages they belong to.
type Config struct {
	// Prefix is put before a name in the root to make a reference name.
	Prefix string
	// Server names the jaccard-store server, by its endpoint ID. The
	// cache remembers it: one that was filled from another is refused.
	Server string
	// Cache is the directory of everything that is kept: the packstore,
	// the pins, the materialized files and the handle table.
	Cache string
	// Dial connects to the jaccard-store server.
	Dial func(ctx context.Context) (refs.Conn, error)
	// Listen is the TCP address of the NFS server.
	Listen string
	// Mount is the directory the server is mounted on. Empty means the
	// sidecar only serves.
	Mount string
	// MountOptions are the options of the mount after the server's
	// address and port, as nfs(5) has them.
	MountOptions string

	PullTimeout     time.Duration
	PullJobs        int
	MaterializeJobs int

	// Log receives what the sidecar does. Nil means slog.Default().
	Log *slog.Logger
}

// Sidecar is a sidecar that runs.
type Sidecar struct {
	cfg      Config
	log      *slog.Logger
	store    *refs.Store
	files    *materialize.Store
	table    *handles.Table
	fs       *nfsd.FS
	server   *nfsd.Server
	listener net.Listener
	served   chan struct{}
}

// Start opens the cache, serves it on cfg.Listen and, when cfg.Mount
// names a directory, mounts the server there. It returns when all of that
// is done: the directory is the store from then on.
func Start(cfg Config) (_ *Sidecar, err error) {
	s := &Sidecar{cfg: cfg, log: cfg.Log, served: make(chan struct{})}
	if s.log == nil {
		s.log = slog.Default()
	}
	// What was opened is closed again when a later step fails.
	defer func() {
		if err != nil {
			s.Close()
		}
	}()

	if err := os.MkdirAll(cfg.Cache, 0o755); err != nil {
		return nil, fmt.Errorf("the cache directory: %w", err)
	}
	s.store, err = refs.Open(refs.Options{
		Prefix:      cfg.Prefix,
		Server:      cfg.Server,
		Dir:         cfg.Cache,
		Dial:        cfg.Dial,
		PullTimeout: cfg.PullTimeout,
		PullJobs:    cfg.PullJobs,
		// A reference is materialized from the moment it is pinned. No
		// name is pinned before the server below answers, and files is
		// set by then.
		OnPin: func(name string, root key.Key) { s.files.Queue(name, root) },
		Log:   s.log,
	})
	if err != nil {
		return nil, err
	}
	s.files, err = materialize.Open(filepath.Join(cfg.Cache, "files"), s.store.Get, materialize.Options{
		Jobs: cfg.MaterializeJobs,
		Log:  s.log,
	})
	if err != nil {
		return nil, err
	}
	// What an earlier run fetched and did not get to write out.
	for _, pin := range s.store.Pins() {
		s.files.Queue(pin.Name, pin.Root)
	}
	if s.table, err = handles.Open(filepath.Join(cfg.Cache, "handles")); err != nil {
		return nil, err
	}
	s.fs = nfsd.New(nfsd.Config{
		Refs:    s.store,
		Tree:    tree.New(s.store.Get, tree.Options{}),
		Handles: s.table,
		Files:   s.files,
		Log:     s.log,
	})

	if s.listener, err = net.Listen("tcp", cfg.Listen); err != nil {
		return nil, fmt.Errorf("the NFS server: %w", err)
	}
	s.server = nfsd.NewServer(s.fs)
	go func() {
		defer close(s.served)
		if err := s.server.Serve(s.listener); err != nil {
			s.log.Error("the NFS server stopped accepting connections", "error", err)
		}
	}()
	s.log.Info("serving", "address", s.Addr().String(), "prefix", cfg.Prefix,
		"cache", cfg.Cache, "pinned", len(s.store.Pins()), "handles", s.table.Len())

	if cfg.Mount != "" {
		if err := s.mount(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// mount mounts the server on the directory of the configuration, unless
// an NFS mount is there already. That one is the mount of an earlier run
// of the sidecar in the same pod, which ended without unmounting: the
// kernel finds the server again at the address it knows, and what it
// holds of handles means what it meant, so there is nothing to mount.
func (s *Sidecar) mount() error {
	fstype, mounted, err := mount.Mounted(s.cfg.Mount)
	if err != nil {
		return err
	}
	if mounted && strings.HasPrefix(fstype, "nfs") {
		s.log.Info("the mount of an earlier run is there, and is served again", "mount", s.cfg.Mount)
		return nil
	}
	if err := mount.Mount(s.cfg.Mount, s.Addr(), s.cfg.MountOptions); err != nil {
		return err
	}
	s.log.Info("mounted", "mount", s.cfg.Mount, "options", s.cfg.MountOptions)
	return nil
}

// Addr returns the address the NFS server listens on.
func (s *Sidecar) Addr() netip.AddrPort {
	return s.listener.Addr().(*net.TCPAddr).AddrPort()
}

// Stop ends the sidecar in order: the materializing stops, the mount is
// taken away while the server still answers, and then everything is
// closed.
func (s *Sidecar) Stop() error {
	s.files.Close()
	var err error
	if s.cfg.Mount != "" {
		err = leaver{unmount: mount.Unmount, detach: mount.Detach, sleep: time.Sleep, log: s.log}.leave(s.cfg.Mount)
		if err == nil {
			s.log.Info("unmounted", "mount", s.cfg.Mount)
		}
	}
	return errors.Join(err, s.Close())
}

// Close stops the server and closes the cache. The mount is left as it
// is: one that is there afterwards has no server until a sidecar is
// started on the same cache and address again. Stop is how a sidecar
// ends; Close is what is left of a start that failed, and what a test
// has in place of a crash.
func (s *Sidecar) Close() error {
	var errs []error
	if s.listener != nil {
		s.listener.Close()
		if s.server != nil {
			<-s.served
			errs = append(errs, s.server.Close())
		}
	}
	if s.fs != nil {
		errs = append(errs, s.fs.Close())
	}
	if s.files != nil {
		errs = append(errs, s.files.Close())
	}
	if s.table != nil {
		errs = append(errs, s.table.Close())
	}
	if s.store != nil {
		errs = append(errs, s.store.Close())
	}
	return errors.Join(errs...)
}

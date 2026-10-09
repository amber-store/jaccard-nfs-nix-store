//go:build linux

package sidecar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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
	"github.com/amber-store/jaccard-nfs-nix-store/preload"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
	"github.com/amber-store/jaccard-nfs-nix-store/tree"
	"golang.org/x/sys/unix"
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

	// PreloadListen is the TCP address of the HTTP endpoint that takes
	// lists of store paths to fetch ahead (package preload). Empty means
	// there is none.
	PreloadListen string
	// PreloadJobs is how many paths of one list are fetched at once.
	PreloadJobs int

	// AdoptTimeout is how long a mount that an earlier run of the
	// sidecar left on Mount is given to reach this server before it is
	// detached and the server mounted anew. Zero means 10 seconds.
	AdoptTimeout time.Duration

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

	// The preload endpoint, when there is one. endPreloads ends the
	// requests that are being answered.
	preloads        *http.Server
	preloadListener net.Listener
	endPreloads     context.CancelFunc
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

	// Before the mount, which is what a pod waits for to start its app
	// containers: whoever preloads finds the endpoint there.
	if cfg.PreloadListen != "" {
		if err := s.servePreloads(); err != nil {
			return nil, err
		}
	}
	if cfg.Mount != "" {
		if err := s.mount(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// servePreloads starts the HTTP endpoint that fetches lists of store
// paths. Its requests run under a context of the sidecar's, so that
// stopping ends them, and with them the pulls nobody else waits for.
func (s *Sidecar) servePreloads() error {
	l, err := net.Listen("tcp", s.cfg.PreloadListen)
	if err != nil {
		return fmt.Errorf("the preload endpoint: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.preloadListener, s.endPreloads = l, cancel
	s.preloads = &http.Server{
		Handler:           preload.New(s.store, s.cfg.PreloadJobs, s.log),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	go func() {
		if err := s.preloads.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("the preload endpoint stopped", "error", err)
		}
	}()
	s.log.Info("preloading", "address", l.Addr().String(), "path", preload.Path)
	return nil
}

// stopPreloads closes the preload endpoint and ends its requests.
func (s *Sidecar) stopPreloads() {
	if s.preloads == nil {
		return
	}
	s.endPreloads()
	s.preloads.Close()
}

// PreloadAddr returns the address the preload endpoint listens on. There
// has to be one.
func (s *Sidecar) PreloadAddr() netip.AddrPort {
	return s.preloadListener.Addr().(*net.TCPAddr).AddrPort()
}

// mount mounts the server on the directory of the configuration.
//
// An NFS mount that is there already is the mount of an earlier run of
// the sidecar, which ended without unmounting. In the same pod and at the
// same address the kernel finds the server again, and what it holds of
// handles means what it meant: that mount is served again, and whoever had
// a file of it open goes on reading. But the kernel looks for the server
// where the mount was made, and a mount of another network namespace (the
// pod's sandbox was made anew) or of another port never finds this one. It
// would be a store that answers nobody, so it is taken away and the
// server mounted in its place.
func (s *Sidecar) mount() error {
	dir := s.cfg.Mount
	fstype, mounted, err := mount.Mounted(dir)
	if err != nil {
		return err
	}
	if mounted && strings.HasPrefix(fstype, "nfs") {
		wait := s.cfg.AdoptTimeout
		if wait <= 0 {
			wait = adoptFor
		}
		if s.reached(dir, wait) {
			s.log.Info("the mount of an earlier run is there, and is served again", "mount", dir)
			return nil
		}
		s.log.Warn("the mount of an earlier run does not reach this server: it is detached, and the server mounted anew",
			"mount", dir, "waited", wait)
		if err := s.discard(dir); err != nil {
			return err
		}
	}
	if err := mount.Mount(dir, s.Addr(), s.cfg.MountOptions); err != nil {
		return err
	}
	s.log.Info("mounted", "mount", dir, "options", s.cfg.MountOptions)
	return nil
}

// reached reports whether the kernel behind the mount on dir connects to
// this server within wait. It is given a reason to: a statfs, which is
// asked of the server every time. On a mount that is dead the statfs
// waits for the mount's timeouts, which nobody here waits for.
func (s *Sidecar) reached(dir string, wait time.Duration) bool {
	go func() {
		var st unix.Statfs_t
		unix.Statfs(dir, &st)
	}()
	return s.server.WaitClient(wait)
}

// discard takes a dead mount off dir. The detaching takes the mount out
// of the tree at once and then, in the same call, lets go of the file
// system, which waits for its server as long as the mount's timeouts
// allow: the call is left to end by itself.
func (s *Sidecar) discard(dir string) error {
	go func() {
		if err := mount.Detach(dir); err != nil && !errors.Is(err, unix.EINVAL) {
			s.log.Warn("detaching the mount of an earlier run", "mount", dir, "error", err)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		fstype, mounted, err := mount.Mounted(dir)
		if err != nil {
			return err
		}
		if !mounted || !strings.HasPrefix(fstype, "nfs") {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the dead mount of an earlier run is still on %s", dir)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Addr returns the address the NFS server listens on.
func (s *Sidecar) Addr() netip.AddrPort {
	return s.listener.Addr().(*net.TCPAddr).AddrPort()
}

// Stop ends the sidecar in order: the materializing stops, the mount is
// taken away while the server still answers, the server goes on until the
// kernel has let go of it, and then everything is closed.
//
// The third step is for the pod whose grace period has run out. The
// kubelet then kills the app and stops the sidecar in the same moment,
// and the kernel is still closing what the app had open when the unmount
// has long succeeded: each of those files takes an answer from the
// server, and a server that is gone leaves the dying app, and the pod,
// to wait for the mount's timeouts.
func (s *Sidecar) Stop() error {
	// Nobody is to ask for more while everything is taken down.
	s.stopPreloads()
	s.files.Close()
	var err error
	if s.cfg.Mount != "" {
		err = leaver{unmount: mount.Unmount, detach: mount.Detach, log: s.log}.leave(s.cfg.Mount)
		if err == nil {
			s.log.Info("unmounted", "mount", s.cfg.Mount)
			if !s.server.WaitIdle(lingerFor, settleFor) {
				s.log.Warn("the kernel still holds a connection to the server, which stops now: "+
					"whoever has files of the store open waits for the mount's timeouts", "after", lingerFor)
			}
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
	s.stopPreloads()
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

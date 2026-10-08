// Package jstest is a jaccard-store server in one process, for tests: the
// server with a database in a temporary directory and a bucket in memory,
// reachable over iroh on loopback. It is a package and no test file so
// that the tests of other packages can import it.
//
// The bucket (jaccard-store's buckettest) sets the environment of the test
// with t.Setenv, so a test that calls Start cannot be parallel.
package jstest

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/ingest"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/jaccard-store/bucket/buckettest"
	"github.com/amber-store/jaccard-store/client"
	"github.com/amber-store/jaccard-store/db"
	"github.com/amber-store/jaccard-store/node"
	"github.com/amber-store/jaccard-store/server"
	"github.com/amber-store/jaccard-store/wire"
	"github.com/tmc/go-iroh/iroh"
)

// pushTimeout is how long one PushDir may take.
const pushTimeout = 2 * time.Minute

// minDedup is the share of a directory's bytes a pack on the server must
// hold for the directory to be pushed as a patch against it.
const minDedup = 0.5

// quiet drops what the server and the endpoints log.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// Server is a jaccard-store server that lives as long as the test it was
// started for.
type Server struct {
	t  testing.TB
	ep *iroh.Endpoint
}

// Start starts a server: its database, its bucket and its endpoint. All of
// it is stopped and removed when the test ends.
func Start(t testing.TB) *Server {
	t.Helper()
	b := buckettest.New(t)
	dir := t.TempDir()

	database, err := db.Open(filepath.Join(dir, "store.sqlite"))
	if err != nil {
		t.Fatalf("jstest: opening the database: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	srv, err := server.New(server.Config{DB: database, Bucket: b, Scratch: filepath.Join(dir, "scratch"), Log: quiet})
	if err != nil {
		t.Fatalf("jstest: %v", err)
	}
	sk, err := node.LoadOrCreateKey(filepath.Join(dir, "server.key"))
	if err != nil {
		t.Fatalf("jstest: %v", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	ep, err := node.Bind(ctx, node.ServerConfig{Key: sk, ALPN: wire.ALPN, Local: true, Log: quiet})
	if err != nil {
		stop()
		t.Fatalf("jstest: binding the endpoint: %v", err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		srv.Serve(ctx, ep)
	}()
	// Cleanups run last in, first out: the serving ends before the
	// database under it is closed.
	t.Cleanup(func() {
		stop()
		<-served
		ep.Shutdown(context.Background())
	})
	return &Server{t: t, ep: ep}
}

// DialConfig returns what a client dials the server with: a key of its
// own, new with every call, and the server's ID and loopback address.
func (s *Server) DialConfig() node.DialConfig {
	s.t.Helper()
	sk, err := node.LoadOrCreateKey(filepath.Join(s.t.TempDir(), "client.key"))
	if err != nil {
		s.t.Fatalf("jstest: %v", err)
	}
	return node.DialConfig{
		Key:    sk,
		ALPN:   wire.ALPN,
		Server: s.ep.ID(),
		Addrs:  []netip.AddrPort{s.ep.LocalAddr()},
		Log:    quiet,
	}
}

// PushDir imports the directory dir into a store of its own and pushes it
// to the server as the reference name, over a connection of its own. It
// returns the root of the directory's tree, and fails the test if any of
// that fails.
//
// The directory is imported with the options jaccard-store's push-dir
// has: core's own, which leave out what an ignore file in dir names.
func (s *Server) PushDir(dir, name string) key.Key {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), pushTimeout)
	defer cancel()

	temp := s.t.TempDir()
	objects, err := packstore.Open(filepath.Join(temp, "packstore"), packstore.WithSync(false))
	if err != nil {
		s.t.Fatalf("jstest: pushing %s: %v", dir, err)
	}
	defer objects.Close()
	root, _, err := ingest.Dir(objects, dir, ingest.Opts{})
	if err != nil {
		s.t.Fatalf("jstest: importing %s: %v", dir, err)
	}

	conn, err := node.Dial(ctx, s.DialConfig())
	if err != nil {
		s.t.Fatalf("jstest: dialing the server: %v", err)
	}
	defer conn.Close()
	_, err = client.New(conn, nil).Push(ctx, objects, name, root, client.PushOptions{MinDedup: minDedup, TempDir: temp})
	if err != nil {
		s.t.Fatalf("jstest: pushing %s as %q: %v", dir, name, err)
	}
	return root
}

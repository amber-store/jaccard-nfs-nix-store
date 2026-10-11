//go:build linux

package sidecar

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/jaccard-nfs-nix-store/jstest"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
)

// prefix is what the references of these tests begin with on the server.
const prefix = "sidecar/store/"

// quiet drops what the sidecar logs.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// pinned returns the names the sidecar has fetched, sorted.
func pinned(s *Sidecar) []string {
	var names []string
	for _, pin := range s.store.Pins() {
		names = append(names, pin.Name)
	}
	slices.Sort(names)
	return names
}

// A program that has the sidecar in it preloads with a call: there is no
// endpoint to send a list to, and none is needed. The sidecar only
// serves here, which needs no root.
func TestPreloadByCall(t *testing.T) {
	srv := jstest.Start(t)
	src := t.TempDir()
	paths := []string{"aaa-one-1.0", "bbb-two-2.0", "ccc-three-3.0"}
	for _, name := range paths {
		file := filepath.Join(src, name, "share", "name")
		if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(name+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		srv.PushDir(filepath.Join(src, name), prefix+name)
	}
	sc, err := Start(Config{
		Prefix: prefix,
		Cache:  t.TempDir(),
		Dial: func(ctx context.Context) (refs.Conn, error) {
			return refs.Connect(ctx, srv.DialConfig())
		},
		Listen:      "127.0.0.1:0",
		PreloadJobs: 2,
		Log:         quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Stop()
	if sc.preloads != nil {
		t.Fatal("the sidecar has a preload endpoint, and was given no address for one")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := sc.Preload(ctx, nil); err != nil {
		t.Errorf("a preload of no name: %v", err)
	}
	if err := sc.Preload(ctx, paths[:2]); err != nil {
		t.Fatalf("Preload: %v", err)
	}
	if got := pinned(sc); !slices.Equal(got, paths[:2]) {
		t.Fatalf("after the preload the store holds %q, want %q", got, paths[:2])
	}
	// What is there is not fetched again, and a name given twice is one.
	if err := sc.Preload(ctx, []string{paths[0], paths[2], paths[2]}); err != nil {
		t.Fatalf("a preload of what is partly there: %v", err)
	}
	if got := pinned(sc); !slices.Equal(got, paths) {
		t.Fatalf("the store holds %q, want %q", got, paths)
	}

	// A path the server does not have is the error, which names it.
	err = sc.Preload(ctx, []string{paths[0], "zzz-missing-0.0"})
	if !errors.Is(err, refs.ErrNotFound) || !strings.Contains(err.Error(), "/nix/store/zzz-missing-0.0") {
		t.Errorf("a preload with a missing path = %v, want refs.ErrNotFound and the path", err)
	}
	// And so is a name that is none.
	if err := sc.Preload(ctx, []string{"../etc"}); !errors.Is(err, refs.ErrNotFound) {
		t.Errorf("a preload of a name that is none = %v, want refs.ErrNotFound", err)
	}
}

// unreached starts a sidecar whose server is never reached: every dial
// says on the channel that it has begun and then waits for its context.
func unreached(t *testing.T) (*Sidecar, <-chan struct{}) {
	t.Helper()
	dialing := make(chan struct{}, 16)
	sc, err := Start(Config{
		Prefix: prefix,
		Cache:  t.TempDir(),
		Dial: func(ctx context.Context) (refs.Conn, error) {
			dialing <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		Listen: "127.0.0.1:0",
		Log:    quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sc, dialing
}

// ended waits for a preload to return, and returns what it returned.
func ended(t *testing.T, after string, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("the preload went on after %s", after)
		return nil
	}
}

// A preload ends with its context.
func TestPreloadEndsWithItsContext(t *testing.T) {
	sc, dialing := unreached(t)
	defer sc.Stop()
	ctx, giveUp := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sc.Preload(ctx, []string{"aaa-one-1.0"}) }()
	<-dialing
	giveUp()
	if err := ended(t, "its context ended", done); !errors.Is(err, context.Canceled) {
		t.Errorf("a preload whose context ended = %v, want the context's error", err)
	}
}

// A preload ends with the sidecar: stopping does not wait for a fetch
// that nobody else asked for, and nothing is preloaded after it.
func TestPreloadEndsWithTheSidecar(t *testing.T) {
	sc, dialing := unreached(t)
	done := make(chan error, 1)
	go func() { done <- sc.Preload(context.Background(), []string{"bbb-two-2.0"}) }()
	<-dialing
	began := time.Now()
	if err := sc.Stop(); err != nil {
		t.Errorf("Stop: %v", err)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Errorf("Stop took %v with a preload under way", took)
	}
	if err := ended(t, "the sidecar was stopped", done); !errors.Is(err, ErrStopped) {
		t.Errorf("a preload under way when the sidecar stopped = %v, want ErrStopped", err)
	}
	if err := sc.Preload(context.Background(), []string{"ccc-three-3.0"}); !errors.Is(err, ErrStopped) {
		t.Errorf("a preload after the stop = %v, want ErrStopped", err)
	}
}

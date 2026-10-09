//go:build linux

package nfsd

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/amber-store/jaccard-nfs-nix-store/handles"
)

func TestServeEndsWhenTheListenerIsClosed(t *testing.T) {
	w := newWorld(t)
	srv := NewServer(w.fs)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(l) }()

	// A client that is connected when the server is closed is let go.
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	l.Close()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return when its listener was closed")
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != io.EOF && !isReset(err) {
		t.Fatalf("the client's connection after Close: %v, want it ended", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}
}

// flaky is a listener whose first accepts fail, as they do when the
// process is out of file descriptors for a moment.
type flaky struct {
	net.Listener
	failures atomic.Int32
	accepts  atomic.Int32
}

func (f *flaky) Accept() (net.Conn, error) {
	f.accepts.Add(1)
	if f.failures.Add(-1) >= 0 {
		return nil, &net.OpError{Op: "accept", Err: syscall.EMFILE}
	}
	return f.Listener.Accept()
}

func TestAnAcceptThatFailsIsTriedAgain(t *testing.T) {
	w := newWorld(t)
	srv := NewServer(w.fs)
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &flaky{Listener: inner}
	l.failures.Store(3)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(l) }()

	// A client that connects after the failures is served: the server
	// did not stop accepting.
	c, err := net.Dial("tcp", inner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline := time.Now().Add(10 * time.Second)
	for l.accepts.Load() < 5 { // three that failed, the client's, and the one that waits
		select {
		case err := <-served:
			t.Fatalf("Serve returned after an accept that failed: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d accepts: the server did not go on after the ones that failed", l.accepts.Load())
		}
		time.Sleep(time.Millisecond)
	}

	l.Close()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return when its listener was closed")
	}
	srv.Close()
}

func TestWaitIdle(t *testing.T) {
	w := newWorld(t)
	srv := NewServer(w.fs)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go srv.Serve(l)
	defer srv.Close()
	const settle = 30 * time.Millisecond

	// Nobody is connected, and nobody was for the time it takes.
	began := time.Now()
	if !srv.WaitIdle(10*time.Second, settle) {
		t.Fatal("a server nobody is connected to is not idle")
	}
	if took := time.Since(began); took < settle || took > 5*time.Second {
		t.Fatalf("it took %v to find the server idle, want about %v", took, settle)
	}

	// While a client is connected the server is not idle, however long.
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	began = time.Now()
	if srv.WaitIdle(150*time.Millisecond, settle) {
		t.Fatal("a server with a client connected is idle")
	}
	if took := time.Since(began); took < 150*time.Millisecond {
		t.Fatalf("it gave up after %v, before its limit", took)
	}

	// The client goes, and the server is idle soon after.
	idle := make(chan bool, 1)
	go func() { idle <- srv.WaitIdle(10*time.Second, settle) }()
	time.Sleep(50 * time.Millisecond)
	c.Close()
	select {
	case ok := <-idle:
		if !ok {
			t.Fatal("the server is not idle after its client went")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the server did not notice that its client went")
	}

	// A client that comes and goes while the server waits to be idle
	// starts the wait over: it is not idle before nobody came for settle.
	stop := make(chan struct{})
	knocked := make(chan struct{})
	go func() {
		defer close(knocked)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if c, err := net.Dial("tcp", l.Addr().String()); err == nil {
				c.Close()
			}
			time.Sleep(settle / 6)
		}
	}()
	if srv.WaitIdle(8*settle, settle) {
		t.Error("a server that clients keep coming to is idle")
	}
	close(stop)
	<-knocked
}

func isReset(err error) bool {
	_, ok := err.(*net.OpError)
	return ok
}

func TestOnlySoManyFilesAreKeptOpen(t *testing.T) {
	dir := t.TempDir()
	open := newOpenFiles(2)
	ids := make([]handles.ID, 5)
	for i := range ids {
		ids[i] = handles.Child(handles.Root, string(rune('a'+i)))
		if err := os.WriteFile(filepath.Join(dir, string(rune('a'+i))), []byte{byte('a' + i), '!'}, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	read := func(i int) {
		t.Helper()
		p := make([]byte, 2)
		if err := open.readAt(ids[i], filepath.Join(dir, string(rune('a'+i))), p, 0); err != nil {
			t.Fatal(err)
		}
		if p[0] != byte('a'+i) {
			t.Fatalf("file %d read as %q", i, p)
		}
	}
	for round := 0; round < 3; round++ {
		for i := range ids {
			read(i)
			if n := open.len(); n > 2 {
				t.Fatalf("%d files open, want 2 at most", n)
			}
		}
	}
	if n := open.len(); n != 2 {
		t.Fatalf("%d files open, want 2", n)
	}

	// A read that the file cannot answer costs it its place.
	if err := open.readAt(ids[4], filepath.Join(dir, "e"), make([]byte, 10), 0); err == nil {
		t.Fatal("ten bytes were read from a file of two")
	}
	if n := open.len(); n != 1 {
		t.Fatalf("%d files open after a failed read, want 1", n)
	}
	if err := open.readAt(ids[0], filepath.Join(dir, "missing"), make([]byte, 1), 0); err == nil {
		t.Fatal("a file that is not there was read")
	}
	// A link where a file is to be is not followed.
	if err := os.Symlink(filepath.Join(dir, "a"), filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := open.readAt(handles.Child(handles.Root, "link"), filepath.Join(dir, "link"), make([]byte, 1), 0); err == nil {
		t.Fatal("a symbolic link was read as a materialized file")
	}

	open.closeAll()
	if n := open.len(); n != 0 {
		t.Fatalf("%d files open after closeAll", n)
	}
	read(3) // and it opens files again
}

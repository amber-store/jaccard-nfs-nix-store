//go:build linux

package nfsd

import (
	"io"
	"net"
	"os"
	"path/filepath"
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

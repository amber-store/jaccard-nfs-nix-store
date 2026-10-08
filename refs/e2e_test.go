package refs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-nfs-nix-store/jstest"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
)

// The file type bits of a recorded mode.
const (
	modeType = 0o170000
	modeDir  = 0o040000
	modeFile = 0o100000
)

// noise returns n bytes that do not compress, the same for the same seed.
func noise(seed uint64, n int) []byte {
	b := make([]byte, n)
	r := rand.NewChaCha8([32]byte{byte(seed), byte(seed >> 8)})
	r.Read(b)
	return b
}

// writeDir writes files, named by their slash-separated paths, to a new
// directory.
func writeDir(t *testing.T, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// onDisk returns the regular files under dir by their slash-separated
// paths.
func onDisk(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)], err = os.ReadFile(p)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// inStore walks the tree of dir over get and adds its regular files to
// files by their paths.
func inStore(dir key.Key, at string, get func(key.Key) ([]byte, error), files map[string][]byte) error {
	entries, err := fstree.CollectEntries(dir, get)
	if err != nil {
		return fmt.Errorf("directory %q: %w", at, err)
	}
	for _, e := range entries {
		p := path.Join(at, string(e.Name))
		switch e.Mode & modeType {
		case modeDir:
			sub, err := key.Parse(e.ContentKey)
			if err != nil {
				return fmt.Errorf("directory %q: %w", p, err)
			}
			if err := inStore(sub, p, get, files); err != nil {
				return err
			}
		case modeFile:
			content, err := key.Parse(e.ContentKey)
			if err != nil {
				return fmt.Errorf("file %q: %w", p, err)
			}
			var buf bytes.Buffer
			if err := fstree.WriteContent(&buf, content, get); err != nil {
				return fmt.Errorf("file %q: %w", p, err)
			}
			files[p] = buf.Bytes()
		}
	}
	return nil
}

// sameFiles compares what the store has under root with the directory that
// was pushed, file by file.
func sameFiles(t *testing.T, s *refs.Store, root key.Key, dir string) {
	t.Helper()
	got := map[string][]byte{}
	if err := inStore(root, "", s.Get, got); err != nil {
		t.Fatal(err)
	}
	want := onDisk(t, dir)
	for name, content := range want {
		have, ok := got[name]
		switch {
		case !ok:
			t.Errorf("%s is not in the store", name)
		case !bytes.Equal(have, content):
			t.Errorf("%s: %d bytes in the store that are not the %d pushed", name, len(have), len(content))
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s is in the store and was not pushed", name)
		}
	}
}

func TestEndToEnd(t *testing.T) {
	srv := jstest.Start(t)

	files := map[string][]byte{
		"bin/tool":         noise(1, 300<<10),
		"lib/libone.so":    noise(2, 200<<10),
		"lib/deep/two.txt": []byte("two\n"),
		"share/doc/README": []byte("read me\n"),
		"empty":            nil,
	}
	v1 := writeDir(t, files)
	// The second version: one file changed, one added.
	files["share/doc/README"] = []byte("read me again\n")
	files["share/doc/NEWS"] = noise(3, 10<<10)
	v2 := writeDir(t, files)

	root1 := srv.PushDir(v1, "pkg-v1")
	root2 := srv.PushDir(v2, "pkg-v2")
	if root1 == root2 {
		t.Fatal("the two versions have one root")
	}

	cfg := srv.DialConfig()
	s, err := refs.Open(refs.Options{
		Prefix: "pkg-",
		Dir:    filepath.Join(t.TempDir(), "cache"),
		Dial: func(ctx context.Context) (refs.Conn, error) {
			return refs.Connect(ctx, cfg)
		},
		PullTimeout: 20 * time.Second,
		RetryWait:   50 * time.Millisecond,
		RetryMax:    500 * time.Millisecond,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, v := range []struct {
		name string
		root key.Key
		dir  string
	}{{"v1", root1, v1}, {"v2", root2, v2}} {
		root, err := s.Ensure(ctx, v.name)
		if err != nil {
			t.Fatalf("Ensure(%q): %v", v.name, err)
		}
		if root != v.root {
			t.Fatalf("Ensure(%q) = %s, want %s", v.name, root, v.root)
		}
		sameFiles(t, s, root, v.dir)
	}

	if _, err := s.Ensure(ctx, "v3"); !errors.Is(err, refs.ErrNotFound) {
		t.Fatalf("Ensure of a name never pushed: %v, want ErrNotFound", err)
	}
	pins := s.Pins()
	if len(pins) != 2 || pins[0].Name != "v1" || pins[1].Name != "v2" {
		t.Fatalf("pins %v, want v1 and v2", pins)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

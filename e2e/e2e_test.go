//go:build linux

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/amber-store/jaccard-nfs-nix-store/jstest"
	"github.com/amber-store/jaccard-nfs-nix-store/mount"
	"github.com/amber-store/jaccard-nfs-nix-store/preload"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
	"github.com/amber-store/jaccard-nfs-nix-store/sidecar"
	"golang.org/x/sys/unix"
)

// prefix is what the references of the test begin with on the server.
const prefix = "e2e/store/"

const bigSize = 64 << 20

func randomBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{seed}).Read(b)
	return b
}

func write(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

// storePath writes a directory as a path of a Nix store may be: a program,
// a large file, a directory of many files, links, and what is empty.
func storePath(t *testing.T, dir string) {
	t.Helper()
	write(t, filepath.Join(dir, "bin", "hello"), []byte("#!/bin/sh\necho hello from the store\n"), 0o755)
	write(t, filepath.Join(dir, "lib", "big.bin"), randomBytes(bigSize, 7), 0o644)
	write(t, filepath.Join(dir, "lib", "empty"), nil, 0o644)
	write(t, filepath.Join(dir, "share", "a name with spaces"), []byte("spaces\n"), 0o644)
	for i := range 3000 {
		write(t, filepath.Join(dir, "many", fmt.Sprintf("f%04d", i)), fmt.Appendf(nil, "file %d\n", i), 0o644)
	}
	if err := os.Mkdir(filepath.Join(dir, "empty-directory"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{
		"hello":    "bin/hello",
		"untidy":   "./bin//hello",
		"absolute": "/nix/store/somewhere-else/bin/tool",
		"dangling": "nothing/here",
	} {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	// What a NAR does not have is not served.
	if err := unix.Mkfifo(filepath.Join(dir, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// servedAs is how the target of a link reads in the mount when it is not
// what was recorded: the NFS server answers with a target in a form of
// its own, which names the same file.
var servedAs = map[string]string{"./bin//hello": "bin/hello"}

// served reports whether an entry of a source tree is one the mount has.
func served(e fs.DirEntry) bool {
	return e.IsDir() || e.Type().IsRegular() || e.Type()&fs.ModeSymlink != 0
}

func names(t *testing.T, dir string, keep func(fs.DirEntry) bool) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if keep == nil || keep(e) {
			out = append(out, e.Name())
		}
	}
	return out
}

func sum(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return [sha256.Size]byte(h.Sum(nil))
}

// sameTree holds the tree at got, in the mount, against the directory it
// was pushed from: names, types, link targets, contents, and the
// attributes as the sidecar serves them.
func sameTree(t *testing.T, want, got string) {
	t.Helper()
	wantNames, gotNames := names(t, want, served), names(t, got, nil)
	if !slices.Equal(wantNames, gotNames) {
		t.Errorf("%s lists %d names, want %d: %.5q..., want %.5q...", got, len(gotNames), len(wantNames), gotNames, wantNames)
		return
	}
	for _, name := range wantNames {
		wp, gp := filepath.Join(want, name), filepath.Join(got, name)
		wi, err := os.Lstat(wp)
		if err != nil {
			t.Fatal(err)
		}
		gi, err := os.Lstat(gp)
		if err != nil {
			t.Errorf("%v", err)
			continue
		}
		if wi.Mode().Type() != gi.Mode().Type() {
			t.Errorf("%s is a %v, want a %v", gp, gi.Mode().Type(), wi.Mode().Type())
			continue
		}
		if !gi.ModTime().Equal(wi.ModTime()) {
			t.Errorf("%s was modified %v, want %v", gp, gi.ModTime(), wi.ModTime())
		}
		if g, w := gi.Sys().(*syscall.Stat_t), wi.Sys().(*syscall.Stat_t); g.Uid != w.Uid || g.Gid != w.Gid {
			t.Errorf("%s belongs to %d:%d, want %d:%d", gp, g.Uid, g.Gid, w.Uid, w.Gid)
		}
		switch {
		case wi.IsDir():
			if gi.Mode().Perm() != 0o555 {
				t.Errorf("%s has mode %o, want 555", gp, gi.Mode().Perm())
			}
			sameTree(t, wp, gp)
		case wi.Mode()&fs.ModeSymlink != 0:
			wt, _ := os.Readlink(wp)
			if as, ok := servedAs[wt]; ok {
				wt = as
			}
			gt, err := os.Readlink(gp)
			if err != nil || gt != wt {
				t.Errorf("%s points at %q (%v), want %q", gp, gt, err, wt)
			}
			// The size of a link is the length of what a readlink gives.
			if gi.Size() != int64(len(gt)) {
				t.Errorf("%s says it is %d bytes, and points at the %d of %q", gp, gi.Size(), len(gt), gt)
			}
		default:
			mode := fs.FileMode(0o444)
			if wi.Mode()&0o111 != 0 {
				mode = 0o555
			}
			if gi.Mode().Perm() != mode {
				t.Errorf("%s has mode %o, want %o", gp, gi.Mode().Perm(), mode)
			}
			if gi.Size() != wi.Size() {
				t.Errorf("%s is %d bytes, want %d", gp, gi.Size(), wi.Size())
			}
			if sum(t, gp) != sum(t, wp) {
				t.Errorf("%s does not hold what %s does", gp, wp)
			}
		}
	}
}

// run runs the program at path, which is in the mount, and returns what it
// printed.
//
// The shell runs it, and not this process: this process is the NFS server
// as well. The thread that starts a program stands still until the program
// has been loaded, and loading it from the mount needs an answer from the
// server. A sidecar never runs anything from its own mount.
func run(t *testing.T, path string) string {
	t.Helper()
	out, err := exec.Command("/bin/sh", "-c", `exec "$0"`, path).CombinedOutput()
	if err != nil {
		t.Fatalf("running %s: %v: %s", path, err, out)
	}
	return string(out)
}

// dropCaches makes the kernel forget what it read, so that the next read
// asks the server.
func dropCaches(t *testing.T) {
	t.Helper()
	if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0); err != nil {
		t.Fatalf("dropping the caches: %v", err)
	}
}

// logTo writes what the sidecar logs to the test's log.
type logTo struct{ t *testing.T }

func (l logTo) Write(p []byte) (int, error) {
	l.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func TestTheStoreMounted(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("mounting needs root: scripts/test-linux.sh runs this in a privileged container")
	}
	srv := jstest.Start(t)
	src := t.TempDir()
	hello := filepath.Join(src, "hello")
	storePath(t, hello)
	srv.PushDir(hello, prefix+"hello")
	// A second version, which the server keeps as a patch pack on the
	// first: fetching it fetches both.
	hello2 := filepath.Join(src, "hello2")
	storePath(t, hello2)
	write(t, filepath.Join(hello2, "bin", "hello"), []byte("#!/bin/sh\necho hello again\n"), 0o755)
	write(t, filepath.Join(hello2, "share", "added"), []byte("added\n"), 0o644)
	srv.PushDir(hello2, prefix+"hello2")

	mnt, cache := t.TempDir(), t.TempDir()
	// Whatever becomes of the test, the mount is not left for the removal
	// of the directory to walk into.
	t.Cleanup(func() { mount.Detach(mnt) })
	cfg := sidecar.Config{
		Prefix: prefix,
		Cache:  cache,
		Dial: func(ctx context.Context) (refs.Conn, error) {
			return refs.Connect(ctx, srv.DialConfig())
		},
		Listen:       "127.0.0.1:0",
		Mount:        mnt,
		MountOptions: sidecar.DefaultMountOptions,
		Log:          slog.New(slog.NewTextHandler(logTo{t}, nil)),
	}
	sc, err := sidecar.Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			sc.Stop()
		}
	}()

	if fstype, mounted, err := mount.Mounted(mnt); err != nil || !mounted || fstype != "nfs4" {
		t.Fatalf("after the start: mounted %v, type %q, %v", mounted, fstype, err)
	}

	t.Run("nothing is listed before anything is named", func(t *testing.T) {
		if got := names(t, mnt, nil); len(got) != 0 {
			t.Fatalf("the root lists %q", got)
		}
	})

	t.Run("the kernel reads a megabyte at a time", func(t *testing.T) {
		table, err := os.ReadFile("/proc/mounts")
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(table), "\n") {
			if fields := strings.Fields(line); len(fields) > 3 && fields[1] == mnt {
				if !strings.Contains(fields[3], "rsize=1048576") || !strings.Contains(fields[3], "vers=4.1") {
					t.Fatalf("the mount has the options %s", fields[3])
				}
				return
			}
		}
		t.Fatal("the mount is not in /proc/mounts")
	})

	t.Run("a program runs from a path that was never fetched", func(t *testing.T) {
		if out := run(t, filepath.Join(mnt, "hello", "bin", "hello")); out != "hello from the store\n" {
			t.Fatalf("it printed %q", out)
		}
	})

	t.Run("a path is what was pushed", func(t *testing.T) {
		sameTree(t, hello, filepath.Join(mnt, "hello"))
	})

	t.Run("a path is read from its files once it is materialized", func(t *testing.T) {
		done := filepath.Join(cache, "files", "done", "hello")
		deadline := time.Now().Add(2 * time.Minute)
		for {
			if _, err := os.Stat(done); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the path was not materialized")
			}
			time.Sleep(50 * time.Millisecond)
		}
		if _, err := os.Stat(filepath.Join(cache, "files", "partial", "hello")); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("the partial tree is still there: %v", err)
		}
		// What is on disk is the content and nothing else.
		if sum(t, filepath.Join(done, "lib", "big.bin")) != sum(t, filepath.Join(hello, "lib", "big.bin")) {
			t.Error("the materialized file does not hold what was pushed")
		}
		if _, err := os.Lstat(filepath.Join(done, "absolute")); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("a link was materialized: %v", err)
		}
		// And the mount, asked again, still serves the tree.
		dropCaches(t)
		sameTree(t, hello, filepath.Join(mnt, "hello"))
	})

	t.Run("a path kept as a patch pack", func(t *testing.T) {
		sameTree(t, hello2, filepath.Join(mnt, "hello2"))
		if out := run(t, filepath.Join(mnt, "hello2", "bin", "hello")); out != "hello again\n" {
			t.Fatalf("it printed %q", out)
		}
	})

	t.Run("a file is opened by name without a lookup", func(t *testing.T) {
		// A reference that is one file, opened as the first thing that
		// is done to it.
		single := filepath.Join(src, "single")
		write(t, single, []byte("one file\n"), 0o644)
		srv.PushDir(single, prefix+"single")
		got, err := os.ReadFile(filepath.Join(mnt, "single"))
		if err != nil || string(got) != "one file\n" {
			t.Fatalf("%q, %v", got, err)
		}
	})

	t.Run("nothing can be written", func(t *testing.T) {
		for what, err := range map[string]error{
			"creating a file in a path":   os.WriteFile(filepath.Join(mnt, "hello", "new"), []byte("x"), 0o644),
			"creating a file in the root": os.WriteFile(filepath.Join(mnt, "new"), []byte("x"), 0o644),
			"removing a file":             os.Remove(filepath.Join(mnt, "hello", "lib", "empty")),
			"making a directory":          os.Mkdir(filepath.Join(mnt, "hello", "dir"), 0o755),
		} {
			if !errors.Is(err, unix.EROFS) {
				t.Errorf("%s: %v, want a read-only file system", what, err)
			}
		}
	})

	t.Run("a name that is missing, and then is not", func(t *testing.T) {
		late := filepath.Join(mnt, "late")
		if _, err := os.Stat(late); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("a name the server does not have: %v", err)
		}
		dir := filepath.Join(src, "late")
		write(t, filepath.Join(dir, "file"), []byte("late\n"), 0o644)
		srv.PushDir(dir, prefix+"late")
		// It is taken for missing for five seconds, and asked for again
		// after: neither the sidecar nor the kernel holds on to the answer.
		deadline := time.Now().Add(30 * time.Second)
		for {
			got, err := os.ReadFile(filepath.Join(late, "file"))
			if err == nil {
				if string(got) != "late\n" {
					t.Fatalf("%q", got)
				}
				break
			}
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatal(err)
			}
			if time.Now().After(deadline) {
				t.Fatal("the name stayed missing after it was pushed")
			}
			time.Sleep(250 * time.Millisecond)
		}
	})

	t.Run("the root lists what was fetched", func(t *testing.T) {
		got := names(t, mnt, nil)
		slices.Sort(got)
		if want := []string{"hello", "hello2", "late", "single"}; !slices.Equal(got, want) {
			t.Fatalf("the root lists %q, want %q", got, want)
		}
	})

	t.Run("the sidecar is restarted under the mount", func(t *testing.T) {
		big := filepath.Join(mnt, "hello2", "lib", "big.bin")
		want := randomBytes(bigSize, 7)
		open, err := os.Open(big)
		if err != nil {
			t.Fatal(err)
		}
		defer open.Close()
		head := make([]byte, 1<<20)
		if _, err := io.ReadFull(open, head); err != nil || !bytes.Equal(head, want[:1<<20]) {
			t.Fatalf("the first megabyte, before: %v", err)
		}

		// The sidecar goes without unmounting, as one that was killed,
		// and another is started on the same cache and address.
		addr := sc.Addr()
		if err := sc.Close(); err != nil {
			t.Fatal(err)
		}
		dropCaches(t)
		again := cfg
		again.Listen = addr.String()
		if sc, err = sidecar.Start(again); err != nil {
			t.Fatal(err)
		}

		// The file that was open is read on.
		rest, err := io.ReadAll(open)
		if err != nil {
			t.Fatalf("reading on in a file that was open across the restart: %v", err)
		}
		if !bytes.Equal(rest, want[1<<20:]) {
			t.Fatal("what was read after the restart is not the file")
		}
		// What was looked up before is there, what was not is found, and
		// a path that was never fetched is fetched.
		sameTree(t, hello2, filepath.Join(mnt, "hello2"))
		fresh := filepath.Join(src, "fresh")
		write(t, filepath.Join(fresh, "file"), []byte("fresh\n"), 0o644)
		srv.PushDir(fresh, prefix+"fresh")
		if got, err := os.ReadFile(filepath.Join(mnt, "fresh", "file")); err != nil || string(got) != "fresh\n" {
			t.Fatalf("a path fetched after the restart: %q, %v", got, err)
		}
	})

	t.Run("stopping unmounts", func(t *testing.T) {
		began := time.Now()
		stopped = true
		if err := sc.Stop(); err != nil {
			t.Fatal(err)
		}
		if _, mounted, err := mount.Mounted(mnt); err != nil || mounted {
			t.Fatalf("after the stop: mounted %v, %v", mounted, err)
		}
		if took := time.Since(began); took > 20*time.Second {
			t.Errorf("stopping took %v", took)
		}
		if got := names(t, mnt, nil); len(got) != 0 {
			t.Errorf("the directory the store was mounted on holds %q", got)
		}
	})
}

// When the grace period of a pod is over, the kubelet kills the app and
// tells the sidecar to stop in the same moment. The app is dying then: its
// mount namespace is gone, so the sidecar's unmount is not refused as
// busy, and the kernel is still closing the files the app had open, which
// takes an answer from the server for each. A sidecar that stopped serving
// the moment it had unmounted left the app waiting for those answers until
// the mount's timeouts ran out, and the pod with it.
func TestAProcessKilledAsTheSidecarStops(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("mounting needs root: scripts/test-linux.sh runs this in a privileged container")
	}
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("no unshare to give a process a mount namespace of its own")
	}
	srv := jstest.Start(t)
	dir := filepath.Join(t.TempDir(), "path")
	write(t, filepath.Join(dir, "data"), randomBytes(1<<20, 3), 0o644)
	srv.PushDir(dir, prefix+"path")

	mnt, cache := t.TempDir(), t.TempDir()
	t.Cleanup(func() { mount.Detach(mnt) })
	sc, err := sidecar.Start(sidecar.Config{
		Prefix: prefix,
		Cache:  cache,
		Dial: func(ctx context.Context) (refs.Conn, error) {
			return refs.Connect(ctx, srv.DialConfig())
		},
		Listen: "127.0.0.1:0",
		Mount:  mnt,
		// Requests that time out in seconds, not minutes: a process
		// that waits for a server that is gone ends within the test.
		MountOptions: "vers=4.1,soft,timeo=50,retrans=1,lookupcache=positive",
		Log:          slog.New(slog.NewTextHandler(logTo{t}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	// A process in a mount namespace of its own, as a container's is,
	// with a file of the store open.
	app := exec.Command("unshare", "-m", "--propagation", "private",
		"sh", "-c", `exec 3< "$0" && echo ready && exec sleep 1000`, filepath.Join(mnt, "path", "data"))
	out, err := app.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	app.Stderr = logTo{t}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make([]byte, len("ready\n"))
	if _, err := io.ReadFull(out, ready); err != nil || string(ready) != "ready\n" {
		app.Process.Kill()
		t.Fatalf("the process did not get the file open: %q, %v", ready, err)
	}

	if err := app.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	if err := sc.Stop(); err != nil {
		t.Errorf("stopping: %v", err)
	}
	t.Logf("the sidecar stopped after %v", time.Since(began))
	ended := make(chan struct{})
	go func() {
		app.Wait()
		close(ended)
	}()
	select {
	case <-ended:
		t.Logf("the process ended %v after it was killed", time.Since(began))
	case <-time.After(4 * time.Second):
		t.Error("the process that was killed has not ended: it is waiting for a server that has gone")
		<-ended
		t.Logf("it ended %v after it was killed", time.Since(began))
	}
	if _, mounted, err := mount.Mounted(mnt); err != nil || mounted {
		t.Errorf("after the stop: mounted %v, %v", mounted, err)
	}
}

// A sidecar that is told to stop while its mount is in use takes the
// mount out of the tree at once, and serves on for whoever still has a
// file of it open.
func TestStoppingWhileAFileIsOpen(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("mounting needs root: scripts/test-linux.sh runs this in a privileged container")
	}
	srv := jstest.Start(t)
	dir := filepath.Join(t.TempDir(), "path")
	data := randomBytes(1<<20, 5)
	write(t, filepath.Join(dir, "data"), data, 0o644)
	srv.PushDir(dir, prefix+"path")

	mnt, cache := t.TempDir(), t.TempDir()
	t.Cleanup(func() { mount.Detach(mnt) })
	sc, err := sidecar.Start(sidecar.Config{
		Prefix: prefix,
		Cache:  cache,
		Dial: func(ctx context.Context) (refs.Conn, error) {
			return refs.Connect(ctx, srv.DialConfig())
		},
		Listen:       "127.0.0.1:0",
		Mount:        mnt,
		MountOptions: "vers=4.1,soft,timeo=50,retrans=1,lookupcache=positive",
		Log:          slog.New(slog.NewTextHandler(logTo{t}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	open, err := os.Open(filepath.Join(mnt, "path", "data"))
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()

	began := time.Now()
	stopped := make(chan error, 1)
	go func() { stopped <- sc.Stop() }()

	// The mount is gone from the tree long before the sidecar is: it was
	// not waited for.
	for {
		_, mounted, err := mount.Mounted(mnt)
		if err != nil {
			t.Fatal(err)
		}
		if !mounted {
			break
		}
		if time.Since(began) > time.Second {
			t.Fatal("a second after it was told to stop, the sidecar's mount is still in the tree")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("the mount was out of the tree after %v", time.Since(began))
	select {
	case err := <-stopped:
		t.Fatalf("the sidecar stopped while a file of its store was open: %v", err)
	default:
	}

	// The file that is open is read, and closed, while the sidecar waits.
	got := make([]byte, len(data))
	if _, err := io.ReadFull(open, got); err != nil || !bytes.Equal(got, data) {
		t.Errorf("reading the open file after the mount was detached: %v", err)
	}
	open.Close()
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("stopping: %v", err)
		}
		t.Logf("the sidecar stopped %v after it was told to", time.Since(began))
	case <-time.After(4 * time.Second):
		t.Error("the sidecar did not stop when the last file was closed")
		<-stopped
	}
}

// mountsOn counts the NFS mounts on dir: one over the other they would be
// more than one.
func mountsOn(t *testing.T, dir string) int {
	t.Helper()
	table, err := os.ReadFile("/proc/self/mounts")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(string(table), "\n") {
		if fields := strings.Fields(line); len(fields) > 2 && fields[1] == dir && strings.HasPrefix(fields[2], "nfs") {
			n++
		}
	}
	return n
}

// A sidecar that was killed leaves its mount. The one that is started
// after it serves that mount again if the kernel finds it, which it does
// in the same pod at the same address. If the kernel does not, because
// the mount is of another network namespace or another port, the mount is
// dead for good: it is taken away and the server mounted anew, where a
// sidecar that only saw "an NFS mount is there" would serve nobody.
func TestALeftoverMountThatDoesNotReachTheSidecar(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("mounting needs root: scripts/test-linux.sh runs this in a privileged container")
	}
	srv := jstest.Start(t)
	dir := filepath.Join(t.TempDir(), "path")
	write(t, filepath.Join(dir, "data"), []byte("content\n"), 0o644)
	srv.PushDir(dir, prefix+"path")

	mnt, cache := t.TempDir(), t.TempDir()
	t.Cleanup(func() { mount.Detach(mnt) })
	cfg := sidecar.Config{
		Prefix: prefix,
		Cache:  cache,
		Dial: func(ctx context.Context) (refs.Conn, error) {
			return refs.Connect(ctx, srv.DialConfig())
		},
		Listen:       "127.0.0.1:0",
		Mount:        mnt,
		MountOptions: "vers=4.1,soft,timeo=50,retrans=1,lookupcache=positive",
		AdoptTimeout: 2 * time.Second,
		Log:          slog.New(slog.NewTextHandler(logTo{t}, nil)),
	}
	first, err := sidecar.Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(mnt, "path", "data")); err != nil || string(got) != "content\n" {
		t.Fatalf("through the first sidecar: %q, %v", got, err)
	}
	// It goes without unmounting, and the next one listens elsewhere: the
	// mount that is left asks for a server at a port nobody answers on.
	was := first.Addr()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	second, err := sidecar.Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	if second.Addr() == was {
		t.Skip("the second sidecar was given the port of the first: the mount that was left is not dead")
	}
	t.Logf("the second sidecar was started in %v", time.Since(began))

	if n := mountsOn(t, mnt); n != 1 {
		t.Fatalf("%d NFS mounts on the directory, want the one of the second sidecar", n)
	}
	read := make(chan error, 1)
	go func() {
		got, err := os.ReadFile(filepath.Join(mnt, "path", "data"))
		if err == nil && string(got) != "content\n" {
			err = fmt.Errorf("read %q", got)
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("through the second sidecar: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the store does not answer: the mount that was left is still the one that is seen")
	}

	began = time.Now()
	if err := second.Stop(); err != nil {
		t.Errorf("stopping: %v", err)
	}
	if took := time.Since(began); took > 4*time.Second {
		t.Errorf("stopping took %v", took)
	}
	if n := mountsOn(t, mnt); n != 0 {
		t.Errorf("%d NFS mounts on the directory after the stop", n)
	}
}

// A list of store paths sent to the sidecar is fetched before anything
// looks the paths up: they are in the store when the answer comes.
func TestPreloading(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("mounting needs root: scripts/test-linux.sh runs this in a privileged container")
	}
	srv := jstest.Start(t)
	src := t.TempDir()
	paths := []string{"aaa-one-1.0", "bbb-two-2.0", "ccc-three-3.0", "ddd-four-4.0"}
	for _, name := range paths {
		write(t, filepath.Join(src, name, "share", "name"), []byte(name+"\n"), 0o644)
		srv.PushDir(filepath.Join(src, name), prefix+name)
	}

	mnt, cache := t.TempDir(), t.TempDir()
	t.Cleanup(func() { mount.Detach(mnt) })
	sc, err := sidecar.Start(sidecar.Config{
		Prefix: prefix,
		Cache:  cache,
		Dial: func(ctx context.Context) (refs.Conn, error) {
			return refs.Connect(ctx, srv.DialConfig())
		},
		Listen:        "127.0.0.1:0",
		Mount:         mnt,
		MountOptions:  sidecar.DefaultMountOptions,
		PreloadListen: "127.0.0.1:0",
		PreloadJobs:   2,
		Log:           slog.New(slog.NewTextHandler(logTo{t}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Stop()
	url := "http://" + sc.PreloadAddr().String() + preload.Path
	post := func(list string) (int, string) {
		t.Helper()
		res, err := http.Post(url, "text/plain", strings.NewReader(list))
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res.StatusCode, string(body)
	}

	if got := names(t, mnt, nil); len(got) != 0 {
		t.Fatalf("the store holds %q before anything was asked for", got)
	}
	// A path of the store, a path into one, and a name by itself.
	status, body := post("/nix/store/aaa-one-1.0\n/nix/store/bbb-two-2.0/share/name\nccc-three-3.0\n")
	if status != http.StatusOK || body != "fetched 3 paths\n" {
		t.Fatalf("status %d: %s", status, body)
	}
	// They are in the store, and nobody has looked one of them up: the
	// root lists what was fetched.
	got := names(t, mnt, nil)
	slices.Sort(got)
	if want := paths[:3]; !slices.Equal(got, want) {
		t.Fatalf("after the preload the store holds %q, want %q", got, want)
	}
	for _, name := range paths[:3] {
		if content, err := os.ReadFile(filepath.Join(mnt, name, "share", "name")); err != nil || string(content) != name+"\n" {
			t.Errorf("%s: %q, %v", name, content, err)
		}
	}

	// What is there is not fetched again.
	if status, body := post("aaa-one-1.0\nbbb-two-2.0\n"); status != http.StatusOK || body != "fetched 2 paths\n" {
		t.Errorf("a list of what is there: status %d: %s", status, body)
	}
	// A path the server does not have ends the request and is named.
	status, body = post("/nix/store/ddd-four-4.0\n/nix/store/zzz-missing-0.0/bin/x\n")
	if status != http.StatusNotFound || !strings.Contains(body, "/nix/store/zzz-missing-0.0") {
		t.Errorf("a list with a missing path: status %d: %s", status, body)
	}
	if status, body := post("/etc/passwd\n"); status != http.StatusBadRequest {
		t.Errorf("a list with a line that is no path: status %d: %s", status, body)
	}
	if res, err := http.Get(url); err != nil || res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("a GET: %v, %v", res, err)
	}
}

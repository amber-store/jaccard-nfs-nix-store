# jaccard-nfs-nix-store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A sidecar that serves the references of a jaccard-store server
under one prefix as a read-only NFSv4.1 mount, fetching each on first
lookup and materializing it as plain files in the background.

**Architecture:** One Go process. `refs` pulls a reference into a core
packstore and pins it; `materialize` writes a pinned reference out as
files; `tree` reads directories and file content from the objects;
`handles` gives every path a stable 16-byte handle; `nfsd` puts those
behind Buildbarn's NFSv4 server; `mount` mounts it. The owner asked for the
implementation to proceed without a review of this plan; the code of each
step is written against the interfaces below rather than repeated here.

**Tech Stack:** Go 1.27; `amber-store/core` v0.10.0; `amber-store/jaccard-store`
v0.7.0; `buildbarn/bb-remote-execution` at `582d07ff2acc` with `bb-storage`
and `go-xdr`; `zeebo/blake3`; `urfave/cli/v2`; standard `testing`.

**Spec:** `docs/superpowers/specs/2026-10-08-jaccard-nfs-nix-store-design.md`

## Global Constraints

- Module `github.com/amber-store/jaccard-nfs-nix-store`. No `internal/` packages: every package is at the top level.
- `refs`, `tree`, `materialize`, `handles`, `reclog` and `jstest` import nothing of Buildbarn and build and test on macOS and Linux.
- `nfsd`, `mount` (its syscalls), `e2e` and `cmd/jaccard-nfs-nix-store` are Linux only: every file that imports Buildbarn or calls `mount(2)` begins with `//go:build linux`.
- Tests use the standard `testing` package, no assertion library. A test is written, and seen to fail, before the code it covers.
- Comments and messages follow the sibling repository: plain sentences that say what a thing is for; errors lower-case, wrapped with `%w`, prefixed by what was being done.
- `go.mod` and `go.sum` are changed only in Task 1 and Task 11. Nobody runs `go mod tidy` in between.
- Nothing is committed that was built: binaries are removed after use.
- Defaults, copied from the spec: listen `127.0.0.1:2049`; mount options `vers=4.1,soft,timeo=600,retrans=2,lookupcache=positive`; pull timeout `2m`; pull jobs `4`; materialize jobs `2`; missing-name memory `5s`; retry wait `1s` doubling to `15s`; blob cache `64 MiB`; open files `256`; announced read size `1 MiB`; leases `120s` enforced, `60s` announced.

## Review Focus

- **A directory read in several pieces.** A listing larger than one answer is continued from a cookie; nothing is skipped or repeated. Tests: `tree` cookie continuation, `nfsd` listing with a reporter that stops after every entry, `e2e` a directory of 3,000 files.
- **Names that are not tidy.** A name with a space, a newline or bytes that are not UTF-8 goes through the handle table, the pin file and the materialized tree unchanged. Tests: `reclog`/`handles` round trip of such a name, `materialize` a file of such a name.
- **Many names looked up at once.** Twenty lookups of twenty names start no more pulls at a time than `--pull-jobs`, and all are answered. Test: `refs` job limit.
- **A read across the change of source.** A file read half from the objects and half from its materialized file is the file. Test: `nfsd` read before and after.
- **An empty reference and an empty file.** A directory with nothing in it and a file of no bytes are fetched, listed, read and materialized. Tests: `tree`, `materialize`, `e2e` tree.

---

## File structure

```
go.mod, go.sum, LICENSE, COPYING, .gitignore, .dockerignore, .envrc, flake.nix, Dockerfile, README.md
.github/workflows/test.yml, release.yml
reclog/reclog.go, reclog_test.go          an append-only file of records
handles/handles.go, handles_test.go       the handle of a path; the table
tree/tree.go, cache.go, tree_test.go      objects as directories and files
materialize/materialize.go, sync_linux.go, sync_other.go, materialize_test.go
refs/refs.go, pins.go, connect.go, refs_test.go, e2e_test.go
jstest/jstest.go                          a jaccard-store server in one process, for tests
nfsd/doc.go, fs.go, root.go, dir.go, leaf.go, attrs.go, maxread.go, server.go, *_test.go
mount/mountinfo.go, mount_linux.go, mountinfo_test.go
cmd/jaccard-nfs-nix-store/main.go, main_test.go
e2e/doc.go, e2e_test.go
deploy/pod.yaml
```

## Interfaces

These are fixed. A task implements exactly these names and signatures.

```go
// package reclog
func Open(path string) (*Log, [][]byte, error) // every whole record; a tail cut short or failing its checksum is cut off
func (l *Log) Append(rec []byte) error          // buffered
func (l *Log) Flush() error                     // to the file
func (l *Log) Sync() error                      // Flush, then fsync
func (l *Log) Close() error                     // Flush, then close
// A record on disk: uvarint length, the bytes, CRC-32C of the bytes (4 bytes, big-endian).

// package handles
type ID [16]byte
var Root ID                                     // sixteen zero bytes
func Child(parent ID, name string) ID           // first 16 bytes of BLAKE3(parent || name)
func (id ID) Inode() uint64                     // the first eight bytes, little-endian
func Open(path string) (*Table, error)          // reads the file; flushes every second
func (t *Table) Add(parent ID, name string) ID  // Child(parent, name), recorded once
func (t *Table) Path(id ID) ([]string, bool)    // names from the root down; Root is (nil, true)
func (t *Table) Len() int
func (t *Table) Flush() error
func (t *Table) Close() error
// A record: the parent's 16 bytes, then the name.

// package tree
type Getter func(key.Key) ([]byte, error)
type Kind int
const ( Dir Kind = iota + 1; File; Symlink )
type Node struct {
    Kind     Kind
    Key      key.Key   // Dir: the directory's key. File: the content key.
    Target   string    // Symlink
    Size     uint64    // File: bytes of content. Symlink: len(Target). Dir: 0.
    Exec     bool      // any execute bit of the recorded mode
    UID, GID uint32
    Mtime    time.Time
}
type Entry struct { Name string; Node }
type Options struct { Dirs, Files int; BlobBytes int64 } // zero values: 4096, 1024, 64 MiB
var ErrNotFound = errors.New("tree: no such entry")
func New(get Getter, opts Options) *Reader
func (r *Reader) Root(root key.Key) (Node, error)                 // of a reference: Dir for a directory or commit key, File for content; UID, GID 0, Mtime 1s, Exec true
func (r *Reader) Lookup(dir key.Key, name string) (Node, error)   // ErrNotFound for a missing name and for a type not served
func (r *Reader) List(dir key.Key) ([]Entry, error)               // served entries in stored order; the slice is shared, not to be changed
func (r *Reader) ReadAt(content key.Key, p []byte, off int64) (int, error) // as io.ReaderAt

// package materialize
type Options struct { Jobs int; Log *slog.Logger }                 // zero Jobs: 2
func Open(dir string, get func(key.Key) ([]byte, error), opts Options) (*Store, error) // dir is <cache>/files; empties dir/partial
func (s *Store) Queue(name string, root key.Key)                   // once per name; nothing if done, queued, being written or failed
func (s *Store) Done(name string) bool
func (s *Store) Path(name string, rel []string) (string, bool)     // dir/done/name/rel...; false until the reference is whole
func (s *Store) Wait()                                             // until nothing is queued or being written
func (s *Store) Close() error                                      // stops the writing, waits for the workers

// package refs
type Conn interface {
    Pull(ctx context.Context, objects *packstore.Store, name string, opts client.PullOptions) (client.PullResult, error)
    Close() error
}
type Pin struct { Name string; Root key.Key }
type Options struct {
    Prefix      string
    Dir         string                                   // the cache directory: packstore/ and refs
    Dial        func(ctx context.Context) (Conn, error)
    PullTimeout time.Duration                            // 2m
    PullJobs    int                                      // 4
    MissingFor  time.Duration                            // 5s
    RetryWait   time.Duration                            // 1s
    RetryMax    time.Duration                            // 15s
    OnPin       func(name string, root key.Key)          // once for every new pin, after it is on disk
    Log         *slog.Logger
}
var ErrNotFound = errors.New("refs: no such reference")
func Open(opts Options) (*Store, error)
func (s *Store) Ensure(ctx context.Context, name string) (key.Key, error)
func (s *Store) Pinned(name string) (key.Key, bool)
func (s *Store) Pins() []Pin                              // in the order pinned; a copy
func (s *Store) Get(k key.Key) ([]byte, error)            // an object of the packstore
func (s *Store) Close() error
func Connect(ctx context.Context, cfg node.DialConfig) (Conn, error) // node.Dial with wire.ALPN, client.New over it
// A pin on disk (reclog record): the root's 32 bytes, then the name.

// package jstest
func Start(t testing.TB) *Server                         // server, database, in-memory bucket, iroh on loopback; cleaned up with t
func (s *Server) DialConfig() node.DialConfig            // a new client key, the server's ID and addresses
func (s *Server) PushDir(dir, name string) key.Key       // imports dir with core's ingest and pushes it; fails t on error

// package mount (Linux)
func Mount(target string, server netip.AddrPort, options string) error
func Unmount(target string) error                        // plain; EBUSY comes back as it is
func Detach(target string) error                         // MNT_DETACH
func Mounted(target string) (fstype string, mounted bool, err error)
```

---

### Task 1: The module and its dependencies

**Files:** `go.mod`, `go.sum`, `LICENSE`, `COPYING`, `.gitignore`, `.dockerignore`, `seed/seed.go`, `seed/seed_linux.go`

- [x] `go mod init github.com/amber-store/jaccard-nfs-nix-store`, `go 1.27.1`.
- [x] `seed/` blank-imports every package the tasks need (core, jaccard-store with its `server`, `bucket/buckettest`, `client`, `node`, `wire`; blake3; cli; x/sys/unix; and, in the Linux file, Buildbarn's `virtual`, `nfsv4`, bb-storage `clock`, `filesystem`, `filesystem/path`, `random`, go-xdr `protocols/nfsv4`, `protocols/rpcv2`, `rpcserver`), so that one `go mod tidy` fills `go.sum` for all of them. Task 11 removes it.
- [x] `go build ./...` on macOS and `GOOS=linux go build ./...` both succeed.
- [x] `LICENSE` and `COPYING` copied from jaccard-store. Commit.

### Task 2: `reclog`

- [x] Tests first: records appended and read back in order; an empty and a 70,000-byte record; a name with a newline and bytes that are not UTF-8; a file cut in the middle of the last record loses that record, is cut back to the last whole one, and takes new records after; a flipped byte in the last record does the same; a missing file is created; `Sync` leaves the bytes in the file.
- [x] Implement, run, commit.

### Task 3: `handles`

- [x] Tests first: `Child` is the same in two processes' worth of tables and differs for two names and for two parents; `Root` is zero; `Inode` of a known ID; `Add` twice writes one record; `Path` of a node three deep, of `Root`, of an unknown ID; a table closed and opened again resolves everything added, a name with a newline among it; a record cut short is dropped and the rest kept; 10,000 adds from eight goroutines under `-race`.
- [x] Implement, run, commit.

### Task 4: `tree` (parallel)

- [x] Tests first, over directories written to a temp dir and imported into a temporary packstore with core's `ingest` (as `importDir` in jaccard-store's `cmd/jaccard-store/dir.go` does): `Root` of a directory and of a single file; `Lookup` of a file, a directory, a link, a missing name; attributes (exec bit, mtime, size, uid) against `os.Lstat`; `List` in order, equal to `os.ReadDir`; a directory of 3,000 files listed and each looked up; an empty directory; `ReadAt` at 0, in the middle, across a leaf boundary of a 9 MiB file, at the end (`io.EOF`), past the end, of an empty file, of a one-blob file, each against the file's bytes; a FIFO in the source left out of `List` and `ErrNotFound` by name; the same reads from eight goroutines under `-race`; that a second `ReadAt` of the same blob does not call the getter again.
- [x] Implement with three caches (decoded directories, leaf lists by content key, blobs by bytes), run, commit.

### Task 5: `materialize` (parallel)

- [x] Tests first, over imported directories as in Task 4: a reference written out equals its source file by file (nested and empty directories, an empty file, a 9 MiB file, a name with spaces and a newline), with a link and a FIFO left out; modes `0755`/`0644`; a single-file reference is the file `done/<name>`; `Done` and `Path` false while the getter is held on the last blob and true after; `Path` joins `rel`; `Queue` twice writes once; with `Jobs: 2` and four references no more than two are in flight; `Open` removes what was under `partial`; a tree already under `done` is `Done` after `Open` without writing; a getter that fails leaves no tree, `Done` false, and a second `Queue` does nothing; `Close` in the middle returns and leaves nothing under `done`; an entry name with a `/` in it (built by hand with `fstree.EncodeDirLeaf`) fails the reference.
- [x] Implement: workers, `syncfs` on Linux and per-file sync elsewhere, rename, parent directory synced. Run, commit.

### Task 6: `refs` and `jstest` (parallel)

- [x] `refs` tests first, against a fake `Conn` that writes a prepared set of objects into the packstore it is given: a pull pins and returns the root; a second `Ensure` does not call the fake; two at once share one pull; `ErrNotFound` from `client.ErrNotFound`, remembered for `MissingFor` and asked again after; a name with `/`, `.`, `..`, empty, or invalid with the prefix is `ErrNotFound` without a call; a failure retried and then succeeding, with a new `Dial` after the failure; a failure outlasting `PullTimeout` is an error that is not `ErrNotFound`, and the next `Ensure` starts over; twenty names with `PullJobs: 4` never have more than four pulls running; `Ensure` whose context ends returns while the pull goes on and pins; pins survive `Close` and `Open`, in order; `OnPin` once per pin, after `Pinned` is true; `Pins` in order.
- [x] `jstest` after jaccard-store's `e2e/e2e_test.go` (`newWorld`, `buckettest.New`), without the clock and the backends.
- [x] `refs` end to end (`e2e_test.go`, package `refs_test`): two versions of a directory pushed with `PushDir`, both fetched through `Ensure` with `Connect`, and a tree walk of each root over `Store.Get` equal to the directory; a name that was never pushed is `ErrNotFound`.
- [x] Implement, run under `-race`, commit.

### Task 7: `nfsd`

- [x] `maxread.go` from the probe, tests first: an answer with the supported-attributes list gains the two bits; values spliced after the first word's values with and without a file handle among them; a request that does not ask is left alone; an unknown attribute in the first word leaves the answer alone.
- [x] `fs.go`: `FS` over small interfaces (`Refs`, `Files`), `tree.Reader`, `handles.Table`; the resolver; a cache of resolved nodes and of open files (256).
- [x] `root.go`, `dir.go`, `leaf.go`, `attrs.go`: the nodes. Tests through Buildbarn's interfaces with a fake `Refs` over an imported directory: root lookup, open by name, listing by cookie with a reporter that stops after each entry, change ID; directory, file, link; attributes of the table in spec 4.3; mutations refused; read before and after materialization (the file on disk altered, the altered bytes returned); a materialized file removed, read from the objects; resolver over a table read back; an unknown and a short handle.
- [x] `server.go`: the two programs, the wrapper, the RPC server, `Serve(listener)`.
- [x] Run in a Linux container, commit.

### Task 8: `mount`

- [x] `mountinfo.go` parser with tests over captured `/proc/self/mountinfo` text: the target found with its type, a path with an escaped space, a target that is only a prefix of a mount point, the last of two mounts stacked on one point.
- [x] `mount_linux.go`: `Mount` with `MS_RDONLY|MS_NOSUID|MS_NODEV`, `Unmount`, `Detach`, `Mounted`. Commit.

### Task 9: The command

- [x] Flags and variables of spec section 9 with `urfave/cli/v2`; a test of each flag against its variable through a seam that records the settings.
- [x] Wiring: `refs.Open` with `OnPin` queueing into `materialize`; every pin without a tree queued at start; `tree`, `handles`, `nfsd`; listen; mount unless mounted already.
- [x] Shutdown of spec section 8: first signal cancels materializing, unmounts (2 s of retries on `EBUSY`, then detach and 5 s more of serving), flushes the table, closes; second signal exits. Commit.

### Task 10: `e2e`, with the kernel

- [x] Tests that skip unless root on Linux: `jstest` server, a tree pushed (3,000-file directory, 64 MiB file, executable script, links, empty directory and file), the sidecar's parts started in-process and mounted on a temp dir; the walk compared with the source; the script run; a missing name; a file read while materializing is held and again after; `rsize` in `/proc/mounts` is 1048576; the NFS server stopped and started under the mount with a file open; unmount.
- [x] `scripts/test-linux.sh`: the whole suite in a privileged `golang:1.27` container. Run it, commit.

### Task 11: Image, CI, README, pod

- [x] Remove `seed/`, `go mod tidy`.
- [x] `Dockerfile` (golang:1.27-alpine to alpine, one command, root), `flake.nix` and `.envrc` after jaccard-store's with Go 1.27, `.github/workflows/test.yml` (gofmt, vet, tests, race, the kernel tests under `sudo`) and `release.yml` (image and release on a tag), `deploy/pod.yaml` of spec section 10, `README.md`.
- [x] Build the image, run it privileged against the real server with a prefix of the image's architecture, run a program out of the mount. Commit.

### Task 12: Publish

- [x] Bring the spec in line with what was built. Create `amber-store/jaccard-nfs-nix-store` public, push `main`, watch CI.

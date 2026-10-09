# jaccard-nfs-nix-store: design

Date: 2026-10-08. Status: approved by the owner and implemented. Section
2.3 records what a throwaway probe measured before, section 2.4 what the
implementation settled; where the code needed something this document did
not say, the document was brought in line.

## 1. Purpose

A sidecar that gives the other containers of a pod a directory in which
every reference of a [jaccard-store](https://github.com/amber-store/jaccard-store)
server under one name prefix appears as a directory or a file, and is
fetched the first time it is named. With the prefix under which a machine's
`/nix/store` was pushed, mounted at `/nix/store`, a container runs programs
out of a Nix store it does not hold: what is touched is fetched, the rest
never is.

It is four things in one process:

- the **fetching**: a reference is pulled from the jaccard-store server into
  a local [Amber-Store Core](https://github.com/amber-store/core) packstore
  when its name is first looked up;
- the **materializing**: a fetched reference is written out as plain files,
  in the background;
- an **NFSv4 server** on the pod's loopback that serves a read-only tree:
  names, attributes and links from the objects in the packstore, and the
  contents of files from the objects until the reference is materialized
  and from its files after;
- the **mount** of that server onto a volume the app containers share.

Success: a pod whose app container has an image without the program it
runs, and whose command is a path under `/nix/store`, starts and runs, with
only the store paths the program touched in the sidecar's cache; a sidecar
that is killed and restarted leaves the app running.

## 2. Decisions

### 2.1 Made by the owner

- The shape is that of the brief: the sidecar serves NFS on `127.0.0.1`
  and mounts it onto a volume shared with the app container, which stays
  unprivileged and sees a directory.
- References come from a jaccard-store server, by a name prefix; each is a
  directory or a file; a reference is fetched when its directory is
  stat'ed or something in it is accessed.
- New repository `amber-store/jaccard-nfs-nix-store`, **public**. Module
  `github.com/amber-store/jaccard-nfs-nix-store`. No `internal/` packages.
- Listing the root shows **only what has been fetched**. Any name still
  resolves when it is looked up.
- The NFS server is **Buildbarn's** (`github.com/buildbarn/bb-remote-execution`,
  packages `pkg/filesystem/virtual` and `pkg/filesystem/virtual/nfsv4`),
  chosen over go-nfs and over Rust's nfsserve. It speaks NFSv4.0 and 4.1
  and no NFSv3: the mount is NFSv4.1.
- A reference that is hit is served from the packstore at once and
  **materialized** on disk at the same time; once it is materialized, its
  files are served from there. That is more to build, and is for the sake
  of reads.
- A sample application is deployed when the sidecar works. Its entry point
  is the owner's to name, and is asked for then.

### 2.2 Made in design, for the owner to accept

- The prefix is a **plain string**, as `push-subdirs --prefix` has it: the
  name `X` in the root is the reference `prefix + X`. A store pushed with
  the prefix `/framework/nix/store` has references called
  `/framework/nix/store<hash>-<name>`, and one pushed with
  `/laptop/nix/store/` has `/laptop/nix/store/<hash>-<name>`: the prefix
  to serve them with is the one they were pushed with, slash or none.
- A reference is pulled **whole** into a core packstore. A pack is one zstd
  stream, so nothing smaller than a reference can be fetched.
- What is materialized is the **contents of regular files**, in a directory
  tree of the reference's own shape. Names, types, attributes and link
  targets are answered from the objects before and after: they are decoded
  once and held in memory, and the tree on disk then does not have to
  reproduce owners, modes, times or links.
- A reference is materialized when its **whole** tree is written and
  synced: the tree is built under another name and renamed. Files do not
  change over one by one.
- Only a lookup of a name **in the root** fetches. Everything under a
  fetched reference is local.
- A file handle is a **hash of the path**, and the table from handles back
  to paths is kept on disk, so handles outlive the process.
- The mount is `soft`. A request the server does not answer fails with
  `EIO` after three minutes instead of hanging for ever.
- Ownership and modification times are served as recorded. Permissions are
  read, and execute where the entry has it; nothing is writable.
- A reference keeps the root it had when it was first fetched for as long
  as the cache lives.
- LGPL-3.0-only, as the sibling projects. The Nix build is gonixgo's, CI
  and the release workflow are after jaccard-store's.

### 2.3 Found by the probe

A throwaway program served a small tree through Buildbarn's packages and
was mounted by the Linux kernel client (7.0, arm64) in a privileged
container.

- The packages work as a library in a plain Go module. The kernel mounts
  the server with `mount(2)`, type `nfs4`, `vers=4.1`, and nothing but
  `addr=` and `port=`: no MOUNT protocol and no port mapper.
- Listing, reading, symbolic links, running a script, modes, owners and
  times came out as served; 16 MiB read back with the right hash.
- Requests are handled **concurrently**: a lookup that took 8 s did not
  delay a read elsewhere.
- Errors arrive as themselves: a missing name is `ENOENT`, a failed fetch
  is `EIO`, a write is `EROFS`.
- A name is resolved by **two** operations, LOOKUP and OPEN by name
  (`VirtualLookup`, `VirtualOpenChild`), and a listing hands out handles
  without either. All three have to be covered.
- With handles that are the same in every run, the server was killed and
  started again under the mount: reads, listings and a file that was open
  across the restart went on, with the page cache dropped.
- Buildbarn does not announce a maximum read size, and Linux then reads
  **1024 bytes** at a time (256 MiB in 4.4 s on loopback). A wrapper that
  adds the two attributes to the answers brought it to 1 MiB (256 MiB in
  0.13 s).
- Without `lookupcache=positive` a name that was missing stayed missing
  after it had appeared.
- A mount whose server is gone holds up the unmount, and with it the end
  of the container: twice the request timeout (6 min at the default
  `timeo=600,retrans=2`). Unmounting while the server answers took no
  time. After a restart of the server a plain unmount was refused as busy.
- `bb-storage` does not compile for macOS outside Bazel, which patches
  `golang.org/x/sys` for it. Linux compiles as it is.
- The head of `bb-remote-execution` needs Go 1.27.1. The probe was 31 MB,
  stripped.

### 2.4 Made in implementation

- The pins and the handle table are kept in one kind of file (`reclog`):
  records one after another, each with its length before it and a CRC-32C
  after. A record that a crash cut short, or that fails its checksum, ends
  the file there.
- One attempt at a pull may take an hour. A transfer that hangs is ended
  then and counts as a failure, so that it does not keep its name from
  being fetched for ever.
- Resolved nodes are not kept. A handle is resolved again every time,
  through the table and the directories the tree has decoded, which costs
  a few lookups in memory.
- The parts are put together in a package of their own, `sidecar`, which
  the command starts and the end-to-end tests start too: what is tested
  with the kernel is what runs.
- A mount that is in use when the sidecar is told to stop is detached at
  once and not waited for, and a mount that an earlier run left is served
  again only if the kernel behind it reaches the server (section 8). Both
  came of asking what leaves a mount on a node (8.1).
- The first deployment found that a sidecar must not stop serving the
  moment it has unmounted (section 8): an app that ignored SIGTERM was
  killed as the sidecar was stopped, and hung in its own exit for the
  length of the mount's timeouts, waiting for a server that had gone.
- A process cannot run a program from a mount it serves itself: the
  thread that starts a program stands still until the program is loaded.
  The sidecar never does; the tests go through a shell.

## 3. Terms

- **prefix**: the string put before a name to make a reference name.
- **name**: one path component in the root of the mount.
- **pin**: the record that a name was fetched, with the root key it had.
- **materialized**: of a reference, that the contents of all its regular
  files are in plain files in the cache.
- **node**: a directory, regular file or symbolic link in the served tree.
- **handle**: the 16 bytes by which the NFS client names a node.

## 4. The file system

### 4.1 The root

The root is a directory that nothing can be written to.

- **Looking a name up**, or opening it, fetches the reference `prefix +
  name` (section 5) and answers with its node: a directory when the root
  key is a directory or a commit, a regular file when it is file content.
  A name the server has no reference for is `ENOENT`. A fetch that fails
  is `EIO`.
- A name that could not be a reference is `ENOENT` without asking the
  server: one that is empty, `.` or `..`, or contains `/`, and one for
  which `prefix + name` is no valid reference name.
- **Listing** reports the pinned names, in the order they were pinned. The
  position in that order is the cookie, so a listing that is continued
  while names are added neither skips nor repeats.
- The change ID of the root is the number of pins, so the client reads the
  listing again after a fetch.

### 4.2 Inside a reference

Everything is read from the objects in the packstore with core's `fstree`.

- A directory looks an entry up by name and lists its entries in the order
  they are stored; the index in that order is the cookie. A directory's
  decoded entries are cached, least recently used first out.
- A regular file is read by offset, from one of two places that hold the
  same bytes:
  - **its file**, once the reference is materialized (section 6): a read
    at an offset of the plain file. Up to 256 files are kept open, the one
    read longest ago closed first. A read the file cannot answer, because
    it is gone or the read fails, is logged and answered from the objects.
  - **the objects**, until then. The content key is a blob, or a file node
    whose leaves are blobs: the offsets of the leaves follow from the
    lengths in their keys and are worked out once for a file and cached.
    The blobs last read are cached too, 64 MiB of them, because a blob is
    larger than a read.
- A symbolic link answers with its target, in the form Buildbarn gives a
  path: `./a//b` reads as `a/b`, which names the same file, and a tidy
  target reads as it was recorded. Its size is the length of what is
  answered. An absolute target such as `/nix/store/...` resolves in the
  app container, which is why the mount belongs at `/nix/store` there.
- Entries of any other type (devices, FIFOs, sockets) are not served: they
  are left out of listings and are `ENOENT` by name. A NAR has none.
- Extended attributes are not served.

### 4.3 Attributes

| | directory in the root | file in the root | entry inside a reference |
| --- | --- | --- | --- |
| permissions | `r-x` | `r-x` | `r`, and `x` where any execute bit is set; directories `r-x` |
| owner, group | 0, 0 | 0, 0 | as recorded |
| modified | 1 s after the epoch | 1 s after the epoch | as recorded |
| size | 0 | from the root key | from the content key; a link: its target's length |
| links | 1 | 1 | 1 |

Buildbarn has one set of permission bits for owner, group and others, so a
mode is served as `0444` or `0555`, which is what a Nix store has. The root
key of a reference records no mode, owner or time, so those of a name in
the root are the ones a Nix store gives its paths. A file in the root is
served executable because nothing says whether it is.

The inode number is the first eight bytes of the handle.

### 4.4 Handles

- The handle of the root is 16 zero bytes. The handle of any other node is
  the first 16 bytes of the BLAKE3 hash of its parent's handle followed by
  its name. A path has one handle, always the same, and two paths never
  share one: two directories of the same content are two nodes, because
  the kernel does not accept one directory in two places.
- The **table** maps a handle to its parent's handle and its name. A node
  enters it when it is first given out: by a lookup, an open by name, or a
  listing that reports it.
- The table is the file `handles` in the cache directory: a record is the
  parent's handle and the name. It is read whole at start and appended to,
  through a buffer flushed every second and at shutdown. A record cut
  short by a crash is dropped. A handle lost that way is `ESTALE`: a
  lookup of the path gives the same handle again, and a file that was open
  under it has to be opened again.
- A handle is resolved by following the table to the root, taking the pin
  of the first name, and looking the remaining names up in the tree. A
  handle that is not in the table, or whose first name has no pin, is
  `ESTALE`. Nothing is fetched to resolve a handle.
- The table is held in memory and never shrinks: about 150 bytes for every
  path that was ever looked up or listed.

## 5. Fetching

`Ensure(name)` returns the root key of a name:

1. A pinned name has its root at once, without the server.
2. A name found missing less than 5 s ago is missing.
3. Otherwise the reference is pulled with jaccard-store's `client.Pull`
   into the packstore: its pack, and the pack's parent if objects are
   still missing after that. Every object is checked against its key.
4. The packstore is synced, and then the pin, the root and the name, is
   appended to the file `refs` in the cache directory and that file
   synced: a pin never names objects that are not on disk.
5. The reference is queued to be materialized (section 6). `Ensure` does
   not wait for that.

- Lookups of one name at the same moment share one pull. At most
  `--pull-jobs` pulls run at once; the others wait.
- A pull belongs to no request. A client that gives up does not end it,
  and whoever asks next finds it running or done.
- A pull that fails for any reason but a missing reference is tried again,
  after 1 s and then twice as long each time up to 15 s, with a new
  connection, until `--pull-timeout` has passed since the first failure.
  Then everyone waiting gets the error, which is served as `EIO`, and the
  next lookup starts over.
- An attempt that has neither ended nor failed after an hour is ended and
  is a failure like any other.
- The connection to the server is dialed when it is first needed, by
  endpoint ID through jaccard-store's `node.Dial`, and kept. It is dropped
  after a failed request and dialed again by the next.
- The client's key is created on first use in the cache directory.
- A cache is of one server and one prefix, which its file `origin` names
  from the first start on. A cache that was filled from another server or
  under another prefix is refused at start: its pins would be answered as
  names of references they never were.
- Nothing is ever removed from the cache.

## 6. Materializing

A pinned reference is written out under `files/` in the cache directory, in
the background, while it is already served from the objects.

- The tree of the reference is walked and every regular file written with
  its content, in directories of the same names:
  `files/done/<name>/<path>`. A reference that is a single file is the file
  `files/done/<name>`. Nothing else of an entry is reproduced: directories
  are `0755` and files `0644`, owned by the process, and symbolic links
  and entries of other types are not written.
- The tree is built as `files/partial/<name>`. When every file is written,
  the file system is synced (`syncfs`), the tree renamed to
  `files/done/<name>` and the directory above synced. The rename is the
  record: a tree under `done` is whole and on disk. Where there is no
  `syncfs`, which is where only tests run, each file is synced as it is
  closed.
- From then on reads of the reference's files go to that tree (4.2). A
  file that was being read is read on from there: the bytes are the same.
- A reference is queued when it is pinned, and at start for every pin that
  has no tree under `done`. At most `--materialize-jobs` are written at
  once, in the order they were queued. A name is queued once.
- At start everything under `files/partial` is removed.
- A reference that cannot be written, on a full disk or after an I/O
  error, is logged and its partial tree removed. It goes on being served
  from the objects and is tried again at the next start, not before.
- The objects stay in the packstore.

## 7. NFS server

- Buildbarn's NFSv4.1 and NFSv4.0 programs behind its fallback program,
  as Buildbarn itself sets them up: lease times of 120 s enforced and 60 s
  announced, `AUTH_NONE`, a reboot verifier drawn at start.
- The root directory, the directories and the leaves are this project's
  implementations of `virtual.Directory` and `virtual.Leaf`. Every
  operation that would change something answers `EROFS`.
- The handles are this project's own (4.4): the nodes report them as their
  file handle attribute, and the resolver given to Buildbarn's pool of
  opened files is the table's.
- The program is wrapped so that answers to GETATTR also name
  `FATTR4_MAXREAD` and `FATTR4_MAXWRITE`, 1 MiB each, among the supported
  attributes and give them when asked. The wrapper places the two values
  after those of the first bitmap word, all of which it knows the sizes
  of; an answer holding an attribute it does not know is passed on as it
  is.
- A fetch blocks the request that caused it and no other. Buildbarn has no
  status for "ask again later".
- The listener is TCP on `--listen`. Access is whoever can reach it, so it
  listens on loopback. An accept that fails is tried again, after 5 ms
  and then twice as long each time up to a second: only the closing of
  the listener ends the serving.

## 8. Mount and shutdown

- With `--mount DIR` the process mounts its own server on DIR once it
  accepts connections: `mount(2)`, source `:/`, type `nfs4`, read-only,
  `nosuid` and `nodev`, with the address and port of the listener and
  `--mount-options`. The default options are
  `vers=4.1,soft,timeo=600,retrans=2,lookupcache=positive`.
- If DIR is an NFS mount already, it is the mount of an earlier run of the
  sidecar, which ended without unmounting. In the same pod and at the
  same address the kernel finds the server again, nothing is mounted, and
  a file that was open is read on: this is what the handles are stable
  for. Whether the kernel does find the server is tried: the mount is
  given a reason to ask (a `statfs`) and 10 s for a connection to arrive.
  The kernel looks for the server where the mount was made, so a mount of
  another network namespace, as after the pod's sandbox was made anew, or
  of another port never arrives. Such a mount is detached and the server
  mounted in its place.
- Without `--mount` the process only serves.
- On SIGTERM or SIGINT the process unmounts DIR **while it still serves**:
  a plain unmount, and a detaching one at once when that is refused as
  busy. A mount in use is not waited for: the kubelet may leave a sidecar
  two seconds between the signal and the kill, and does so just when the
  mount is in use. The server then goes on **until the kernel has let go
  of it**: until no client has been connected for 250 ms, and for 5 s at the
  most. Then the table is flushed and the process ends. A second signal
  ends it at once. Materializing stops with the first signal; what it had
  begun is removed at the next start.
- The wait after the unmount is for the pod whose grace period has run
  out. The kubelet then kills the app and stops the sidecar in the same
  moment. The app is dying: its mount namespace is gone, so the unmount
  is not refused, and the kernel is still closing the files the app had
  open, each of which takes an answer from the server, and at last ends
  its session. It closes its connection when it is done, and makes one at
  once if it has none and needs the server, which is what the 250 ms are
  for.
- `timeo` and `retrans` bound how long a request may take before the app
  sees `EIO` (a fetch of more than three minutes, at the default), and how
  long anything that touches a mount without a server waits.

### 8.1 A mount that is left on the node

The mount reaches the node through the propagation that carries it to the
app containers, and only the sidecar takes it away again: by unmounting,
which goes the same way back. A sidecar that ends without unmounting
leaves the mount on the node, under the pod's volume directory. Its mount
namespace ending takes nothing with it.

- A sidecar **told to stop** unmounts first, in a few milliseconds, also
  when the mount is in use. Tried: on the cluster the node's mount table
  was read before and after a pod was replaced, and the mount was gone.
- A sidecar that is **killed while the pod lives on** (out of memory, a
  crash) is started again, finds the mount and serves it again, or
  replaces it. Tried, for both.
- A sidecar that is **killed as the pod ends** leaves the mount for good.
  That takes a kill without the signal before it: the kill of the whole
  pod at once, as when its sandbox dies under a shared process namespace,
  or a sidecar that hangs. The mount then has no server. A directory with
  a mount on it cannot be removed (tried), so the pod's volume is expected
  to stay and the pod to stay Terminating until the mount is taken away on
  the node, with `umount -l` of the path; the kubelet's part in this was
  not tried.
- A **node that restarts** has no mounts left.

## 9. Command

```
jaccard-nfs-nix-store --server ENDPOINT_ID --prefix PREFIX --cache DIR [--mount DIR]
```

| flag | environment | default |
| --- | --- | --- |
| `--server ENDPOINT_ID` | `JACCARD_SERVER` | required |
| `--prefix PREFIX` | `JACCARD_PREFIX` | required; `--prefix ''` is every reference |
| `--cache DIR` | `JACCARD_NFS_CACHE` | required |
| `--key FILE` | `JACCARD_KEY` | `client.key` in the cache directory |
| `--listen ADDR` | `JACCARD_NFS_LISTEN` | `127.0.0.1:2049` |
| `--mount DIR` | `JACCARD_NFS_MOUNT` | none: serve only |
| `--mount-options OPTS` | `JACCARD_NFS_MOUNT_OPTIONS` | `vers=4.1,soft,timeo=600,retrans=2,lookupcache=positive` |
| `--pull-timeout D` | `JACCARD_NFS_PULL_TIMEOUT` | `2m` |
| `--pull-jobs N` | `JACCARD_NFS_PULL_JOBS` | `4` |
| `--materialize-jobs N` | `JACCARD_NFS_MATERIALIZE_JOBS` | `2` |

`urfave/cli/v2`; a flag wins over its variable. The cache directory holds
`packstore/`, `files/`, `refs`, `handles`, `origin` and the key. The log is `slog`
text on standard error: a line for every fetch with its name, what it
downloaded and how long it took, a line for every reference materialized
with its files, bytes and time, and a line for every failure.

## 10. Pod

```yaml
spec:
  initContainers:
  - name: nix-store
    image: ghcr.io/amber-store/jaccard-nfs-nix-store:<tag>
    restartPolicy: Always                 # a sidecar: before the app, after it
    securityContext:
      privileged: true                    # Bidirectional needs it
    env:
    - { name: JACCARD_SERVER, value: <endpoint id> }
    - { name: JACCARD_PREFIX, value: /framework/nix/store }
    - { name: JACCARD_NFS_CACHE, value: /cache }
    - { name: JACCARD_NFS_MOUNT, value: /export }
    volumeMounts:
    - { name: store, mountPath: /export, mountPropagation: Bidirectional }
    - { name: cache, mountPath: /cache }
    startupProbe:                         # the app starts when the mount is there
      exec: { command: ["grep", "-q", " /export nfs4 ", "/proc/mounts"] }
      periodSeconds: 1
  containers:
  - name: app
    image: <any>
    command: ["/nix/store/<hash>-<name>/bin/<program>"]
    volumeMounts:
    - { name: store, mountPath: /nix/store, mountPropagation: HostToContainer }
  volumes:
  - { name: store, emptyDir: {} }
  - { name: cache, emptyDir: {} }
```

- Kubernetes allows `Bidirectional` only to a privileged container;
  `SYS_ADMIN` alone is refused.
- The probe looks for the NFS mount in the mount table. The brief's
  `mountpoint -q /export` is true before anything is mounted: the volume
  makes the directory a mount point.
- There is no `preStop` hook: the process unmounts on SIGTERM, and a
  sidecar is stopped after the app containers.
- The node needs the kernel's NFSv4.1 client and a kubelet root that is
  `rshared`.
- The image is Alpine with the one command, which runs as root; `grep`
  for the probe is BusyBox's.

## 11. Layout

```
cmd/jaccard-nfs-nix-store/   the command: the flags, and a sidecar until the signal
sidecar/   the parts put together, the mount, the order of the end (Linux)
refs/      Ensure, the pins, the connection, the retries
tree/      lookup, listing and reading over core's objects; the caches
materialize/  a reference written out as files; which are whole; the queue
handles/   the handle of a path; the table and its file
reclog/    the append-only file of records the pins and the table are in
nfsd/      Buildbarn's Directory and Leaf over refs, tree and handles;
           the read-size wrapper; the server (Linux)
mount/     mount, unmount, is-mounted (Linux)
jstest/    a jaccard-store server in one process, for tests
e2e/       the kernel-mount tests (Linux, root)
scripts/   test-linux.sh: the tests in a privileged Linux container
deploy/    the pod of section 10
docs/
```

`refs`, `tree`, `materialize`, `handles` and `reclog` import nothing of
Buildbarn and build everywhere, and so do the parts of `mount`, `sidecar`
and the command that touch neither Buildbarn nor `mount(2)`: the reading
of a mount table, the order of the unmount, the flags. `nfsd`, the rest of
those three and `e2e` are Linux only, by build tag. `refs` reaches the
server through an interface that jaccard-store's client satisfies, so its
tests need none.

Dependencies: `amber-store/core` and `amber-store/jaccard-store` at the
versions the server runs with; `bb-remote-execution`, `bb-storage` and
`go-xdr` pinned by commit, since they have no releases; Go 1.27.

## 12. Testing

Tests are written before the code they cover.

- `reclog`: records back in order, an empty and a large one, bytes that
  are no text; a file cut at every place of its last record, and one with
  a byte flipped, loses that record, is cut back and takes records again.
- `handles`: the handle of a path is the same in two tables; two paths
  differ; the table read back from its file resolves what was added; a
  file cut short loses its last record and nothing else; an unknown handle.
- `tree`, over directories imported into a temporary packstore with core's
  `ingest`: lookup of what is there and what is not; a listing continued
  from a cookie; reads at the start, across a leaf boundary, at and past
  the end, of an empty file and of a file of one blob, each held against
  the file on disk; a symbolic link; an entry of another type left out.
- `materialize`, over directories imported as for `tree`: a reference
  written out is its source file by file, with nested and empty
  directories, an empty file, a file of several leaves and a name with
  spaces and a newline in it, and with the link left out; a reference that
  is one file; nothing under `done` while the writing is held, by a hook,
  before its last file; a name queued twice is written once; no more at
  once than `--materialize-jobs`; `partial` emptied at start; pins without
  a tree queued at start; a write that fails leaves no tree and the
  reference not materialized; a shutdown in the middle.
- `refs`, against a fake of the server: a pull that pins; a second
  `Ensure` that asks nobody; two at once that share a pull; a missing
  reference, and that it is asked for again after 5 s; a failure that is
  retried and then succeeds; one that outlasts `--pull-timeout`; an
  attempt that hangs; no more pulls at once than `--pull-jobs`; a cache
  opened for another server or prefix; pins read back after a restart; a pin
  only after the objects are synced; a pinned reference handed on to be
  materialized, once.
- `refs`, end to end in one process: a jaccard-store server with a real
  iroh endpoint on loopback and an in-memory S3, directories pushed with
  jaccard-store's client, a base pack and a patch pack fetched through
  `Ensure`, and the tree held against the directory that was pushed.
- `nfsd`, through Buildbarn's interfaces: the root's lookup, open by name
  and listing; a directory, a file and a link inside a reference; every
  mutation refused; a file read before and after its reference is
  materialized, with the same bytes, and that the second read is the
  file's (the test alters the file on disk and gets the altered bytes); a
  materialized file that was removed, read from the objects; handles
  resolved after the table is read back by a second server; the wrapper over answers with and without the list of
  supported attributes, with a file handle among the values, and with an
  attribute it does not know.
- `mount`: a mount table read: the mount on top of two on one point, a
  path with an escaped space, a path a mount point only begins with.
- `sidecar`: the unmount against a kernel that is busy, has nothing
  mounted, or fails.
- `nfsd`, the server: an accept that fails is tried again; the wait for
  the server to be idle, with a client connected, after it went, and with
  clients that keep coming; the wait for a client to come.
- The command: every flag against its variable, the flag over the
  variable, the defaults, and what is refused.
- `e2e`, with the kernel, as root: a sidecar started on the server of the
  in-process test and mounted; a program run from a path that was never
  fetched; a tree walked and compared with its source, a directory of
  3,000 files, links, modes, times and a 64 MiB file among it; the same
  again from the files once the reference is materialized, with the
  kernel's caches dropped; a reference kept as a patch pack; a reference
  that is one file, opened without a lookup; writes refused; a name that
  is missing and, once it is pushed, is not; the read size the kernel
  settled on; the sidecar closed and started again under the mount with a
  file open; the unmount of section 8; and a process in a mount namespace
  of its own that is killed, with a file of the store open, as the
  sidecar is stopped, which has to end at once; a sidecar stopped while a
  file of the store is open, whose mount has to be out of the tree at
  once; and a mount left by a sidecar that listened on another port, which
  the next one has to replace. They are skipped without
  root and run in CI under `sudo` and locally in a privileged container.
- By hand before the deployment: the image in a privileged container
  against the real server, a program run out of a fetched store path.

CI runs `gofmt`, `go vet`, the tests, the tests under the race detector
and the tests with the kernel, on Linux. On macOS `go test ./...` covers
what builds there.

## 13. Out of scope

Removing anything from the cache, the blobs of a materialized reference
among it; answering names, attributes or links from the materialized tree;
a reference that changes after it was fetched; writing; NFSv3; a DaemonSet or CSI driver that mounts on the host;
authentication of NFS clients; extended attributes; file types a NAR does
not have.

## 14. Known limits

- A fetch that takes longer than the mount's timeout (three minutes by
  default) is `EIO` for the app. The fetch goes on, and the next access
  finds it done.
- The whole reference is fetched for one file of it, and its parent pack
  too when it is a patch pack.
- A materialized reference is on disk twice: as objects in the packstore
  and as files. Files that are the same in two references are written
  twice.
- Until a reference is whole on disk, all its reads are served from the
  objects, also of the files already written.
- The cache and the handle table only grow.
- A name that could not be fetched after `--pull-timeout` is `EIO`, not
  `ENOENT`; a name that is missing is `ENOENT` for the next 5 s even if it
  is pushed meanwhile.
- A sidecar that is killed as its pod ends, without the signal to stop
  before it, leaves its mount on the node (8.1). Nothing in the pod can
  prevent that; a mount that the kubelet makes and takes away itself, as
  for a CSI driver, would.
- A sidecar that is stopped while an app still runs and has files of the
  store open leaves that app with a store that answers nothing: its
  requests fail when the mount's timeouts run out. The kubelet stops a
  sidecar after the app containers, but all at once when the pod's grace
  period has run out.
- After a restart of the sidecar a file that was open stays readable, but
  byte-range locks are gone. Nothing in a read-only store takes them.
- Permissions are `0444` and `0555` whatever was recorded; setuid, setgid
  and sticky bits are not served.
- The target of a link is not always the text that was recorded (4.2).
- Buildbarn's packages have no releases and no promise of a stable API,
  bring some ninety modules with them, and do not compile for macOS. The
  read-size wrapper depends on which attributes Buildbarn writes; an
  attribute it does not know makes it step aside, and reads are 1024 bytes
  again.
- The sidecar is privileged.

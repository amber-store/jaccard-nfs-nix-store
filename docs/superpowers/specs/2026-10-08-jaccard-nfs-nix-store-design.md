# jaccard-nfs-nix-store: design

Date: 2026-10-08. Status: written for the owner's review. Not implemented;
section 2.3 records what a throwaway probe measured.

## 1. Purpose

A sidecar that gives the other containers of a pod a directory in which
every reference of a [jaccard-store](https://github.com/amber-store/jaccard-store)
server under one name prefix appears as a directory or a file, and is
fetched the first time it is named. With the prefix under which a machine's
`/nix/store` was pushed, mounted at `/nix/store`, a container runs programs
out of a Nix store it does not hold: what is touched is fetched, the rest
never is.

It is three things in one process:

- the **fetching**: a reference is pulled from the jaccard-store server into
  a local [Amber-Store Core](https://github.com/amber-store/core) packstore
  when its name is first looked up;
- an **NFSv4 server** on the pod's loopback that serves a read-only tree
  straight from the objects in that packstore;
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
- A sample application is deployed when the sidecar works. Its entry point
  is the owner's to name, and is asked for then.

### 2.2 Made in design, for the owner to accept

- The prefix is a **plain string**, as `push-subdirs --prefix` has it: the
  name `X` in the root is the reference `prefix + X`. The names on the
  server are `/framework/nix/store<hash>-<name>` and
  `/laptop/nix/store/<hash>-<name>`, so the prefixes are
  `/framework/nix/store` and `/laptop/nix/store/`.
- A reference is pulled **whole** into a core packstore and served from the
  objects there. Nothing is extracted to files. A pack is one zstd stream,
  so nothing smaller than a reference can be fetched.
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

## 3. Terms

- **prefix**: the string put before a name to make a reference name.
- **name**: one path component in the root of the mount.
- **pin**: the record that a name was fetched, with the root key it had.
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
- A regular file is read by offset. Its content key is a blob, or a file
  node whose leaves are blobs: the offsets of the leaves follow from the
  lengths in their keys and are worked out once for a file and cached. The
  blobs last read are cached too, 64 MiB of them, because a blob is larger
  than a read.
- A symbolic link answers with its target as recorded. An absolute target
  such as `/nix/store/...` resolves in the app container, which is why the
  mount belongs at `/nix/store` there.
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
  parent's handle, the length of the name and the name. It is read whole
  at start and appended to, through a buffer flushed every second and at
  shutdown. A record cut short by a crash is dropped. A handle lost that
  way is `ESTALE`: a lookup of the path gives the same handle again, and a
  file that was open under it has to be opened again.
- A handle is resolved by following the table to the root, taking the pin
  of the first name, and looking the remaining names up in the tree. What
  that finds is cached with the table's entry. A handle that is not in the
  table, or whose first name has no pin, is `ESTALE`.
- The table is held in memory and never shrinks: about 150 bytes for every
  path that was ever looked up or listed.

## 5. Fetching

`Ensure(name)` returns the root key of a name:

1. A pinned name has its root at once, without the server.
2. A name found missing less than 5 s ago is missing.
3. Otherwise the reference is pulled with jaccard-store's `client.Pull`
   into the packstore: its pack, and the pack's parent if objects are
   still missing after that. Every object is checked against its key.
4. The packstore is synced, and then the pin is appended to the file `refs`
   in the cache directory and that file synced: a pin never names objects
   that are not on disk.

- Lookups of one name at the same moment share one pull. At most
  `--pull-jobs` pulls run at once; the others wait.
- A pull belongs to no request. A client that gives up does not end it,
  and whoever asks next finds it running or done.
- A pull that fails for any reason but a missing reference is tried again,
  after 1 s and then twice as long each time up to 15 s, with a new
  connection, until `--pull-timeout` has passed since the first failure.
  Then everyone waiting gets the error, which is served as `EIO`, and the
  next lookup starts over.
- The connection to the server is dialed when it is first needed, by
  endpoint ID through jaccard-store's `node.Dial`, and kept. It is dropped
  after a failed request and dialed again by the next.
- The client's key is created on first use in the cache directory.
- Nothing is ever removed from the cache.

## 6. NFS server

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
  listens on loopback.

## 7. Mount and shutdown

- With `--mount DIR` the process mounts its own server on DIR once it
  accepts connections: `mount(2)`, source `:/`, type `nfs4`, read-only,
  `nosuid` and `nodev`, with the address and port of the listener and
  `--mount-options`. The default options are
  `vers=4.1,soft,timeo=600,retrans=2,lookupcache=positive`.
- If DIR is an NFS mount already, it is the mount of an earlier run of the
  sidecar in the same pod: nothing is mounted, and the kernel finds the
  server again at the address it knows. This is what the handles are
  stable for.
- Without `--mount` the process only serves.
- On SIGTERM or SIGINT the process unmounts DIR **while it still serves**:
  a plain unmount, tried for 2 s while it is refused as busy, then a
  detaching one, after which the server goes on for 5 s so that the kernel
  can end its session. Then the table is flushed and the process ends. A
  second signal ends it at once.
- `timeo` and `retrans` bound two things: how long a request may take
  before the app sees `EIO` (a fetch of more than three minutes, at the
  default), and how long a pod's end is held up when the sidecar is killed
  with the mount in place (twice that).

## 8. Command

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

`urfave/cli/v2`; a flag wins over its variable. The cache directory holds
`packstore/`, `refs`, `handles` and the key. The log is `slog` text on
standard error: a line for every fetch with its name, what it downloaded
and how long it took, and for every failure.

## 9. Pod

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
      exec: { command: ["mountpoint", "-q", "/export"] }
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
- There is no `preStop` hook: the process unmounts on SIGTERM, and a
  sidecar is stopped after the app containers.
- The node needs the kernel's NFSv4.1 client and a kubelet root that is
  `rshared`.
- The image is Alpine with the one command, which runs as root; `mountpoint`
  for the probe is BusyBox's.

## 10. Layout

```
cmd/jaccard-nfs-nix-store/   the command (Linux)
refs/      Ensure, the pins, the connection, the retries
tree/      lookup, listing and reading over core's objects; the caches
handles/   the handle of a path; the table and its file
nfsd/      Buildbarn's Directory and Leaf over refs, tree and handles;
           the read-size wrapper; the server (Linux)
mount/     mount, unmount, is-mounted (Linux)
e2e/       the kernel-mount tests (Linux, root)
deploy/    the pod of section 9
docs/
```

`refs`, `tree` and `handles` import nothing of Buildbarn and build
everywhere. `nfsd`, `mount`, `e2e` and the command are Linux only, by build
tag. `refs` reaches the server through an interface that jaccard-store's
client satisfies, so its tests need none.

Dependencies: `amber-store/core` and `amber-store/jaccard-store` at the
versions the server runs with; `bb-remote-execution`, `bb-storage` and
`go-xdr` pinned by commit, since they have no releases; Go 1.27.

## 11. Testing

Tests are written before the code they cover.

- `handles`: the handle of a path is the same in two tables; two paths
  differ; the table read back from its file resolves what was added; a
  file cut short loses its last record and nothing else; an unknown handle.
- `tree`, over directories imported into a temporary packstore with core's
  `ingest`: lookup of what is there and what is not; a listing continued
  from a cookie; reads at the start, across a leaf boundary, at and past
  the end, of an empty file and of a file of one blob, each held against
  the file on disk; a symbolic link; an entry of another type left out.
- `refs`, against a fake of the server: a pull that pins; a second
  `Ensure` that asks nobody; two at once that share a pull; a missing
  reference, and that it is asked for again after 5 s; a failure that is
  retried and then succeeds; one that outlasts `--pull-timeout`; no more
  pulls at once than `--pull-jobs`; pins read back after a restart; a pin
  only after the objects are synced.
- `refs`, end to end in one process: a jaccard-store server with a real
  iroh endpoint on loopback and an in-memory S3, directories pushed with
  jaccard-store's client, a base pack and a patch pack fetched through
  `Ensure`, and the tree held against the directory that was pushed.
- `nfsd`, through Buildbarn's interfaces: the root's lookup, open by name
  and listing; a directory, a file and a link inside a reference; every
  mutation refused; handles resolved after the table is read back by a
  second server; the wrapper over answers with and without the list of
  supported attributes, with a file handle among the values, and with an
  attribute it does not know.
- `e2e`, with the kernel, as root: the server of the in-process test
  mounted through `mount`; a tree walked and compared with its source,
  modes, links and a 64 MiB file among it; a program run from the mount;
  a missing name; the read size the kernel settled on; the server stopped
  and started under the mount with a file open; the unmount of section 7.
  They are skipped without root and run in CI under `sudo` and locally in
  a privileged container.
- By hand before the deployment: the image in a privileged container
  against the real server, a program run out of a fetched store path.

CI runs `gofmt`, `go vet`, the tests and the tests under the race detector
on Linux. On macOS the three portable packages are what `go test ./...`
covers.

## 12. Out of scope

Removing anything from the cache; a reference that changes after it was
fetched; writing; NFSv3; a DaemonSet or CSI driver that mounts on the host;
authentication of NFS clients; extended attributes; file types a NAR does
not have.

## 13. Known limits

- A fetch that takes longer than the mount's timeout (three minutes by
  default) is `EIO` for the app. The fetch goes on, and the next access
  finds it done.
- The whole reference is fetched for one file of it, and its parent pack
  too when it is a patch pack.
- The cache and the handle table only grow.
- A name that could not be fetched after `--pull-timeout` is `EIO`, not
  `ENOENT`; a name that is missing is `ENOENT` for the next 5 s even if it
  is pushed meanwhile.
- A sidecar killed while mounted, as the pod ends, holds the end of the
  pod up for up to six minutes.
- After a restart of the sidecar a file that was open stays readable, but
  byte-range locks are gone. Nothing in a read-only store takes them.
- Permissions are `0444` and `0555` whatever was recorded; setuid, setgid
  and sticky bits are not served.
- Buildbarn's packages have no releases and no promise of a stable API,
  bring some ninety modules with them, and do not compile for macOS. The
  read-size wrapper depends on which attributes Buildbarn writes; an
  attribute it does not know makes it step aside, and reads are 1024 bytes
  again.
- The sidecar is privileged.

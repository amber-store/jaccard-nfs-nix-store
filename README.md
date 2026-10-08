# jaccard-nfs-nix-store

A sidecar that gives the other containers of a pod a directory in which
every reference of a [jaccard-store](https://github.com/amber-store/jaccard-store)
server under one name prefix is a directory (or a file) that is fetched the
first time it is named.

With the prefix a machine's `/nix/store` was pushed under, mounted at
`/nix/store`, a container runs programs out of a Nix store it does not
hold: the store paths the program touches are fetched, the others never
are. The app container needs no privileges and no image of its own to
speak of; it sees a directory.

```
            pod
 ┌──────────────────────────────────────────────────────────┐
 │  app container            sidecar                        │
 │  /nix/store ◀── mount ─── NFSv4 on 127.0.0.1:2049        │
 │                            │  names, attributes, links   │
 │                            │  and file contents          │
 │                            ▼                             │
 │                           cache: objects ─▶ plain files  │
 └────────────────────────────▲─────────────────────────────┘
                              │ a reference, the first time it is named
                     jaccard-store server (iroh) and its bucket
```

The design is in
[`docs/superpowers/specs/2026-10-08-jaccard-nfs-nix-store-design.md`](docs/superpowers/specs/2026-10-08-jaccard-nfs-nix-store-design.md).

## How it works

- **Fetching.** Looking a name up in the root of the mount, or opening it,
  pulls the reference `prefix + name` from the server into a local
  [Amber-Store Core](https://github.com/amber-store/core) packstore: its
  pack, and that pack's parent if objects are still missing. The name is
  then *pinned*: it keeps the root it had for as long as the cache lives.
  Everything below a fetched name is local. Listing the root shows what
  has been fetched and fetches nothing.
- **Serving.** Names, attributes and symbolic links are read from the
  objects in the packstore. So is the content of a file, at first.
- **Materializing.** As soon as a reference is pinned, its regular files
  are written out as plain files in the background. When all of them are
  written and synced, reads of that reference's files go to the plain
  files, which answer with less work.
- **Mounting.** The sidecar mounts its own NFS server on a directory of a
  volume the pod shares, with mount propagation that carries the mount to
  the app containers.

The NFS server is [Buildbarn](https://github.com/buildbarn/bb-remote-execution)'s,
and speaks NFSv4.1.

## Pod

[`deploy/pod.yaml`](deploy/pod.yaml) is a whole pod. Its parts:

```yaml
initContainers:
  - name: nix-store
    image: ghcr.io/amber-store/jaccard-nfs-nix-store:latest
    restartPolicy: Always           # a sidecar: before the app, after it
    securityContext:
      privileged: true              # Bidirectional needs it
    env:
      - { name: JACCARD_SERVER, value: <endpoint id> }
      - { name: JACCARD_PREFIX, value: /framework/nix/store }
      - { name: JACCARD_NFS_CACHE, value: /cache }
      - { name: JACCARD_NFS_MOUNT, value: /export }
    volumeMounts:
      - { name: store, mountPath: /export, mountPropagation: Bidirectional }
      - { name: cache, mountPath: /cache }
    startupProbe:                   # the app starts when the store is there
      exec: { command: ["mountpoint", "-q", "/export"] }
      periodSeconds: 1
containers:
  - name: app
    image: busybox:1.37             # any image
    command: ["/nix/store/<hash>-<name>/bin/<program>"]
    volumeMounts:
      - { name: store, mountPath: /nix/store, mountPropagation: HostToContainer }
volumes:
  - { name: store, emptyDir: {} }
  - { name: cache, emptyDir: {} }
```

- **The prefix is put before a name as it is.** References called
  `/framework/nix/store<hash>-<name>` have the prefix
  `/framework/nix/store`; ones called `/laptop/nix/store/<hash>-<name>` have
  `/laptop/nix/store/`.
- **The sidecar is privileged.** Kubernetes allows `Bidirectional` mount
  propagation to privileged containers only.
- **Mount it at `/nix/store` in the app.** Symbolic links in a store point
  at `/nix/store/...`, and are followed in the app container.
- **There is no `preStop` hook.** On SIGTERM the sidecar unmounts while it
  still serves, and a sidecar is stopped after the app containers.
- **The node** needs the kernel's NFSv4.1 client and a kubelet root that
  is `rshared`, which is what the common distributions have.
- **A sidecar that is restarted** finds its mount still there and serves
  it again: file handles are derived from paths and kept in the cache, so
  what the kernel holds still means what it meant. A file that was open
  stays readable.

## Command

```sh
jaccard-nfs-nix-store --server ENDPOINT_ID --prefix PREFIX --cache DIR [--mount DIR]
```

| flag | environment | default |
| --- | --- | --- |
| `--server ENDPOINT_ID` | `JACCARD_SERVER` | required |
| `--prefix PREFIX` | `JACCARD_PREFIX` | required; `--prefix ''` is every reference |
| `--cache DIR` | `JACCARD_NFS_CACHE` | required |
| `--key FILE` | `JACCARD_KEY` | `client.key` in the cache directory |
| `--listen ADDR` | `JACCARD_NFS_LISTEN` | `127.0.0.1:2049` |
| `--mount DIR` | `JACCARD_NFS_MOUNT` | none: the server is only served |
| `--mount-options OPTS` | `JACCARD_NFS_MOUNT_OPTIONS` | `vers=4.1,soft,timeo=600,retrans=2,lookupcache=positive` |
| `--pull-timeout D` | `JACCARD_NFS_PULL_TIMEOUT` | `2m` |
| `--pull-jobs N` | `JACCARD_NFS_PULL_JOBS` | `4` |
| `--materialize-jobs N` | `JACCARD_NFS_MATERIALIZE_JOBS` | `2` |

A flag wins over its variable. The cache directory holds the objects
(`packstore/`), the plain files (`files/`), the pins (`refs`), the handle
table (`handles`) and the client's key. The key is created on first use;
its endpoint ID is what the server sees the sidecar as.

The log, on standard error, has a line for every reference fetched and for
every one materialized, with sizes and times, and for every failure.

Without `--mount` the sidecar only serves, and the server can be mounted
by hand:

```sh
mount -t nfs4 -o ro,vers=4.1,port=2049 127.0.0.1:/ /mnt
```

Whoever can reach the server can read the store: there is no
authentication, which is why it listens on loopback.

## What to know

- **A whole reference is fetched for one file of it**, and its parent pack
  too when the server keeps it as a patch pack and the cache does not
  already hold the parent's content.
- **A fetch that takes longer than three minutes is an I/O error** for the
  process that waits for it (`soft`, `timeo=600`, `retrans=2`). The fetch
  goes on, and the next access finds it done. `--mount-options` changes
  the limit; `hard` waits for ever.
- **A name that cannot be fetched** after `--pull-timeout` of trying is an
  I/O error, not a missing file. A name the server does not have is a
  missing file, and is asked for again after five seconds: a reference may
  be pushed at any time.
- **A fetched reference never changes.** A name keeps the root it was
  first fetched with until the cache is gone.
- **Nothing is ever removed from the cache**, and a materialized reference
  is in it twice: as objects and as files.
- **Permissions are `0444` and `0555`**, whatever was recorded: read for
  all, and execute where any execute bit was set. That is what a Nix store
  has. Owners and times are served as recorded.
- **Only directories, regular files and symbolic links are served**, which
  is what a NAR has.
- **A sidecar killed with its mount in place**, as the pod ends, holds the
  end of the pod up for as long as two request timeouts: six minutes with
  the default options.

## Development

Go 1.27. The packages that hold the logic build and test anywhere:

```sh
go test ./...
```

The NFS server (`nfsd`), the mounting, the command and the end-to-end
tests are for Linux alone: Buildbarn's packages do not compile for macOS
outside its own build. The end-to-end tests in `e2e/` start a jaccard-store
server and a sidecar in one process and mount the sidecar with the
kernel's NFS client, so they also need root. On another system, or without
root, a privileged container runs everything:

```sh
scripts/test-linux.sh                       # every package, on Linux
scripts/test-linux.sh -run Mounted -v ./e2e/
```

| package | what it is |
| --- | --- |
| `refs` | fetching: a reference pulled once and pinned |
| `materialize` | a pinned reference written out as plain files |
| `tree` | directories and file content read from the objects |
| `handles` | the file handle of a path, and the table back from it |
| `reclog` | the append-only file the pins and the handles are kept in |
| `nfsd` | the tree behind Buildbarn's NFSv4 server |
| `mount` | mounting with the kernel alone |
| `sidecar` | all of it put together, and the order in which it ends |
| `jstest` | a jaccard-store server in one process, for tests |

The Nix build is [gonixgo](https://github.com/draganm/gonixgo)'s, which
runs a program while Nix evaluates and so takes an option:

```sh
nix build --option allow-unsafe-native-code-during-evaluation true
```

`.github/workflows/test.yml` runs `gofmt`, `go vet`, the tests, the tests
under the race detector and the tests with the kernel on every push.
Pushing a tag `v*` runs `release.yml`: the image is built for amd64 and
arm64 and pushed to `ghcr.io/amber-store/jaccard-nfs-nix-store`, and a
GitHub release is created.

## License

Licensed under the GNU Lesser General Public License, version 3 only
(`LGPL-3.0-only`). See [`LICENSE`](LICENSE) for the LGPL terms and
[`COPYING`](COPYING) for the GPL terms incorporated by the LGPL.

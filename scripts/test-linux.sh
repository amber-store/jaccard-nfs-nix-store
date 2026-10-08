#!/bin/sh
# Runs the tests on Linux, in a container: the packages that build only
# there (nfsd, mount, the command) and the ones that mount with the kernel
# (e2e), which is why the container is privileged. The arguments are
# `go test`'s; without any, every package is tested.
#
#   scripts/test-linux.sh                    # everything
#   scripts/test-linux.sh -run Mount ./e2e/
set -eu
cd "$(dirname "$0")/.."
[ $# -gt 0 ] || set -- ./...
exec docker run --rm --privileged \
	-v "$PWD:/src" -w /src \
	-v "$(go env GOMODCACHE):/go/pkg/mod" \
	-v jaccard-nfs-nix-store-go-build:/root/.cache/go-build \
	golang:1.27 go test "$@"

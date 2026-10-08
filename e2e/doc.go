// Package e2e holds the tests of the sidecar as a whole, with the kernel:
// a jaccard-store server in one process, a sidecar started on it, and its
// NFS server mounted by the Linux client. They need Linux and root, and are
// skipped without: scripts/test-linux.sh runs them in a privileged
// container.
package e2e

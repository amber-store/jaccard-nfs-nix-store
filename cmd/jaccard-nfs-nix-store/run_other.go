//go:build !linux

package main

import (
	"context"
	"errors"
)

// run is the sidecar's on Linux. Elsewhere there is nothing to run: the
// NFS server builds for Linux alone, and so does the mounting.
func run(context.Context, settings) error {
	return errors.New("the sidecar runs on Linux alone")
}

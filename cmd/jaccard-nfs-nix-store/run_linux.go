//go:build linux

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/amber-store/jaccard-nfs-nix-store/refs"
	"github.com/amber-store/jaccard-nfs-nix-store/sidecar"
	"github.com/amber-store/jaccard-store/node"
	irohkey "github.com/tmc/go-iroh/key"
)

// run starts a sidecar with the settings and stops it when ctx ends.
func run(ctx context.Context, s settings) error {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	id, err := irohkey.ParseEndpointID(s.server)
	if err != nil {
		return fmt.Errorf("--server: not an endpoint ID: %w", err)
	}
	// Before the key, which would make the directory for itself alone.
	if err := os.MkdirAll(s.cache, 0o755); err != nil {
		return fmt.Errorf("the cache directory: %w", err)
	}
	sk, err := node.LoadOrCreateKey(s.key)
	if err != nil {
		return err
	}
	log.Info("starting", "version", version, "server", s.server, "client", sk.Public().EndpointID().String())

	sc, err := sidecar.Start(sidecar.Config{
		Prefix: s.prefix,
		Server: s.server,
		Cache:  s.cache,
		Dial: func(ctx context.Context) (refs.Conn, error) {
			return refs.Connect(ctx, node.DialConfig{Key: sk, Server: id, Log: log})
		},
		Listen:          s.listen,
		Mount:           s.mount,
		MountOptions:    s.mountOptions,
		PullTimeout:     s.pullTimeout,
		PullJobs:        s.pullJobs,
		MaterializeJobs: s.materializeJobs,
		Log:             log,
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	log.Info("stopping")
	return sc.Stop()
}

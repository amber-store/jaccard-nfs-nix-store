// Command jaccard-nfs-nix-store is a sidecar that gives the other
// containers of a pod a directory in which every reference of a
// jaccard-store server under one prefix is fetched the first time it is
// named: with the prefix a machine's /nix/store was pushed under, a
// container runs programs out of a Nix store it does not hold.
//
//	jaccard-nfs-nix-store --server ENDPOINT_ID --prefix PREFIX --cache DIR [--mount DIR]
//
// It serves the references over NFSv4 on loopback and, with --mount,
// mounts its own server on a directory, which the pod shares with the
// other containers. It also listens for HTTP on loopback: a list of store
// paths sent to POST /v1/preload is fetched ahead of any use. Every option is a flag and an environment variable;
// the flag wins. On SIGTERM it unmounts while it still serves, and ends.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/amber-store/core/reference"
	"github.com/amber-store/jaccard-nfs-nix-store/preload"
	"github.com/amber-store/jaccard-nfs-nix-store/sidecar"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/urfave/cli/v2"
)

// version is the release this binary was built as. A release build sets it
// with -ldflags "-X main.version=..."; any other build is "dev".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The first signal ends the context and the sidecar winds down; from
	// then on signals are the system's again, so a second one ends the
	// process at once.
	go func() {
		<-ctx.Done()
		stop()
	}()
	if err := newApp(run).RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "jaccard-nfs-nix-store:", err)
		os.Exit(1)
	}
}

// settings is what the flags and the environment say.
type settings struct {
	server          string
	prefix          string
	cache           string
	key             string
	listen          string
	mount           string
	mountOptions    string
	pullTimeout     time.Duration
	pullJobs        int
	materializeJobs int
	preloadListen   string
	preloadJobs     int
}

// newApp returns the command. start is what it does once the settings are
// read and found sound: it runs a sidecar until ctx ends.
func newApp(start func(ctx context.Context, s settings) error) *cli.App {
	return &cli.App{
		Name:            "jaccard-nfs-nix-store",
		Usage:           "serve the references of a jaccard-store server as a directory that fetches what is named",
		UsageText:       "jaccard-nfs-nix-store --server ENDPOINT_ID --prefix PREFIX --cache DIR [--mount DIR]",
		Version:         version,
		HideHelpCommand: true,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "server", EnvVars: []string{"JACCARD_SERVER"},
				Usage: "`ENDPOINT_ID` of the jaccard-store server (required)"},
			&cli.StringFlag{Name: "prefix", EnvVars: []string{"JACCARD_PREFIX"},
				Usage: "what the names of the references begin with: `PREFIX` is put before a name in the root as it is, " +
					"so it wants its own / at the end if the names have one (required; '' is every reference)"},
			&cli.StringFlag{Name: "cache", EnvVars: []string{"JACCARD_NFS_CACHE"},
				Usage: "`DIR` that holds what was fetched: the objects, the materialized files, the pins and the handles (required)"},
			&cli.StringFlag{Name: "key", EnvVars: []string{"JACCARD_KEY"},
				Usage: "`FILE` with the key the server knows this client by, created on first use (default: client.key in the cache)"},
			&cli.StringFlag{Name: "listen", EnvVars: []string{"JACCARD_NFS_LISTEN"}, Value: sidecar.DefaultListen,
				Usage: "`ADDRESS` of the NFS server; whoever can reach it can read the store"},
			&cli.StringFlag{Name: "mount", EnvVars: []string{"JACCARD_NFS_MOUNT"},
				Usage: "`DIR` to mount the server on (default: none, the server is only served)"},
			&cli.StringFlag{Name: "mount-options", EnvVars: []string{"JACCARD_NFS_MOUNT_OPTIONS"}, Value: sidecar.DefaultMountOptions,
				Usage: "`OPTIONS` of the mount after the server's address and port, as nfs(5) has them"},
			&cli.DurationFlag{Name: "pull-timeout", EnvVars: []string{"JACCARD_NFS_PULL_TIMEOUT"}, Value: 2 * time.Minute,
				Usage: "how long a fetch that fails is tried again before the name is an I/O error, as a `DURATION`"},
			&cli.IntFlag{Name: "pull-jobs", EnvVars: []string{"JACCARD_NFS_PULL_JOBS"}, Value: 4,
				Usage: "`NUMBER` of references fetched at once"},
			&cli.IntFlag{Name: "materialize-jobs", EnvVars: []string{"JACCARD_NFS_MATERIALIZE_JOBS"}, Value: 2,
				Usage: "`NUMBER` of fetched references written out as files at once"},
			&cli.StringFlag{Name: "preload-listen", EnvVars: []string{"JACCARD_NFS_PRELOAD_LISTEN"}, Value: preload.DefaultListen,
				Usage: "`ADDRESS` of the HTTP endpoint that takes a list of store paths to fetch ahead, POST " + preload.Path +
					"; '' is no endpoint"},
			&cli.IntFlag{Name: "preload-jobs", EnvVars: []string{"JACCARD_NFS_PRELOAD_JOBS"}, Value: preload.DefaultJobs,
				Usage: "`NUMBER` of store paths of one list fetched at once; they are not counted among --pull-jobs"},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() > 0 {
				return fmt.Errorf("the command takes no arguments, got %q", c.Args().Slice())
			}
			s, err := readSettings(c)
			if err != nil {
				return err
			}
			return start(c.Context, s)
		},
	}
}

// readSettings reads the flags and refuses what no sidecar can run with.
func readSettings(c *cli.Context) (settings, error) {
	s := settings{
		server:          c.String("server"),
		prefix:          c.String("prefix"),
		cache:           c.String("cache"),
		key:             c.String("key"),
		listen:          c.String("listen"),
		mount:           c.String("mount"),
		mountOptions:    c.String("mount-options"),
		pullTimeout:     c.Duration("pull-timeout"),
		pullJobs:        c.Int("pull-jobs"),
		materializeJobs: c.Int("materialize-jobs"),
		preloadListen:   c.String("preload-listen"),
		preloadJobs:     c.Int("preload-jobs"),
	}
	switch {
	case s.server == "":
		return settings{}, errors.New("no server: give --server or set JACCARD_SERVER")
	// An empty prefix is every reference, so it has to be said.
	case !c.IsSet("prefix"):
		return settings{}, errors.New("no prefix: give --prefix or set JACCARD_PREFIX to what the names of the references begin with")
	case s.cache == "":
		return settings{}, errors.New("no cache directory: give --cache or set JACCARD_NFS_CACHE")
	case s.listen == "":
		return settings{}, errors.New("--listen: no address")
	case s.pullTimeout <= 0:
		return settings{}, fmt.Errorf("--pull-timeout: %v is no time to try a fetch again in", s.pullTimeout)
	case s.pullJobs < 1:
		return settings{}, fmt.Errorf("--pull-jobs: %d is not a number of references to fetch at once: want 1 or more", s.pullJobs)
	case s.materializeJobs < 1:
		return settings{}, fmt.Errorf("--materialize-jobs: %d is not a number of references to write out at once: want 1 or more", s.materializeJobs)
	case s.preloadJobs < 1:
		return settings{}, fmt.Errorf("--preload-jobs: %d is not a number of store paths to fetch at once: want 1 or more", s.preloadJobs)
	}
	if _, err := irohkey.ParseEndpointID(s.server); err != nil {
		return settings{}, fmt.Errorf("--server: not an endpoint ID: %w", err)
	}
	if s.prefix != "" {
		if err := reference.ValidateName(s.prefix); err != nil {
			return settings{}, fmt.Errorf("--prefix %q: %w", s.prefix, err)
		}
	}
	if s.key == "" {
		s.key = filepath.Join(s.cache, "client.key")
	}
	return s, nil
}

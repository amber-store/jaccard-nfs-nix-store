// Package preload is the sidecar's way to be told what will be needed: an
// HTTP endpoint that takes a list of store paths and fetches them all,
// before anything looks them up.
//
//	POST /v1/preload
//
//	/nix/store/6in5jlbspq9szjvlrdxq9rpmxyvca529-busybox-1.37.0
//	/nix/store/wb6rhpznjfczwlwx23zmdrrw74bayxw4-glibc-2.42-47/lib/libc.so.6
//	2sm7dby7g2sfp4cjr3537zhwkgmq3f3b-nginx-1.24.0
//
// A line is a path under /nix/store, of which the store path it lies in is
// taken, or the name of a store path by itself. The paths are fetched
// several at a time. The answer is 200 when all of them are there. The
// first that cannot be fetched ends the others, the ones that are running
// too, and is what the answer says: 404 for a path the server does not
// have, 502 for one that could not be fetched.
//
// Fetched means what it means for a lookup: the reference is pulled and
// pinned, and is served from then on. It is not waited for to be
// materialized.
//
// Fetch is the same without the endpoint, for a program that has the
// sidecar in it and the names at hand.
package preload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
	"golang.org/x/sync/errgroup"
)

const (
	// Path is the path the endpoint answers on.
	Path = "/v1/preload"
	// DefaultListen is where the endpoint listens unless it is told
	// otherwise: on loopback, which the containers of a pod share.
	DefaultListen = "127.0.0.1:9889"
	// DefaultJobs is how many paths of a list are fetched at once.
	DefaultJobs = 20
	// MaxBody is the longest list that is taken, in bytes.
	MaxBody = 16 << 20

	// storeDir is where a Nix store is: what a path of a list begins with.
	storeDir = "/nix/store"
)

// Fetcher fetches a store path by its name. *refs.Store does, with
// Preload: a fetch that is not one of the lookups' jobs, and that ends
// when its context does.
type Fetcher interface {
	Preload(ctx context.Context, name string) (key.Key, error)
}

type handler struct {
	fetch Fetcher
	jobs  int
	log   *slog.Logger
}

// New returns the endpoint: a handler that fetches the paths it is sent
// with fetch, jobs of them at once. Fewer than one job means DefaultJobs,
// and a nil log slog.Default().
func New(fetch Fetcher, jobs int, log *slog.Logger) http.Handler {
	if jobs < 1 {
		jobs = DefaultJobs
	}
	if log == nil {
		log = slog.Default()
	}
	return &handler{fetch: fetch, jobs: jobs, log: log}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != Path {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "preload takes a POST of store paths, one to a line", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBody))
	if err != nil {
		var tooLong *http.MaxBytesError
		if errors.As(err, &tooLong) {
			http.Error(w, fmt.Sprintf("the list is longer than %d bytes", MaxBody), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "reading the list: "+err.Error(), http.StatusBadRequest)
		return
	}
	names, err := parse(string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	began := time.Now()
	err = Fetch(r.Context(), h.fetch, h.jobs, names)
	var failed *Failure
	switch {
	case err == nil:
		h.log.Info("preloaded", "paths", len(names), "took", time.Since(began))
		fmt.Fprintf(w, "fetched %d paths\n", len(names))
	case r.Context().Err() != nil:
		// Whoever asked is gone, or the sidecar is stopping: there is
		// nobody to answer.
		h.log.Info("preload given up", "paths", len(names), "took", time.Since(began))
	case errors.As(err, &failed) && errors.Is(failed.Err, refs.ErrNotFound):
		h.log.Warn("preload failed", "path", failed.Path(), "error", failed.Err)
		http.Error(w, failed.Path()+": the server has no such path", http.StatusNotFound)
	case errors.As(err, &failed):
		h.log.Warn("preload failed", "path", failed.Path(), "error", failed.Err)
		http.Error(w, failed.Path()+": could not be fetched: "+failed.Err.Error(), http.StatusBadGateway)
	default:
		http.Error(w, err.Error(), http.StatusBadGateway)
	}
}

// Failure is the store path of a list that could not be fetched, and why.
type Failure struct {
	// Name is the name of the store path, without /nix/store/.
	Name string
	// Err is why: refs.ErrNotFound for a path the server does not have.
	Err error
}

// Path returns the store path: /nix/store/<name>.
func (f *Failure) Path() string { return storeDir + "/" + f.Name }

// Error returns the store path and why it could not be fetched.
func (f *Failure) Error() string { return f.Path() + ": " + f.Err.Error() }

// Unwrap returns why the store path could not be fetched.
func (f *Failure) Unwrap() error { return f.Err }

// Fetch fetches the store paths of the names with fetch, as many at once
// as there are jobs; fewer than one job means DefaultJobs. A name is the
// name of a store path, without /nix/store/.
//
// The first name that cannot be fetched ends the context the others run
// under, and no further one is begun; it is what Fetch returns, as a
// *Failure. When ctx ends Fetch returns an error that is ctx's.
func Fetch(ctx context.Context, fetch Fetcher, jobs int, names []string) error {
	if jobs < 1 {
		jobs = DefaultJobs
	}
	group, fetching := errgroup.WithContext(ctx)
	group.SetLimit(jobs)
	for _, name := range names {
		// Go waits for a job to be free. One may have failed meanwhile.
		if fetching.Err() != nil {
			break
		}
		group.Go(func() error {
			if err := fetching.Err(); err != nil {
				return err
			}
			if _, err := fetch.Preload(fetching, name); err != nil {
				return &Failure{Name: name, Err: err}
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	// No fetch failed. If ctx ended meanwhile, not every name was begun:
	// that nothing went wrong does not make it true that all are there.
	return ctx.Err()
}

// parse returns the names of the store paths a list holds, each once, in
// the order they first appear. A line that is empty is none; one that is
// neither a path under the store nor a name is an error that says which.
func parse(list string) ([]string, error) {
	var names []string
	seen := map[string]bool{}
	for n, line := range strings.Split(list, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name := line
		if rest, under := strings.CutPrefix(line, storeDir+"/"); under {
			// The store path a path lies in is its first name.
			name, _, _ = strings.Cut(rest, "/")
		} else if strings.Contains(line, "/") {
			name = ""
		}
		if name == "" || name == "." || name == ".." {
			return nil, fmt.Errorf("line %d: %q is neither a path under %s nor the name of a store path", n+1, line, storeDir)
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names, nil
}

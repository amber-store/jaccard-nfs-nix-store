package preload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-nfs-nix-store/refs"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fetcher is the fetching as a test wants it: it has every name but the
// ones it was told to fail, takes as long as it is held, and records what
// it was asked and how many it fetched at once.
type fetcher struct {
	mu   sync.Mutex
	fail map[string]error
	hold chan struct{} // when set, every fetch waits for it, or for its context
	// failWhen is how many fetches have to be running, the failing one
	// among them, before a fetch that is to fail does.
	failWhen int
	asked    []string
	ended    []string // the fetches whose context ended while they were held
	running  int
	most     int
}

func (f *fetcher) Preload(ctx context.Context, name string) (key.Key, error) {
	f.mu.Lock()
	f.asked = append(f.asked, name)
	f.running++
	f.most = max(f.most, f.running)
	err, hold := f.fail[name], f.hold
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.running--
		f.mu.Unlock()
	}()
	if err != nil {
		for {
			if running, _ := f.now(); running >= f.failWhen {
				return key.Key{}, err
			}
			time.Sleep(time.Millisecond)
		}
	}
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			f.mu.Lock()
			f.ended = append(f.ended, name)
			f.mu.Unlock()
			return key.Key{}, ctx.Err()
		}
	}
	return key.Key{}, nil
}

func (f *fetcher) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(slices.Values(f.asked))
}

func (f *fetcher) now() (running, asked int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.running, len(f.asked)
}

// post sends a list to the handler and returns the status and the body.
func post(t *testing.T, h http.Handler, list string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/preload", strings.NewReader(list)))
	return rec.Code, rec.Body.String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("it never came about that %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestEveryPathOfTheListIsFetched(t *testing.T) {
	f := &fetcher{}
	status, body := post(t, New(f, 4, quiet), strings.Join([]string{
		"/nix/store/aaa-hello-2.12",
		"/nix/store/bbb-glibc-2.42/lib/libc.so.6", // a path into a store path is the store path
		"ccc-bash-5.3",                            // a name by itself
		"",                                        // a line with nothing on it
		"   /nix/store/ddd-coreutils-9.11  ",      // spaces around it
		"/nix/store/eee-jq-1.8\r",                 // a line that ended in CR LF
		"/nix/store/aaa-hello-2.12/bin/hello",     // a store path that was named already
		"ccc-bash-5.3",
		"/nix/store/fff-last/", // and one with a slash at its end
	}, "\n"))
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, body)
	}
	if want := "fetched 6 paths\n"; body != want {
		t.Errorf("body %q, want %q", body, want)
	}
	want := []string{"aaa-hello-2.12", "bbb-glibc-2.42", "ccc-bash-5.3", "ddd-coreutils-9.11", "eee-jq-1.8", "fff-last"}
	if got := f.names(); !slices.Equal(got, want) {
		t.Errorf("fetched %q, want each of %q once", got, want)
	}
}

func TestAListOfNothing(t *testing.T) {
	f := &fetcher{}
	for _, list := range []string{"", "\n\n", "  \n\r\n"} {
		if status, body := post(t, New(f, 4, quiet), list); status != http.StatusOK || body != "fetched 0 paths\n" {
			t.Errorf("list %q: status %d, body %q", list, status, body)
		}
	}
	if _, asked := f.now(); asked != 0 {
		t.Errorf("%d fetches for lists of nothing", asked)
	}
}

func TestALineThatIsNoPathIsRefusedBeforeAnythingIsFetched(t *testing.T) {
	for _, bad := range []string{
		"/etc/passwd",          // not of the store
		"/nix/store/",          // the store itself
		"/nix/store",           // the same
		"/nix/store//bin",      // no name where the name is
		"relative/path",        // a path that is no name
		"/nix/storeaaa-hello",  // the name of a reference, not of a path
		".",                    // no name of anything
		"..",                   //
		"/nix/store/..",        //
		"/nix/store/../../etc", //
	} {
		f := &fetcher{}
		status, body := post(t, New(f, 4, quiet), "/nix/store/aaa-fine\n"+bad+"\n/nix/store/bbb-fine\n")
		if status != http.StatusBadRequest {
			t.Errorf("%q: status %d, want 400", bad, status)
		}
		if !strings.Contains(body, "line 2") || !strings.Contains(body, fmt.Sprintf("%q", bad)) {
			t.Errorf("%q: the answer does not say which line: %q", bad, body)
		}
		if _, asked := f.now(); asked != 0 {
			t.Errorf("%q: %d fetches before the list was refused", bad, asked)
		}
	}
}

func TestAPathTheServerDoesNotHave(t *testing.T) {
	f := &fetcher{fail: map[string]error{"bbb-missing": fmt.Errorf("%w: %q", refs.ErrNotFound, "bbb-missing")}}
	status, body := post(t, New(f, 1, quiet), "/nix/store/aaa-there\n/nix/store/bbb-missing/bin/x\n")
	if status != http.StatusNotFound {
		t.Fatalf("status %d, want 404: %s", status, body)
	}
	if !strings.Contains(body, "/nix/store/bbb-missing") {
		t.Errorf("the answer does not name the path: %q", body)
	}
}

func TestAPathThatCannotBeFetched(t *testing.T) {
	f := &fetcher{fail: map[string]error{"bbb-broken": errors.New("the bucket answered 503")}}
	status, body := post(t, New(f, 1, quiet), "/nix/store/aaa-there\n/nix/store/bbb-broken\n")
	if status != http.StatusBadGateway {
		t.Fatalf("status %d, want 502: %s", status, body)
	}
	if !strings.Contains(body, "/nix/store/bbb-broken") || !strings.Contains(body, "the bucket answered 503") {
		t.Errorf("the answer does not name the path and the reason: %q", body)
	}
}

func TestNoMoreAtOnceThanTheJobs(t *testing.T) {
	const jobs, paths = 5, 40
	f := &fetcher{hold: make(chan struct{})}
	var list strings.Builder
	for i := range paths {
		fmt.Fprintf(&list, "/nix/store/p%03d\n", i)
	}
	done := make(chan int, 1)
	go func() {
		status, _ := post(t, New(f, jobs, quiet), list.String())
		done <- status
	}()
	// As many as the jobs run at once, and they are all that was begun.
	eventually(t, "as many fetches run as there are jobs", func() bool { r, _ := f.now(); return r == jobs })
	time.Sleep(50 * time.Millisecond)
	if running, asked := f.now(); running != jobs || asked != jobs {
		t.Fatalf("%d running and %d begun, want %d of each", running, asked, jobs)
	}
	close(f.hold)
	if status := <-done; status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.most != jobs || len(f.asked) != paths {
		t.Fatalf("at most %d at once and %d in all, want %d and %d", f.most, len(f.asked), jobs, paths)
	}
}

// One path that cannot be fetched ends the others: the ones that run are
// told to stop, and the ones that have not begun never do.
func TestOneFailureEndsTheRest(t *testing.T) {
	const jobs = 4
	f := &fetcher{hold: make(chan struct{}), fail: map[string]error{"p003-broken": errors.New("it broke")}, failWhen: jobs}
	// Three that are held, then the one that fails once those three run,
	// then many more.
	list := "p000\np001\np002\np003-broken\n"
	for i := 4; i < 50; i++ {
		list += fmt.Sprintf("p%03d\n", i)
	}
	began := time.Now()
	status, body := post(t, New(f, jobs, quiet), list)
	if status != http.StatusBadGateway || !strings.Contains(body, "p003-broken") {
		t.Fatalf("status %d: %s", status, body)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Fatalf("the answer took %v: the failure did not end the request", took)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	slices.Sort(f.ended)
	if want := []string{"p000", "p001", "p002"}; !slices.Equal(f.ended, want) {
		t.Errorf("the fetches that were told to stop: %q, want %q", f.ended, want)
	}
	if len(f.asked) > jobs+1 {
		t.Errorf("%d fetches were begun, want none after the failure", len(f.asked))
	}
	if f.running != 0 {
		t.Errorf("%d fetches still run after the answer", f.running)
	}
}

func TestARequestThatIsGivenUpEndsItsFetches(t *testing.T) {
	f := &fetcher{hold: make(chan struct{})}
	ctx, giveUp := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/preload", strings.NewReader("p0\np1\np2\n")).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		New(f, 2, quiet).ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	eventually(t, "two fetches run", func() bool { r, _ := f.now(); return r == 2 })
	giveUp()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the request went on after it was given up")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.ended) != 2 || len(f.asked) != 2 {
		t.Errorf("%d fetches told to stop of %d begun, want 2 of 2", len(f.ended), len(f.asked))
	}
}

func TestOnlyPostAndOnlyThatPath(t *testing.T) {
	h := New(&fetcher{}, 4, quiet)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodHead} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, "/v1/preload", nil))
		if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
			t.Errorf("%s: status %d, Allow %q", method, rec.Code, rec.Header().Get("Allow"))
		}
	}
	for _, path := range []string{"/", "/v1", "/v1/preload/", "/v1/preload/x", "/v2/preload"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("p0\n")))
		if rec.Code != http.StatusNotFound {
			t.Errorf("POST %s: status %d, want 404", path, rec.Code)
		}
	}
}

func TestAListThatIsTooLong(t *testing.T) {
	f := &fetcher{}
	list := strings.Repeat("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-some-store-path-1.0\n", MaxBody/60+1)
	status, _ := post(t, New(f, 4, quiet), list)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", status)
	}
	if _, asked := f.now(); asked != 0 {
		t.Fatalf("%d fetches of a list that was refused", asked)
	}
}

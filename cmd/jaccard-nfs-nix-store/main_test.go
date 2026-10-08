package main

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amber-store/jaccard-nfs-nix-store/sidecar"
)

// An endpoint ID is the 32 bytes of a public key in hex.
const server = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"

// variables are every variable the command reads. Each test starts with
// none of them set, whatever the environment of the test is.
var variables = []string{
	"JACCARD_SERVER", "JACCARD_PREFIX", "JACCARD_KEY", "JACCARD_NFS_CACHE", "JACCARD_NFS_LISTEN",
	"JACCARD_NFS_MOUNT", "JACCARD_NFS_MOUNT_OPTIONS", "JACCARD_NFS_PULL_TIMEOUT",
	"JACCARD_NFS_PULL_JOBS", "JACCARD_NFS_MATERIALIZE_JOBS",
}

// parse runs the command with the arguments and the environment given and
// returns the settings it would have started a sidecar with.
func parse(t *testing.T, env map[string]string, args ...string) (settings, error) {
	t.Helper()
	for _, name := range variables {
		// Setenv restores what was there when the test ends; an empty
		// value is no value to the command only if it is unset.
		t.Setenv(name, "")
		unsetenv(t, name)
	}
	for name, value := range env {
		t.Setenv(name, value)
	}
	var got settings
	ran := false
	app := newApp(func(_ context.Context, s settings) error {
		got, ran = s, true
		return nil
	})
	app.Writer, app.ErrWriter = io.Discard, io.Discard
	err := app.RunContext(context.Background(), append([]string{"jaccard-nfs-nix-store"}, args...))
	if err == nil && !ran {
		t.Fatal("the command returned without starting anything")
	}
	return got, err
}

func TestTheDefaults(t *testing.T) {
	s, err := parse(t, nil, "--server", server, "--prefix", "/framework/nix/store", "--cache", "/cache")
	if err != nil {
		t.Fatal(err)
	}
	want := settings{
		server:          server,
		prefix:          "/framework/nix/store",
		cache:           "/cache",
		key:             filepath.Join("/cache", "client.key"),
		listen:          sidecar.DefaultListen,
		mount:           "",
		mountOptions:    sidecar.DefaultMountOptions,
		pullTimeout:     2 * time.Minute,
		pullJobs:        4,
		materializeJobs: 2,
	}
	if s != want {
		t.Fatalf("settings\n %+v\nwant\n %+v", s, want)
	}
}

func TestEveryFlagAndItsVariable(t *testing.T) {
	want := settings{
		server:          server,
		prefix:          "/laptop/nix/store/",
		cache:           "/var/cache/store",
		key:             "/keys/client.key",
		listen:          "127.0.0.1:12049",
		mount:           "/export",
		mountOptions:    "vers=4.1,hard",
		pullTimeout:     90 * time.Second,
		pullJobs:        7,
		materializeJobs: 3,
	}
	flags := []string{
		"--server", server, "--prefix", "/laptop/nix/store/", "--cache", "/var/cache/store",
		"--key", "/keys/client.key", "--listen", "127.0.0.1:12049", "--mount", "/export",
		"--mount-options", "vers=4.1,hard", "--pull-timeout", "90s", "--pull-jobs", "7",
		"--materialize-jobs", "3",
	}
	env := map[string]string{
		"JACCARD_SERVER": server, "JACCARD_PREFIX": "/laptop/nix/store/", "JACCARD_NFS_CACHE": "/var/cache/store",
		"JACCARD_KEY": "/keys/client.key", "JACCARD_NFS_LISTEN": "127.0.0.1:12049", "JACCARD_NFS_MOUNT": "/export",
		"JACCARD_NFS_MOUNT_OPTIONS": "vers=4.1,hard", "JACCARD_NFS_PULL_TIMEOUT": "90s", "JACCARD_NFS_PULL_JOBS": "7",
		"JACCARD_NFS_MATERIALIZE_JOBS": "3",
	}

	if s, err := parse(t, nil, flags...); err != nil || s != want {
		t.Errorf("from the flags: %v\n %+v\nwant\n %+v", err, s, want)
	}
	if s, err := parse(t, env); err != nil || s != want {
		t.Errorf("from the variables: %v\n %+v\nwant\n %+v", err, s, want)
	}

	// A flag wins over its variable.
	other := map[string]string{
		"JACCARD_SERVER": strings.Repeat("0", 63) + "1", "JACCARD_PREFIX": "other/", "JACCARD_NFS_CACHE": "/other",
		"JACCARD_KEY": "/other.key", "JACCARD_NFS_LISTEN": "127.0.0.1:1", "JACCARD_NFS_MOUNT": "/other",
		"JACCARD_NFS_MOUNT_OPTIONS": "other", "JACCARD_NFS_PULL_TIMEOUT": "1s", "JACCARD_NFS_PULL_JOBS": "1",
		"JACCARD_NFS_MATERIALIZE_JOBS": "1",
	}
	if s, err := parse(t, other, flags...); err != nil || s != want {
		t.Errorf("flags over variables: %v\n %+v\nwant\n %+v", err, s, want)
	}
}

func TestAnEmptyPrefixIsEveryReference(t *testing.T) {
	s, err := parse(t, nil, "--server", server, "--prefix", "", "--cache", "/cache")
	if err != nil {
		t.Fatalf("an empty prefix that was given: %v", err)
	}
	if s.prefix != "" {
		t.Fatalf("prefix %q", s.prefix)
	}
	if _, err := parse(t, map[string]string{"JACCARD_PREFIX": ""}, "--server", server, "--cache", "/cache"); err != nil {
		t.Fatalf("an empty prefix in the variable: %v", err)
	}
}

func TestWhatIsRefused(t *testing.T) {
	full := map[string]string{"--server": server, "--prefix": "p/", "--cache": "/cache"}
	with := func(change map[string]string) []string {
		var args []string
		for _, name := range []string{"--server", "--prefix", "--cache", "--pull-jobs", "--materialize-jobs", "--pull-timeout", "--listen"} {
			value, ok := full[name]
			if v, changed := change[name]; changed {
				value, ok = v, v != "\x00"
			}
			if ok {
				args = append(args, name, value)
			}
		}
		return args
	}
	const absent = "\x00"
	for what, c := range map[string]struct {
		change map[string]string
		says   string
	}{
		"no server":                 {map[string]string{"--server": absent}, "--server"},
		"a server that is no ID":    {map[string]string{"--server": "not-an-id"}, "--server"},
		"no prefix":                 {map[string]string{"--prefix": absent}, "--prefix"},
		"a prefix no name has":      {map[string]string{"--prefix": "with@sign/"}, "--prefix"},
		"no cache":                  {map[string]string{"--cache": absent}, "--cache"},
		"no pull at a time":         {map[string]string{"--pull-jobs": "0"}, "--pull-jobs"},
		"nothing written at a time": {map[string]string{"--materialize-jobs": "0"}, "--materialize-jobs"},
		"no time to pull in":        {map[string]string{"--pull-timeout": "0s"}, "--pull-timeout"},
		"no address to listen on":   {map[string]string{"--listen": ""}, "--listen"},
	} {
		_, err := parse(t, nil, with(c.change)...)
		if err == nil {
			t.Errorf("%s: no error", what)
			continue
		}
		if !strings.Contains(err.Error(), c.says) {
			t.Errorf("%s: %q does not name %s", what, err, c.says)
		}
	}
	if _, err := parse(t, nil, append(with(nil), "extra")...); err == nil {
		t.Error("an argument was taken")
	}
}

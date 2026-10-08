package mount

import (
	"strings"
	"testing"
)

// table is what /proc/self/mountinfo holds in a container that has an NFS
// mount of the sidecar's at /export: mount ID, parent ID, device, root,
// mount point, options, any number of optional fields, a dash, the type of
// the file system, its source and its own options.
const table = `2317 2209 0:306 / / rw,relatime master:603 - overlay overlay rw,lowerdir=/a:/b
2318 2317 0:310 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
2343 2317 254:1 /pods/123/volumes/store /export rw,relatime shared:1 master:2 - ext4 /dev/vda1 rw,discard
2344 2317 254:1 /pods/123/volumes/cache /cache rw,relatime - ext4 /dev/vda1 rw
2401 2343 0:55 / /export ro,nosuid,nodev,relatime shared:9 - nfs4 :/ ro,vers=4.1,rsize=1048576,soft,addr=127.0.0.1
2402 2317 0:56 / /mnt/with\040space\011tab\012line\134slash rw - tmpfs tmpfs rw
2403 2317 0:57 / /exportother rw - tmpfs tmpfs rw
2404 2317 0:58 / /export/below rw - tmpfs tmpfs rw
`

func TestFind(t *testing.T) {
	for _, c := range []struct {
		name, target string
		fstype       string
		mounted      bool
	}{
		// Two mounts on one point: the one mounted last is the one seen.
		{"the mount on top", "/export", "nfs4", true},
		{"a mount with nothing on it", "/cache", "ext4", true},
		{"the root", "/", "overlay", true},
		{"a path that is not escaped", "/mnt/with space\ttab\nline\\slash", "tmpfs", true},
		{"a directory under a mount", "/export/bin", "", false},
		{"a path a mount point begins with", "/exp", "", false},
		{"a directory that is no mount point", "/var", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			fstype, mounted, err := find(strings.NewReader(table), c.target)
			if err != nil {
				t.Fatal(err)
			}
			if fstype != c.fstype || mounted != c.mounted {
				t.Fatalf("find(%q) = %q, %v; want %q, %v", c.target, fstype, mounted, c.fstype, c.mounted)
			}
		})
	}
}

func TestFindRefusesATableThatIsNone(t *testing.T) {
	for name, text := range map[string]string{
		"a line with no dash":          "2401 2343 0:55 / /export ro shared:9 nfs4 :/ ro\n",
		"a line with too few fields":   "2401 2343 0:55\n",
		"a line that ends at the dash": "2401 2343 0:55 / /export ro -\n",
	} {
		if _, _, err := find(strings.NewReader(text), "/export"); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	// A table with nothing in it is a table.
	if _, mounted, err := find(strings.NewReader(""), "/export"); err != nil || mounted {
		t.Errorf("an empty table: %v, %v", mounted, err)
	}
}

func TestUnescape(t *testing.T) {
	for in, want := range map[string]string{
		`/plain`:             "/plain",
		`/a\040b`:            "/a b",
		`/a\134b`:            `/a\b`,
		`/ends\`:             `/ends\`,
		`/short\04`:          `/short\04`,
		`/not\888octal`:      `/not\888octal`,
		`/two\040\040spaces`: "/two  spaces",
	} {
		if got := unescape(in); got != want {
			t.Errorf("unescape(%q) = %q, want %q", in, got, want)
		}
	}
}

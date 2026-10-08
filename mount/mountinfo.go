// Package mount mounts the sidecar's own NFS server on a directory, takes
// the mount away again, and tells whether a directory is a mount point. The
// mounting is for Linux; reading a mount table is for anywhere.
package mount

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// find reads a mount table in the form of /proc/self/mountinfo and returns
// the type of the file system mounted on target, which is a clean absolute
// path. When several are mounted on it, one over the other, it is the last:
// the one that is seen.
func find(mountinfo io.Reader, target string) (fstype string, mounted bool, err error) {
	lines := bufio.NewScanner(mountinfo)
	lines.Buffer(make([]byte, 64<<10), 1<<20)
	for lines.Scan() {
		line := lines.Text()
		if line == "" {
			continue
		}
		// ID, parent, device, root, mount point, options, optional
		// fields that end with a dash, type, source, options.
		before, after, dashed := strings.Cut(line, " - ")
		fields := strings.Fields(before)
		rest := strings.Fields(after)
		if !dashed || len(fields) < 6 || len(rest) < 1 {
			return "", false, fmt.Errorf("mount: a line of the mount table that is none: %q", line)
		}
		if unescape(fields[4]) == target {
			fstype, mounted = rest[0], true
		}
	}
	if err := lines.Err(); err != nil {
		return "", false, fmt.Errorf("mount: reading the mount table: %w", err)
	}
	return fstype, mounted, nil
}

// unescape undoes what the kernel does to a path in a mount table: a
// space, a tab, a line feed and a backslash are written as a backslash and
// three octal digits.
func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && octal(s[i+1]) && octal(s[i+2]) && octal(s[i+3]) {
			b.WriteByte((s[i+1]-'0')<<6 | (s[i+2]-'0')<<3 | (s[i+3] - '0'))
			i += 3
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func octal(c byte) bool { return c >= '0' && c <= '7' }

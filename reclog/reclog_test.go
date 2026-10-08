package reclog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// write makes a log at path holding recs and closes it.
func write(t *testing.T, path string, recs ...[]byte) {
	t.Helper()
	l, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := l.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// read opens the log at path, returns its records and closes it.
func read(t *testing.T, path string) [][]byte {
	t.Helper()
	l, recs, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return recs
}

func same(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d is %d bytes %.20q, want %d bytes %.20q", i, len(got[i]), got[i], len(want[i]), want[i])
		}
	}
}

func size(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func TestRecordsComeBackInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	recs := [][]byte{
		[]byte("first"),
		{},
		bytes.Repeat([]byte{0xab}, 70_000),
		[]byte("a name\nwith a newline and \xff\xfe bytes that are no UTF-8"),
	}
	write(t, path, recs...)
	same(t, read(t, path), recs)
}

func TestAMissingFileIsCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	if recs := read(t, path); len(recs) != 0 {
		t.Fatalf("%d records in a new log", len(recs))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestAppendingToWhatWasThere(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	write(t, path, []byte("one"), []byte("two"))
	write(t, path, []byte("three"))
	same(t, read(t, path), [][]byte{[]byte("one"), []byte("two"), []byte("three")})
}

// A record that a crash cut short is dropped, with whatever follows it, the
// file is cut back to the last whole record, and records go on from there.
func TestARecordCutShortIsDropped(t *testing.T) {
	whole := [][]byte{[]byte("one"), []byte("two")}
	last := bytes.Repeat([]byte("x"), 300)
	// Every way the last record can be cut: in its length, in its bytes,
	// in its checksum.
	for cut := int64(1); cut <= int64(len(last))+6; cut += 7 {
		t.Run(fmt.Sprintf("%d bytes short", cut), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "log")
			write(t, path, whole...)
			kept := size(t, path)
			write(t, path, last)
			if err := os.Truncate(path, size(t, path)-cut); err != nil {
				t.Fatal(err)
			}

			same(t, read(t, path), whole)
			if got := size(t, path); got != kept {
				t.Fatalf("the file is %d bytes after it was read, want the %d of its whole records", got, kept)
			}
			write(t, path, []byte("after"))
			same(t, read(t, path), append(whole[:2:2], []byte("after")))
		})
	}
}

func TestARecordThatFailsItsChecksumIsDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	write(t, path, []byte("one"), []byte("two"))
	kept := size(t, path)
	write(t, path, []byte("three"), []byte("four"))

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[kept+2] ^= 0x01 // in the bytes of "three"
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	// What follows a record that is wrong cannot be told from garbage.
	same(t, read(t, path), [][]byte{[]byte("one"), []byte("two")})
	if got := size(t, path); got != kept {
		t.Fatalf("the file is %d bytes, want %d", got, kept)
	}
}

func TestALengthNoRecordHasEndsTheLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	write(t, path, []byte("one"))
	kept := size(t, path)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A length of 2^35 and then nothing.
	if _, err := f.Write([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x01, 'x', 'y'}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	same(t, read(t, path), [][]byte{[]byte("one")})
	if got := size(t, path); got != kept {
		t.Fatalf("the file is %d bytes, want %d", got, kept)
	}
}

func TestARecordTooLargeIsRefused(t *testing.T) {
	l, _, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Append(make([]byte, MaxRecord+1)); err == nil {
		t.Fatal("a record above MaxRecord was taken")
	}
	if err := l.Append(make([]byte, MaxRecord)); err != nil {
		t.Fatal(err)
	}
}

func TestSyncPutsTheRecordsInTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Append([]byte("buffered")); err != nil {
		t.Fatal(err)
	}
	if got := size(t, path); got != 0 {
		t.Fatalf("%d bytes in the file before a flush: the record was to wait in the buffer", got)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	// Another reader of the file sees it while the log is still open.
	other, recs, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
	same(t, recs, [][]byte{[]byte("buffered")})
}

func TestFlushPutsTheRecordsInTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Append([]byte("buffered")); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := size(t, path); got == 0 {
		t.Fatal("nothing in the file after a flush")
	}
}

func TestAppendFromManyGoroutines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				if err := l.Append(fmt.Appendf(nil, "%d/%d", g, i)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	var got, want []string
	for _, r := range read(t, path) {
		got = append(got, string(r))
	}
	for g := range 8 {
		for i := range 500 {
			want = append(want, fmt.Sprintf("%d/%d", g, i))
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %q, want %q", got[i], want[i])
		}
	}
}

func TestAppendAfterCloseFails(t *testing.T) {
	l, _, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte("late")); err == nil {
		t.Fatal("a closed log took a record")
	}
	if err := l.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}
}

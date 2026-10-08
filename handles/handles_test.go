package handles

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/amber-store/jaccard-nfs-nix-store/reclog"
	"github.com/zeebo/blake3"
)

func open(t *testing.T, path string) *Table {
	t.Helper()
	tab, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tab.Close() })
	return tab
}

func TestTheRootIsSixteenZeroBytes(t *testing.T) {
	if Root != (ID{}) {
		t.Fatalf("Root is %x", Root)
	}
}

func TestAChildIsTheHashOfItsParentAndItsName(t *testing.T) {
	parent := ID{1, 2, 3}
	sum := blake3.Sum256(append(parent[:], "lib"...))
	if got, want := Child(parent, "lib"), ID(sum[:16]); got != want {
		t.Fatalf("Child = %x, want %x", got, want)
	}
}

func TestAPathHasOneHandle(t *testing.T) {
	a, b := open(t, filepath.Join(t.TempDir(), "a")), open(t, filepath.Join(t.TempDir(), "b"))
	// The same path in two tables, as in two runs of the sidecar.
	inA := a.Add(a.Add(Root, "store-path"), "bin")
	inB := b.Add(b.Add(Root, "store-path"), "bin")
	if inA != inB {
		t.Fatalf("one path, two handles: %x and %x", inA, inB)
	}
	if inA != Child(Child(Root, "store-path"), "bin") {
		t.Fatal("Add does not return what Child does")
	}
}

func TestTwoPathsHaveTwoHandles(t *testing.T) {
	x, y := Child(Root, "x"), Child(Root, "y")
	if x == y {
		t.Fatal("two names in one directory share a handle")
	}
	// Two directories of the same name and content are still two nodes.
	if Child(x, "lib") == Child(y, "lib") {
		t.Fatal("one name in two directories shares a handle")
	}
	// Where a name ends and the next begins is part of the path.
	if Child(Child(Root, "ab"), "c") == Child(Child(Root, "a"), "bc") {
		t.Fatal("a/bc and ab/c share a handle")
	}
}

func TestTheInodeIsTheFirstEightBytes(t *testing.T) {
	id := ID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xff, 0xff}
	if got, want := id.Inode(), uint64(0x0807060504030201); got != want {
		t.Fatalf("Inode = %#x, want %#x", got, want)
	}
}

func TestPath(t *testing.T) {
	tab := open(t, filepath.Join(t.TempDir(), "handles"))
	top := tab.Add(Root, "abc-hello-2.12")
	bin := tab.Add(top, "bin")
	hello := tab.Add(bin, "hello")

	for _, c := range []struct {
		id   ID
		want []string
	}{
		{Root, nil},
		{top, []string{"abc-hello-2.12"}},
		{bin, []string{"abc-hello-2.12", "bin"}},
		{hello, []string{"abc-hello-2.12", "bin", "hello"}},
	} {
		got, ok := tab.Path(c.id)
		if !ok || !slices.Equal(got, c.want) {
			t.Errorf("Path(%x) = %q, %v; want %q", c.id, got, ok, c.want)
		}
	}
	if got, ok := tab.Path(ID{9, 9, 9}); ok {
		t.Errorf("a handle nobody was given has the path %q", got)
	}
	// A node whose parent the table never saw leads nowhere.
	orphan := tab.Add(ID{7}, "orphan")
	if got, ok := tab.Path(orphan); ok {
		t.Errorf("a node under an unknown parent has the path %q", got)
	}
}

func TestAddingTwiceRecordsOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles")
	tab := open(t, path)
	first := tab.Add(Root, "x")
	if err := tab.Flush(); err != nil {
		t.Fatal(err)
	}
	before := fileSize(t, path)
	if again := tab.Add(Root, "x"); again != first {
		t.Fatalf("the second Add gave %x, the first %x", again, first)
	}
	if err := tab.Flush(); err != nil {
		t.Fatal(err)
	}
	if after := fileSize(t, path); after != before {
		t.Fatalf("the file grew from %d to %d bytes for a node it had", before, after)
	}
	if tab.Len() != 1 {
		t.Fatalf("Len = %d, want 1", tab.Len())
	}
}

func TestATableReadBackResolvesWhatWasAdded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles")
	tab, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// A name is bytes: it need be neither one line nor UTF-8.
	names := []string{"plain", "with space", "new\nline", "not\xff\xfeutf8", ""}
	ids := map[ID][]string{}
	top := tab.Add(Root, "top")
	for _, n := range names {
		ids[tab.Add(top, n)] = []string{"top", n}
	}
	if err := tab.Close(); err != nil {
		t.Fatal(err)
	}

	back := open(t, path)
	if back.Len() != len(names)+1 {
		t.Fatalf("Len = %d after reading back, want %d", back.Len(), len(names)+1)
	}
	for id, want := range ids {
		got, ok := back.Path(id)
		if !ok || !slices.Equal(got, want) {
			t.Errorf("Path(%x) = %q, %v after reading back; want %q", id, got, ok, want)
		}
	}
	// What was read back is not written again.
	before := fileSize(t, path)
	back.Add(top, "plain")
	if err := back.Flush(); err != nil {
		t.Fatal(err)
	}
	if after := fileSize(t, path); after != before {
		t.Fatalf("the file grew from %d to %d bytes for a node that was read back", before, after)
	}
}

func TestARecordCutShortIsDroppedAndTheRestKept(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles")
	tab, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	kept := tab.Add(Root, "kept")
	lost := tab.Add(Root, "lost-to-the-crash")
	if err := tab.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, fileSize(t, path)-3); err != nil {
		t.Fatal(err)
	}

	back := open(t, path)
	if _, ok := back.Path(kept); !ok {
		t.Error("the whole record before the cut is gone")
	}
	if _, ok := back.Path(lost); ok {
		t.Error("the record that was cut short is there")
	}
	// The path that lost its handle gets the same one again.
	if again := back.Add(Root, "lost-to-the-crash"); again != lost {
		t.Errorf("the handle is %x after the crash, it was %x", again, lost)
	}
}

func TestARecordThatIsNoneIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles")
	l, _, err := reclog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// Shorter than a parent's handle.
	if err := l.Append([]byte("too short")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if tab, err := Open(path); err == nil {
		tab.Close()
		t.Fatal("a table was read from a file that is none")
	}
}

func TestTheTableIsFlushedByItself(t *testing.T) {
	defer func(d time.Duration) { flushEvery = d }(flushEvery)
	flushEvery = 5 * time.Millisecond

	path := filepath.Join(t.TempDir(), "handles")
	tab := open(t, path)
	tab.Add(Root, "x")
	deadline := time.Now().Add(5 * time.Second)
	for fileSize(t, path) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("nothing was written to the file without a Flush")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAddFromManyGoroutines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handles")
	tab, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	const goroutines, each = 8, 1250
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dir := tab.Add(Root, fmt.Sprintf("dir%d", g%4)) // shared between two goroutines
			for i := range each {
				id := tab.Add(dir, fmt.Sprintf("file%d", i))
				if _, ok := tab.Path(id); !ok {
					t.Errorf("a node just added has no path")
					return
				}
			}
		}()
	}
	wg.Wait()
	want := 4 + 4*each
	if tab.Len() != want {
		t.Fatalf("Len = %d, want %d", tab.Len(), want)
	}
	if err := tab.Close(); err != nil {
		t.Fatal(err)
	}
	if back := open(t, path); back.Len() != want {
		t.Fatalf("Len = %d after reading back, want %d", back.Len(), want)
	}
}

func TestCloseTwice(t *testing.T) {
	tab, err := Open(filepath.Join(t.TempDir(), "handles"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tab.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tab.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}
	// A table that was closed still answers; what it is given is not kept
	// past the process.
	id := tab.Add(Root, "late")
	if _, ok := tab.Path(id); !ok {
		t.Fatal("a closed table lost a node it was given")
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

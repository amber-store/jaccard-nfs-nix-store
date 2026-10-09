package sidecar

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"golang.org/x/sys/unix"
)

// kernel is the mount package as a test wants it: its unmount fails with
// what the test says, and it records what was done.
type kernel struct {
	fail       error // what unmount fails with
	detachFail error // what detach fails with
	unmounts   int
	detaches   int
}

func (k *kernel) leaver() leaver {
	return leaver{
		unmount: func(string) error {
			k.unmounts++
			if k.fail != nil {
				return fmt.Errorf("unmounting: %w", k.fail)
			}
			return nil
		},
		detach: func(string) error {
			k.detaches++
			if k.detachFail != nil {
				return fmt.Errorf("detaching: %w", k.detachFail)
			}
			return nil
		},
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestLeavingAMountThatIsNotInUse(t *testing.T) {
	k := &kernel{}
	if err := k.leaver().leave("/export"); err != nil {
		t.Fatal(err)
	}
	if k.unmounts != 1 || k.detaches != 0 {
		t.Fatalf("%d unmounts, %d detaches: want one unmount and nothing else", k.unmounts, k.detaches)
	}
}

// A mount that is in use is detached at once and not waited for: the
// kubelet may give a sidecar two seconds between telling it to stop and
// killing it, and a sidecar that is killed with its mount in place leaves
// the mount on the node.
func TestLeavingAMountThatIsInUse(t *testing.T) {
	k := &kernel{fail: unix.EBUSY}
	if err := k.leaver().leave("/export"); err != nil {
		t.Fatal(err)
	}
	if k.unmounts != 1 || k.detaches != 1 {
		t.Fatalf("%d unmounts, %d detaches: want it detached after the one unmount that was refused", k.unmounts, k.detaches)
	}
}

func TestLeavingWhereNothingIsMounted(t *testing.T) {
	k := &kernel{fail: unix.EINVAL}
	if err := k.leaver().leave("/export"); err != nil {
		t.Fatalf("nothing mounted is nothing to leave: %v", err)
	}
	if k.unmounts != 1 || k.detaches != 0 {
		t.Fatalf("%d unmounts, %d detaches", k.unmounts, k.detaches)
	}
}

func TestLeavingFails(t *testing.T) {
	k := &kernel{fail: unix.EPERM}
	if err := k.leaver().leave("/export"); !errors.Is(err, unix.EPERM) {
		t.Fatalf("err = %v, want the failure of the unmount", err)
	}
	if k.unmounts != 1 || k.detaches != 0 {
		t.Fatalf("%d unmounts, %d detaches: a failure that is not busy is not answered with a detach", k.unmounts, k.detaches)
	}

	k = &kernel{fail: unix.EBUSY, detachFail: unix.EPERM}
	if err := k.leaver().leave("/export"); !errors.Is(err, unix.EPERM) {
		t.Fatalf("err = %v, want the failure of the detach", err)
	}
}

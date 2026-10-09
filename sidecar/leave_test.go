package sidecar

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// kernel is the mount package as a test wants it: it refuses to unmount as
// busy so many times, and records what was done.
type kernel struct {
	busy     int   // unmounts that are still refused as busy
	fail     error // what unmount fails with instead
	unmounts int
	detached bool
	slept    time.Duration
}

func (k *kernel) leaver() leaver {
	return leaver{
		unmount: func(string) error {
			k.unmounts++
			switch {
			case k.fail != nil:
				return fmt.Errorf("unmounting: %w", k.fail)
			case k.busy > 0:
				k.busy--
				return fmt.Errorf("unmounting: %w", unix.EBUSY)
			}
			return nil
		},
		detach: func(string) error {
			k.detached = true
			return nil
		},
		sleep: func(d time.Duration) { k.slept += d },
		log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestLeavingAMountThatIsNotInUse(t *testing.T) {
	k := &kernel{}
	if err := k.leaver().leave("/export"); err != nil {
		t.Fatal(err)
	}
	if k.unmounts != 1 || k.detached || k.slept != 0 {
		t.Fatalf("%d unmounts, detached %v, %v slept: want one unmount and nothing else", k.unmounts, k.detached, k.slept)
	}
}

func TestLeavingAMountThatIsBusyForAMoment(t *testing.T) {
	k := &kernel{busy: 3}
	if err := k.leaver().leave("/export"); err != nil {
		t.Fatal(err)
	}
	if k.unmounts != 4 || k.detached {
		t.Fatalf("%d unmounts, detached %v: want it unmounted at the fourth try", k.unmounts, k.detached)
	}
	if k.slept != 3*busyEvery {
		t.Fatalf("%v slept, want %v", k.slept, 3*busyEvery)
	}
}

func TestLeavingAMountThatStaysBusy(t *testing.T) {
	k := &kernel{busy: 1_000_000}
	if err := k.leaver().leave("/export"); err != nil {
		t.Fatal(err)
	}
	if !k.detached {
		t.Fatal("a mount that stayed busy was not detached")
	}
	// It was tried for busyFor and no longer.
	if k.slept != busyFor {
		t.Fatalf("%v slept, want %v", k.slept, busyFor)
	}
}

func TestLeavingWhereNothingIsMounted(t *testing.T) {
	k := &kernel{fail: unix.EINVAL}
	if err := k.leaver().leave("/export"); err != nil {
		t.Fatalf("nothing mounted is nothing to leave: %v", err)
	}
	if k.unmounts != 1 || k.detached {
		t.Fatalf("%d unmounts, detached %v", k.unmounts, k.detached)
	}
}

func TestLeavingFails(t *testing.T) {
	k := &kernel{fail: unix.EPERM}
	err := k.leaver().leave("/export")
	if !errors.Is(err, unix.EPERM) {
		t.Fatalf("err = %v, want the failure of the unmount", err)
	}
	if k.unmounts != 1 || k.detached {
		t.Fatalf("%d unmounts, detached %v: a failure that is not busy is not tried again", k.unmounts, k.detached)
	}
}

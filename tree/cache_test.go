package tree

import (
	"sync"
	"testing"
)

func TestLRUPushesOutTheLeastRecentlyUsed(t *testing.T) {
	c := newLRU[string, int](2)
	c.add("a", 1, 1)
	c.add("b", 2, 1)
	if v, ok := c.get("a"); !ok || v != 1 {
		t.Fatalf("a: %d, %v", v, ok)
	}
	c.add("c", 3, 1) // b was used longest ago
	if _, ok := c.get("b"); ok {
		t.Error("b is still there")
	}
	for name, want := range map[string]int{"a": 1, "c": 3} {
		if v, ok := c.get(name); !ok || v != want {
			t.Errorf("%s: %d, %v", name, v, ok)
		}
	}
	if c.size() != 2 {
		t.Errorf("size %d, want 2", c.size())
	}
}

func TestLRUByCost(t *testing.T) {
	c := newLRU[string, string](10)
	c.add("a", "aaaa", 4)
	c.add("b", "bbbb", 4)
	c.add("c", "cccccc", 6) // pushes out a: 4+6 fits, 4+4+6 does not
	if _, ok := c.get("a"); ok {
		t.Error("a is still there")
	}
	if _, ok := c.get("b"); !ok {
		t.Error("b is gone")
	}
	if c.size() != 10 {
		t.Errorf("size %d, want 10", c.size())
	}
	c.add("huge", "more than everything", 11) // not kept, and nothing lost for it
	if _, ok := c.get("huge"); ok {
		t.Error("a value larger than the budget is kept")
	}
	if _, ok := c.get("c"); !ok || c.size() != 10 {
		t.Errorf("c gone or size %d after a value that was not kept", c.size())
	}
}

func TestLRUAddAgain(t *testing.T) {
	c := newLRU[string, int](5)
	c.add("a", 1, 2)
	c.add("a", 2, 3)
	if v, ok := c.get("a"); !ok || v != 2 {
		t.Errorf("a: %d, %v", v, ok)
	}
	if c.size() != 3 {
		t.Errorf("size %d, want 3", c.size())
	}
}

func TestLRUConcurrent(t *testing.T) {
	c := newLRU[int, int](16)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				k := (g*31 + i) % 40
				if v, ok := c.get(k); ok && v != k {
					t.Errorf("%d: %d", k, v)
				}
				c.add(k, k, 1)
			}
		}()
	}
	wg.Wait()
	if c.size() > 16 {
		t.Errorf("size %d over the budget of 16", c.size())
	}
}

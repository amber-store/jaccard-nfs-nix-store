package tree

import (
	"container/list"
	"sync"
)

// lru is a cache that holds values up to a budget and, when a new value
// does not fit, drops the ones used longest ago. Every value has a cost,
// and the budget is the sum of the costs the cache may hold: with a cost of
// 1 for every value it is a number of values, with the length of a value
// it is a number of bytes. It is safe for concurrent use.
type lru[K comparable, V any] struct {
	mu     sync.Mutex
	budget int64
	used   int64
	order  *list.List // of *lruItem[K, V], the most recently used first
	items  map[K]*list.Element
}

// lruItem is one value of the cache, as the list holds it.
type lruItem[K comparable, V any] struct {
	key   K
	value V
	cost  int64
}

func newLRU[K comparable, V any](budget int64) *lru[K, V] {
	return &lru[K, V]{budget: budget, order: list.New(), items: make(map[K]*list.Element)}
}

// get returns the value kept under k and marks it as just used.
func (c *lru[K, V]) get(k K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[k]
	if !ok {
		var zero V
		return zero, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*lruItem[K, V]).value, true
}

// add keeps v under k, in place of what was there, and drops the values
// used longest ago until the budget holds. A value that costs more than the
// whole budget is not kept, and nothing is dropped for it.
func (c *lru[K, V]) add(k K, v V, cost int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		c.remove(el)
	}
	if cost > c.budget {
		return
	}
	c.items[k] = c.order.PushFront(&lruItem[K, V]{key: k, value: v, cost: cost})
	c.used += cost
	for c.used > c.budget {
		c.remove(c.order.Back())
	}
}

// size returns the sum of the costs of the values held.
func (c *lru[K, V]) size() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

// remove drops an element. The caller holds the lock.
func (c *lru[K, V]) remove(el *list.Element) {
	item := c.order.Remove(el).(*lruItem[K, V])
	delete(c.items, item.key)
	c.used -= item.cost
}

package refs

import (
	"fmt"

	"github.com/amber-store/core/key"
)

// Pin is a reference that was fetched: the name it was looked up under,
// without the prefix, and the root of its tree.
type Pin struct {
	Name string
	Root key.Key
}

// encode returns the pin as a record of the pin file: the root's 32 bytes
// and then the name.
func (p Pin) encode() []byte {
	rec := make([]byte, 0, key.Size+len(p.Name))
	rec = append(rec, p.Root[:]...)
	return append(rec, p.Name...)
}

// decodePin reads a record of the pin file. A pin has a name, so a record
// is longer than a key.
func decodePin(rec []byte) (Pin, error) {
	if len(rec) <= key.Size {
		return Pin{}, fmt.Errorf("a record of %d bytes, a pin has %d at least", len(rec), key.Size+1)
	}
	root, err := key.Parse(rec[:key.Size])
	if err != nil {
		return Pin{}, err
	}
	return Pin{Name: string(rec[key.Size:]), Root: root}, nil
}

// pinSet is the pins in the order they were made, and by name.
type pinSet struct {
	list   []Pin
	byName map[string]key.Key
}

// add records a pin. It reports false for a name that is pinned already,
// which keeps the root and the place it has.
func (ps *pinSet) add(p Pin) bool {
	if _, ok := ps.byName[p.Name]; ok {
		return false
	}
	if ps.byName == nil {
		ps.byName = make(map[string]key.Key)
	}
	ps.byName[p.Name] = p.Root
	ps.list = append(ps.list, p)
	return true
}

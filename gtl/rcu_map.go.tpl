package PACKAGE

import (
	"sync/atomic"
	"unsafe"
)

// ZZMap is a concurrent map. A reader can access the map without lock,
// regardless of background updates.  The writer side must coordinate using an
// external mutex if there are multiple writers. This map is linearizable.
//
// Example:
//
//   m := NewZZMap(10)
//   go func() {  // writer
//     m.Store("foo", "bar")
//   }()
//   go func() {  // reader
//     val, ok := m.Load("foo")
//   }
type ZZMap struct {
	p unsafe.Pointer // *zzMapState
}

// ZZMapState represents a fixed-size chained hash table. It can store up to
// maxCapacity key/value pairs.  Beyond that, the caller must create a new
// ZZMapState with a larger capacity.
type zzMapState struct {
	log2Len     uint             // ==log2(len(table))
	mask        uint64           // == ^(log2Len-1)
	table       []unsafe.Pointer // *zzMapNode
	n           int              // # of objects currently stored in the table
	maxCapacity int              // max # of object that can be stored
}

// ZZMapNode represents a hash bucket.
type zzMapNode struct {
	key   KEY
	value VALUE

	// next points to the next element in the same hash bucket
	next unsafe.Pointer // *zzMapNode
}

func newZZMapState(log2Len uint) *zzMapState {
	len := int(1 << log2Len)
	table := &zzMapState{
		log2Len:     log2Len,
		mask:        uint64(log2Len - 1),
		table:       make([]unsafe.Pointer, 1<<log2Len),
		maxCapacity: int(float64(len) * 0.8),
	}
	if table.maxCapacity < len {
		table.maxCapacity = len
	}
	return table
}

// NewZZMap creates a new map. Arg initialLenHint suggests the the initial
// capacity.  If you plan to store 100 keys, then pass 100 as the value. If you
// don't know the capacity, pass 0 as initialLenHint.
func NewZZMap(initialLenHint int) *ZZMap {
	log2Len := uint(3) // 8 nodes
	for (1 << log2Len) < initialLenHint {
		if log2Len > 31 {
			// TODO(saito) We could make the table to grow larger than 32 bits, but
			// doing so will break 32bit builds.
			panic(initialLenHint)
		}
		log2Len++
	}
	m := ZZMap{p: unsafe.Pointer(newZZMapState(log2Len))}
	return &m
}

// Load finds a value with the given key. Returns false if not found.
func (m *ZZMap) Load(key KEY) (VALUE, bool) {
	hash := HASH(key)
	table := (*zzMapState)(atomic.LoadPointer(&m.p))
	b := int(hash & table.mask)
	node := (*zzMapNode)(atomic.LoadPointer(&table.table[b]))
	for node != nil {
		if node.key == key {
			return node.value, true
		}
		node = (*zzMapNode)(atomic.LoadPointer(&node.next))
	}
	var dummy VALUE
	return dummy, false
}

// store returns false iff the table needs resizing.
func (t *zzMapState) store(key KEY, value VALUE) bool {
	var (
		hash     = HASH(key)
		b        = int(hash & t.mask)
		node     = (*zzMapNode)(t.table[b])
		probeLen = 0
		prevNode *zzMapNode
	)
	for node != nil {
		if node.key == key {
			newNode := *node
			newNode.value = value
			if prevNode == nil {
				atomic.StorePointer(&t.table[b], unsafe.Pointer(&newNode))
			} else {
				atomic.StorePointer(&prevNode.next, unsafe.Pointer(&newNode))
			}
			return true
		}
		prevNode = node
		node = (*zzMapNode)(node.next)
		probeLen++
		if probeLen >= 4 && t.n >= t.maxCapacity {
			return false
		}
	}
	newNode := zzMapNode{key: key, value: value}
	if prevNode == nil {
		atomic.StorePointer(&t.table[b], unsafe.Pointer(&newNode))
	} else {
		atomic.StorePointer(&prevNode.next, unsafe.Pointer(&newNode))
	}
	t.n++
	return true
}

// Store stores the value for the given key. If the key is already in the map,
// it updates the mapping to the given value.
//
// Caution: if Store() is going to be called concurrently, it must be serialized
// externally.
func (m *ZZMap) Store(key KEY, value VALUE) {
	table := (*zzMapState)(atomic.LoadPointer(&m.p))
	if table.store(key, value) {
		return
	}
	log2Len := table.log2Len + 1
	if log2Len > 31 {
		panic(log2Len)
	}
	newTable := newZZMapState(log2Len)
	// Copy the contents of the old table over to the new table.
	for _, p := range table.table {
		node := (*zzMapNode)(p)
		for node != nil {
			if !newTable.store(node.key, node.value) {
				panic(node)
			}
			node = (*zzMapNode)(node.next)
		}
	}
	if !newTable.store(key, value) {
		panic(key)
	}
	atomic.StorePointer(&m.p, unsafe.Pointer(newTable))
}

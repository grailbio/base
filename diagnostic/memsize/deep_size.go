package memsize

import (
	"reflect"
	"unsafe"
)

// DeepSize estimates the amount of memory used by a Go value. It's intended as a
// memory usage debugging aid. Argument must be a pointer to a value.
//
// Not thread safe. Behavior is undefined if any value reachable from the argument
// is concurrently mutated. In general, do not call this in production.
//
// Behavior:
// * Recursively descends into contained values (struct fields, slice elements,
// etc.), tracking visitation (by memory address) to handle cycles.
// * Only counts slice length, not unused capacity.
// * Only counts map key and value size, not map overhead.
// * Does not count functions or channels.
//
// The implementation relies on the Go garbage collector being non-compacting (not
// moving values in memory), due to thread non-safety noted above. This is true as
// of Go 1.13, but could change in the future.
func DeepSize(x interface{}) (bytes int) {
	if x == nil {
		return 0
	}
	v := reflect.ValueOf(x)
	if v.Kind() != reflect.Ptr {
		panic("must be a pointer")
	}
	if v.IsNil() {
		return 0
	}
	scanner := &memoryScanner{
		memory:  &intervalSet{},
		visited: make(map[memoryAndKind]struct{}),
	}

	unaddressableBytes := scanner.scan(v.Elem(), true)
	return scanner.memory.totalCovered() + unaddressableBytes
}

type memoryAndKind struct {
	interval
	reflect.Kind
}

func getMemoryAndType(x reflect.Value) memoryAndKind {
	start := int64(x.UnsafeAddr())
	size := int64(x.Type().Size())
	kind := x.Kind()
	return memoryAndKind{
		interval: interval{start: start, length: size},
		Kind:     kind,
	}
}

// memoryScanner can recursively scan memory used by a reflect.Value
// not thread safe
// scan should only be called once
type memoryScanner struct {
	memory  *intervalSet               // memory is a set of memory locations that are used in scan()
	visited map[memoryAndKind]struct{} // visited is a map of locations that have already been visited by scan
}

// scan recursively traverses a reflect.Value and populates all
// x is the Value whose size is to be counted
// includeX indicates whether the bytes for x itself should be counted
// returns a count of unaddressable bytes.
func (s *memoryScanner) scan(x reflect.Value, includeX bool) (unaddressableBytes int) {
	if x.CanAddr() {
		memtype := getMemoryAndType(x)
		if _, ok := s.visited[memtype]; ok {
			return
		}
		s.visited[memtype] = struct{}{}
		s.memory.add(memtype.interval)
	} else if includeX {
		unaddressableBytes += int(x.Type().Size())
	}

	switch x.Kind() {
	case reflect.String:
		m := x.String()
		hdr := (*reflect.StringHeader)(unsafe.Pointer(&m))
		s.memory.add(interval{int64(hdr.Data), int64(hdr.Len)})
	case reflect.Array:
		if containsPointers(x.Type()) { // must scan each element individually
			for i := 0; i < x.Len(); i++ {
				unaddressableBytes += s.scan(x.Index(i), false)
			}
		}
	case reflect.Slice:
		if x.Len() > 0 {
			if containsPointers(x.Index(0).Type()) { // must scan each element individually
				for i := 0; i < x.Len(); i++ {
					unaddressableBytes += s.scan(x.Index(i), true)
				}
			} else { // add the content of the slice to the memory counter
				start := int64(x.Pointer())
				size := int64(x.Index(0).Type().Size()) * int64(x.Len())
				s.memory.add(interval{start: start, length: size})
			}
		}
	case reflect.Interface, reflect.Ptr:
		if !x.IsNil() {
			unaddressableBytes += s.scan(x.Elem(), true)
		}
	case reflect.Struct:
		for _, fieldI := range structChild(x) {
			unaddressableBytes += s.scan(fieldI, false)
		}
	case reflect.Map:
		for _, key := range x.MapKeys() {
			val := x.MapIndex(key)
			unaddressableBytes += s.scan(key, true)
			unaddressableBytes += s.scan(val, true)
		}
	case reflect.Func, reflect.Chan:
		// Can't do better than this:
	default:
	}
	return
}

func containsPointers(x reflect.Type) bool {
	switch x.Kind() {
	case reflect.String, reflect.Slice, reflect.Map, reflect.Interface, reflect.Ptr:
		return true
	case reflect.Array:
		if x.Len() > 0 {
			return containsPointers(x.Elem())
		}
	case reflect.Struct:
		for i, n := 0, x.NumField(); i < n; i++ {
			if containsPointers(x.Field(i).Type) {
				return true
			}
		}
	}
	return false
}

// v must be a struct kind.
// returns all the fields of this struct (recursively for nested structs) that are pointer types
func structChild(x reflect.Value) []reflect.Value {
	var ret []reflect.Value
	for i, n := 0, x.NumField(); i < n; i++ {
		fieldI := x.Field(i)
		switch fieldI.Kind() {
		case reflect.Struct:
			ret = append(ret, structChild(fieldI)...)
		case reflect.Ptr, reflect.String, reflect.Interface, reflect.Slice, reflect.Map:
			ret = append(ret, fieldI)
		}
	}
	return ret
}

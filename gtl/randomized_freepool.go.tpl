// +build !race

// ZZFreePool is thread-safe pool that uses power-of-two loadbalancing across
// CPUs.

// This library requires the following two additional files per package. For
// now, create them manually.
//
// 1. A go file with the following contents
//
// package PACKAGE
//
// // This import is needed to use go:linkname.
// import _ "unsafe"
// // The following functions are defined in go runtime.  To use them, we need to
// // import "unsafe", and elsewhere in this package, import "C" to force compiler
// // to recognize the "go:linktime" directive. Some of the details are explained
// // in the below blog post.
// //
// // procPin() pins the caller to the current processor, and returns the processor
// // id in range [0,GOMAXPROCS). procUnpin() undos the effect of procPin().
// //
// // http://www.alangpierce.com/blog/2016/03/17/adventures-in-go-accessing-unexported-functions/
//
// //go:linkname runtime_procPin sync.runtime_procPin
// //go:nosplit
// func runtime_procPin() int
//
// //go:linkname runtime_procUnpin sync.runtime_procUnpin
// //go:nosplit
// func runtime_procUnpin()
//
// //go:linkname fastrandn sync.fastrandn
// func fastrandn(n uint32) uint32
//
// 2. An empty .s file.

package PACKAGE

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ZZFreePool is a variation of sync.Pool, specialized for a concrete type.
//
// - Put() performs power-of-two loadbalancing, and Get() looks only at the
//   local queue.  This improves the performance of Get() on many-core machines,
//   at the cost of slightly more allocations.
//
// - It assumes that GOMAXPROCS is fixed at boot.
//
// - It never frees objects accumulated in the pool. We could add this feature
//   if needed.
type ZZFreePool struct {
	new          func() ELEM
	local        []zzPoolLocal
	maxLocalSize int64
}

const (
	zzMaxPrivateElems = 4
	zzCacheLineSize   = 64
)

type zzPoolLocalInner struct {
	private     [zzMaxPrivateElems]ELEM // Can be used only by the respective P.
	privateSize int

	shared     []ELEM     // Can be used by any P.
	sharedSize int64      // ==len(shared), but can be accessed w/o holding mu.
	mu         sync.Mutex // Protects shared.
}

type zzPoolLocal struct {
	zzPoolLocalInner
	// Pad prevents false sharing.
	pad [zzCacheLineSize - unsafe.Sizeof(zzPoolLocalInner{})%zzCacheLineSize]byte
}

// NewZZFreePool creates a new free object pool. new should create a new
// object. It is called when the pool is empty on Get(). maxSize bounds the
// approx max number of objects that can be stored in the pool. Beyond this
// limit, Put() call will drop the objects.
func NewZZFreePool(new func() ELEM, maxSize int) *ZZFreePool {
	maxProcs := runtime.GOMAXPROCS(0)
	maxLocalSize := -1
	if maxSize > 0 {
		maxLocalSize = maxSize / maxProcs
		if maxLocalSize <= 0 {
			maxLocalSize = 1
		}
	}
	p := &ZZFreePool{
		new:          new,
		local:        make([]zzPoolLocal, maxProcs),
		maxLocalSize: int64(maxLocalSize),
	}
	return p
}

func (p *ZZFreePool) pin() *zzPoolLocal {
	pid := runtime_procPin()
	if int(pid) >= len(p.local) {
		panic(pid)
	}
	return &p.local[pid]
}

// Put adds an object to the freepool. The caller shall not touch the object
// after the call.
func (p *ZZFreePool) Put(x ELEM) {
	done := false
	l := p.pin()
	if l.privateSize < zzMaxPrivateElems {
		l.private[l.privateSize] = x
		l.privateSize++
		done = true
	}
	runtime_procUnpin()
	if !done {
		// Pick another random queue, then add x to the shorter one.
		// This policy ("power of two") reduces load imbalance across
		// queues to log(log(#queues)) .
		//
		// https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf
		l2 := &p.local[int(fastrandn(uint32(len(p.local))))]
		lSize := atomic.LoadInt64(&l.sharedSize)
		l2Size := atomic.LoadInt64(&l2.sharedSize)
		if l2Size < lSize {
			l = l2
		}
		l.mu.Lock()
		if p.maxLocalSize >= 0 && l.sharedSize < p.maxLocalSize {
			l.shared = append(l.shared, x)
			atomic.StoreInt64(&l.sharedSize, l.sharedSize+1) // Release store.
		}
		l.mu.Unlock()
	}
}

// Get removes an object from the freepool. If pool is empty, it calls the
// callback passed to NewFreePool.
func (p *ZZFreePool) Get() ELEM {
	l := p.pin()
	var x ELEM
	done := false
	if l.privateSize > 0 {
		l.privateSize--
		x = l.private[l.privateSize]
		var empty ELEM
		l.private[l.privateSize] = empty
		done = true
	}
	runtime_procUnpin()
	if done {
		return x
	}
	l.mu.Lock()
	last := len(l.shared) - 1
	if last >= 0 {
		x = l.shared[last]
		l.shared = l.shared[:last]
		atomic.StoreInt64(&l.sharedSize, l.sharedSize-1)
		done = true
	}
	l.mu.Unlock()
	if !done {
		x = p.new()
	}
	return x
}

// ApproxLen returns an approximate length of the pool. For unittesting only.
//
// It returns an accurate value iff. no other thread is accessing the pool.
func (p *ZZFreePool) ApproxLen() int {
	n := 0
	for i := range p.local {
		n += p.local[i].privateSize
		n += int(p.local[i].sharedSize)
	}
	return n
}

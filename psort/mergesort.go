package psort

import (
	"reflect"
	"sort"
	"sync"

	"github.com/grailbio/base/traverse"
)

const (
	serialThreshold = 128
)

// Slice sorts the given slice according to the ordering induced by the provided
// less function. Parallel computation will be attempted, up to the limit imposed by
// parallelism. This function can be much faster than the standard library's sort.Slice()
// when sorting large slices on multicore machines.
func Slice(slice interface{}, less func(i, j int) bool, parallelism int) {
	if parallelism < 1 {
		panic("parallelism must be at least 1")
	}
	if reflect.TypeOf(slice).Kind() != reflect.Slice {
		panic("input interface was not of slice type")
	}
	rv := reflect.ValueOf(slice)
	if rv.Len() < 2 {
		return
	}
	// For clarity, we will sort a slice containing indices from the input slice. Then,
	// we will set the elements of the input slice according to this permutation. This
	// avoids difficult-to-understand reflection types and calls in most of the code.
	perm := make([]int, rv.Len())
	for i := range perm {
		perm[i] = i
	}
	scratch := make([]int, len(perm))
	mergeSort(perm, less, parallelism, scratch)
	result := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
	_ = traverse.Limit(parallelism).Range(rv.Len(), func(start, end int) error {
		for i := start; i < end; i++ {
			result.Index(i).Set(rv.Index(perm[i]))
		}
		return nil
	})
	_ = traverse.Limit(parallelism).Range(rv.Len(), func(start, end int) error {
		reflect.Copy(rv.Slice(start, end), result.Slice(start, end))
		return nil
	})
}

func mergeSort(perm []int, less func(i, j int) bool, parallelism int, scratch []int) {
	if parallelism == 1 || len(perm) < serialThreshold {
		sortSerial(perm, less)
		return
	}

	// Sort two halves of the slice in parallel, allocating half of our parallelism to
	// each subroutine.
	left := perm[:len(perm)/2]
	right := perm[len(perm)/2:]
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		mergeSort(left, less, (parallelism+1)/2, scratch[:len(perm)/2])
		waitGroup.Done()
	}()
	mergeSort(right, less, parallelism/2, scratch[len(perm)/2:])
	waitGroup.Wait()

	merge(left, right, less, parallelism, scratch)
	parallelCopy(perm, scratch, parallelism)
}

func parallelCopy(dst, src []int, parallelism int) {
	_ = traverse.Limit(parallelism).Range(len(dst), func(start, end int) error {
		copy(dst[start:end], src[start:end])
		return nil
	})
}

func sortSerial(perm []int, less func(i, j int) bool) {
	sort.Slice(perm, func(i, j int) bool {
		return less(perm[i], perm[j])
	})
}

func merge(perm1, perm2 []int, less func(i, j int) bool, parallelism int, out []int) {
	if parallelism == 1 || len(perm1)+len(perm2) < serialThreshold {
		mergeSerial(perm1, perm2, less, out)
		return
	}

	if len(perm1) < len(perm2) {
		perm1, perm2 = perm2, perm1
	}
	// Find the index in perm2 such that all elements to the left are smaller than
	// the midpoint element of perm1.
	r := len(perm1) / 2
	s := sort.Search(len(perm2), func(i int) bool {
		return !less(perm2[i], perm1[r])
	})
	// Merge in parallel, allocating half of our parallelism to each subroutine.
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		merge(perm1[:r], perm2[:s], less, (parallelism+1)/2, out[:r+s])
		waitGroup.Done()
	}()
	merge(perm1[r:], perm2[s:], less, parallelism/2, out[r+s:])
	waitGroup.Wait()
}

func mergeSerial(perm1, perm2 []int, less func(i, j int) bool, out []int) {
	var idx1, idx2, idxOut int
	for idx1 < len(perm1) && idx2 < len(perm2) {
		if less(perm1[idx1], perm2[idx2]) {
			out[idxOut] = perm1[idx1]
			idx1++
		} else {
			out[idxOut] = perm2[idx2]
			idx2++
		}
		idxOut++
	}
	for idx1 < len(perm1) {
		out[idxOut] = perm1[idx1]
		idx1++
		idxOut++
	}
	for idx2 < len(perm2) {
		out[idxOut] = perm2[idx2]
		idx2++
		idxOut++
	}
}

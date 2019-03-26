package psort

import (
	"reflect"
	"sort"
	"sync"
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
	mergeSort(perm, less, parallelism)
	result := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
	for i := range perm {
		result.Index(i).Set(rv.Index(perm[i]))
	}
	reflect.Copy(rv, result)
}

func mergeSort(perm []int, less func(i, j int) bool, parallelism int) {
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
		mergeSort(left, less, (parallelism+1)/2)
		waitGroup.Done()
	}()
	mergeSort(right, less, parallelism/2)
	waitGroup.Wait()

	sorted := merge(left, right, less, parallelism)
	copy(perm, sorted)
}

func sortSerial(perm []int, less func(i, j int) bool) {
	sort.Slice(perm, func(i, j int) bool {
		return less(perm[i], perm[j])
	})
}

func merge(perm1, perm2 []int, less func(i, j int) bool, parallelism int) []int {
	if parallelism == 1 || len(perm1)+len(perm2) < serialThreshold {
		return mergeSerial(perm1, perm2, less)
	}

	out := make([]int, len(perm1)+len(perm2))
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
		merged := merge(perm1[:r], perm2[:s], less, (parallelism+1)/2)
		copy(out[:r+s], merged)
		waitGroup.Done()
	}()
	merged := merge(perm1[r:], perm2[s:], less, parallelism/2)
	copy(out[r+s:], merged)
	waitGroup.Wait()
	return out
}

func mergeSerial(perm1, perm2 []int, less func(i, j int) bool) []int {
	out := make([]int, len(perm1)+len(perm2))
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
	return out
}

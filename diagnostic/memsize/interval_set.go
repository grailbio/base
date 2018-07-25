package memsize

import (
	"sort"
)

// interval represents a range of integers from start (inclusive) to start+length (not inclusive)
type interval struct {
	start  uintptr
	length int64
}

// intervalSet is a collection of intervals
// new intervals can be added and the total covered size computed.
// with some frequency, intervals will be compacted to save memory.
type intervalSet struct {
	data        []interval
	nextCompact int
}

func (r *intervalSet) add(interval interval) {
	if interval.length == 0 {
		return
	}
	r.data = append(r.data, interval)
	if len(r.data) >= r.nextCompact {
		r.compact()
	}
}

// compact sorts the intervals and merges adjacent intervals if they overlap.
func (r *intervalSet) compact() {
	defer r.setNextCompact()
	if len(r.data) < 2 {
		return
	}
	sort.Slice(r.data, func(i, j int) bool {
		return r.data[i].start < r.data[j].start
	})
	basePtr := 0
	aheadPtr := 1
	for aheadPtr < len(r.data) {
		if overlaps(r.data[basePtr], r.data[aheadPtr]) {
			r.data[basePtr].length = max(r.data[basePtr].length, int64(r.data[aheadPtr].start-r.data[basePtr].start)+r.data[aheadPtr].length)
			aheadPtr++
		} else {
			basePtr++
			r.data[basePtr] = r.data[aheadPtr]
			aheadPtr++
		}
	}
	r.data = r.data[0 : basePtr+1]

	// if the data will fit into a much smaller backing array, then copy to a smaller backing array to save memory.
	if len(r.data) < cap(r.data)/4 && len(r.data) > 100 {
		dataCopy := append([]interval{}, r.data...) // copy r.data to smaller array
		r.data = dataCopy
	}
}

// setNextCompact sets the size that r.data must reach before the next compacting
func (r *intervalSet) setNextCompact() {
	r.nextCompact = int(float64(len(r.data)) * 1.2) // increase current length by at least 20%
	if r.nextCompact < cap(r.data) {                // do not compact before reaching capacity of data
		r.nextCompact = cap(r.data)
	}
	if r.nextCompact < 10 { // do not compact before reaching 10 elements.
		r.nextCompact = 10
	}
}

// tests for overlaps between two intervals.
// precondition: x.start <= y.start
func overlaps(x, y interval) bool {
	return x.start+uintptr(x.length) >= y.start
}

// totalCovered returns the total number of integers covered by the intervalSet
func (r *intervalSet) totalCovered() int {
	if len(r.data) == 0 {
		return 0
	}
	sort.Slice(r.data, func(i, j int) bool {
		return r.data[i].start < r.data[j].start
	})
	total := 0
	curInterval := interval{start: r.data[0].start, length: 0} // zero width interval for initialization
	for _, val := range r.data {
		if overlaps(curInterval, val) { // extend the current interval
			curInterval.length = max(curInterval.length, int64(val.start-curInterval.start)+val.length)
		} else { // start a new interval
			total += int(curInterval.length)
			curInterval = val
		}
	}
	total += int(curInterval.length)
	return total
}

func max(i, j int64) int64 {
	if i > j {
		return i
	}
	return j
}

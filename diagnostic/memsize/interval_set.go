package memsize

import (
	"sort"
)

// interval represents a range of integers from start (inclusive) to start+length (not inclusive)
type interval struct {
	start  int64
	length int64
}

// intervalSet is a collection of intervals
// new intervals can be added and the total covered size computed.
// It is potentially memory inefficient.
type intervalSet struct {
	data []interval
}

func (r *intervalSet) add(interval interval) {
	r.data = append(r.data, interval)
}

// tests for overlap between two intervals.
// precondition: x.start <= y.start
func overlap(x, y interval) bool {
	return x.start+x.length >= y.start
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
	curInterval := interval{start: r.data[0].start - 1, length: 0} // zero width interval for initialization
	for _, val := range r.data {
		if overlap(curInterval, val) { // extend the current interval
			curInterval.length = val.start + val.length - curInterval.start
		} else { // start a new interval
			total += int(curInterval.length)
			curInterval = val
		}
	}
	total += int(curInterval.length)
	return total
}

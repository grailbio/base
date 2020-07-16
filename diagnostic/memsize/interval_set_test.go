package memsize

import (
	"testing"
)

func TestIntervalSet(t *testing.T) {
	tests := []struct {
		data            []interval
		expectedCovered []int
	}{
		{
			data:            []interval{{1, 10}},
			expectedCovered: []int{10},
		},
		{
			data:            []interval{{1, 10}, {11, 10}},
			expectedCovered: []int{10, 20},
		},
		{
			data:            []interval{{1, 10}, {5, 10}},
			expectedCovered: []int{10, 14},
		},
		{
			data:            []interval{{1, 10}, {5, 10}, {6, 9}, {6, 10}, {100, 1}, {100, 2}, {101, 1}},
			expectedCovered: []int{10, 14, 14, 15, 16, 17, 17},
		},
		{
			data:            []interval{{100, 1}, {99, 1}, {99, 2}, {0, 10}, {10, 10}},
			expectedCovered: []int{1, 2, 2, 12, 22},
		},
	}

	for testId, test := range tests {
		for numToAdd := range test.data {
			var set intervalSet
			for i := 0; i <= numToAdd; i++ {
				newInterval := test.data[i]
				set.add(newInterval)
			}
			got := set.totalCovered()
			if got != test.expectedCovered[numToAdd] {
				t.Errorf("test: %d, query: %d: got %v, expected %v", testId, numToAdd, got, test.expectedCovered[numToAdd])
			}
		}
	}
}

func TestEmptySet(t *testing.T) {
	var set intervalSet
	if got := set.totalCovered(); got != 0 {
		t.Errorf("empty set should have 0 coverage.  got %d", got)
	}
}

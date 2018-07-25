package memsize

import (
	"math/rand"
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

// TestRandomAddition creates a random slice of intervals and adds them in random order to the intervalSet.
// It periodically checks TotalCovered() against another implementation of interval set.
func TestRandomAddition(t *testing.T) {
	tests := []struct {
		setsize         int
		testrepeats     int
		size            int
		intervalrepeats int
		sampleProb      float64
	}{
		{
			setsize:         100,
			testrepeats:     100,
			size:            1,
			intervalrepeats: 2,
			sampleProb:      .2,
		},
		{
			setsize:         1000,
			testrepeats:     100,
			size:            3,
			intervalrepeats: 2,
			sampleProb:      .2,
		},
		{
			setsize:         1000,
			testrepeats:     100,
			size:            3,
			intervalrepeats: 2,
			sampleProb:      0,
		},
	}

	for _, test := range tests {
		for testRepeat := 0; testRepeat < test.testrepeats; testRepeat++ {
			r := rand.New(rand.NewSource(int64(testRepeat)))
			var intervals []interval
			for i := 0; i < test.setsize; i++ {
				for j := 0; j < test.intervalrepeats; j++ {
					intervals = append(intervals, interval{start: uintptr(i), length: int64(test.size)})
				}
			}
			shuffle(intervals, int64(testRepeat))
			set := intervalSet{}
			coveredMap := make(map[uintptr]struct{})
			for _, x := range intervals {
				set.add(x)
				for j := uintptr(0); j < uintptr(x.length); j++ {
					coveredMap[x.start+j] = struct{}{}
				}
				if r.Float64() < test.sampleProb {
					gotCovered := set.totalCovered()
					if gotCovered != len(coveredMap) {
						t.Errorf("set.Covered()=%d, len(coveredMap) = %d", gotCovered, len(coveredMap))
					}
				}
			}

			if gotSize := set.totalCovered(); gotSize != test.size+test.setsize-1 {
				t.Errorf("total covering - got: %d, expected %d", gotSize, test.setsize+test.setsize-1)
			}
		}
	}
}

// randomly shuffle a set of intervals
func shuffle(set []interval, seed int64) {
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(set), func(i, j int) {
		set[i], set[j] = set[j], set[i]
	})
}

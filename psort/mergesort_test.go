package psort

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

type TestInput int

const (
	Random TestInput = iota
	Ascending
	Descending
)

func TestSlice(t *testing.T) {
	tests := []struct {
		input       TestInput
		size        int
		parallelism int
		reps        int
	}{
		{
			input:       Random,
			size:        10000,
			parallelism: 7,
			reps:        100,
		},
		{
			input:       Random,
			size:        1000000,
			parallelism: 6,
			reps:        4,
		},
		{
			input:       Ascending,
			size:        10000,
			parallelism: 9,
			reps:        1,
		},
		{
			input:       Descending,
			size:        10000,
			parallelism: 8,
			reps:        1,
		},
	}

	for _, test := range tests {
		random := rand.New(rand.NewSource(0))
		for rep := 0; rep < test.reps; rep++ {
			in := make([]int, test.size)
			switch test.input {
			case Random:
				for i := range in {
					in[i] = random.Intn(test.size)
				}
			case Ascending:
				for i := range in {
					in[i] = i
				}
			case Descending:
				for i := range in {
					in[i] = len(in) - i
				}
			}
			expected := make([]int, len(in))
			copy(expected, in)
			sort.Slice(expected, func(i, j int) bool {
				return expected[i] < expected[j]
			})
			Slice(in, func(i, j int) bool {
				return in[i] < in[j]
			}, test.parallelism)
			if !reflect.DeepEqual(expected, in) {
				t.Errorf("Wrong sort result: want %v\n, got %v\n", expected, in)
			}
		}
	}
}

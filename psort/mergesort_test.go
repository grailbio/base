package psort

import (
	"fmt"
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

func BenchmarkSlice(b *testing.B) {
	tests := []struct {
		size        int
		parallelism int //parallelism = 0 means use sort.Slice() sort
	}{
		{
			size:        100000000,
			parallelism: 4096,
		},
		{
			size:        100000000,
			parallelism: 2048,
		},
		{
			size:        100000000,
			parallelism: 1024,
		},
		{
			size:        100000000,
			parallelism: 512,
		},
		{
			size:        100000000,
			parallelism: 256,
		},
		{
			size:        100000000,
			parallelism: 128,
		},
		{
			size:        100000000,
			parallelism: 64,
		},
		{
			size:        100000000,
			parallelism: 32,
		},
		{
			size:        100000000,
			parallelism: 16,
		},
		{
			size:        100000000,
			parallelism: 8,
		},
		{
			size:        100000000,
			parallelism: 4,
		},
		{
			size:        100000000,
			parallelism: 2,
		},
		{
			size:        100000000,
			parallelism: 1,
		},
		{
			size:        100000000,
			parallelism: 0,
		},
	}

	for _, test := range tests {
		b.Run(fmt.Sprintf("size:%d-%d", test.size, test.parallelism), func(b *testing.B) {
			data := make([]float64, test.size)
			r := rand.New(rand.NewSource(0))
			dataCopy := make([]float64, len(data))
			for i := range data {
				data[i] = r.Float64()
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				copy(dataCopy, data)
				b.StartTimer()
				if test.parallelism == 0 {
					sort.Slice(dataCopy, func(i, j int) bool {
						return dataCopy[i] < dataCopy[j]
					})
				} else {
					Slice(dataCopy, func(i, j int) bool {
						return dataCopy[i] < dataCopy[j]
					}, test.parallelism)
				}
			}
		})
	}
}

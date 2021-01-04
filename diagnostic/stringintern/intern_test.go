package stringintern

import (
	"testing"

	"github.com/grailbio/base/diagnostic/memsize"
)

type AB struct {
	A string
	B string
}

// return a string with the same content which does not share the same underlying memory.
func unintern(x string) string {
	ret := ""
	for _, t := range x {
		ret = ret + string(t)
	}
	return ret
}

type Test struct {
	x                     []interface{}
	sizeBefore, sizeAfter int
}

func TestBasic(t *testing.T) {
	var int64Var int = 3
	tests := []Test{
		{
			x:          []interface{}{&AB{"abc", unintern("abc")}},
			sizeBefore: 86,
			sizeAfter:  83,
		},
		{
			x:          []interface{}{&map[int]string{1: "abc", 2: unintern("abc")}},
			sizeBefore: 110,
			sizeAfter:  107,
		},
		{
			x:          []interface{}{&int64Var},
			sizeBefore: 56,
			sizeAfter:  56,
		},
	}

	for _, test := range tests {
		runTest(test, t)
	}
}

func TestCircular(t *testing.T) {
	type Circular struct {
		A, B string
		ptr  *Circular
	}

	type Nested struct {
		X Circular
	}

	circ := Circular{
		A: "abc",
		B: unintern("abc"),
	}
	circ.ptr = &circ

	nested := Nested{X: Circular{
		A: "abc",
		B: unintern("abc"),
	}}
	nested.X.ptr = &nested.X

	tests := []Test{
		{
			x:          []interface{}{&circ},
			sizeBefore: 94,
			sizeAfter:  91,
		},
		{
			x:          []interface{}{&nested},
			sizeBefore: 94,
			sizeAfter:  91,
		},
	}

	for _, test := range tests {
		runTest(test, t)
	}
}

func runTest(test Test, t *testing.T) {
	sizeBefore := memsize.DeepSize(&test.x)
	Intern(test.x...)
	sizeAfter := memsize.DeepSize(&test.x)
	if sizeBefore != test.sizeBefore {
		t.Errorf("sizeBefore:  expected=%d, got=%d", test.sizeBefore, sizeBefore)
	}
	if sizeAfter != test.sizeAfter {
		t.Errorf("sizeAfter:  expected=%d, got=%d", test.sizeAfter, sizeAfter)
	}
}

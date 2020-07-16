package memsize

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

var (
	x             int64  = 5
	pointerSize          = int(unsafe.Sizeof(&x))
	sliceSize            = int(unsafe.Sizeof(make([]int64, 0)))
	stringSize           = int(unsafe.Sizeof("abcde"))
	mapSize              = int(unsafe.Sizeof(make(map[int32]int32)))
	temper        Temper = nil
	interfaceSize        = int(unsafe.Sizeof(temper))
)

type TestCase struct {
	x        interface{}
	expected int
}

func TestPrimitives(t *testing.T) {
	var int8val int8 = 3
	var uint8val uint8 = 3
	var int16val int16 = -2
	var uint16val uint16 = 2
	var int32val int32 = -54
	var uint32val uint32 = 54
	var int64val int64 = -34
	var uint64val uint64 = 34
	var boolval bool = true
	var float64val float64 = 3.29
	var float32val float32 = 543.23
	var int8ptr *int8
	tests := []TestCase{
		{nil, 0},
		{&int64val, 8},
		{&uint64val, 8},
		{&int32val, 4},
		{&uint32val, 4},
		{&int16val, 2},
		{&uint16val, 2},
		{&int8val, 1},
		{&uint8val, 1},
		{&float64val, 8},
		{&float32val, 4},
		{&boolval, 1},
		{int8ptr, 0},            // nil pointer
		{&int8ptr, pointerSize}, // pointer pointer
	}
	runTests(tests, t)
}

func TestSlicesAndArray(t *testing.T) {
	var int64val int64
	var int64val2 int64
	var int64ptr *int64
	var int8slice []int8 = []int8{0, 1, 2, 3, 4, 5, 6}
	var int8sliceB = int8slice[0:4]

	type smallStruct struct { // size = 16 bytes
		A, B int64
	}

	type smallPointerStruct struct { // size = 24 bytes + maybe 8 for ptr
		A, B int64
		APtr *int64
	}

	type complexStruct struct { // size = 40 bytes + maybe 8 for ptr
		T smallStruct
		Y smallPointerStruct
	}

	type structWithZeroArray struct { // size = 40 bytes + maybe 8 for ptr
		M [0]int64
		W complexStruct
	}

	tests := []TestCase{
		{&[]int64{1, 2, 3}, sliceSize + 8*3},
		{&[]*int64{int64ptr, &int64val}, sliceSize + pointerSize*2 + 8},
		{&[]*int64{&int64val, &int64val}, sliceSize + pointerSize*2 + 8},
		{&[]*int64{&int64val2, &int64val}, sliceSize + pointerSize*2 + 2*8},
		{&[]smallStruct{{}, {}}, sliceSize + 16*2},
		{&[]complexStruct{{}, {Y: smallPointerStruct{APtr: &int64val}}}, sliceSize + 40*2 + 8},
		{&[][3]int64{{1, 2, 3}, {4, 5, 6}}, sliceSize + 2*24},
		{&[...]int64{1, 2, 3}, 8 * 3},
		{&[0]int64{}, 0},
		{&[]structWithZeroArray{{}, {}}, sliceSize + 2*40},
		{&[...]smallPointerStruct{{A: 1, B: 1, APtr: &int64val}, {A: 1, B: 1, APtr: nil}}, 2*24 + 8},
		{&int8sliceB, sliceSize + 4},
		{&[][]int8{int8slice[0:4], int8slice[0:4]}, 3*sliceSize + 4}, // overlapping memory locations
		{&[][]int8{int8slice[0:4], int8slice[2:6]}, 3*sliceSize + 6}, // overlapping memory locations
	}
	runTests(tests, t)

}

func TestStrings(t *testing.T) {
	var emptyString = ""
	var abcdefgString = "abcdefg"
	tests := []TestCase{
		{&emptyString, stringSize},
		{&abcdefgString, stringSize + 7},
		{&[]string{"abcd", "defg"}, sliceSize + 2*stringSize + 2*4}, // no string interning
		{&[]string{"abcd", "abcd"}, sliceSize + 2*stringSize + 4},   // string interning
	}
	runTests(tests, t)
}

func TestMap(t *testing.T) {
	var int8val int8
	tests := []TestCase{
		{&map[int64]int64{2: 3}, mapSize + 8 + 8},
		{&map[string]int32{"abc": 3}, mapSize + stringSize + 3 + 4},
		{&map[string]*int8{"abc": &int8val, "def": &int8val}, mapSize + 2*stringSize + 2*3 + 2*pointerSize + 1},
	}
	runTests(tests, t)
}

type Temper interface {
	Temp()
}

type TemperMock struct {
	A int64
}

func (TemperMock) Temp() {}

func TestStructs(t *testing.T) {
	type struct1 struct {
		A int64
		B float64
	}

	type struct2 struct {
		A      int64
		B      float64
		temper Temper
	}

	type recursiveType struct {
		A   int64
		ptr *recursiveType
	}

	type nestedType1 struct { // 8 bytes + maybe 8 bytes
		A *int64
	}
	type nestedType2 struct { // 8 bytes + maybe 8 bytes
		X nestedType1
	}
	type nestedType3 struct { // 8 bytes + maybe 8 bytes
		Y nestedType2
	}

	type structWithZeroArray struct { // 8 bytes + maybe 8 bytes
		X [0]int64
		Y nestedType2
	}

	var int64val int64
	var recursiveVar1 = recursiveType{A: 1}
	var recursiveVar2 = recursiveType{A: 2}
	recursiveVar1.ptr = &recursiveVar2
	recursiveVar2.ptr = &recursiveVar1

	tests := []TestCase{
		{&nestedType3{Y: nestedType2{X: nestedType1{A: &int64val}}}, pointerSize + 8},
		{&struct1{1, 1}, 16},
		{&struct2{1, 1, TemperMock{}}, 16 + interfaceSize + 8},
		{&struct2{1, 1, nil}, 16 + interfaceSize},
		{&recursiveVar1, 2 * (8 + pointerSize)},
		{&structWithZeroArray{Y: nestedType2{}, X: [0]int64{}}, 8},
	}

	runTests(tests, t)
}

func TestCornerCaseTypes(t *testing.T) {
	var chanVar chan int
	tests := []TestCase{
		{&struct{ A func(x int) int }{A: func(x int) int { return x + 1 }}, pointerSize},
		{&chanVar, pointerSize},
	}
	runTests(tests, t)
}

func TestPanicOnNonNil(t *testing.T) {
	tests := []interface{}{
		"abc",
		5,
		3.5,
		struct {
			A int
			B int
		}{A: 5, B: 5},
	}
	for i := range tests {
		assert.Panics(t, func() { DeepSize(tests[i]) }, "should panic")
	}
}

func runTests(tests []TestCase, t *testing.T) {
	for i, test := range tests {
		if got := DeepSize(test.x); got != test.expected {
			t.Errorf("test %d: got %d, expected %d", i, got, test.expected)
		}
	}
}

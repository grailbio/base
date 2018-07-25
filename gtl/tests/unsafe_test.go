package tests

//go:generate ../generate.py --prefix= -DELEM=int32 --package=tests --output=unsafe.go ../unsafe.go.tpl

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsafe(t *testing.T) {
	b := int32sToBytes([]int32{10, 20, 30})
	assert.Equal(t, len(b), 4*3)
	runtime.GC()
	assert.Equal(t, []int32{10, 20, 30}, BytesToint32s(b))
}

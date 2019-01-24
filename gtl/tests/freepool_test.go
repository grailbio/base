package tests

//go:generate ../generate.py --prefix=byte --PREFIX=byte -DMAXSIZE=128 -DELEM=[]byte --package=tests --output=freepool.go ../freepool.go.tpl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFreepool(t *testing.T) {
	pool := bytePool{New: func() []byte { return []byte{10, 11} }}

	assert.Equal(t, []byte{10, 11}, pool.Get())
	pool.Put([]byte{20, 21})
	assert.Equal(t, []byte{20, 21}, pool.Get())
}

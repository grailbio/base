package tests

import (
	"hash/fnv"
)

func testhash(val string) uint64 {
	h := fnv.New64()
	h.Write([]byte(val))
	return h.Sum64()
}

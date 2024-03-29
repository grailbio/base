// Code generated by "../generate.py --output=int_freepool_race.go --prefix=ints -DELEM=[]int --package=tests ../randomized_freepool_race.go.tpl". DO NOT EDIT.

//go:build race
// +build race

package tests

import "sync/atomic"

type IntsFreePool struct {
	new func() []int
	len int64
}

func NewIntsFreePool(new func() []int, maxSize int) *IntsFreePool {
	return &IntsFreePool{new: new}
}

func (p *IntsFreePool) Put(x []int) {
	atomic.AddInt64(&p.len, -1)
}

func (p *IntsFreePool) Get() []int {
	atomic.AddInt64(&p.len, 1)
	return p.new()
}

func (p *IntsFreePool) ApproxLen() int { return int(atomic.LoadInt64(&p.len)) }

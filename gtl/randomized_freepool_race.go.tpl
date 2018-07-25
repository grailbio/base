// +build race

package PACKAGE

import "sync/atomic"

type ZZFreePool struct {
	new func() ELEM
	len int64
}

func NewZZFreePool(new func() ELEM, maxSize int) *ZZFreePool {
	return &ZZFreePool{new: new}
}

func (p *ZZFreePool) Put(x ELEM) {
	atomic.AddInt64(&p.len, -1)
}

func (p *ZZFreePool) Get() ELEM {
	atomic.AddInt64(&p.len, 1)
	return p.new()
}

func (p *ZZFreePool) ApproxLen() int { return int(atomic.LoadInt64(&p.len)) }

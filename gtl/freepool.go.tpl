package PACKAGE

// A freepool for a single thread. The interface is the same as sync.Pool, but
// it avoids locks and interface conversion overhead.
//
// Example:
//  generate.py -package=foo -prefix=int -DELEM=foo -DMAXSIZE=128
//
//
// Parameters:
//  ELEM: the object to be kept in the freepool
//  MAXSIZE: the maxium number of objects to keep in the freepool

type ZZPool struct {
	New func() ELEM
	p   []ELEM
}

func (p *ZZPool) Get() ELEM {
	if len(p.p) == 0 {
		return p.New()
	}
	tmp := p.p[len(p.p)-1]
	p.p = p.p[:len(p.p)-1]
	return tmp
}

func (p *ZZPool) Put(tmp ELEM) {
	if len(p.p) < MAXSIZE {
		p.p = append(p.p, tmp)
	}
}

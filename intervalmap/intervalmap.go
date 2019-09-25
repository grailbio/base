// Package intervalmap stores a set of (potentially overlapping) intervals.  It
// supports searching for intervals that overlap user-provided interval.
//
// The implementation uses an 1-D version of Kd tree with randomized
// surface-area heuristic
// (http://www.sci.utah.edu/~wald/Publications/2007/ParallelBVHBuild/fastbuild.pdf).
package intervalmap

//go:generate ../gtl/generate_randomized_freepool.py --output=search_freepool --prefix=searcher -DELEM=*searcher --package=intervalmap

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"unsafe"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
)

// Key is the type for interval boundaries.
type Key = int64

// Interval defines an half-open interval, [Start, Limit).
type Interval struct {
	// Start is included
	Start Key
	// Limit is excluded.
	Limit Key
}

var emptyInterval = Interval{math.MaxInt64, math.MinInt64}

func min(x, y Key) Key {
	if x < y {
		return x
	}
	return y
}

func max(x, y Key) Key {
	if x < y {
		return y
	}
	return x
}

// Intersects checks if (i∩j) != ∅
func (i Interval) Intersects(j Interval) bool {
	return i.Limit > j.Start && j.Limit > i.Start
}

// Intersect computes i ∩ j.
func (i Interval) Intersect(j Interval) Interval {
	minKey := max(i.Start, j.Start)
	maxKey := min(i.Limit, j.Limit)
	return Interval{minKey, maxKey}
}

// Empty checks if the interval is empty.
func (i Interval) Empty() bool { return i.Start >= i.Limit }

// Span computes a minimal interval that spans over both i and j.  If either i
// or j is an empty set, this function returns the other set.
func (i Interval) Span(j Interval) Interval {
	switch {
	case i.Empty():
		return j
	case j.Empty():
		return i
	default:
		return Interval{min(i.Start, j.Start), max(i.Limit, j.Limit)}
	}
}

const (
	maxEntsInNode = 16 // max size of node.ents.
)

// Entry represents one interval.
type Entry struct {
	// Interval defines a half-open interval, [Start,Limit)
	Interval Interval
	// Data is an arbitrary user-defined payload
	Data interface{}
}

type entry struct {
	Entry
	id int // dense sequence number 0, 1, 2, ...
}

// node represents one node in Kdtree.
type node struct {
	bounds      Interval // interval covered by this node.
	left, right *node    // children. Maybe nil.
	ents        []*entry // Nonempty iff. left=nil&&right=nil.
	label       string   // for debugging only.
}

// TreeStats shows tree-wide stats.
type TreeStats struct {
	// Nodes is the total number of tree nodes.
	Nodes int
	// Nodes is the total number of leaf nodes.
	//
	// Invariant: LeafNodes < Nodes
	LeafNodes int
	// MaxDepth is the max depth of the tree.
	MaxDepth int
	// MaxLeafNodeSize is the maximum len(node.ents) of all nodes in the tree.
	MaxLeafNodeSize int
	// TotalLeafDepth is the sum of depth of all leaf nodes.
	TotalLeafDepth int
	// TotalLeafDepth is the sum of len(node.ents) of all leaf nodes.
	TotalLeafNodeSize int
}

// T represents the intervalmap. It must be created using New().
type T struct {
	root  node
	stats TreeStats
	pool  *searcherFreePool
}

// New creates a new tree with the given set of entries.  The intervals may
// overlap, and they need not be sorted.
func New(ents []Entry) *T {
	entsCopy := make([]entry, len(ents))
	for i := range ents {
		entsCopy[i] = entry{Entry: ents[i], id: i}
	}
	ients := make([]*entry, len(ents))
	for i := range entsCopy {
		ients[i] = &entsCopy[i]
	}
	r := rand.New(rand.NewSource(0))
	t := &T{}
	t.stats.MaxDepth = -1
	t.stats.MaxLeafNodeSize = -1
	t.root.init("", ients, keyRange(ients), r, &t.stats)
	t.pool = newSearcherFreePool(t, len(ents))
	return t
}

func newSearcherFreePool(t *T, nEnt int) *searcherFreePool {
	return NewsearcherFreePool(func() *searcher {
		return &searcher{
			tree: t,
			hits: make([]uint32, nEnt),
		}
	}, runtime.NumCPU()*2)
}

// searcher keeps state needed during one search episode.  It is owned by one
// goroutine.
type searcher struct {
	tree     *T
	searchID uint32   // increments on every search
	hits     []uint32 // hits[i] == searchID if the i'th entry has already been visited
}

func (s *searcher) visit(i int) bool {
	if s.hits[i] != s.searchID {
		s.hits[i] = s.searchID
		return true
	}
	return false
}

// Stats returns tree-wide stats.
func (t *T) Stats() TreeStats { return t.stats }

// Get finds all the entries that intersect the given interval and return them
// in *ents.
func (t *T) Get(interval Interval, ents *[]*Entry) {
	s := t.pool.Get()
	s.searchID++
	*ents = (*ents)[:0]
	t.root.get(interval, ents, s)
	if s.searchID < math.MaxUint32 {
		t.pool.Put(s)
	}
}

// Any checks if any of the entries intersect the given interval.
func (t *T) Any(interval Interval) bool {
	s := t.pool.Get()
	s.searchID++
	found := t.root.any(interval, s)
	if s.searchID < math.MaxUint32 {
		t.pool.Put(s)
	}
	return found
}

func keyRange(ents []*entry) Interval {
	i := emptyInterval
	for _, e := range ents {
		i = i.Span(e.Interval)
	}
	return i
}

const maxSample = 8

// randomSample picks maxSample random elements from ents[]. It shuffles ents[]
// in place.
func randomSample(ents []*entry, r *rand.Rand) []*entry {
	if len(ents) <= maxSample {
		return ents
	}
	shuffleFirstN := func(n int) { // Fisher-Yates shuffle
		for i := 0; i < n-1; i++ {
			j := i + r.Intn(len(ents)-i)
			ents[i], ents[j] = ents[j], ents[i]
		}
	}
	n := maxSample
	if len(ents)-n < n {
		// When maxSample < len(n) < maxSample*2, it's faster to compute the
		// complement set.
		n = len(ents) - n
		shuffleFirstN(len(ents) - n)
		return ents[n:]
	}
	shuffleFirstN(n)
	return ents[:n]
}

// This function splits interval "bounds" into two balanced subintervals,
// [bounds.Start, mid) and [mid, bounds.Limit). left (right) will store a subset
// of ents[] that fits in the first (second, resp) subinterval. Note that an
// entry in ents[] may belong to both left and right, if the entry spans over
// the midpoint.
//
// Ok=false if this function fails to find a good split point.
func split(label string, ents []*entry, bounds Interval, r *rand.Rand) (mid Key, left []*entry, right []*entry, ok bool) {
	// A good interval split point is guaranteed to be at one of the interval
	// endpoints.  To bound the compute time, we sample up to 16 intervals in
	// ents[], and examine their endpoints one by one.
	sample := randomSample(ents, r)
	sampleRange := keyRange(sample).Intersect(bounds)
	log.Debug.Printf("%s: Split %+v, %d ents", label, sampleRange, len(ents))
	if sampleRange.Empty() {
		panic(sample)
	}
	var (
		candidates [maxSample * 2]Key
		nCandidate int
	)
	for i, e := range sample {
		candidates[i*2] = e.Interval.Start
		candidates[i*2+1] = e.Interval.Limit
		nCandidate += 2
	}

	// splitAt splits ents[] into two subsets, assuming bounds is split at mid.
	splitAt := func(ents []*entry, mid Key, left, right *[]*entry) {
		*left = (*left)[:0]
		*right = (*right)[:0]
		for _, e := range ents {
			if e.Interval.Intersects(Interval{bounds.Start, mid}) {
				*left = append(*left, e)
			}
			if e.Interval.Intersects(Interval{mid, bounds.Limit}) {
				*right = append(*right, e)
			}
		}
	}

	// Compute the cost of splitting at each of candidates[].
	// We use the surface-area heuristics. The best explanation is in
	// the following paper:
	//
	// Ingo Wald, Realtime ray tracing and interactive global illumination,
	// http://www.sci.utah.edu/~wald/Publications/2004/PhD/phd.pdf
	//
	// The basic idea is the following:
	//
	// - Assume we split the parent interval [s, e) into two intervals
	//   [s,m) and [m,e)
	//
	// - The cost C(x) of searching a subinterval x is roughly
	//    C(x) = (length of x) * (# of entries that intersect x).
	//
	//    The first term is the probability that a query hits the subinterval, and
	//    the 2nd term is the cost of searching inside the subinterval.
	//
	//    This assumes that a query is distributed uniformly over the domain (in
	//    our case, [-maxint32, maxint32].
	//
	// - The best split point is m that minimizes C([s,m)) + C([m,e))
	minCost := math.MaxFloat64
	var minMid Key
	var minLeft, minRight []*entry
	var tmpLeft, tmpRight []*entry

	for _, mid := range candidates[:nCandidate] {
		splitAt(ents, mid, &tmpLeft, &tmpRight)
		if len(tmpLeft) == 0 || len(tmpRight) == 0 {
			continue
		}
		cost := float64(len(tmpLeft))*float64(mid-sampleRange.Start) +
			float64(len(tmpRight))*float64(sampleRange.Limit-mid)
		if cost < minCost {
			minMid = mid
			minLeft, tmpLeft = tmpLeft, minLeft
			minRight, tmpRight = tmpRight, minRight
			minCost = cost
		}
	}
	if minCost == math.MaxFloat64 || len(minLeft) == len(ents) || len(minRight) == len(ents) {
		return
	}
	mid = minMid
	left = minLeft
	right = minRight
	ok = true
	return
}

func (n *node) init(label string, ents []*entry, bounds Interval, r *rand.Rand, stats *TreeStats) {
	defer func() {
		// Update the stats.
		stats.Nodes++
		depth := len(n.label)
		if depth > stats.MaxDepth {
			stats.MaxDepth = depth
		}
		if e := len(n.ents); e > 0 { // Leaf node
			stats.LeafNodes++
			stats.TotalLeafNodeSize += e
			stats.TotalLeafDepth += depth
			if e > stats.MaxLeafNodeSize {
				stats.MaxLeafNodeSize = e
			}
		}
	}()

	n.label = label
	n.bounds = bounds
	if len(ents) <= maxEntsInNode {
		n.ents = ents
		return
	}
	mid, left, right, ok := split(n.label, ents, bounds, r)
	if !ok {
		n.ents = ents
		return
	}
	n.left = &node{}

	leftInterval := Interval{n.bounds.Start, mid}
	leftKR := keyRange(left)
	log.Debug.Printf("%v (bounds %v): left %v %v %v", n.label, n.bounds, leftKR, leftInterval, leftKR.Intersect(leftInterval))
	n.left.init(label+"L", left, leftKR.Intersect(leftInterval), r, stats)
	n.right = &node{}
	n.right.init(label+"R", right, keyRange(right).Intersect(Interval{mid, n.bounds.Limit}), r, stats)
}

func addEntry(ents *[]*Entry, e *entry, s *searcher) {
	if s.visit(e.id) {
		*ents = append(*ents, (*Entry)(unsafe.Pointer(e)))
	}
}

func (n *node) get(interval Interval, ents *[]*Entry, s *searcher) {
	interval = interval.Intersect(n.bounds)
	if interval.Empty() {
		return
	}
	if len(n.ents) > 0 { // Leaf node
		for _, e := range n.ents {
			if interval.Intersects(e.Interval) {
				addEntry(ents, e, s)
			}
		}
		return
	}
	n.left.get(interval, ents, s)
	n.right.get(interval, ents, s)
}

func (n *node) any(interval Interval, s *searcher) bool {
	interval = interval.Intersect(n.bounds)
	if interval.Empty() {
		return false
	}
	if len(n.ents) > 0 { // Leaf node
		for _, e := range n.ents {
			if interval.Intersects(e.Interval) {
				return true
			}
		}
		return false
	}
	found := n.left.any(interval, s)
	if !found {
		found = n.right.any(interval, s)
	}
	return found
}

// GOB support

const gobFormatVersion = 1

// MarshalBinary implements encoding.BinaryMarshaler interface.  It allows T to
// be encoded and decoded using Gob.
func (t *T) MarshalBinary() (data []byte, err error) {
	buf := bytes.Buffer{}
	e := gob.NewEncoder(&buf)
	must.Nil(e.Encode(gobFormatVersion))
	marshalNode(e, &t.root)
	must.Nil(e.Encode(t.stats))
	return buf.Bytes(), nil
}

func marshalNode(e *gob.Encoder, n *node) {
	if n == nil {
		must.Nil(e.Encode(false))
		return
	}
	must.Nil(e.Encode(true))
	must.Nil(e.Encode(n.bounds))
	marshalNode(e, n.left)
	marshalNode(e, n.right)
	must.Nil(e.Encode(len(n.ents)))
	for _, ent := range n.ents {
		must.Nil(e.Encode(ent.Entry))
		must.Nil(e.Encode(ent.id))
	}
	must.Nil(e.Encode(n.label))
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
// It allows T to be encoded and decoded using Gob.
func (t *T) UnmarshalBinary(data []byte) error {
	buf := bytes.NewReader(data)
	d := gob.NewDecoder(buf)
	var version int
	if err := d.Decode(&version); err != nil {
		return err
	}
	if version != gobFormatVersion {
		return fmt.Errorf("gob decode: got version %d, want %d", version, gobFormatVersion)
	}
	var (
		maxid = -1
		err   error
		root  *node
	)
	if root, err = unmarshalNode(d, &maxid); err != nil {
		return err
	}
	t.root = *root
	if err := d.Decode(&t.stats); err != nil {
		return err
	}
	t.pool = newSearcherFreePool(t, maxid+1)
	return nil
}

func unmarshalNode(d *gob.Decoder, maxid *int) (*node, error) {
	var (
		exist bool
		err   error
	)
	if err = d.Decode(&exist); err != nil {
		return nil, err
	}
	if !exist {
		return nil, nil
	}
	n := &node{}
	if err := d.Decode(&n.bounds); err != nil {
		return nil, err
	}
	if n.left, err = unmarshalNode(d, maxid); err != nil {
		return nil, err
	}
	if n.right, err = unmarshalNode(d, maxid); err != nil {
		return nil, err
	}
	var nEnt int
	if err := d.Decode(&nEnt); err != nil {
		return nil, err
	}
	n.ents = make([]*entry, nEnt)
	for i := 0; i < nEnt; i++ {
		n.ents[i] = &entry{}
		if err := d.Decode(&n.ents[i].Entry); err != nil {
			return nil, err
		}
		if err := d.Decode(&n.ents[i].id); err != nil {
			return nil, err
		}
		if n.ents[i].id > *maxid {
			*maxid = n.ents[i].id
		}
	}
	if err := d.Decode(&n.label); err != nil {
		return nil, err
	}
	return n, nil
}

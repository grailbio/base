package intervalmap

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/biogo/store/interval"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
)

func TestInterval(t *testing.T) {
	expect.False(t, Interval{1, 1}.Intersects(Interval{1, 2}))
	expect.True(t, Interval{1, 2}.Intersects(Interval{1, 2}))
	expect.True(t, Interval{1, 2}.Intersects(Interval{1, 3}))
	expect.True(t, Interval{1, 2}.Intersects(Interval{-1, 2}))
	expect.True(t, Interval{1, 2}.Intersects(Interval{-1, 3}))
	expect.False(t, Interval{1, 2}.Intersects(Interval{2, 3}))
	expect.False(t, Interval{1, 2}.Intersects(Interval{3, 4}))
	expect.EQ(t, Interval{1, 2}.Span(Interval{3, 4}), Interval{1, 4})
	expect.EQ(t, Interval{1, 4}.Span(Interval{2, 3}), Interval{1, 4})

	expect.EQ(t, Interval{1, 4}.Span(Interval{3, 2}), Interval{1, 4})
	expect.EQ(t, Interval{10, 14}.Span(Interval{3, 2}), Interval{10, 14})
	expect.EQ(t, Interval{4, 1}.Span(Interval{2, 3}), Interval{2, 3})
	expect.EQ(t, Interval{4, 1}.Span(Interval{12, 13}), Interval{12, 13})
}

// Sort the given intervals in place.
func sortIntervals(matches []Interval) []Interval {
	sort.Slice(matches, func(i, j int) bool {
		e0 := matches[i]
		e1 := matches[j]
		if e0.Start != e1.Start {
			return e0.Start < e1.Start
		}
		if e0.Limit != e1.Limit {
			return e0.Limit < e1.Limit
		}
		return false
	})
	return matches
}

// slowModel is a slow, simple intervalmap.
type slowModel []Interval

func (m *slowModel) insert(start, limit Key) {
	*m = append(*m, Interval{start, limit})
}

func (m slowModel) get(start, limit Key) []Interval {
	matches := []Interval{}
	for _, i := range m {
		if i.Intersects(Interval{start, limit}) {
			matches = append(matches, i)
		}
	}
	return matches
}

// biogoModel is a intervalmap using biogo inttree.
type biogoModel interval.IntTree

func (m *biogoModel) insert(start, limit Key) {
	ii := testInterval{
		start: start,
		limit: limit,
		id:    uintptr(((*interval.IntTree)(m)).Len() + 100),
	}
	if err := ((*interval.IntTree)(m)).Insert(ii, false); err != nil {
		panic(err)
	}
}

func (m *biogoModel) get(start, limit Key) []Interval {
	matches := []Interval{}
	for _, match := range ((*interval.IntTree)(m)).Get(testInterval{start: start, limit: limit}) {
		matches = append(matches, Interval{Start: int64(match.Range().Start), Limit: int64(match.Range().End)})
	}
	return matches
}

func testGet(tree *T, start, limit Key) []Interval {
	p := []*Entry{}
	tree.Get(Interval{start, limit}, &p)
	matches := make([]Interval, len(p))
	for i, e := range p {
		payload := e.Data.(string)
		if payload != fmt.Sprintf("[%d,%d)", e.Interval.Start, e.Interval.Limit) {
			panic(e)
		}
		matches[i] = e.Interval
	}
	return matches
}

func newEntry(start, limit Key) Entry {
	return Entry{
		Interval: Interval{start, limit},
		Data:     fmt.Sprintf("[%d,%d)", start, limit),
	}
}

func TestEmpty(t *testing.T) {
	expect.EQ(t, testGet(New(nil), 1, 2), []Interval{})
}

func TestSmall(t *testing.T) {
	tree := New([]Entry{newEntry(1, 2), newEntry(10, 15)})
	expect.EQ(t, testGet(tree, -1, 0), []Interval{})
	expect.EQ(t, testGet(tree, 0, 2), []Interval{Interval{1, 2}})
	expect.EQ(t, testGet(tree, 0, 10), []Interval{Interval{1, 2}})
	expect.EQ(t, sortIntervals(testGet(tree, 0, 11)), []Interval{Interval{1, 2}, Interval{10, 15}})
}

func randInterval(r *rand.Rand, max Key, width float64) (Key, Key) {
	for {
		start := Key(r.Intn(int(max)))
		limit := start + Key(r.ExpFloat64()*width)
		if start == limit {
			continue
		}
		if start > limit {
			start, limit = limit, start
		}
		return start, limit
	}
}

func testRandom(t *testing.T, seed int, nElem int, max Key, width float64) {
	r := rand.New(rand.NewSource(int64(seed)))
	m0 := slowModel{}
	m1 := &biogoModel{}

	entries := []Entry{}
	for i := 0; i < nElem; i++ {
		start, limit := randInterval(r, max, width)
		m0.insert(start, limit)
		m1.insert(start, limit)
		entries = append(entries, newEntry(start, limit))
	}
	tree := New(entries)
	tree2 := gobEncodeAndDecode(t, tree)

	for i := 0; i < 1000; i++ {
		start, limit := randInterval(r, max, width)

		r0 := sortIntervals(m0.get(start, limit))
		r1 := sortIntervals(m1.get(start, limit))
		result := sortIntervals(testGet(tree, start, limit))
		assert.EQ(t, result, r0, "seed=%d, i=%d, search=[%d,%d)", seed, i, start, limit)
		assert.EQ(t, result, r1, "seed=%d, i=%d, search=[%d,%d)", seed, i, start, limit)
		assert.EQ(t, result, sortIntervals(testGet(tree2, start, limit)))
	}
}

func TestRandom0(t *testing.T) { testRandom(t, 0, 128, 1024, 10) }
func TestRandom1(t *testing.T) { testRandom(t, 1, 128, 1024, 100) }
func TestRandom2(t *testing.T) { testRandom(t, 1, 1000, 8192, 1000) }

func gobEncodeAndDecode(t *testing.T, tree *T) *T {
	buf := bytes.Buffer{}
	e := gob.NewEncoder(&buf)
	assert.NoError(t, e.Encode(tree))

	d := gob.NewDecoder(&buf)
	var tree2 *T
	assert.NoError(t, d.Decode(&tree2))
	return tree2
}

func TestGobEmpty(t *testing.T) {
	tree := New(nil)
	tree2 := gobEncodeAndDecode(t, tree)
	expect.EQ(t, testGet(tree2, 1, 2), []Interval{})
}

func TestGobSmall(t *testing.T) {
	tree := gobEncodeAndDecode(t, New([]Entry{newEntry(1, 2), newEntry(10, 15)}))
	expect.EQ(t, testGet(tree, -1, 0), []Interval{})
	expect.EQ(t, testGet(tree, 0, 2), []Interval{Interval{1, 2}})
	expect.EQ(t, testGet(tree, 0, 10), []Interval{Interval{1, 2}})
	expect.EQ(t, sortIntervals(testGet(tree, 0, 11)), []Interval{Interval{1, 2}, Interval{10, 15}})
}

func benchmarkRandom(b *testing.B, seed int, nElem int, max Key, width float64) {
	b.StopTimer()
	r := rand.New(rand.NewSource(int64(seed)))
	entries := []Entry{}
	for i := 0; i < nElem; i++ {
		start, limit := randInterval(r, max, width)
		entries = append(entries, newEntry(start, limit))
	}
	tree := New(entries)
	b.Logf("Tree stats: %+v", tree.Stats())
	b.StartTimer()
	p := []*Entry{}
	for i := 0; i < b.N; i++ {
		start, limit := randInterval(r, max, width)
		p = p[:0]
		tree.Get(Interval{start, limit}, &p)
	}
}

func BenchmarkRandom0(b *testing.B) {
	benchmarkRandom(b, 0, 100, 10000, 10)
}

func BenchmarkRandom1(b *testing.B) {
	benchmarkRandom(b, 0, 1000, 200000, 100)
}

func BenchmarkRandom2(b *testing.B) {
	benchmarkRandom(b, 0, 1000, 1000000, 100)
}

type testInterval struct {
	id           uintptr
	start, limit Key
}

func (i testInterval) Overlap(b interval.IntRange) bool {
	return i.limit > Key(b.Start) && i.start < Key(b.End)
}

// ID implements interval.IntInterface.
func (i testInterval) ID() uintptr { return i.id }

// Range implements interval.IntInterface.
func (i testInterval) Range() interval.IntRange { return interval.IntRange{int(i.start), int(i.limit)} }

// String implements interval.IntInterface
func (i testInterval) String() string { return fmt.Sprintf("[%d,%d)#%d", i.start, i.limit, i.id) }

func benchmarkBiogoRandom(b *testing.B, seed int, nElem int, max Key, width float64) {
	b.StopTimer()
	r := rand.New(rand.NewSource(int64(seed)))
	tree := interval.IntTree{}

	for i := 0; i < nElem; i++ {
		start, limit := randInterval(r, max, width)
		ii := testInterval{
			start: start,
			limit: limit,
			id:    uintptr(i),
		}
		if err := tree.Insert(ii, false); err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		start, limit := randInterval(r, max, width)
		tree.Get(testInterval{start: start, limit: limit})
	}
}

func BenchmarkBiogoRandom0(b *testing.B) {
	benchmarkBiogoRandom(b, 0, 100, 10000, 10)
}

func BenchmarkBiogoRandom1(b *testing.B) {
	benchmarkBiogoRandom(b, 0, 1000, 200000, 100)
}

func BenchmarkBiogoRandom2(b *testing.B) {
	benchmarkBiogoRandom(b, 0, 1000, 1000000, 100)
}

func Example() {
	newEntry := func(start, limit Key) Entry {
		return Entry{
			Interval: Interval{start, limit},
			Data:     fmt.Sprintf("[%d,%d)", start, limit),
		}
	}

	doGet := func(tree *T, start, limit Key) string {
		matches := []*Entry{}
		tree.Get(Interval{start, limit}, &matches)
		s := []string{}
		for _, m := range matches {
			s = append(s, m.Data.(string))
		}
		sort.Strings(s)
		return strings.Join(s, ",")
	}

	tree := New([]Entry{newEntry(1, 4), newEntry(3, 5), newEntry(6, 7)})
	fmt.Println(doGet(tree, 0, 2))
	fmt.Println(doGet(tree, 0, 4))
	fmt.Println(doGet(tree, 4, 6))
	fmt.Println(doGet(tree, 4, 7))
	// Output:
	// [1,4)
	// [1,4),[3,5)
	// [3,5)
	// [3,5),[6,7)
}

// ExampleGob is an example of serializing an intervalmap using Gob.
func ExampleGob() {
	newEntry := func(start, limit Key) Entry {
		return Entry{
			Interval: Interval{start, limit},
			Data:     fmt.Sprintf("[%d,%d)", start, limit),
		}
	}

	tree := New([]Entry{newEntry(1, 4), newEntry(3, 5), newEntry(6, 7)})

	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(tree); err != nil {
		panic(err)
	}
	dec := gob.NewDecoder(&buf)
	var tree2 *T
	if err := dec.Decode(&tree2); err != nil {
		panic(err)
	}

	doGet := func(tree *T, start, limit Key) string {
		matches := []*Entry{}
		tree.Get(Interval{start, limit}, &matches)
		s := []string{}
		for _, m := range matches {
			s = append(s, m.Data.(string))
		}
		sort.Strings(s)
		return strings.Join(s, ",")
	}

	fmt.Println(doGet(tree2, 0, 2))
	fmt.Println(doGet(tree2, 0, 4))
	fmt.Println(doGet(tree2, 4, 6))
	fmt.Println(doGet(tree2, 4, 7))
	// Output:
	// [1,4)
	// [1,4),[3,5)
	// [3,5)
	// [3,5),[6,7)
}

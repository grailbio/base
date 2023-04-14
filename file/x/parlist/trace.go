package parlist

import (
	"bytes"
	"sort"
	"strings"
	"sync"

	"github.com/grailbio/base/must"
)

type trace struct {
	mu     sync.Mutex
	splits map[KeyRange][]KeyRange
}

func newTrace() *trace { return &trace{splits: make(map[KeyRange][]KeyRange)} }

func (t *trace) Add(parent, child KeyRange) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.splits[parent] = append(t.splits[parent], child)
	t.mu.Unlock()
}

func (t *trace) String() string {
	if t == nil {
		return "trace disabled"
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	var (
		allChildren = make(map[KeyRange]struct{})
		root        KeyRange
		rootFound   bool
	)
	for _, children := range t.splits {
		for _, child := range children {
			allChildren[child] = struct{}{}
		}
	}
	for parent := range t.splits {
		if _, ok := allChildren[parent]; !ok {
			must.Truef(!rootFound, "found second root: %v, first: %v", parent, root)
			root = parent
			rootFound = true
		}
	}
	must.Truef(rootFound, "%#v", t.splits)

	var buf bytes.Buffer
	t.printTraceNode(&buf, root, 0)
	return buf.String()
}

func (t *trace) printTraceNode(buf *bytes.Buffer, node KeyRange, indent int) {
	var prefix string
	if indent > 0 {
		prefix = strings.Repeat("│ ", indent-1) + "├──"
	}
	mustWriteString(buf, prefix+node.String())
	children := t.splits[node]
	sort.Slice(children, func(i, j int) bool {
		if children[i].MinExcl != children[j].MinExcl {
			return children[i].MinExcl < children[j].MinExcl
		}
		return children[i].MaxIncl < children[j].MaxIncl
	})
	for _, child := range children {
		mustWriteString(buf, "\n")
		t.printTraceNode(buf, child, indent+1)
	}
}

func mustWriteString(buf *bytes.Buffer, s string) {
	_, err := buf.WriteString(s)
	must.Nil(err)
}

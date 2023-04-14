package parlist

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
)

const (
	chanBufLen    = 1024
	batchSizeHint = 100
)

func List(ctx context.Context, path string, opts ListOpts) (ShardLister, error) {
	scheme, _, err := file.ParsePath(path)
	if err != nil {
		return nil, err
	}
	raw := file.FindImplementation(scheme)
	if raw == nil {
		return nil, fmt.Errorf("no implementation registered for scheme %s", scheme)
	}
	impl, ok := raw.(Implementation)
	if !ok {
		return nil, fmt.Errorf("implementation %T for scheme %s not supported", raw, scheme)
	}
	return EmbeddedImpl{impl}.ParList(ctx, path, opts), nil
}

// EmbeddedImpl facilitates testing.
type EmbeddedImpl struct{ Implementation }

func (impl EmbeddedImpl) ParList(ctx context.Context, path string, opts ListOpts) ShardLister {
	if opts.BatchSizeHint == 0 {
		opts.BatchSizeHint = batchSizeHint
	}
	tasksC := make(chan KeyRange, chanBufLen)
	p := lister{
		impl:   impl,
		path:   path,
		opts:   opts,
		tasksC: tasksC,
	}
	tasksC <- KeyRange{"", ""}
	p.tasksN.Add(1)
	if log.At(log.Debug) {
		p.trace = newTrace()
	}
	go func() {
		defer close(tasksC)
		p.tasksN.Wait()

		if log.At(log.Debug) {
			// TODO: Printing to stderr directly because log.Printf seems to truncate. Fix?
			_, err := fmt.Fprintf(os.Stderr, "trace: <<<\n%s\n>>>\n", p.trace.String())
			must.Nil(err)
		}
	}()
	return &p
}

type lister struct {
	impl Implementation
	path string
	opts ListOpts

	tasksC chan KeyRange
	tasksN sync.WaitGroup

	// trace is only enabled when necessary (debug logging).
	trace *trace
}

func (p *lister) NewShard() BatchLister { return &shard{lister: p} }

type shard struct {
	lister        *lister
	currentRange  KeyRange
	currentLister BatchLister
	err           error
}

func (s *shard) Scan(ctx context.Context) ([]Info, bool) {
	if s.err != nil {
		return nil, false
	}
	if s.currentLister == nil {
		var gotTask bool
		s.currentRange, gotTask = <-s.lister.tasksC
		if !gotTask {
			return nil, false
		}
		s.currentLister = s.lister.impl.ListBatch(ctx, s.lister.path, ListOpts{
			Recursive:     s.lister.opts.Recursive,
			StartAfter:    s.currentRange.MinExcl,
			BatchSizeHint: s.lister.opts.BatchSizeHint,
		})
	}

	batch, more := s.currentLister.Scan(ctx)
	filtered := s.keepContained(batch)
	if wasted := len(batch) - len(filtered); wasted > 0 {
		log.Debug.Printf("wasted %d records from %v", wasted, s.currentRange)
	}
	remainingRange := s.increaseRangeMin(s.currentRange, batch)
	if !more || // s.currentLister says it's done so we're done with s.currentRange, or...
		// there may be more items, but they'll be above s.currentRange, so we can short circuit.
		remainingRange.IsEmpty() {
		s.currentLister = nil
		s.currentRange = EmptyKeyRange()
		s.lister.tasksN.Done()
		return filtered, true
	}

	halve := remainingRange.HalveNoSlashes
	if s.lister.opts.Recursive {
		halve = remainingRange.Halve
	}
	lo, hi, err := halve()
	if err == nil {
		// Share the work.
		s.lister.tasksN.Add(1)
		s.lister.tasksC <- hi
		traceParent := s.currentRange
		// log.Debug.Printf("enqueuing range: %v, %v", s.lister.path, hi)
		must.True(s.currentRange != lo)
		must.Truef(s.currentRange != hi, "%v\n%v\n%v\n%#v", s.currentRange, lo, hi, batch[len(batch)-2:])
		log.Debug.Printf("split: %s, %v => %v, %v", s.lister.path, s.currentRange, lo, hi)
		s.currentRange.MaxIncl = lo.MaxIncl
		s.lister.trace.Add(traceParent, hi)
		s.lister.trace.Add(traceParent, s.currentRange)
	} else if err == ErrHalveUnsupported {
		// Couldn't divide up the work (perhaps there are Unicode chars we don't handle).
		// We'll keep going alone, for now.
		log.Debug.Printf("unable to split range: %v, %#v", s.lister.path, remainingRange)
	} else {
		s.err = fmt.Errorf("error halving range %v: %w", remainingRange, err)
		return nil, false
	}

	return filtered, true
}

// increaseRangeMin subtracts from r all keys covered in infos (from the min side).
// TODO(josh): Require that ListBatch returns sorted objects to avoid quadratic comparison work.
func (s *shard) increaseRangeMin(r KeyRange, infos []Info) KeyRange {
	for _, info := range infos {
		relKey := s.relKey(info.Path())
		if relKey > r.MinExcl {
			r.MinExcl = relKey
		}
	}
	return r
}

func (s *shard) keepContained(infos []Info) (filtered []Info) {
	filtered = make([]Info, 0, len(infos))
	for _, info := range infos {
		relKey := s.relKey(info.Path())
		if relKey == "" || s.currentRange.ContainsKey(relKey) {
			filtered = append(filtered, info)
		}
	}
	return
}

func (s *shard) relKey(path string) string {
	relKey := path[len(s.lister.path):]
	if relKey != "" && relKey[0] == '/' { // TODO(josh): Use s3file.pathSeparator.
		relKey = relKey[1:]
	}
	return relKey
}

func (s *shard) Err() error {
	if s.err != nil {
		return s.err
	}
	return nil
}

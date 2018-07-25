package workerpool

import (
	"context"
	"sync"

	"github.com/grailbio/base/sync/multierror"
	"v.io/x/lib/vlog"
)

// Task provides an interface for an individual task. Tasks are executed by
// workers by calling the Do function.
type Task interface {
	Do(grp *TaskGroup) error
}

// WorkerPool provides a mechanism for executing Tasks with a specific
// concurrency. A Task is an interface containing a single function Do.
// A TaskGroup allows Tasks to be grouped together so the
// parent process can wait for all Tasks in a TaskGroup to Wait.
// Tasks can create new Tasks and add them to the TaskGroup or new
// TaskGroups and add them to the WorkerPool. A simple example looks like
// this:
//
// wp := fileset.WorkerPool(context.Background(), 3)
// tg1 := wp.NewTaskGroup("context1")
// tg1.Enqueue(MyFirstTask, true)
// tg2 := wp.NewTaskGroup("context2")
// tg2.Enqueue(MyFourthTask, true)
// tg1.Enqueue(MySecondTask, true)
// tg2.Enqueue(MyFifthTask, true)
// tg1.Enqueue(MyThirdTask, true)
// tg1.Wait()
// tg2.Enqueue(MySixthTask, true)
// tg2.Wait()
// wp.Wait()
//
// TaskGroups can come and go until wp.Wait() has been called. Tasks can come
// and go in a TaskGroup until tg.Wait() has been called. All the Tasks
// in this example are executed by 3 go routines.
//
// Note: Each WorkerPool will create a goroutine to keep track of active
// TaskGroups. Each TaskGroup will create a goroutine to keep track of
// pending/active tasks.
type WorkerPool struct {
	Ctx         context.Context
	Concurrency int
	queue       chan deliverable // Contains Tasks waiting to be executed.
	ctxCounter  sync.WaitGroup
}

// New creates a WorkerPool with the given concurrency.
//
// TODO(pknudsgaard): Should return a closure calling Wait.
func New(ctx context.Context, concurrency int) *WorkerPool {
	result := WorkerPool{
		Ctx:         ctx,
		Concurrency: concurrency,
		queue:       make(chan deliverable, 10*concurrency),
	}

	for i := 0; i < concurrency; i++ {
		go result.worker(result.queue)
	}

	return &result
}

// TaskGroup is used group Tasks together so the consumer can wait for a
// specific subgroup of Tasks to Wait.
type TaskGroup struct {
	Name       string
	ErrHandler *multierror.MultiError
	Wp         *WorkerPool
	activity   sync.WaitGroup // Count active tasks
}

// NewTaskGroup creates a TaskGroup for Tasks to be executed in.
//
// TODO(pknudsgaard): TaskGroup should have a context.Context which is
// separate from the WorkerPool context.Context.
//
// TODO(pknudsgaard): Should return a closure calling Wait.
func (wp *WorkerPool) NewTaskGroup(name string, errHandler *multierror.MultiError) *TaskGroup {
	vlog.VI(2).Infof("Creating TaskGroup: %s", name)

	grp := &TaskGroup{
		Name:       name,
		ErrHandler: errHandler,
		Wp:         wp,
	}

	wp.ctxCounter.Add(1)
	return grp
}

// Enqueue puts a Task in the queue. If block is true and the channel is full,
// then the function blocks. If block is false and the channel is full, then
// the function returns false.
func (grp *TaskGroup) Enqueue(t Task, block bool) bool {
	var success bool

	grp.activity.Add(1)
	d := deliverable{grp: grp, t: t}
	if block {
		grp.Wp.queue <- d
		success = true
	} else {
		select {
		case grp.Wp.queue <- d:
			success = true
		default:
			success = false
		}
	}

	if !success {
		grp.activity.Done()
	}

	return success
}

// Wait blocks until all Tasks in this TaskGroup have completed.
func (grp *TaskGroup) Wait() {
	// Trigger the director in case we were already at 0.
	grp.activity.Wait()
	grp.Wp.ctxCounter.Done()
}

type deliverable struct {
	grp *TaskGroup
	t   Task
}

// worker is the goroutine for a worker. It will continue to consume and
// execute tasks from the queue until the channel is closed or the TaskGroup is
// Done.
func (wp *WorkerPool) worker(dlv chan deliverable) {
	vlog.VI(2).Infof("Starting worker")
	defer vlog.VI(2).Infof("Ending worker")

	for {
		select {
		case <-wp.Ctx.Done():
			for d := range dlv {
				d.grp.activity.Done()
			}
			return
		case d, ok := <-dlv:
			if !ok {
				// Channel is closed, quit worker.
				return
			}
			d.grp.ErrHandler.Add(d.t.Do(d.grp))
			d.grp.activity.Done()
		}
	}
}

// Wait blocks until all TaskGroups in the WorkerPool have Waitd.
func (wp *WorkerPool) Wait() {
	// Trigger the director in case we were already at 0:
	wp.ctxCounter.Wait()
	close(wp.queue)
}

// Err returns the context.Context error to determine if WorkerPool Waitd
// due to the context.
func (wp *WorkerPool) Err() error {
	return wp.Ctx.Err()
}

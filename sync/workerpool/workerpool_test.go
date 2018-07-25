package workerpool_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/grail"
	"github.com/grailbio/base/sync/workerpool"
	"github.com/stretchr/testify/assert"
)

// TODO(pknudsgaard): Refactor to table-driven tests?

func TestNoTasks(t *testing.T) {
	ctx := context.Background()

	wp := workerpool.New(ctx, 10)
	wp.Wait()
}

type WaitTask struct {
	delay     time.Duration
	Completed int64
}

func (wt *WaitTask) Do(ctx *workerpool.TaskGroup) error {
	time.Sleep(wt.delay)
	atomic.AddInt64(&wt.Completed, 1)

	return nil
}

func TestSingleTaskBlock(t *testing.T) {
	wp := workerpool.New(context.Background(), 10)
	ctx := wp.NewTaskGroup("test", nil)
	wt := WaitTask{
		delay: 100 * time.Millisecond,
	}
	assert.Equal(t, true, ctx.Enqueue(&wt, true))
	ctx.Wait()
	assert.Equal(t, int64(1), wt.Completed)
	wp.Wait()
}

func TestSingleTaskNoBlock(t *testing.T) {
	wp := workerpool.New(context.Background(), 10)
	ctx := wp.NewTaskGroup("test", nil)
	wt := WaitTask{
		delay: 100 * time.Millisecond,
	}
	assert.Equal(t, true, ctx.Enqueue(&wt, false))
	ctx.Wait()
	assert.Equal(t, int64(1), wt.Completed)
	wp.Wait()
}

func TestContextManyTasks(t *testing.T) {
	wp := workerpool.New(context.Background(), 10)
	ctx := wp.NewTaskGroup("test", nil)
	wt := WaitTask{
		delay: 10 * time.Millisecond,
	}
	for i := 0; i < 1000; i++ {
		assert.Equal(t, true, ctx.Enqueue(&wt, true))
	}
	ctx.Wait()
	assert.Equal(t, int64(1000), wt.Completed)
	wp.Wait()
}

func TestContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	wp := workerpool.New(ctx, 10)
	wpctx := wp.NewTaskGroup("test", nil)
	wt := WaitTask{
		delay: time.Second,
	}
	for i := 0; i < 100; i++ {
		assert.Equal(t, true, wpctx.Enqueue(&wt, true))
	}
	wpctx.Wait()
	assert.True(t, wt.Completed > 0)
	assert.True(t, wt.Completed < 100, fmt.Sprintf("All the tasks Completed %v", wt.Completed))
	assert.Equal(t, "context deadline exceeded", ctx.Err().Error())
	wp.Wait()
}

func TestMain(m *testing.M) {
	shutdown := grail.Init()
	defer shutdown()
	m.Run()
}

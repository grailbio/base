package traverse

import (
	"fmt"
	"math"
	"os"
	"sync"
	"time"
)

type timeEstimateReporter struct {
	name string

	mu sync.Mutex

	numWorkers int32
	numQueued  int32
	numRunning int32
	numDone    int32
	// start time of the Traverse
	startTime time.Time

	cumulativeRuntime time.Duration
	ticker            *time.Ticker

	startTimes map[int]time.Time

	// used to prevent race conditions with printStatus and startTimes queue
}

// NewTimeEstimateReporter returns a reporter that reports the number
// of jobs queued, running, and done, as well as the running time of
// the Traverse and an estimate for the amount of time remaining.
// Note: for estimation, it assumes jobs have roughly equal running
// time and are FIFO-ish (that is, it does not try to account for the
// bias of shorter jobs finishing first and therefore skewing the
// average estimated job run time).
func NewTimeEstimateReporter(name string) Reporter {
	return &timeEstimateReporter{
		name:       name,
		startTimes: make(map[int]time.Time),
	}
}

func (r *timeEstimateReporter) Init(n int) {
	r.numQueued = int32(n)
	r.numWorkers = 1
	r.startTime = time.Now()
	r.ticker = time.NewTicker(time.Second)

	go func() {
		for range r.ticker.C {
			r.mu.Lock()
			r.printStatus()
			r.mu.Unlock()
		}
		fmt.Fprintf(os.Stderr, "\n")
	}()
}

func (r *timeEstimateReporter) Complete() {
	r.ticker.Stop()
}

func (r *timeEstimateReporter) Begin(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startTimes[i] = time.Now()
	r.numQueued--
	r.numRunning++
	if r.numRunning > r.numWorkers {
		r.numWorkers = r.numRunning
	}
	r.printStatus()
}

func (r *timeEstimateReporter) End(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	start, ok := r.startTimes[i]
	if !ok {
		panic("end called without start")
	}
	delete(r.startTimes, i)
	r.numRunning--
	r.numDone++
	r.cumulativeRuntime += time.Since(start)

	r.printStatus()
}

func (r *timeEstimateReporter) printStatus() {
	timeLeftStr := r.buildTimeLeftStr(time.Now())

	fmt.Fprintf(os.Stderr, "%s: (queued: %d -> running: %d -> done: %d) %v %s \r",
		r.name, r.numQueued, r.numRunning, r.numDone,
		time.Since(r.startTime).Round(time.Second), timeLeftStr)
}

func (r *timeEstimateReporter) buildTimeLeftStr(currentTime time.Time) string {
	// If some jobs have finished, use their running time for the estimate.  Otherwise, use the duration
	// that the first job has been running.
	var modifier string
	var avgRunTime time.Duration
	if r.cumulativeRuntime > 0 {
		modifier = "~"
		avgRunTime = r.cumulativeRuntime / time.Duration(r.numDone)
	} else if r.numRunning > 0 {
		modifier = ">"
		for _, t := range r.startTimes {
			avgRunTime += currentTime.Sub(t)
		}
		avgRunTime /= time.Duration(len(r.startTimes))
	}

	runningJobsTimeLeft := time.Duration(r.numRunning)*avgRunTime - r.sumCurrentRunningTimes(currentTime)
	if r.numRunning > 0 {
		runningJobsTimeLeft /= time.Duration(r.numRunning)
	}
	if runningJobsTimeLeft < 0 {
		runningJobsTimeLeft = time.Duration(0)
	}
	queuedJobsTimeLeft := time.Duration(math.Ceil(float64(r.numQueued)/float64(r.numWorkers))) * avgRunTime

	return fmt.Sprintf("(%s%v left  %v avg)", modifier,
		(queuedJobsTimeLeft + runningJobsTimeLeft).Round(time.Second),
		avgRunTime.Round(time.Second))
}

func (r *timeEstimateReporter) sumCurrentRunningTimes(currentTime time.Time) time.Duration {
	var totalRunningTime time.Duration
	for _, startTime := range r.startTimes {
		totalRunningTime += currentTime.Sub(startTime)
	}
	return totalRunningTime
}

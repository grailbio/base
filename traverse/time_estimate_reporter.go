package traverse

import (
	"fmt"
	"math"
	"os"
	"sync"
	"time"
)

// TimeEstimateReporter is a Reporter that prints to stderr the number of jobs queued,
// running, and done, as well as the running time of the Traverse and an estimate for
// the amount of time remaining.
// Note: for estimation, it assumes jobs have roughly equal running time and are FIFO-ish
// (that is, it does not try to account for the bias of shorter jobs finishing first and
// therefore skewing the average estimated job run time).
type TimeEstimateReporter struct {
	// Name is the name of the job to display.
	Name string

	numWorkers int32
	numQueued  int32
	numRunning int32
	numDone    int32
	// start time of the Traverse
	startTime time.Time
	// start times of the individual jobs that are currently running
	startTimes         timeQueue
	cummulativeRuntime time.Duration
	ticker             *time.Ticker

	// used to prevent race conditions with printStatus and startTimes queue
	mut sync.Mutex
}

// Report prints the number of jobs currently queued, running, and done.
func (reporter *TimeEstimateReporter) Report(queued, running, done int32) {
	reporter.mut.Lock()

	currentTime := time.Now()

	if running == 0 && done == 0 {
		reporter.startTime = currentTime
		reporter.startTimes.init(queued)
		reporter.numWorkers = 1
		reporter.numQueued = queued
		reporter.ticker = time.NewTicker(1 * time.Second)

		go func(reporter *TimeEstimateReporter) {
			for range reporter.ticker.C {
				reporter.mut.Lock()
				reporter.printStatus()
				reporter.mut.Unlock()
			}
		}(reporter)
	}

	if running > reporter.numWorkers {
		reporter.numWorkers = running
	}

	// Job started
	if reporter.numQueued-1 == queued && reporter.numRunning+1 == running && reporter.numDone == done {
		reporter.startTimes.push(time.Now())
	}

	// Job finished
	if reporter.numQueued == queued && reporter.numRunning-1 == running && reporter.numDone+1 == done {
		reporter.cummulativeRuntime += time.Since(reporter.startTimes.pop())
	}

	reporter.numQueued = queued
	reporter.numRunning = running
	reporter.numDone = done

	reporter.printStatus()
	if queued == 0 && running == 0 {
		reporter.ticker.Stop()
		fmt.Fprintf(os.Stderr, "\n")
	}
	reporter.mut.Unlock()
}

func (reporter *TimeEstimateReporter) printStatus() {
	timeLeftStr := reporter.buildTimeLeftStr(time.Now())

	fmt.Fprintf(os.Stderr, "%s: (queued: %d -> running: %d -> done: %d) %v %s \r",
		reporter.Name, reporter.numQueued, reporter.numRunning, reporter.numDone,
		time.Since(reporter.startTime).Round(time.Second), timeLeftStr)
}

func (reporter TimeEstimateReporter) buildTimeLeftStr(currentTime time.Time) string {
	// If some jobs have finished, use their running time for the estimate.  Otherwise, use the duration
	// that the first job has been running.
	var modifier string
	var avgRunTime time.Duration
	if reporter.cummulativeRuntime > 0 {
		modifier = "~"
		avgRunTime = reporter.cummulativeRuntime / time.Duration(reporter.numDone)
	} else if reporter.numRunning > 0 {
		modifier = ">"
		avgRunTime = currentTime.Sub(reporter.startTimes.peek())
	}

	runningJobsTimeLeft := time.Duration(reporter.numRunning)*avgRunTime - reporter.sumCurrentRunningTimes(currentTime)
	if reporter.numRunning > 0 {
		runningJobsTimeLeft /= time.Duration(reporter.numRunning)
	}
	if runningJobsTimeLeft < 0 {
		runningJobsTimeLeft = time.Duration(0)
	}
	queuedJobsTimeLeft := time.Duration(math.Ceil(float64(reporter.numQueued)/float64(reporter.numWorkers))) * avgRunTime

	return fmt.Sprintf("(%s%v left  %v avg)", modifier,
		(queuedJobsTimeLeft + runningJobsTimeLeft).Round(time.Second),
		avgRunTime.Round(time.Second))
}

func (reporter TimeEstimateReporter) sumCurrentRunningTimes(currentTime time.Time) time.Duration {
	var totalRunningTime time.Duration
	for _, startTime := range reporter.startTimes {
		totalRunningTime += currentTime.Sub(startTime)
	}
	return totalRunningTime
}

type timeQueue []time.Time

func (q *timeQueue) init(capacity int32) {
	*q = make([]time.Time, 0, capacity)
}

func (q *timeQueue) push(t time.Time) {
	(*q) = append((*q), t)
}

func (q *timeQueue) pop() time.Time {
	elem := (*q)[0]
	*q = (*q)[1:]
	return elem
}

func (q timeQueue) peek() time.Time {
	return q[0]
}

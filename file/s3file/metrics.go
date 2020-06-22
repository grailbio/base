package s3file

import (
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

var (
	metricAutologOnce   sync.Once
	metricAutologPeriod = flag.Duration("s3file.metric_log_period", 0,
		"Interval for logging S3 operation metrics. Zero disables logging.")
)

func metricAutolog() {
	metricAutologOnce.Do(func() {
		if period := *metricAutologPeriod; period > 0 {
			go logMetricsLoop(period)
		}
	})
}

type metricOpMap struct{ m sync.Map }

func (m *metricOpMap) Op(key string) *metricOp {
	var init metricOp
	got, _ := m.m.LoadOrStore(key, &init)
	return got.(*metricOp)
}

func (m *metricOpMap) VisitAndReset(f func(string, *metricOp)) {
	m.m.Range(func(key, value interface{}) bool {
		m.m.Delete(key)
		f(key.(string), value.(*metricOp))
		return true
	})
}

var metrics metricOpMap

type metricOp struct {
	Count expvar.Int

	Retry1 expvar.Int
	Retry2 expvar.Int
	Retry4 expvar.Int
	Retry8 expvar.Int

	DurationFast  expvar.Int
	Duration1Ms   expvar.Int
	Duration10Ms  expvar.Int
	Duration100Ms expvar.Int
	Duration1S    expvar.Int
	Duration10S   expvar.Int
	Duration100S  expvar.Int

	Bytes expvar.Int
}

type metricOpProgress struct {
	parent  *metricOp
	start   time.Time
	retries int // == 0 if first try succeeds
}

func (m *metricOp) Start() *metricOpProgress {
	m.Count.Add(1)
	return &metricOpProgress{m, time.Now(), 0}
}

func (m *metricOpProgress) Retry() { m.retries++ }

func (m *metricOpProgress) Bytes(b int) { m.parent.Bytes.Add(int64(b)) }

func (m *metricOpProgress) Done() {
	switch {
	case m.retries >= 8:
		m.parent.Retry8.Add(1)
	case m.retries >= 4:
		m.parent.Retry4.Add(1)
	case m.retries >= 2:
		m.parent.Retry2.Add(1)
	case m.retries >= 1:
		m.parent.Retry1.Add(1)
	}

	took := time.Since(m.start)
	switch {
	case took > 100*time.Second:
		m.parent.Duration100S.Add(1)
	case took > 10*time.Second:
		m.parent.Duration10S.Add(1)
	case took > time.Second:
		m.parent.Duration1S.Add(1)
	case took > 100*time.Millisecond:
		m.parent.Duration100Ms.Add(1)
	case took > 10*time.Millisecond:
		m.parent.Duration10Ms.Add(1)
	case took > 1*time.Millisecond:
		m.parent.Duration1Ms.Add(1)
	default:
		m.parent.DurationFast.Add(1)
	}
}

func (m *metricOp) Write(w io.Writer, period time.Duration) (int, error) {
	perMinute := 60 / period.Seconds()
	return fmt.Fprintf(w, "n:%d r:%d/%d/%d/%d t:%d/%d/%d/%d/%d/%d/%d mib:%d [/min]",
		int(float64(m.Count.Value())*perMinute),
		int(float64(m.Retry1.Value())*perMinute),
		int(float64(m.Retry2.Value())*perMinute),
		int(float64(m.Retry4.Value())*perMinute),
		int(float64(m.Retry8.Value())*perMinute),
		int(float64(m.DurationFast.Value())*perMinute),
		int(float64(m.Duration1Ms.Value())*perMinute),
		int(float64(m.Duration10Ms.Value())*perMinute),
		int(float64(m.Duration100Ms.Value())*perMinute),
		int(float64(m.Duration1S.Value())*perMinute),
		int(float64(m.Duration10S.Value())*perMinute),
		int(float64(m.Duration100S.Value())*perMinute),
		int(float64(m.Bytes.Value())/(1<<20)*perMinute),
	)
}

func logMetricsLoop(period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	var buf strings.Builder
	for {
		select {
		case <-ticker.C:
			metrics.VisitAndReset(func(op string, metrics *metricOp) {
				buf.Reset()
				fmt.Fprintf(&buf, "s3file metrics: op:%s ", op)
				_, _ = metrics.Write(&buf, period)
				log.Print(buf.String())
			})
		}
	}
}

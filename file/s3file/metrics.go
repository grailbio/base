package s3file

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	metricAutologOnce   sync.Once
	metricAutologPeriod = flag.Duration("s3file.metric_log_period", 0,
		"Interval for logging S3 operation metrics. Zero disables logging.")
	metricHTTPAddr = flag.Bool("s3file.metric_http_addr", false,
		"Modifies the S3 client HTTP transport to add remote IP address metrics. "+
			"Not for production. See s3file/internal/cmd/resolvetest/main.go")
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

var (
	metrics           metricOpMap
	metricRemoteAddrs expvar.Map
)

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
	remoteAddrCounts := make([]int, 16)
	for {
		select {
		case <-ticker.C:
			metrics.VisitAndReset(func(op string, metrics *metricOp) {
				buf.Reset()
				fmt.Fprintf(&buf, "s3file metrics: op:%s ", op)
				_, _ = metrics.Write(&buf, period)
				log.Print(buf.String())
			})
			if *metricHTTPAddr {
				buf.Reset()
				for i := range remoteAddrCounts {
					remoteAddrCounts[i] = 0
				}
				fmt.Fprint(&buf, "s3file metrics: op:dial ")
				metricRemoteAddrs.Do(func(kv expvar.KeyValue) {
					count := int(kv.Value.(*expvar.Int).Value())
					for i, old := range remoteAddrCounts {
						if count <= old {
							continue
						}
						copy(remoteAddrCounts[i+1:], remoteAddrCounts[i:])
						remoteAddrCounts[i] = count
						break
					}
				})
				for i, count := range remoteAddrCounts {
					if i > 0 {
						fmt.Fprint(&buf, "/")
					}
					fmt.Fprint(&buf, count)
				}
				log.Print(buf.String())
			}
		}
	}
}

// metricInstrumentedTransports is a cache mapping seen transports to their instrumented clones.
// This avoids 1) modifying input transports that may be used elsewhere, 2) cloning transports
// unnecessarily, which can distort results by separating connection pools.
//
// This is never released. Users accept this cost by opting in to *metricHTTPAddr.
var metricInstrumentedTransports sync.Map

type metricInstrumentedTransport struct{ *http.Transport }

func makeMetricHTTPClient(c *http.Client) *http.Client {
	roundTripper := c.Transport
	if roundTripper == nil {
		roundTripper = http.DefaultTransport
	}
	switch transport := roundTripper.(type) {
	default:
		log.Printf("s3file metrics: unrecognized transport %T, dial logging disabled for this client", c.Transport)
		return c
	case metricInstrumentedTransport:
		return c
	case *http.Transport:
		cached, ok := metricInstrumentedTransports.Load(transport)
		if !ok {
			instrumented := metricInstrumentedTransport{transport.Clone()}
			defaultDialContext := instrumented.DialContext
			instrumented.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := defaultDialContext(ctx, network, addr)
				if err == nil {
					metricRemoteAddrs.Add(conn.RemoteAddr().String(), 1)
				}
				return conn, err
			}
			metricInstrumentedTransports.Store(transport, instrumented)
			cached = instrumented
		}
		return &http.Client{cached.(http.RoundTripper), c.CheckRedirect, c.Jar, c.Timeout}
	}
}

type metricClientProvider struct{ ClientProvider }

func (m metricClientProvider) Get(ctx context.Context, op, path string) ([]s3iface.S3API, error) {
	clients, err := m.ClientProvider.Get(ctx, op, path)
	if err != nil {
		return clients, err
	}
	for _, client := range clients {
		s3Client, ok := client.(*s3.S3)
		if !ok {
			continue
		}
		s3Client.Client.Config.HTTPClient = makeMetricHTTPClient(s3Client.Client.Config.HTTPClient)
	}
	return clients, err
}

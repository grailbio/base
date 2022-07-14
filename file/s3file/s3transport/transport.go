package s3transport

import (
	"crypto/tls"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/grailbio/base/file/s3file/internal/autolog"
	"github.com/grailbio/base/log"
)

// T is an http.RoundTripper specialized for S3. See https://github.com/aws/aws-sdk-go/issues/3739.
type T struct {
	factory func() *http.Transport

	hostRTsMu sync.Mutex
	hostRTs   map[string]http.RoundTripper

	nOpenConnsPerIPMu sync.Mutex
	nOpenConnsPerIP   map[string]int

	hostIPs *expiringMap
}

var (
	stdDefaultTransport = http.DefaultTransport.(*http.Transport)
	httpTransport       = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second, // Copied from http.DefaultTransport.
			KeepAlive: 30 * time.Second, // Copied from same.
		}).DialContext,
		ForceAttemptHTTP2:     false,                           // S3 doesn't support HTTP2.
		MaxIdleConns:          200,                             // Keep many peers for future bursts.
		MaxIdleConnsPerHost:   4,                               // But limit connections to each.
		IdleConnTimeout:       expireAfter + 2*expireLoopEvery, // Keep until we forget the peer.
		TLSClientConfig:       &tls.Config{},
		TLSHandshakeTimeout:   stdDefaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: stdDefaultTransport.ExpectContinueTimeout,
	}

	defaultOnce   sync.Once
	defaultT      *T
	defaultClient *http.Client
)

func defaults() (*T, *http.Client) {
	defaultOnce.Do(func() {
		defaultT = New(httpTransport.Clone)
		defaultClient = &http.Client{Transport: defaultT}
	})
	return defaultT, defaultClient
}

// Default returns an http.RoundTripper with recommended settings.
func Default() *T { t, _ := defaults(); return t }

// DefaultClient returns an *http.Client that uses the http.RoundTripper
// returned by Default (suitable for general use, analogous to
// "net/http".DefaultClient).
func DefaultClient() *http.Client { _, c := defaults(); return c }

// New constructs *T using factory to create internal transports. Each call to factory()
// must return a separate http.Transport and they must not share TLSClientConfig.
func New(factory func() *http.Transport) *T {
	t := T{
		factory:         factory,
		hostRTs:         map[string]http.RoundTripper{},
		hostIPs:         newExpiringMap(runPeriodicForever(), time.Now),
		nOpenConnsPerIP: map[string]int{},
	}
	autolog.Register(func() {
		var nOpen []int
		t.nOpenConnsPerIPMu.Lock()
		for _, n := range t.nOpenConnsPerIP {
			nOpen = append(nOpen, n)
		}
		t.nOpenConnsPerIPMu.Unlock()
		sort.Sort(sort.Reverse(sort.IntSlice(nOpen)))
		log.Printf("s3file transport: open RTs per IP: %v", nOpen)
	})
	return &t
}

func (t *T) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()

	ips, err := defaultResolver.LookupIP(host)
	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("s3transport: lookup ip: %w", err)
	}
	ips = t.hostIPs.AddAndGet(host, ips)

	hostReq := req.Clone(req.Context())
	hostReq.Host = host
	// TODO: Consider other load balancing strategies.
	ip := ips[rand.Intn(len(ips))].String()
	hostReq.URL.Host = ip

	hostRT := t.hostRoundTripper(host)
	resp, err := hostRT.RoundTrip(hostReq)
	if resp != nil {
		t.addOpenConnsPerIP(ip, 1)
		resp.Body = &rcOnClose{resp.Body, func() { t.addOpenConnsPerIP(ip, -1) }}
	}
	return resp, err
}

func (t *T) hostRoundTripper(host string) http.RoundTripper {
	t.hostRTsMu.Lock()
	defer t.hostRTsMu.Unlock()
	if rt, ok := t.hostRTs[host]; ok {
		return rt
	}
	transport := t.factory()
	// We modify request URL to contain an IP, but server certificates list hostnames, so we
	// configure our client to check against original hostname.
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	transport.TLSClientConfig.ServerName = host
	t.hostRTs[host] = transport
	return transport
}

func (t *T) addOpenConnsPerIP(ip string, add int) {
	t.nOpenConnsPerIPMu.Lock()
	t.nOpenConnsPerIP[ip] += add
	t.nOpenConnsPerIPMu.Unlock()
}

type rcOnClose struct {
	io.ReadCloser
	onClose func()
}

func (r *rcOnClose) Close() error {
	// In rare cases, this Close() is called a second time, with a call stack from the AWS SDK's
	// cleanup code.
	if r.onClose != nil {
		defer r.onClose()
	}
	r.onClose = nil
	return r.ReadCloser.Close()
}

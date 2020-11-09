package s3transport

import (
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/grailbio/base/backgroundcontext"
)

type T struct {
	hostRTsMu sync.Mutex
	hostRTs   map[string]http.RoundTripper

	hostIPs *expiringMap
}

var (
	httpDefaultTransport = http.DefaultTransport.(*http.Transport)
	httpTransport        = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second, // Copied from http.DefaultTransport.
			KeepAlive: 30 * time.Second, // Copied from same.
		}).DialContext,
		ForceAttemptHTTP2:     false,                           // S3 doesn't support HTTP2.
		MaxIdleConns:          200,                             // Keep many peers for future bursts.
		MaxIdleConnsPerHost:   4,                               // But limit connections to each.
		IdleConnTimeout:       expireAfter + 2*expireLoopEvery, // Keep until we forget the peer.
		TLSClientConfig:       &tls.Config{},
		TLSHandshakeTimeout:   httpDefaultTransport.TLSHandshakeTimeout,
		ExpectContinueTimeout: httpDefaultTransport.ExpectContinueTimeout,
	}
	defaultOnce   sync.Once
	defaultClient *http.Client
)

func Client() *http.Client {
	defaultOnce.Do(func() {
		defaultClient = &http.Client{Transport: &T{
			hostRTs: map[string]http.RoundTripper{},
			hostIPs: newExpiringMap(runPeriodicUntilCancel(backgroundcontext.Get()), time.Now),
		}}
	})
	return defaultClient
}

func (t *T) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()

	ips, err := defaultResolver.LookupIP(host)
	if err != nil {
		_ = req.Body.Close()
		return nil, fmt.Errorf("s3transport: lookup ip: %w", err)
	}
	ips = t.hostIPs.AddAndGet(host, ips)

	hostReq := req.Clone(req.Context())
	hostReq.Host = host
	// TODO: Consider other load balancing strategies.
	hostReq.URL.Host = ips[rand.Intn(len(ips))].String()

	return t.hostRoundTripper(host).RoundTrip(hostReq)
}

func (t *T) hostRoundTripper(host string) http.RoundTripper {
	t.hostRTsMu.Lock()
	defer t.hostRTsMu.Unlock()
	if rt, ok := t.hostRTs[host]; ok {
		return rt
	}
	transport := httpTransport.Clone()
	// We modify request URL to contain an IP, but server certificates list hostnames, so we
	// configure our client to check against original hostname.
	transport.TLSClientConfig.ServerName = host
	t.hostRTs[host] = transport
	return transport
}

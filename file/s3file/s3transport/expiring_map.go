package s3transport

import (
	"net"
	"sync"
	"time"

	"github.com/grailbio/base/file/s3file/internal/autolog"
	"github.com/grailbio/base/log"
)

const (
	// expireAfter balances saving seen IPs to distribute ongoing load vs. tying up resources
	// for a long time. Given that DNS provides new S3 IP addresses every few seconds, retaining
	// for an hour means I/O intensive batch jobs can maintain hundreds of S3 peers. But, an API server
	// with weeks of uptime won't accrete huge numbers of old records.
	expireAfter = time.Hour
	// expireLoopEvery controls how frequently the expireAfter threshold is tested, so it controls
	// "slack" in expireAfter. The loop takes locks that block requests, so it should not be too
	// frequent (relative to request rate).
	expireLoopEvery = time.Minute
)

type expiringMap struct {
	now func() time.Time

	mu sync.Mutex
	// elems is URL host -> string(net.IP) -> last seen.
	elems map[string]map[string]time.Time
}

func newExpiringMap(runPeriodic runPeriodic, now func() time.Time) *expiringMap {
	s := expiringMap{now: now, elems: map[string]map[string]time.Time{}}
	go runPeriodic(expireLoopEvery, s.expireOnce)
	autolog.Register(s.logOnce)
	return &s
}

func (s *expiringMap) AddAndGet(host string, newIPs []net.IP) (allIPs []net.IP) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	ips, ok := s.elems[host]
	if !ok {
		ips = map[string]time.Time{}
		s.elems[host] = ips
	}
	for _, ip := range newIPs {
		ips[string(ip)] = now
	}
	for ip := range ips {
		allIPs = append(allIPs, net.IP(ip))
	}
	return
}

func (s *expiringMap) expireOnce(now time.Time) {
	earliestUnexpiredTime := now.Add(-expireAfter)
	s.mu.Lock()
	for host, ips := range s.elems {
		deleteBefore(ips, earliestUnexpiredTime)
		if len(ips) == 0 {
			delete(s.elems, host)
		}
	}
	s.mu.Unlock()
}

func deleteBefore(times map[string]time.Time, threshold time.Time) {
	for key, time := range times {
		if time.Before(threshold) {
			delete(times, key)
		}
	}
}

func (s *expiringMap) logOnce() {
	s.mu.Lock()
	var (
		hosts          = len(s.elems)
		ips, hostIPMax int
	)
	for _, e := range s.elems {
		ips += len(e)
		if len(e) > hostIPMax {
			hostIPMax = len(e)
		}
	}
	s.mu.Unlock()
	log.Printf("s3file transport: hosts:%d ips:%d hostipmax:%d", hosts, ips, hostIPMax)
}

// runPeriodic runs the given func with the given period.
type runPeriodic func(time.Duration, func(time.Time))

func runPeriodicForever() runPeriodic {
	return func(period time.Duration, tick func(time.Time)) {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				tick(now)
			}
		}
	}
}

func noOpRunPeriodic(time.Duration, func(time.Time)) {}

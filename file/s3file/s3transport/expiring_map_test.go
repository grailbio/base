package s3transport

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExpiringMap(t *testing.T) {
	ips := func(is ...byte) (ret []net.IP) {
		for _, i := range is {
			ret = append(ret, net.IP{i, i, i, i})
		}
		return
	}
	var stubNow time.Time

	m := newExpiringMap(noOpRunPeriodic, func() time.Time { return stubNow })

	stubNow = time.Unix(1600000000, 0)
	assert.ElementsMatch(t, ips(0, 1), m.AddAndGet("s3.example.com", ips(0, 1)))

	stubNow = stubNow.Add(expireAfter / 2)
	assert.ElementsMatch(t, ips(0, 1), m.AddAndGet("s3.example.com", ips(0)))
	assert.ElementsMatch(t, ips(0, 1, 3), m.AddAndGet("s3.example.com", ips(3)))

	stubNow = stubNow.Add(expireAfter/2 + 2)
	m.expireOnce(stubNow) // Drop ips(1).
	assert.ElementsMatch(t, ips(0, 3, 4), m.AddAndGet("s3.example.com", ips(4)))
	assert.ElementsMatch(t, ips(100), m.AddAndGet("s3-2.example.com", ips(100)))

	stubNow = stubNow.Add(expireAfter/2 + 2)
	m.expireOnce(stubNow) // Drop ips(0, 3).
	assert.ElementsMatch(t, ips(4), m.AddAndGet("s3.example.com", nil))
	assert.ElementsMatch(t, ips(100), m.AddAndGet("s3-2.example.com", nil))

	m.logOnce(stubNow) // No assertions other than it shouldn't panic.
}

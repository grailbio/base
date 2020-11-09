package s3transport

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResolver(t *testing.T) {
	var (
		gotHost   string
		stubIP    []net.IP
		stubError error
		stubNow   time.Time
	)
	stubLookupIP := func(host string) ([]net.IP, error) {
		gotHost = host
		return stubIP, stubError
	}
	r := newResolver(stubLookupIP, func() time.Time { return stubNow })

	stubIP, stubError = []net.IP{{1, 2, 3, 4}, {10, 20, 30, 40}}, nil
	stubNow = time.Unix(1600000000, 0)
	gotIP, gotError := r.LookupIP("s3.example.com")
	assert.Equal(t, "s3.example.com", gotHost)
	assert.NoError(t, gotError)
	assert.Equal(t, []net.IP{{1, 2, 3, 4}, {10, 20, 30, 40}}, gotIP)

	stubIP, stubError = nil, fmt.Errorf("stub err")
	stubNow = stubNow.Add(dnsCacheTime - 1)
	gotHost = "should not be called"
	gotIP, gotError = r.LookupIP("s3.example.com")
	assert.Equal(t, "should not be called", gotHost)
	assert.NoError(t, gotError)
	assert.Equal(t, []net.IP{{1, 2, 3, 4}, {10, 20, 30, 40}}, gotIP)

	stubIP, stubError = []net.IP{{5, 6, 7, 8}}, nil
	gotIP, gotError = r.LookupIP("s3-us-west-2.example.com")
	assert.Equal(t, "s3-us-west-2.example.com", gotHost)
	assert.NoError(t, gotError)
	assert.Equal(t, []net.IP{{5, 6, 7, 8}}, gotIP)

	stubIP, stubError = []net.IP{{21, 22, 23, 24}}, nil
	gotHost = ""
	stubNow = stubNow.Add(2)
	gotIP, gotError = r.LookupIP("s3.example.com")
	assert.Equal(t, "s3.example.com", gotHost)
	assert.NoError(t, gotError)
	assert.Equal(t, []net.IP{{21, 22, 23, 24}}, gotIP)
}

package gfs

import (
	"sync/atomic"
	"time"
)

// Amount of time to cache directory entries and file stats (size, mtime).
const cacheExpiration = 5 * time.Minute

var currentEpoch uint64

// timestamp has two parts. The upper epoch part is incremented on SIGHUP to
// invalidate all cached stats. The lower time part represents the wallclock
// time.
type timestamp struct {
	epoch uint64
	time  time.Time
}

// NewEpoch increments the current epoch. All the timestamps created before a
// newEpoch call will be considered infinitely old.
func newEpoch() { atomic.AddUint64(&currentEpoch, 1) }

// Now returns the current <epoch, walltime>.
func now() timestamp {
	epoch := atomic.LoadUint64(&currentEpoch)
	return timestamp{epoch: epoch, time: time.Now()}
}

// Add creates a new timestamp that adds the given duration.
func (t timestamp) Add(d time.Duration) timestamp {
	return timestamp{epoch: t.epoch, time: t.time.Add(d)}
}

// After reports if t > t2.
func (t timestamp) After(t2 timestamp) bool {
	if t.epoch != t2.epoch {
		return t.epoch > t2.epoch
	}
	return t.time.After(t2.time)
}

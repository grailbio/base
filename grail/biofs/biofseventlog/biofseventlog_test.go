package biofseventlog

import (
	"math/rand"
	"testing"
	"time"
)

func TestCoarseTime(t *testing.T) {
	const (
		weeks = 10
		N     = 10000
	)
	minMillis := coarseMillis(1600000000000) // Arbitrary time in 2020.
	maxMillis := minMillis + 7*24*weeks*time.Hour.Milliseconds()

	gotCoarseMillis := map[int64]struct{}{}
	rnd := rand.New(rand.NewSource(1))
	for i := 0; i < N; i++ {
		fineMillis := minMillis + rnd.Int63n(maxMillis-minMillis)
		gotCoarseMillis[coarseMillis(fineMillis)] = struct{}{}
	}

	if got := len(gotCoarseMillis); got != weeks {
		t.Errorf("got %d, want %d", got, weeks)
	}
}

func coarseMillis(millis int64) int64 {
	return CoarseTime(time.UnixMilli(millis)).UnixMilli()
}

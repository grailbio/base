// biofseventlog creates usage events for biofs, a GRAIL-internal program. biofs has to be internal
// because it runs fsnodefuse with some fsnode.T's derived from other internal code, but it also
// uses github.com/grailbio packages like s3file.
package biofseventlog

import (
	"strconv"
	"sync"
	"time"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/eventlog"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
)

const configName = "biofs/eventer"

func init() {
	config.Default(configName, "eventer/nop")
}

// UsedFeature creates an event for usage of the named feature.
func UsedFeature(featureName string) {
	var eventer eventlog.Eventer
	must.Nil(config.Instance(configName, &eventer))
	eventer.Event("usedFeature",
		"name", featureName,
		"buildTime", getCoarseBuildTimestamp())
}

// CoarseNow returns times with precision truncated by CoarseTime.
func CoarseNow() time.Time { return CoarseTime(time.Now()) }

// CoarseTime truncates t's precision to a nearby week. It's used to improve event log anonymity.
func CoarseTime(t time.Time) time.Time {
	weekMillis := 7 * 24 * time.Hour.Milliseconds()
	now := t.UnixMilli()
	now /= weekMillis
	now *= weekMillis
	return time.UnixMilli(now)
}

var (
	buildTimestamp       string
	coarseBuildTimestamp = "unknown"
	buildTimestampOnce   sync.Once
)

// getCoarseBuildTimestamp returns the (probably bazel-injected) build timestamp
// with precision truncated to CoarseTime, or a message if data is unavailable.
func getCoarseBuildTimestamp() string {
	buildTimestampOnce.Do(func() {
		if buildTimestamp == "" {
			return
		}
		buildSecs, err := strconv.ParseInt(buildTimestamp, 10, 64)
		if err != nil {
			log.Error.Printf("biofseventlog: error parsing build timestamp: %v", err)
			return
		}
		coarseBuildTime := CoarseTime(time.Unix(buildSecs, 0))
		coarseBuildTimestamp = coarseBuildTime.Format("20060102")
	})
	return coarseBuildTimestamp
}

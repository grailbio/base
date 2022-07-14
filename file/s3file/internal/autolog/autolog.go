package autolog

import (
	"flag"
	"time"
)

var autologPeriod = flag.Duration("s3file.autolog_period", 0,
	"Interval for logging s3transport metrics. Zero disables logging.")

// Register configures an internal ticker to periodically call logFn.
func Register(logFn func()) {
	if *autologPeriod == 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(*autologPeriod)
		defer ticker.Stop()
		for {
			select {
			case _ = <-ticker.C:
				logFn()
			}
		}
	}()
}

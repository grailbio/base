// resolvetest simply resolves a hostname at an increasing time interval to
// observe the diversity in DNS lookup addresses for the host.
//
// This quick experiment is motivated by the S3 performance guide, which
// recommends using multiple clients with different remote IPs:
//
//   Finally, it’s worth paying attention to DNS and double-checking that
//   requests are being spread over a wide pool of Amazon S3 IP addresses. DNS
//   queries for Amazon S3 cycle through a large list of IP endpoints. But
//   caching resolvers or application code that reuses a single IP address do
//   not benefit from address diversity and the load balancing that follows from it.
//
// http://web.archive.org/web/20200624062712/https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance-design-patterns.html
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/grailbio/base/log"
)

func main() {
	if len(os.Args) > 2 {
		log.Fatal("expect 1 argument: hostname to resolve")
	}
	host := "us-west-2.s3.amazonaws.com"
	if len(os.Args) == 2 {
		host = os.Args[1]
	}

	last := time.Now()
	bufOut := bufio.NewWriter(os.Stdout)
	for sleepDuration := time.Millisecond; ; {
		now := time.Now()
		_, _ = fmt.Fprintf(bufOut, "%.6f:\t", now.Sub(last).Seconds())
		last = now

		ips, err := net.LookupIP(host)
		if err != nil {
			_, _ = bufOut.WriteString(err.Error())
		} else {
			for i, ip := range ips {
				if i > 0 {
					_ = bufOut.WriteByte(' ')
				}
				_, _ = bufOut.WriteString(ip.String())
			}
		}

		_ = bufOut.WriteByte('\n')
		_ = bufOut.Flush()

		time.Sleep(sleepDuration)
		if sleepDuration < time.Second {
			sleepDuration *= 2
		}
	}
}

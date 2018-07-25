// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package webutil

import (
	"net"
	"strings"
)

// CanonicalizeHost accepts a server host name as typically encountered by web
// server and generally obtained from the http request Host header field and
// canonicalizes it into:
// fqhp   - fully qualified host+domain with a port - ie: host.domain:port
// fqh    - fully qualified host+domain without a port - ie: host.domain
// host   - the hostname, i.e. the first component of a . separated name
// domain - the second and subsequent components of a . separated name
// port   - the port.
//
// It is generally used for constructing redirects, determine/scoping
// cookie names etc.
//
// The server name can be in any of the forms accepted by a web browser:
// 1. [<protocol>://]<ip-addr>[:port]
// 2. [<protocol>://]<name>[:<port>]
// 3. [<protocol>://]<name>[.<subdomain>]+[:<port>]
// The defaultDomain and defaultPort, if not "", will be used if there is
// no domain or port in hostName.
func CanonicalizeHost(hostName, defaultDomain, defaultPort string) (protocol, fqhp, fqh, host, domain, port string, hosterr error) {

	if idx := strings.Index(hostName, "//"); idx >= 0 {
		protocol = hostName[:idx]
		hostName = hostName[idx+len("//"):]
	}

	h, p, err := net.SplitHostPort(hostName)
	if err == nil {
		// There is a port.
		port = p
		host = h
	} else {
		// There is no port, or there is some kind of other error, so
		// we append a port and split again to see if it's an error.
		_, _, err := net.SplitHostPort(net.JoinHostPort(hostName, "80"))
		if err != nil {
			hosterr = err
			host = ""
			return
		}
		host = hostName
	}
	if len(port) == 0 {
		port = defaultPort
	}
	// host without port and port are now determined.

	jhp := func(h, p string) string {
		if len(p) > 0 {
			return net.JoinHostPort(h, p)
		}
		return h
	}

	if net.ParseIP(host) != nil {
		// an IP address
		fqhp = jhp(host, port)
		fqh = host
		host = host
		return
	}

	if dot := strings.Index(host, "."); dot >= 0 {
		// we have a domain...
		fqhp = jhp(host, port)
		fqh = host
		host, domain = host[:dot], host[dot+1:]
		return
	}
	// use default domain
	domain = strings.TrimPrefix(defaultDomain, ".")
	if len(domain) > 0 {
		fqh = host + "." + domain
		fqhp = jhp(fqh, port)
		return
	}
	fqhp = jhp(host, port)
	fqh = host
	return
}

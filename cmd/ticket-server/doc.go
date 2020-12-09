// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// This file was auto-generated via go generate.
// DO NOT UPDATE MANUALLY

/*
Command ticket-server runs a Vanadium server that provides restricted access to
tickets. A ticket contains credentials and configurations that allows
communicating with another system. For example, an S3 ticket contains AWS
credentials and also the bucket and object or prefix to fetch while a Docker
ticket contains the TLS certificate expected from the server, a client TLS
certificate + the private key and the URL to reach the Docker daemon.

Usage:
   ticket-server [flags]

The ticket-server flags are:
 -config-dir=
   Directory with tickets in VDL format. Must be provided.
 -danger-danger-danger-ec2-disable-address-check=false
   Disable the IP address check for the EC2-based blessings requests. Only
   useful for local tests.
 -danger-danger-danger-ec2-disable-pending-time-check=false
   Disable the pendint time check for the EC2-based blessings requests. Only
   useful for local tests.
 -danger-danger-danger-ec2-disable-uniqueness-check=false
   Disable the uniqueness check for the EC2-based blessings requests. Only
   useful for local tests.
 -ec2-blesser-role=
   What role to use for the blesser/ec2 endpoint. The role needs to exist in all
   the accounts.
 -ec2-dynamodb-table=
   DynamoDB table to use for enforcing the uniqueness of the EC2-based blessings
   requests.
 -ec2-expiration=8760h0m0s
   Expiration caveat for the EC2-based blessings.
 -google-expiration=168h0m0s
   Expiration caveat for the Google-based blessings.
 -name=
   Name to mount the server under. If empty, don't mount.
 -service-account=
   JSON file with a Google service account credentials.

The global flags are:
 -alsologtostderr=true
   log to standard error as well as files
 -log_backtrace_at=:0
   when logging hits line file:N, emit a stack trace
 -log_dir=
   if non-empty, write log files to this directory
 -logtostderr=false
   log to standard error instead of files
 -max_stack_buf_size=4292608
   max size in bytes of the buffer to use for logging stack traces
 -metadata=<just specify -metadata to activate>
   Displays metadata for the program and exits.
 -stderrthreshold=2
   logs at or above this threshold go to stderr
 -time=false
   Dump timing information to stderr before exiting the program.
 -v=0
   log level for V logs
 -v23.credentials=
   directory to use for storing security credentials
 -v23.i18n-catalogue=
   18n catalogue files to load, comma separated
 -v23.namespace.root=[/(v23.grail.com:internal:mounttabled)@ns.v23.grail.com:8101]
   local namespace root; can be repeated to provided multiple roots
 -v23.permissions.file=map[]
   specify a perms file as <name>:<permsfile>
 -v23.permissions.literal=
   explicitly specify the runtime perms as a JSON-encoded access.Permissions.
   Overrides all --v23.permissions.file flags.
 -v23.proxy=
   object name of proxy service to use to export services across network
   boundaries
 -v23.tcp.address=
   address to listen on
 -v23.tcp.protocol=wsh
   protocol to listen with
 -v23.vtrace.cache-size=1024
   The number of vtrace traces to store in memory.
 -v23.vtrace.collect-regexp=
   Spans and annotations that match this regular expression will trigger trace
   collection.
 -v23.vtrace.dump-on-shutdown=true
   If true, dump all stored traces on runtime shutdown.
 -v23.vtrace.sample-rate=0
   Rate (from 0.0 to 1.0) to sample vtrace traces.
 -v23.vtrace.v=0
   The verbosity level of the log messages to be captured in traces
 -vmodule=
   comma-separated list of globpattern=N settings for filename-filtered logging
   (without the .go suffix).  E.g. foo/bar/baz.go is matched by patterns baz or
   *az or b* but not by bar/baz or baz.go or az or b.*
 -vpath=
   comma-separated list of regexppattern=N settings for file pathname-filtered
   logging (without the .go suffix).  E.g. foo/bar/baz.go is matched by patterns
   foo/bar/baz or fo.*az or oo/ba or b.z but not by foo/bar/baz.go or fo*az
*/
package main

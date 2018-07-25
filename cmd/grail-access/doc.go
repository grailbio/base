// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// This file was auto-generated via go generate.
// DO NOT UPDATE MANUALLY

/*
Command grail-access creates Vanadium credentials (also called principals) using
either Google ID tokens (the default) or the AWS IAM role attached to an EC2
instance (requested using the '-ec2' flag).

For the Google-based auth the user will be prompted to go through an OAuth flow
that requires minimal permissions (only 'Know who you are on Google') and
obtains an ID token scoped to the clientID expected by the server. The ID token
is presented to the server via a Vanadium RPC. For a 'xxx@grailbio.com' email
address the server will hand to the client a '[server]:google:xxx@grailbio.com'
blessing where '[server]' is the blessing of the server.

For the EC2-based auth an instance with ID 'i-0aec7b085f8432699' in the account
number '619867110810' using the 'adhoc' role the server will hand to the client
a '[server]:ec2:619867110810:role:adhoc:i-0aec7b085f8432699' blessing where
'server' is the blessing of the server.

Usage:
   grail-access [flags]

The grail-access flags are:
 -blesser-ec2=/ticket-server.eng.grail.com:8102/blesser/ec2
   Blesser to talk to for the EC2-based flow.
 -blesser-google=/ticket-server.eng.grail.com:8102/blesser/google
   Blesser to talk to for the Google-based flow.
 -browser=true
   Attempt to open a browser.
 -dir=/home/razvanm/.v23
   Where to store the Vanadium credentials. NOTE: the content will be erased if
   the credentials are regenerated.
 -ec2=false
   Use the role of the EC2 VM.

The global flags are:
 -alsologtostderr=true
   log to standard error as well as files
 -block-profile=
   filename prefix for block profiles
 -block-profile-rate=1
   rate for runtime. SetBlockProfileRate
 -cpu-profile=
   filename for cpu profile
 -heap-profile=
   filename prefix for heap profiles
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
 -mutex-profile=
   filename prefix for mutex profiles
 -mutex-profile-rate=1
   rate for runtime.SetMutexProfileFraction
 -pprof=
   address for pprof server
 -profile-interval-s=0
   If >0, output new profiles at this interval (seconds). If <=0, profiles are
   written only when Write() is called
 -stderrthreshold=2
   logs at or above this threshold go to stderr
 -thread-create-profile=
   filename prefix for thread create profiles
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

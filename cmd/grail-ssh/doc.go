// This file was auto-generated via go generate.
// DO NOT UPDATE MANUALLY

/*
Command that simplifies connecting to GRAIL systems using ssh with ssh
certificates for authentication.

Usage:
   ssh [flags] <ticket>

The ssh flags are:
 -i=/mnt/home/jjc/.ssh/id_rsa
   Path to the SSH private key that will be used for the connection
 -l=
   Username to provide to the remote host. If not provided selects the first
   principal defined as part of the ticket definition.
 -ssh=ssh
   What ssh client to use

The global flags are:
 -alsologtostderr=false
   log to standard error as well as files
 -block-profile=
   filename prefix for block profiles
 -block-profile-rate=200
   rate for runtime.SetBlockProfileRate
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
 -mutex-profile-rate=200
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
 -v23.namespace.root=[/(v23.grail.com:internal:mounttabled)@ns-0.v23.grail.com:8101,/(v23.grail.com:internal:mounttabled)@ns-1.v23.grail.com:8101,/(v23.grail.com:internal:mounttabled)@ns-2.v23.grail.com:8101]
   local namespace root; can be repeated to provided multiple roots
 -v23.permissions.file=
   specify a perms file as <name>:<permsfile>
 -v23.permissions.literal=
   explicitly specify the runtime perms as a JSON-encoded access.Permissions.
   Overrides all --v23.permissions.file flags
 -v23.proxy=
   object name of proxy service to use to export services across network
   boundaries
 -v23.proxy.limit=0
   max number of proxies to connect to when the policy is to connect to all
   proxies; 0 implies all proxies
 -v23.proxy.policy=
   policy for choosing from a set of available proxy instances
 -v23.tcp.address=
   address to listen on
 -v23.tcp.protocol=
   protocol to listen with
 -v23.virtualized.advertise-private-addresses=
   if set the process will also advertise its private addresses
 -v23.virtualized.disallow-native-fallback=false
   if set, a failure to detect the requested virtualization provider will result
   in an error, otherwise, native mode is used
 -v23.virtualized.dns.public-name=
   if set the process will use the supplied dns name (and port) without
   resolution for its entry in the mounttable
 -v23.virtualized.docker=
   set if the process is running in a docker container and needs to configure
   itself differently therein
 -v23.virtualized.provider=
   the name of the virtualization/cloud provider hosting this process if the
   process needs to configure itself differently therein
 -v23.virtualized.tcp.public-address=
   if set the process will use this address (resolving via dns if appropriate)
   for its entry in the mounttable
 -v23.virtualized.tcp.public-protocol=
   if set the process will use this protocol for its entry in the mounttable
 -v23.vtrace.cache-size=1024
   The number of vtrace traces to store in memory
 -v23.vtrace.collect-regexp=
   Spans and annotations that match this regular expression will trigger trace
   collection
 -v23.vtrace.dump-on-shutdown=true
   If true, dump all stored traces on runtime shutdown
 -v23.vtrace.enable-aws-xray=false
   Enable the use of AWS x-ray integration with vtrace
 -v23.vtrace.root-span-name=
   Set the name of the root vtrace span created by the runtime at startup
 -v23.vtrace.sample-rate=0
   Rate (from 0.0 to 1.0) to sample vtrace traces
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

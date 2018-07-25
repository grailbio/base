// This file was auto-generated via go generate.
// DO NOT UPDATE MANUALLY

/*
Usage:
   role-group [flags] <command>

The role-group commands are:
   list        List all the role groups
   create      Create a new role group
   update      Update an existing role group
   help        Display help for commands or topics

The global flags are:
 -alsologtostderr=false
   log to standard error as well as files
 -block-profile=
   filename prefix for block profiles
 -block-profile-rate=1
   rate for runtime. SetBlockProfileRate
 -cpu-profile=
   filename for cpu profile
 -gops=false
   enable the gops listener
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

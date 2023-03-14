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
 -bless-remotes=true
   Whether to attempt to bless remotes with local blessings; only applies to
   Google blessings
 -bless-remotes-targets=ec2-name:ubuntu@adhoc.jjc.*
   Comma-separated list of targets to bless; targets may be
   "ssh:[user@]host[:port]" SSH destinations or
   "ec2-name:[user@]ec2-instance-name-filter" EC2 instance name filters; see
   https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Filtering.html
 -blesser=
   Flow specific blesser endpoint to use. Defaults to
   /ticket-server.eng.grail.com:8102/blesser/<flow>.
 -browser=true
   Attempt to open a browser.
 -ca-crt=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
   Path to ca.crt file.
 -dir=/mnt/home/jjc/.v23
   Where to store the Vanadium credentials. NOTE: the content will be erased if
   the credentials are regenerated.
 -do-not-refresh-duration=168h0m0s
   Do not refresh credentials if they are present and do not expire within this
   duration.
 -dump=false
   If credentials are present, dump them on the console instead of refreshing
   them.
 -ec2=false
   Use the role of the EC2 VM.
 -ec2-instance-identity-url=http://169.254.169.254/latest/dynamic/instance-identity/pkcs7
   URL for fetching instance identity document, for testing
 -expiry-caveat=
   Duration of expiry caveat added to blessings (for testing); empty means no
   caveat added
 -google-oauth2-url=https://accounts.google.com/o/oauth2
   URL for oauth2 API calls, for testing
 -internal-bless-remotes-mode=
   (INTERNAL) Controls the mode in which we run for the remote blessing
   protocol; one of {public-key,receive,send}
 -k8s=false
   Use the Kubernetes flow.
 -namespace=/var/run/secrets/kubernetes.io/serviceaccount/namespace
   Path to namespace file.
 -region=us-west-2
   AWS EKS region to use for k8s cluster token review.
 -token=/var/run/secrets/kubernetes.io/serviceaccount/token
   Path to token file.

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

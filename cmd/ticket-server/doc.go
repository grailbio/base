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
 -aws-account-ids=
   Commma-separated list of AWS account IDs used to populate allow-list of k8s
   clusters.
 -aws-regions=us-west-2
   Commma-separated list of AWS regions used to populate allow-list of k8s
   clusters.
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
 -dry-run=false
   Don't run, just check the configs.
 -ec2-blesser-role=
   What role to use for the blesser/ec2 endpoint. The role needs to exist in all
   the accounts.
 -ec2-dynamodb-table=
   DynamoDB table to use for enforcing the uniqueness of the EC2-based blessings
   requests.
 -ec2-expiration=8760h0m0s
   Expiration caveat for the EC2-based blessings.
 -google-admin=admin@grailbio.com
   Google Admin that can read all group memberships - NOTE: all groups will need
   to match the admin user's domain.
 -google-expiration=168h0m0s
   Expiration caveat for the Google-based blessings.
 -google-user-domain=grailbio.com
   Comma-separated list of email domains used for validating users.
 -k8s-blesser-role=ticket-server
   What role to use to lookup EKS cluster information on all authorized
   accounts. The role needs to exist in all the accounts.
 -k8s-expiration=8760h0m0s
   Expiration caveat for the K8s-based blessings.
 -name=
   Name to mount the server under. If empty, don't mount.
 -region=us-west-2
   AWS region to use for cached AWS session.
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

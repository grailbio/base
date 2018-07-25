// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

/*
Package remote implements sending (and receiving) of blessings to remote
machines over SSH.

The remote machine must be accessible by SSH and have a version of grail-access
in $PATH that supports remote blessing.

The protocol looks like this:

	+-------+                                                 +---------+
	| Local |                                                 | Remote  |
	+-------+                                                 +---------+
	    |                                                          |
	    | grail-access -bless-remotes                              |
	    |----------------------------                              |
	    |                           |                              |
	    |<---------------------------                              |
	    |                                                          |
	    | ssh dest grail-access -bless-remotes-mode=PublicKey      |
	    |--------------------------------------------------------->|
	    |                                                          |
	    |                            [remote principal public key] |
	    |<---------------------------------------------------------|
	    |                                                          |
	    | blessings <= bless remote principal public key           |
	    |-----------------------------------------------           |
	    |                                              |           |
	    |<----------------------------------------------           |
	    |                                                          |
	    | ssh dest grail-access -bless-remotes-mode=Receive        |
	    |--------------------------------------------------------->|
	    |                                                          |
	    | transmit blessings (on stdout)                           |
	    |--------------------------------------------------------->|
	    |                                                          |
	    |                                                          | set blessings
	    |                                                          |--------------
	    |                                                          |             |
	    |                                                          |<-------------
	    |                                                          |

Remote machines are specified by the -bless-remotes-targets flag which accepts
a comma-separated list of targets.  There are two types of targets: SSH
destinations and EC2 names, specified with "ssh:" and "ec2-name:" respectively.

SSH destination targets are destinations as ssh accepts, [user@]host[:port],
e.g.:
	ssh:10.1.0.120
	ssh:ubuntu@ec2-34-214-222-123.us-west-2.compute.amazonaws.com
	ssh:10.1.0.120:822

EC2 name targets use AWS EC2 instance names (i.e. the value of the Name tag),
[user@]instancename, e.g.:
	ec2-name:my-instance-name
	ec2-name:core@another-instance

EC2 names are treated as filters, so "ec2-name:core@my-*-name" will target all
instances matching "my-*-name" (and ssh them as user "core").  See
https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Filtering.html .

Example:
	grail-access -bless-remotes -bless-remotes-targets="ssh:me@mine.com,ec2-name:my-instance-*"

This invocation will target the SSH destination "me@mine.com" as well as all
EC2 instances whose Name tag matches "my-instance-*" (using the default ssh
username).

Note that we don't yet support custom ports for ec2-name targets, as ':' is a
valid character in names, and we are preferring to keep the parsing simple.
*/
package remote

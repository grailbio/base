// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package grail

// defaultProfile contains default configuration for use within GRAIL.
// TODO(marius): replace this with an account-based profile
const defaultProfile = `
// Use the ticket provider for AWS credentials by default.
// Our default region is us-west-2.
param aws/ticket (
	region = "us-west-2"
	path = "tickets/eng/dev/aws"
)

instance aws aws/ticket

// Bigmachine defaults for GRAIL (eng/dev).
// This should eventually be replaced by profile auto loading.
param bigmachine/ec2system (
	aws = aws
	instance-profile = "arn:aws:iam::619867110810:instance-profile/bigmachine"
	security-group = "sg-7390e50c"
)

param bigmachine/ec2tensorflow base = bigmachine/ec2system

`

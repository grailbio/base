// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/config"
)

func init() {
	config.Register("aws/env", func(constr *config.Constructor) {
		var config aws.Config
		config.Region = constr.String("region", "us-west-2", "the default AWS region for the session")
		constr.Doc = "configure an AWS session from the environment"
		constr.New = func() (interface{}, error) {
			return session.NewSession(&config)
		}
	})

	config.Default("aws", "aws/env")
}

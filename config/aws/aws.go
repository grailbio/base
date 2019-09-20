// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package aws

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/cloud/awssession"
	"github.com/grailbio/base/config"
	"github.com/grailbio/base/vcontext"
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

	config.Register("aws/ticket", func(constr *config.Constructor) {
		var (
			region = constr.String("region", "us-west-2", "the default AWS region for the session")
			path   = constr.String("path", "tickets/eng/dev/aws", "path to AWS ticket")
		)
		constr.Doc = "configure an AWS session from a GRAIL ticket server path"
		constr.New = func() (interface{}, error) {
			return session.NewSession(&aws.Config{
				Credentials: credentials.NewCredentials(&awssession.Provider{
					Ctx:        vcontext.Background(),
					Timeout:    10 * time.Second,
					TicketPath: *path,
				}),
				Region: region,
			})
		}
	})

	config.Default("aws", "aws/env")
}

// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package awsticket

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
}

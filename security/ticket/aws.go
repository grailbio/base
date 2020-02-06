// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ticket

import (
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/grailbio/base/cloud/ec2util"
	"github.com/grailbio/base/ttlcache"
	"v.io/x/lib/vlog"
)

type cacheKey struct {
	region  string
	role    string
	session string
}

// cacheTTL is how long the entries in cache will be considered valid.
const cacheTTL = time.Minute

var cache = ttlcache.New(cacheTTL)

func (b *AwsAssumeRoleBuilder) newAwsTicket(ctx *TicketContext) (TicketAwsTicket, error) {
	awsCredentials, err := b.genAwsCredentials(ctx)

	if err != nil {
		return TicketAwsTicket{}, err
	}

	return TicketAwsTicket{
		Value: AwsTicket{
			AwsCredentials: awsCredentials,
		},
	}, nil
}

func (b *AwsAssumeRoleBuilder) newS3Ticket(ctx *TicketContext) (TicketS3Ticket, error) {
	awsCredentials, err := b.genAwsCredentials(ctx)

	if err != nil {
		return TicketS3Ticket{}, err
	}

	return TicketS3Ticket{
		Value: S3Ticket{
			AwsCredentials: awsCredentials,
		},
	}, nil
}

func (b *AwsAssumeRoleBuilder) newEcrTicket(ctx *TicketContext) (TicketEcrTicket, error) {
	awsCredentials, err := b.genAwsCredentials(ctx)

	if err != nil {
		return TicketEcrTicket{}, err
	}

	return TicketEcrTicket{
		Value: newEcrTicket(awsCredentials),
	}, nil
}

func (b *AwsAssumeRoleBuilder) genAwsCredentials(ctx *TicketContext) (AwsCredentials, error) {
	vlog.Infof("AwsAssumeRoleBuilder: %+v", b)
	empty := AwsCredentials{}

	sessionName := strings.Replace(ctx.remoteBlessings.String(), ":", ",", -1)
	// AWS session names must be 64 characters or less
	if runes := []rune(sessionName); len(runes) > 64 {
		// Some risk with simple truncation - two large IAM role's would overlap
		// for example. This is mitigated by the format which includes instance id
		// as the last component. Ability to determine exactly which instance made
		// the call will be difficult, but likelihood of 2 instances sharing a prefix
		// is low.
		sessionName = string(runes[0:64])
	}
	key := cacheKey{b.Region, b.Role, sessionName}
	if v, ok := cache.Get(key); ok {
		vlog.VI(1).Infof("cache hit for %+v", key)
		return v.(AwsCredentials), nil
	}
	vlog.VI(1).Infof("cache miss for %+v", key)

	s := ctx.session
	if aws.StringValue(s.Config.Region) != b.Region {
		// This mismatch should be very rare.
		var err error
		s, err = session.NewSession(s.Config.WithRegion(b.Region))
		if err != nil {
			return empty, err
		}
	}

	client := sts.New(s)
	assumeRoleInput := &sts.AssumeRoleInput{
		RoleArn: aws.String(b.Role),
		// TODO(razvanm): the role session name is a string of characters consisting
		// of upper- and lower-case alphanumeric characters with no spaces that can
		// include '=,.@-'. Notably, a blessing can include ':' which is not allowed
		// in here.
		//
		// Reference: http://docs.aws.amazon.com/cli/latest/reference/sts/assume-role.html
		RoleSessionName: aws.String(sessionName),
		DurationSeconds: aws.Int64(int64(b.TtlSec)),
	}

	assumeRoleOutput, err := client.AssumeRole(assumeRoleInput)
	if err != nil {
		return empty, err
	}

	result := AwsCredentials{
		Region:          b.Region,
		AccessKeyId:     aws.StringValue(assumeRoleOutput.Credentials.AccessKeyId),
		SecretAccessKey: aws.StringValue(assumeRoleOutput.Credentials.SecretAccessKey),
		SessionToken:    aws.StringValue(assumeRoleOutput.Credentials.SessionToken),
		Expiration:      assumeRoleOutput.Credentials.Expiration.Format(time.RFC3339Nano),
	}

	vlog.VI(1).Infof("add to cache %+v", key)
	cache.Set(key, result)

	return result, nil
}

func (b *AwsSessionBuilder) newAwsTicket(ctx *TicketContext) (TicketAwsTicket, error) {
	awsCredentials, err := b.genAwsSession(ctx)

	if err != nil {
		return TicketAwsTicket{}, err
	}

	return TicketAwsTicket{
		Value: AwsTicket{
			AwsCredentials: awsCredentials,
		},
	}, nil
}

func (b *AwsSessionBuilder) newS3Ticket(ctx *TicketContext) (TicketS3Ticket, error) {
	awsCredentials, err := b.genAwsSession(ctx)

	if err != nil {
		return TicketS3Ticket{}, err
	}

	return TicketS3Ticket{
		Value: S3Ticket{
			AwsCredentials: awsCredentials,
		},
	}, nil
}

func (b *AwsSessionBuilder) genAwsSession(ctx *TicketContext) (AwsCredentials, error) {
	vlog.Infof("AwsSessionBuilder: %s", b.AwsCredentials.AccessKeyId)
	empty := AwsCredentials{}
	awsCredentials := b.AwsCredentials

	sessionName := strings.Replace(ctx.remoteBlessings.String(), ":", ",", -1)
	// AWS session names must be 64 characters or less
	if runes := []rune(sessionName); len(runes) > 64 {
		// Some risk with simple truncation - two large IAM role's would overlap
		// for example. This is mitigated by the format which includes instance id
		// as the last component. Ability to determine exactly which instance made
		// the call will be difficult, but likelihood of 2 instances sharing a prefix
		// is low.
		sessionName = string(runes[0:64])
	}
	key := cacheKey{awsCredentials.Region, awsCredentials.AccessKeyId, sessionName}
	if v, ok := cache.Get(key); ok {
		vlog.VI(1).Infof("cache hit for %+v", key)
		return v.(AwsCredentials), nil
	}
	vlog.VI(1).Infof("cache miss for %+v", key)
	s, err := session.NewSession(&aws.Config{
		Region: aws.String(awsCredentials.Region),
		Credentials: credentials.NewStaticCredentials(
			awsCredentials.AccessKeyId,
			awsCredentials.SecretAccessKey,
			awsCredentials.SessionToken),
	})
	if err != nil {
		return empty, err
	}

	sessionTokenInput := &sts.GetSessionTokenInput{
		DurationSeconds: aws.Int64(int64(b.TtlSec)),
	}

	client := sts.New(s)
	sessionTokenOutput, err := client.GetSessionToken(sessionTokenInput)
	if err != nil {
		return empty, err
	}

	result := AwsCredentials{
		Region:          awsCredentials.Region,
		AccessKeyId:     aws.StringValue(sessionTokenOutput.Credentials.AccessKeyId),
		SecretAccessKey: aws.StringValue(sessionTokenOutput.Credentials.SecretAccessKey),
		SessionToken:    aws.StringValue(sessionTokenOutput.Credentials.SessionToken),
		Expiration:      sessionTokenOutput.Credentials.Expiration.Format(time.RFC3339Nano),
	}

	vlog.VI(1).Infof("add to cache %+v", key)
	cache.Set(key, result)

	return result, nil
}

func newEcrTicket(awsCredentials AwsCredentials) EcrTicket {
	empty := EcrTicket{}
	s, err := session.NewSession(&aws.Config{
		Region: aws.String(awsCredentials.Region),
		Credentials: credentials.NewStaticCredentials(
			awsCredentials.AccessKeyId,
			awsCredentials.SecretAccessKey,
			awsCredentials.SessionToken),
	})
	if err != nil {
		vlog.Error(err)
		return empty
	}
	r, err := ecr.New(s).GetAuthorizationToken(&ecr.GetAuthorizationTokenInput{})
	if err != nil {
		vlog.Error(err)
		return empty
	}
	if len(r.AuthorizationData) == 0 {
		vlog.Errorf("no authorization data from ECR")
		return empty
	}
	auth := r.AuthorizationData[0]
	if auth.AuthorizationToken == nil || auth.ProxyEndpoint == nil || auth.ExpiresAt == nil {
		vlog.Errorf("bad authorization data from ECR")
		return empty
	}
	return EcrTicket{
		AuthorizationToken: *auth.AuthorizationToken,
		Expiration:         aws.TimeValue(auth.ExpiresAt).Format(time.RFC3339Nano),
		Endpoint:           *auth.ProxyEndpoint,
	}
}

// Returns a list of Compute Instances that match the filter
func AwsEc2InstanceLookup(ctx *TicketContext, builder *AwsComputeInstancesBuilder) ([]ComputeInstance, error) {
	var instances []ComputeInstance

	if len(builder.InstanceFilters) == 0 {
		return instances, errors.New("An instance filters is required")
	}

	// Create the STS session with the provided lookup role
	config := aws.Config{
		Region:      aws.String(builder.Region),
		Credentials: stscreds.NewCredentials(ctx.session, builder.AwsAccountLookupRole),
		Retryer: client.DefaultRetryer{
			NumMaxRetries: 100,
		},
	}

	s, err := session.NewSession(&config)
	if err != nil {
		vlog.Infof("error: %+v", err)
		return instances, err
	}

	var filters []*ec2.Filter
	filters = append(filters,
		&ec2.Filter{
			Name: aws.String("instance-state-name"),
			Values: []*string{
				aws.String("running"),
			},
		},
	)

	for _, f := range builder.InstanceFilters {
		filters = append(filters,
			&ec2.Filter{
				Name: aws.String(f.Key),
				Values: []*string{
					aws.String(f.Value),
				},
			},
		)
	}

	output, err := ec2.New(s, &config).DescribeInstances(&ec2.DescribeInstancesInput{
		Filters: filters,
	})
	if err != nil {
		vlog.Error(err)
		return instances, err
	}

	for _, reservations := range output.Reservations {
		for _, instance := range reservations.Instances {
			var params []Parameter
			publicIp, err := ec2util.GetPublicIPAddress(instance)
			if err != nil {
				vlog.Error(err)
				continue // parse error skip
			}

			privateIp, err := ec2util.GetPrivateIPAddress(instance)
			if err != nil {
				vlog.Error(err)
				continue // parse error skip
			}

			ec2Tags, err := ec2util.GetTags(instance)
			if err != nil {
				vlog.Error(err)
				continue // parse error skip
			}
			for _, tag := range ec2Tags {
				params = append(params,
					Parameter{
						Key:   *tag.Key,
						Value: *tag.Value,
					})
			}

			instanceId, err := ec2util.GetInstanceId(instance)
			if err != nil {
				vlog.Error(err)
				continue // parse error skip
			}

			instances = append(instances,
				ComputeInstance{
					PublicIp:   publicIp,
					PrivateIp:  privateIp,
					InstanceId: instanceId,
					Tags:       params,
				})
		}
	}

	vlog.Infof("instances %+v", instances)
	return instances, nil
}

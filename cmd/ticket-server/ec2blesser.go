// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/cloud/ec2util"
	"v.io/v23/context"
	"v.io/v23/rpc"
	"v.io/v23/security"
	"v.io/x/lib/vlog"
)

const pendingTimeWindow = time.Hour

// setupEc2Blesser creates the DynamoDB table used for enforcing the uniqueness
// of the EC2-based blessing requests. For each VM we only want to handle
// blessings only to the first request. This prevents replay attacks in the case
// when the EC2 instance document was leaked to an adversary.
//
// The schema of the table is the following:
//
//   ID: (string, hash key) '/'-separated of (account, region, instance, IP)
//   IdentityDocument: (string) JSON of the IdentityDocument from the request
//   DescribeInstance: (string) JSON response for the DescribeInstance call
//   Timestamp: (string) Timestamp in RFC3339Nano when the record was created
func setupEc2Blesser(s *session.Session, table string) {
	if table == "" {
		return
	}

	client := dynamodb.New(s)
	out, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})

	if err == nil {
		vlog.Infof("DynamoDB table already exists:\n%+v", out)
		return
	}

	want := dynamodb.ErrCodeResourceNotFoundException
	if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != want {
		vlog.Fatalf("unexpected error: got %+v, want %+v", err, want)
	}

	_, err = client.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(1)),
			WriteCapacityUnits: aws.Int64(int64(1)),
		},
	})
	if err != nil {
		vlog.Fatal(err)
	}
	vlog.Infof("%q DynamoDB table was created", table)
	// TODO(razvanm): wait for the table to reach ACTIVE state?
	// TODO(razvanm): enable the auto scaling?
}

type ec2Blesser struct {
	expirationInterval time.Duration
	role               string
	table              string
	session            *session.Session
}

func newEc2Blesser(ctx *context.T, s *session.Session, expiration time.Duration, role string, table string) *ec2Blesser {
	setupEc2Blesser(s, ec2DynamoDBTableFlag)
	return &ec2Blesser{
		expirationInterval: expiration,
		role:               role,
		table:              table,
		session:            s,
	}
}

func (blesser *ec2Blesser) checkUniqueness(doc *ec2util.IdentityDocument, remoteAddr string, jsonDoc string, jsonInstance string) error {
	if blesser.table == "" {
		return nil
	}
	ipAddr, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return err
	}
	key := strings.Join([]string{doc.AccountID, doc.Region, doc.InstanceID, ipAddr}, "/")
	vlog.Infof("DynamoDB key(%s): %q", remoteAddr, key)
	cond := aws.String("attribute_not_exists(ID)")
	if ec2DisableUniquenessCheckFlag {
		cond = nil
	}
	_, err = dynamodb.New(blesser.session).PutItem(&dynamodb.PutItemInput{
		TableName:           aws.String(blesser.table),
		ConditionExpression: cond,
		Item: map[string]*dynamodb.AttributeValue{
			"ID":               {S: aws.String(key)},
			"IdentityDocument": {S: aws.String(jsonDoc)},
			"DescribeInstance": {S: aws.String(jsonInstance)},
			"Timestamp":        {S: aws.String(time.Now().UTC().Format(time.RFC3339Nano))},
		},
	})
	return err
}

func checkPendingTime(doc *ec2util.IdentityDocument) error {
	pendingTime := doc.PendingTime
	if time.Since(doc.PendingTime) > pendingTimeWindow {
		return fmt.Errorf("launch time is too old: %s should be within %s", pendingTime, pendingTimeWindow)
	}
	return nil
}

func (blesser *ec2Blesser) BlessEc2(ctx *context.T, call rpc.ServerCall, pkcs7b64 string) (security.Blessings, error) {
	var empty security.Blessings

	remoteAddress := call.RemoteAddr().String()
	vlog.Infof("remote endpoint: %+v", call.RemoteEndpoint().Addr())
	vlog.Infof("pkcs7(%s): %d bytes", remoteAddress, len(pkcs7b64))
	doc, jsonDoc, err := ec2util.ParseAndVerifyIdentityDocument(pkcs7b64)
	vlog.Infof("doc(%s): %+v", remoteAddress, doc)
	if err != nil {
		vlog.Infof("error(%s): %+v", remoteAddress, err)
		return empty, err
	}

	if !ec2DisablePendingTimeCheckFlag {
		if err := checkPendingTime(doc); err != nil {
			vlog.Infof("error(%s): %+v", remoteAddress, err)
			return empty, err
		}
	}

	config := aws.Config{
		Credentials: stscreds.NewCredentials(blesser.session, fmt.Sprintf("arn:aws:iam::%s:role/%s", doc.AccountID, blesser.role)),
		Retryer: client.DefaultRetryer{
			NumMaxRetries: 100,
		},
	}
	validateRemoteAddr := remoteAddress
	if ec2DisableAddrCheckFlag {
		validateRemoteAddr = ""
	}

	output, err := ec2.New(blesser.session, &config).DescribeInstances(&ec2.DescribeInstancesInput{
		InstanceIds: []*string{aws.String(doc.InstanceID)},
	})

	if err != nil {
		vlog.Infof("error(%s): %+v", remoteAddress, err)
		return empty, err
	}

	role, err := ec2util.ValidateInstance(output, *doc, validateRemoteAddr)
	if err != nil {
		vlog.Infof("error(%s): %+v", remoteAddress, err)
		return empty, err
	}

	if err := blesser.checkUniqueness(doc, remoteAddress, jsonDoc, output.String()); err != nil {
		vlog.Infof("error(%s): %+v", remoteAddress, err)
		return empty, err
	}

	ext := fmt.Sprintf("ec2:%s:%s:%s", doc.AccountID, role, doc.InstanceID)

	securityCall := call.Security()
	if securityCall.LocalPrincipal() == nil {
		return empty, fmt.Errorf("server misconfiguration: no authentication happened")
	}

	pubKey := securityCall.RemoteBlessings().PublicKey()
	caveat, err := security.NewExpiryCaveat(time.Now().Add(blesser.expirationInterval))
	// TODO(razvanm): using a PublicKeyThirdPartyCaveat we could also invalidate
	// the older blessings. This will force the clients to talk to the
	// ticket-server more frequently though.
	if err != nil {
		return empty, err
	}
	return securityCall.LocalPrincipal().Bless(pubKey, securityCall.LocalBlessings(), ext, caveat)
}

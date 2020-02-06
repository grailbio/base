// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ec2util_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/cloud/ec2util"
)

func TestGetInstance(t *testing.T) {
	cases := []struct {
		output *ec2.DescribeInstancesOutput

		arn       string
		errPrefix string
	}{
		{&ec2.DescribeInstancesOutput{}, "", "unexpected number of Reservations"},
		{&ec2.DescribeInstancesOutput{
			Reservations: []*ec2.Reservation{
				&ec2.Reservation{},
			},
		}, "", "unexpected number of Instances"},
		{&ec2.DescribeInstancesOutput{
			Reservations: []*ec2.Reservation{
				&ec2.Reservation{},
				&ec2.Reservation{},
			},
		}, "", "unexpected number of Reservations"},
		{&ec2.DescribeInstancesOutput{
			Reservations: []*ec2.Reservation{
				&ec2.Reservation{
					Instances: []*ec2.Instance{
						&ec2.Instance{},
						&ec2.Instance{},
					},
				},
			},
		}, "", "unexpected number of Instances"},
		{&ec2.DescribeInstancesOutput{
			Reservations: []*ec2.Reservation{
				&ec2.Reservation{
					Instances: []*ec2.Instance{
						&ec2.Instance{},
					},
				},
			},
		}, "", "non-nil IamInstanceProfile"},
	}

	for _, c := range cases {
		_, err := ec2util.GetInstance(c.output)
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("GetInstance: got %q, want %q", err, c.errPrefix)
		}
	}
}

func TestGetARN(t *testing.T) {
	cases := []struct {
		output *ec2.Instance

		arn       string
		errPrefix string
	}{
		{
			&ec2.Instance{
				IamInstanceProfile: &ec2.IamInstanceProfile{},
			}, "", "non-nil Arn"},
		{newInstancesOutput("", "", ""), "", "non-empty Arn"},
		{newInstancesOutput("", "dummy", ""), "dummy", ""},
	}

	for _, c := range cases {
		arn, err := ec2util.GetIamInstanceProfileARN(c.output)
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("GetIamInstanceProfileARN: got %q, want %q", err, c.errPrefix)
		}
		if arn != c.arn {
			t.Errorf("GetIamInstanceProfileARN: got %q, want %q", arn, c.arn)
		}
	}
}

func TestGetInstanceId(t *testing.T) {
	cases := []struct {
		output *ec2.Instance

		instanceId string
		errPrefix  string
	}{
		{nil, "", "non-nil"},
		{newInstancesOutput("i-1234", "", ""),
			"i-1234", ""},
	}

	for _, c := range cases {
		instanceId, err := ec2util.GetInstanceId(c.output)
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("GetInstanceId: got %q, want %q", err, c.errPrefix)
		}
		if instanceId != c.instanceId {
			t.Errorf("GetInstanceId: got %q, want %q", instanceId, c.instanceId)
		}
	}
}

func TestGetPublicIPAddress(t *testing.T) {
	cases := []struct {
		output *ec2.Instance

		publicIp  string
		errPrefix string
	}{
		{nil, "", "non-nil"},
		{newInstancesOutput("", "", "192.168.1.1"),
			"192.168.1.1", ""},
	}

	for _, c := range cases {
		publicIp, err := ec2util.GetPublicIPAddress(c.output)
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("GetPublicIPAddress: got %q, want %q", err, c.errPrefix)
		}
		if publicIp != c.publicIp {
			t.Errorf("GetPublicIPAddress: got %q, want %q", publicIp, c.publicIp)
		}
	}
}

// TODO(aeiser) Implement test checking for tags
func TestGetTags(t *testing.T) {
	cases := []struct {
		output *ec2.Instance

		tags      string
		errPrefix string
	}{
		{nil, "", "non-nil"},
		{&ec2.Instance{
			IamInstanceProfile: &ec2.IamInstanceProfile{},
		}, "", "non-nil Arn"},
	}

	for _, c := range cases {
		_, err := ec2util.GetTags(c.output)
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("GetTags: got %q, want %q", err, c.errPrefix)
		}
		//		if tags != c.tags {
		//			t.Errorf("GetTags: got %q, want %q", tags, c.tags)
		//		}
	}
}

func TestValidateInstance(t *testing.T) {
	cases := []struct {
		describeInstances *ec2.DescribeInstancesOutput
		doc               ec2util.IdentityDocument
		remoteAddr        string

		role      string
		errPrefix string
	}{
		{newDescribeInstancesOutput("dummy", "x.x.x.x"), ec2util.IdentityDocument{}, "34.215.119.108:1111", "", "mismatch"},
		{newDescribeInstancesOutput("dummy", "34.215.119.108"), ec2util.IdentityDocument{}, "34.215.119.108:", "", "unexpected ARN"},
		{
			newDescribeInstancesOutput("arn:aws:iam::123456789012:instance-profile/dummyRole", "34.215.119.108"),
			ec2util.IdentityDocument{AccountID: "xx"},
			"34.215.119.108:",
			"",
			"mismatch between account ID",
		},
		{
			newDescribeInstancesOutput("arn:aws:iam::123456789012:instance-profile/dummyRole", "34.215.119.108"),
			ec2util.IdentityDocument{AccountID: "123456789012"},
			"34.215.119.108:",
			"dummyRole",
			"",
		},
		//Instance that does not have a public IP
		{
			newDescribeInstancesOutput("arn:aws:iam::987654321012:instance-profile/dummyRole", ""),
			ec2util.IdentityDocument{AccountID: "987654321012"},
			"52.215.119.108:",
			"dummyRole",
			"",
		},
	}

	for _, c := range cases {
		role, err := ec2util.ValidateInstance(c.describeInstances, c.doc, c.remoteAddr)
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("ValidateInstance: got %q, want %q", err, c.errPrefix)
		}
		if role != c.role {
			t.Errorf("GetIamInstanceProfileARN: got %q, want %q", role, c.role)
		}
	}
}

func TestParseIdentityDocument(t *testing.T) {
	pkcs7 := `MIAGCSqGSIb3DQEHAqCAMIACAQExCzAJBgUrDgMCGgUAMIAGCSqGSIb3DQEHAaCAJIAEggGuewog
ICJwcml2YXRlSXAiIDogIjE3Mi4zMS40MS43MyIsCiAgImRldnBheVByb2R1Y3RDb2RlcyIgOiBu
dWxsLAogICJhdmFpbGFiaWxpdHlab25lIiA6ICJ1cy13ZXN0LTJhIiwKICAiYWNjb3VudElkIiA6
ICIwMTA1ODE4MjM4MDgiLAogICJ2ZXJzaW9uIiA6ICIyMDEwLTA4LTMxIiwKICAiaW5zdGFuY2VJ
ZCIgOiAiaS0wMjY5YTc4YzgxNzA4MDg4YSIsCiAgImJpbGxpbmdQcm9kdWN0cyIgOiBudWxsLAog
ICJpbnN0YW5jZVR5cGUiIDogInQyLm5hbm8iLAogICJwZW5kaW5nVGltZSIgOiAiMjAxNi0wOS0w
OVQyMjoyNDo0MFoiLAogICJpbWFnZUlkIiA6ICJhbWktMDZhZjdmNjYiLAogICJhcmNoaXRlY3R1
cmUiIDogIng4Nl82NCIsCiAgImtlcm5lbElkIiA6IG51bGwsCiAgInJhbWRpc2tJZCIgOiBudWxs
LAogICJyZWdpb24iIDogInVzLXdlc3QtMiIKfQAAAAAAADGCARcwggETAgEBMGkwXDELMAkGA1UE
BhMCVVMxGTAXBgNVBAgTEFdhc2hpbmd0b24gU3RhdGUxEDAOBgNVBAcTB1NlYXR0bGUxIDAeBgNV
BAoTF0FtYXpvbiBXZWIgU2VydmljZXMgTExDAgkAlrpI2eVeGmcwCQYFKw4DAhoFAKBdMBgGCSqG
SIb3DQEJAzELBgkqhkiG9w0BBwEwHAYJKoZIhvcNAQkFMQ8XDTE2MDkwOTIyMjQ0NFowIwYJKoZI
hvcNAQkEMRYEFMdcnDOfpT6XUhoRWNCDVMUpEpmvMAkGByqGSM44BAMELjAsAhR6HQzcZybHbbZ5
JIrbrql6Il0jMwIUaeuNd15Shx/G9mpfXL1XIADV+IgAAAAAAAA=`
	want := ec2util.IdentityDocument{
		InstanceID:  "i-0269a78c81708088a",
		AccountID:   "010581823808",
		Region:      "us-west-2",
		PendingTime: time.Date(2016, 9, 9, 22, 24, 40, 0, time.UTC),
	}

	cases := []struct {
		pkcs7 string

		want      ec2util.IdentityDocument
		errPrefix string
	}{
		{
			pkcs7,
			ec2util.IdentityDocument{
				InstanceID:  "i-0269a78c81708088a",
				AccountID:   "010581823808",
				Region:      "us-west-2",
				PendingTime: time.Date(2016, 9, 9, 22, 24, 40, 0, time.UTC),
			},
			""},
		{pkcs7[1:], ec2util.IdentityDocument{}, "failed to decode"},
		{`MIAGCSqGSIb3DQEHAqCAMIACAQExCzAJBgUrDgMCGgUAMIAGCSqGSIb3DQEHAaCAJIAEggHPewog
ICJhdmFpbGFiaWxpdHlab25lIiA6ICJ1cy13ZXN0LTJhIiwKICAiZGV2cGF5UHJvZHVjdENvZGVz
IiA6IG51bGwsCiAgIm1hcmtldHBsYWNlUHJvZHVjdENvZGVzIiA6IG51bGwsCiAgInZlcnNpb24i
IDogIjIwMTctMDktMzAiLAogICJwZW5kaW5nVGltZSIgOiAiMjAxNy0xMi0wNVQwMjoxMDo0Mloi
LAogICJpbnN0YW5jZUlkIiA6ICJpLTA2YWY5YmIyYTNhMjg2MWNkIiwKICAiYmlsbGluZ1Byb2R1
Y3RzIiA6IG51bGwsCiAgImluc3RhbmNlVHlwZSIgOiAidDIubmFubyIsCiAgInByaXZhdGVJcCIg
OiAiMTAuMC43LjE2IiwKICAiaW1hZ2VJZCIgOiAiYW1pLTFmNjNiYTY3IiwKICAiYWNjb3VudElk
IiA6ICI2MTk4NjcxMTA4MTAiLAogICJhcmNoaXRlY3R1cmUiIDogIng4Nl82NCIsCiAgImtlcm5l
bElkIiA6IG51bGwsCiAgInJhbWRpc2tJZCIgOiBudWxsLAogICJyZWdpb24iIDogInVzLXdlc3Qt
MiIKfQAAAAAAADGCARgwggEUAgEBMGkwXDELMAkGA1UEBhMCVVMxGTAXBgNVBAgTEFdhc2hpbmd0
b24gU3RhdGUxEDAOBgNVBAcTB1NlYXR0bGUxIDAeBgNVBAoTF0FtYXpvbiBXZWIgU2VydmljZXMg
TExDAgkAlrpI2eVeGmcwCQYFKw4DAhoFAKBdMBgGCSqGSIb3DQEJAzELBgkqhkiG9w0BBwEwHAYJ
KoZIhvcNAQkFMQ8XDTE3MTIwNTAyMTA0N1owIwYJKoZIhvcNAQkEMRYEFM7lf3kDblbNv0FTTlbH
cxXtq51HMAkGByqGSM44BAMELzAtAhUAhh/F7KIV+NmGGuJ3B2GEAAA50NkCFD/VElA2Qe11PS6d
N9KbKK34hcCtAAAAAAAA`,
			ec2util.IdentityDocument{
				InstanceID:  "i-06af9bb2a3a2861cd",
				AccountID:   "619867110810",
				Region:      "us-west-2",
				PendingTime: time.Date(2017, 12, 5, 2, 10, 42, 0, time.UTC),
			},
			"",
		},
	}

	for i, c := range cases {
		doc, _, err := ec2util.ParseAndVerifyIdentityDocument(pkcs7)
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("ParseIdentityDocument %d: got %q, want %q", i, err, c.errPrefix)
			continue
		}
		if !reflect.DeepEqual(want, *doc) {
			t.Fatalf("ParseIdentityDocument: got %+v, want %+v", *doc, want)
		}
	}
}

func newDescribeInstancesOutput(arn string, publicIP string) *ec2.DescribeInstancesOutput {
	return &ec2.DescribeInstancesOutput{
		Reservations: []*ec2.Reservation{
			&ec2.Reservation{
				Instances: []*ec2.Instance{
					newInstancesOutput("", arn, publicIP),
				},
			},
		},
	}
}

func newInstancesOutput(instanceId string, arn string, publicIP string) *ec2.Instance {
	return &ec2.Instance{
		InstanceId: &instanceId,
		IamInstanceProfile: &ec2.IamInstanceProfile{
			Arn: aws.String(arn),
		},
		PublicIpAddress: aws.String(publicIP),
	}
}

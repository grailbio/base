// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package ec2util contains a few helper functions related to EC2 (validating
// an Instance Identity Document, extracting a Amazon Resource Name, etc).
//
// Some of the code from this file comes from a Hashicorp Vault
// (covered by Mozilla Public License, version 2.0) file:
// https://github.com/hashicorp/vault/blob/2500218a9cbd833057145aefec1802e6dd5ec8cc/builtin/credential/aws-ec2/path_config_certificate.go

package ec2util

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/fullsailor/pkcs7"
	"v.io/x/lib/vlog"
)

type IdentityDocument struct {
	InstanceID  string    `json:"instanceId,omitempty"`
	AccountID   string    `json:"accountId,omitempty"`
	Region      string    `json:"region,omitempty"`
	PendingTime time.Time `json:"pendingTime,omitempty"`
}

var (
	// TODO(razvanm): replace this with a proper parsing of ARNs.
	// Potential source of inspiration: https://github.com/gigawattio/awsarn/blob/master/awsarn.go.
	roleRE                = regexp.MustCompile("^arn:aws:iam::([0-9]*):instance-profile/(.*)$")
	awsPublicCertificates []*x509.Certificate
)

func init() {
	cert, err := DecodePEMAndParseCertificate(awsPublicCertificatePEM)
	if err != nil {
		panic(err)
	}
	awsPublicCertificates = []*x509.Certificate{cert}
}

func GetInstance(output *ec2.DescribeInstancesOutput) (*ec2.Instance, error) {
	if len(output.Reservations) != 1 {
		return nil, fmt.Errorf("unexpected number of Reservations (want 1): %+v", output)
	}

	reservation := output.Reservations[0]
	if len(reservation.Instances) != 1 {
		return nil, fmt.Errorf("unexpected number of Instances (want 1): %+v", output)
	}

	instance := reservation.Instances[0]
	if instance.IamInstanceProfile == nil {
		return nil, fmt.Errorf("non-nil IamInstanceProfile is required: %+v", output)
	}

	return instance, nil
}

// GetIamInstanceProfileARN extracts the ARN from the `instance` output of a call to
// DescribeInstances. The ARN is expected to be non-empty.
func GetIamInstanceProfileARN(instance *ec2.Instance) (string, error) {
	if instance == nil {
		return "", fmt.Errorf("non-nil instance is required: %+v", instance)
	}

	if instance.IamInstanceProfile == nil {
		return "", fmt.Errorf("non-nil IamInstanceProfile is required: %+v", instance)
	}

	profile := instance.IamInstanceProfile
	if profile.Arn == nil {
		return "", fmt.Errorf("non-nil Arn is required: %+v", instance)
	}

	if len(*profile.Arn) == 0 {
		return "", fmt.Errorf("non-empty Arn is required: %+v", instance)
	}

	return *profile.Arn, nil
}

// GetPublicIPAddress extracts the public IP address from the output of a call
// to DescribeInstances Instance. The response is expected to be non-empty if the
// instance has a public IP and empty ("") if the instance is private.
func GetPublicIPAddress(instance *ec2.Instance) (string, error) {
	if instance == nil {
		return "", fmt.Errorf("non-nil instance is required: %+v", instance)
	}

	if instance.PublicIpAddress == nil || len(*instance.PublicIpAddress) == 0 {
		return "", nil
	}

	return *instance.PublicIpAddress, nil
}

// GetPrivateIPAddress extracts the private IP address from the output of a call
// to DescribeInstances Instance. The response is expected to be the first private IP
// attached to the instance.
// If the instances no attached interfaces, the value is empty ("")
func GetPrivateIPAddress(instance *ec2.Instance) (string, error) {
	if instance == nil {
		return "", fmt.Errorf("non-nil instance is required: %+v", instance)
	}

	if instance.PrivateIpAddress == nil || len(*instance.PrivateIpAddress) == 0 {
		return "", nil
	}

	return *instance.PrivateIpAddress, nil
}

// GetTags returns a map of Key/Value pairs representing the tags
func GetTags(instance *ec2.Instance) ([]*ec2.Tag, error) {
	if instance == nil {
		return nil, fmt.Errorf("non-nil instance is required: %+v", instance)
	}

	if instance.Tags == nil || len(instance.Tags) == 0 {
		return nil, nil
	}

	return instance.Tags, nil
}

// GetInstanceId returns the instanceID from the output of a call
// to DescribeInstances Instance.
func GetInstanceId(instance *ec2.Instance) (string, error) {
	if instance == nil {
		return "", fmt.Errorf("non-nil instance is required: %+v", instance)
	}

	if instance.InstanceId == nil || len(*instance.InstanceId) == 0 {
		return "", nil
	}

	return *instance.InstanceId, nil
}

// ValidateInstance checks if an EC2 instance exists and it has the expected
// IP. It returns the name of the instance profile (the IAM role).
//
// Note that this validation will not work for NATed VMs.
func ValidateInstance(output *ec2.DescribeInstancesOutput, doc IdentityDocument, remoteAddr string) (role string, err error) {
	vlog.Infof("reservations:\n%+v", output.Reservations)

	instance, err := GetInstance(output)
	if err != nil {
		return "", err
	}

	publicIP, err := GetPublicIPAddress(instance)
	if err != nil {
		return "", err
	}

	// Instances that do not have a public IP should be able to authenticate
	// with ticket server. Connections from such instances are routed through a
	// NAT gateway with an Elastic IP. The following check which ensures the
	// remoteAddr from which the connection originates is same as the public IP
	// of the instance is skipped for private instances.
	if remoteAddr != "" && publicIP != "" {
		if !strings.HasPrefix(remoteAddr, publicIP+":") {
			return "", fmt.Errorf("mismatch between the real peer address (%s) and public IP of the instance (%s)", remoteAddr, publicIP)
		}
	}

	arn, err := GetIamInstanceProfileARN(instance)
	if err != nil {
		return "", err
	}
	m := roleRE.FindStringSubmatch(arn)
	if len(m) != 3 {
		return "", fmt.Errorf("unexpected ARN format for %q", arn)
	}
	vlog.Infof("IAM role: %q parsed: %q", arn, m)

	accountID, role := m[1], m[2]

	if accountID != doc.AccountID {
		return "", fmt.Errorf("mismatch between account ID in Identity Doc (%q) and role (%q): %q", doc.AccountID, accountID, arn)
	}
	return role, nil
}

// ParseAndVerifyIdentityDocument parses and checks and identity document in
// PKCS#7 format. Only some relevant fields are returned.
func ParseAndVerifyIdentityDocument(pkcs7b64 string) (*IdentityDocument, string, error) {
	// Insert the header and footer for the signature to be able to pem decode it.
	s := fmt.Sprintf("-----BEGIN PKCS7-----\n%s\n-----END PKCS7-----", pkcs7b64)

	// Decode the PEM encoded signature.
	pkcs7BER, pkcs7Rest := pem.Decode([]byte(s))
	if len(pkcs7Rest) != 0 {
		return nil, "", fmt.Errorf("failed to decode the PKCS#7 signature")
	}

	// Parse the signature from asn1 format into a struct.
	pkcs7Data, err := pkcs7.Parse(pkcs7BER.Bytes)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse the BER encoded PKCS#7 signature: %s\n", err)
	}

	pkcs7Data.Certificates = awsPublicCertificates

	// Verify extracts the authenticated attributes in the PKCS#7
	// signature, and verifies the authenticity of the content using
	// 'dsa.PublicKey' embedded in the public certificate.
	if err := pkcs7Data.Verify(); err != nil {
		return nil, "", fmt.Errorf("failed to verify the signature: %v", err)
	}

	// Check if the signature has content inside of it.
	if len(pkcs7Data.Content) == 0 {
		return nil, "", fmt.Errorf("instance identity document could not be found in the signature")
	}

	var identityDoc IdentityDocument
	content := string(pkcs7Data.Content)
	vlog.VI(1).Infof("%v", content)
	decoder := json.NewDecoder(bytes.NewReader(pkcs7Data.Content))
	decoder.UseNumber()
	if err := decoder.Decode(&identityDoc); err != nil {
		return nil, "", err
	}

	return &identityDoc, content, nil
}

// DecodePEMAndParseCertificate decodes the PEM encoded certificate and
// parses it into a x509 cert.
func DecodePEMAndParseCertificate(certificate string) (*x509.Certificate, error) {
	// Decode the PEM block and error out if a block is not detected in
	// the first attempt.
	decodedPublicCert, rest := pem.Decode([]byte(certificate))
	if len(rest) != 0 {
		return nil, fmt.Errorf("invalid certificate; should be one PEM block only")
	}

	// Check if the certificate can be parsed.
	publicCert, err := x509.ParseCertificate(decodedPublicCert.Bytes)
	if err != nil {
		return nil, err
	}
	if publicCert == nil {
		return nil, fmt.Errorf("invalid certificate; failed to parse certificate")
	}
	return publicCert, nil
}

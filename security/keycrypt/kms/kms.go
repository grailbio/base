// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package kms implements a Keycrypt using AWS's KMS service and S3.
// Secrets are stored using the AWS-provided s3crypto package, which
// uses a KMS data key to perform client-side encryption and
// decryption of keys.
//
// For each key stored, s3crypto retrieves a data encryption key
// which is derived from a master key stored securely in KMS's HSMs.
// KMS returns both an encrypted and a plaintext version of the data
// encryption key. The key is subsequently used to encrypt the
// keybundle and is then thrown away. The encrypted version of the
// key is stored together with the bundle.
//
// Access to Amazon's KMS is controlled by IAM security policies.
//
// When a bundle is retrieved, s3crypto asks KMS to decrypt the key
// that is stored with the bundle, which in turn is used to decrypt
// the bundle contents.
package kms

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3crypto"
	"github.com/grailbio/base/security/keycrypt"
)

const (
	// The prefix used for S3 bucket keys. This is defined so that
	// we can support future versions which may make use of
	// different representations and layouts.
	prefix = "v1/"
)

// CredentialsChainVerboseErrors is used to set
// aws.Config.CredentialsChainVerboseErrors when creating a kms session.
var CredentialsChainVerboseErrors = false

// DefaultRegion is used to set the the AWS region for looking up KMS keys.
var DefaultRegion = "us-west-2"

func init() {
	keycrypt.RegisterFunc("kms", func(h string) keycrypt.Keycrypt {
		sess := session.New(&aws.Config{
			Region: &DefaultRegion,
			CredentialsChainVerboseErrors: &CredentialsChainVerboseErrors,
		})
		return New(sess, h)
	})
}

var _ keycrypt.Keycrypt = (*Crypt)(nil)

// Crypt implements a Keycrypt using Amazon's KMS and S3 services.
type Crypt struct {
	sess    *session.Session
	handler s3crypto.CipherDataGenerator
	bucket  string
}

// Create a new Keycrypt instance which uses Amazon's KMS to store
// key material securely.
func New(sess *session.Session, id string) *Crypt {
	return &Crypt{
		sess:    sess,
		handler: s3crypto.NewKMSKeyGenerator(kms.New(sess), fmt.Sprintf("alias/%s", id)),
		bucket:  fmt.Sprintf("grail-keycrypt-%s", id),
	}
}

func (c *Crypt) Lookup(name string) keycrypt.Secret {
	return &secret{c, name}
}

type secret struct {
	*Crypt
	name string
}

func (s *secret) Get() ([]byte, error) {
	svc := s3crypto.NewDecryptionClient(s.sess)

	key := path.Join(prefix, s.name)
	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		if err, ok := err.(awserr.Error); ok && err.Code() == "NoSuchKey" {
			return nil, keycrypt.ErrNoSuchSecret
		}
		return nil, err
	}

	p, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	return p, err
}

func (s *secret) Put(p []byte) error {
	svc := s3crypto.NewEncryptionClient(s.sess, s3crypto.AESGCMContentCipherBuilder(s.handler))

	key := path.Join(prefix, s.name)
	_, err := svc.PutObject(&s3.PutObjectInput{
		Body:   bytes.NewReader(p),
		Bucket: &s.bucket,
		Key:    &key,
	})
	return err
}

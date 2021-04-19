package ec2util

import (
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/sync/once"
)

var (
	iidOnce once.Task
	iid     ec2metadata.EC2InstanceIdentityDocument
)

// GetInstanceIdentityDocument returns the EC2 Instance ID document (if the current
// process is running within an EC2 instance) or an error.
// Unlike the SDK's implementation, this will use longer timeouts and multiple retries
// to improve the reliability of getting the Instance ID document.
// The first result, whether success or failure, is cached for the lifetime of the process.
func GetInstanceIdentityDocument(sess *session.Session) (doc ec2metadata.EC2InstanceIdentityDocument, err error) {
	err = iidOnce.Do(func() (oerr error) {
		// Use HTTP client with custom timeout and max retries to prevent the SDK from
		// using an HTTP client with a small timeout and a small number of retries for
		// the ec2metadata client
		metaClient := ec2metadata.New(sess, &aws.Config{
			HTTPClient: &http.Client{Timeout: 5 * time.Second},
			MaxRetries: aws.Int(5),
		})
		for retries := 0; retries < 5; retries++ {
			iid, oerr = metaClient.GetInstanceIdentityDocument()
			if oerr == nil {
				break
			}
		}
		return
	})
	switch {
	case err != nil:
		err = fmt.Errorf("ec2util.GetInstanceIdentityDocument: %v", err)
	case iid.InstanceID == "":
		err = fmt.Errorf("ec2util.GetInstanceIdentityDocument: Unable to get EC2InstanceIdentityDocument")
	default:
		doc = iid
	}
	return
}

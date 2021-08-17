package ticket_test

import (
	"bytes"
	"fmt"
	"github.com/grailbio/base/security/ticket"
	"reflect"
	"testing"
	"v.io/v23/context"
)

func TestGetter_path(t *testing.T) {
	t.Run("it joins with slashes", func(t *testing.T) {
		want := "ok"
		client := mockString("string/key", want)
		got, err := client.GetString(testContext(), "string", "key")
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestGetter_getTicket(t *testing.T) {
	t.Run("it can error", func(t *testing.T) {
		client := mockString("some/key", "ok")
		_, err := client.GetString(testContext(), "other/key")
		if err == nil {
			t.Fatal("want error, got nil")
		}
	})
}

func TestGetter_GetData(t *testing.T) {
	key := "data/key"
	want := []byte{1, 2, 3}
	client := mockData(key, want)

	got, err := client.GetData(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetString(t *testing.T) {
	key := "string/key"
	want := "this is just a test"
	client := mockString(key, want)

	got, err := client.GetString(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetAws(t *testing.T) {
	key := "aws/key"
	want := ticket.AwsTicket{
		AwsAssumeRoleBuilder: &ticket.AwsAssumeRoleBuilder{
			Region: "region",
			Role:   "role",
			TtlSec: 123,
		},
		AwsCredentials: ticket.AwsCredentials{
			Region:          "region",
			AccessKeyId:     "accessKeyID",
			SecretAccessKey: "secretAccessKey",
			SessionToken:    "sessionToken",
			Expiration:      "expiration",
		},
	}
	client := mockAws(key, want)

	got, err := client.GetAws(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetS3(t *testing.T) {
	key := "s3/key"
	want := ticket.S3Ticket{
		AwsAssumeRoleBuilder: &ticket.AwsAssumeRoleBuilder{
			Region: "region",
			Role:   "role",
			TtlSec: 123,
		},
		Endpoint: "endpoint",
		Bucket:   "bucket",
		Prefix:   "prefix",
	}
	client := mockS3(key, want)

	got, err := client.GetS3(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetSshCertificate(t *testing.T) {
	key := "ssh/key"
	want := ticket.SshCertificateTicket{
		Username: "username",
	}
	client := mockSshCertificate(key, want)

	got, err := client.GetSshCertificate(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetEcr(t *testing.T) {
	key := "ecr/key"
	want := ticket.EcrTicket{
		AwsAssumeRoleBuilder: &ticket.AwsAssumeRoleBuilder{
			Region: "region",
			Role:   "role",
			TtlSec: 123,
		},
		AuthorizationToken: "authorizationToken",
		Expiration:         "expiration",
		Endpoint:           "endpoint",
	}
	client := mockEcr(key, want)

	got, err := client.GetEcr(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetTlsServer(t *testing.T) {
	key := "TlsServer/key"
	want := ticket.TlsServerTicket{
		TlsCertAuthorityBuilder: &ticket.TlsCertAuthorityBuilder{
			Authority:  "authority",
			TtlSec:     123,
			CommonName: "commonName",
		},
		Credentials: ticket.TlsCredentials{
			AuthorityCert: "authorityCert",
			Cert:          "cert",
			Key:           "key",
		},
	}
	client := mockTlsServer(key, want)

	got, err := client.GetTlsServer(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetTlsClient(t *testing.T) {
	key := "TlsClient/key"
	want := ticket.TlsClientTicket{
		TlsCertAuthorityBuilder: &ticket.TlsCertAuthorityBuilder{
			Authority:  "authority",
			TtlSec:     123,
			CommonName: "commonName",
		},
		Credentials: ticket.TlsCredentials{
			AuthorityCert: "authorityCert",
			Cert:          "cert",
			Key:           "key",
		},
	}
	client := mockTlsClient(key, want)

	got, err := client.GetTlsClient(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetDocker(t *testing.T) {
	key := "Docker/key"
	want := ticket.DockerTicket{
		TlsCertAuthorityBuilder: &ticket.TlsCertAuthorityBuilder{
			Authority:  "authority",
			TtlSec:     123,
			CommonName: "commonName",
		},
		Credentials: ticket.TlsCredentials{
			AuthorityCert: "authorityCert",
			Cert:          "cert",
			Key:           "key",
		},
		Url: "url",
	}
	client := mockDocker(key, want)

	got, err := client.GetDocker(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetDockerServer(t *testing.T) {
	key := "DockerServer/key"
	want := ticket.DockerServerTicket{}
	client := mockDockerServer(key, want)

	got, err := client.GetDockerServer(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetDockerClient(t *testing.T) {
	key := "DockerClient/key"
	want := ticket.DockerClientTicket{}
	client := mockDockerClient(key, want)

	got, err := client.GetDockerClient(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetB2(t *testing.T) {
	key := "B2/key"
	want := ticket.B2Ticket{}
	client := mockB2(key, want)

	got, err := client.GetB2(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGetter_GetVanadium(t *testing.T) {
	key := "Vanadium/key"
	want := ticket.VanadiumTicket{}
	client := mockVanadium(key, want)

	got, err := client.GetVanadium(testContext(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// testContext creates a nil context which is safe to use with a mock client.
func testContext() *context.T {
	return nil
}

// mock is a shim to convert a ticket value to a ticket.Getter.
func mock(expectKey string, value interface{}) ticket.Getter {
	return func(_ *context.T, gotKey string) (ticket.Ticket, error) {
		if gotKey == expectKey {
			return value.(ticket.Ticket), nil
		}

		return nil, fmt.Errorf("ticket not found")
	}
}

func mockData(key string, data []byte) ticket.Getter {
	return mock(key, ticket.TicketGenericTicket{
		Value: ticket.GenericTicket{Data: data},
	})
}

func mockString(key string, s string) ticket.Getter {
	return mock(key, ticket.TicketGenericTicket{
		Value: ticket.GenericTicket{Data: []byte(s)},
	})
}

func mockAws(key string, aws ticket.AwsTicket) ticket.Getter {
	return mock(key, ticket.TicketAwsTicket{Value: aws})
}
func mockS3(key string, s3 ticket.S3Ticket) ticket.Getter {
	return mock(key, ticket.TicketS3Ticket{Value: s3})
}
func mockSshCertificate(key string, ssh ticket.SshCertificateTicket) ticket.Getter {
	return mock(key, ticket.TicketSshCertificateTicket{Value: ssh})
}
func mockEcr(key string, ecr ticket.EcrTicket) ticket.Getter {
	return mock(key, ticket.TicketEcrTicket{Value: ecr})
}
func mockTlsServer(key string, TlsServer ticket.TlsServerTicket) ticket.Getter {
	return mock(key, ticket.TicketTlsServerTicket{Value: TlsServer})
}
func mockTlsClient(key string, TlsClient ticket.TlsClientTicket) ticket.Getter {
	return mock(key, ticket.TicketTlsClientTicket{Value: TlsClient})
}
func mockDocker(key string, Docker ticket.DockerTicket) ticket.Getter {
	return mock(key, ticket.TicketDockerTicket{Value: Docker})
}
func mockDockerServer(key string, DockerServer ticket.DockerServerTicket) ticket.Getter {
	return mock(key, ticket.TicketDockerServerTicket{Value: DockerServer})
}
func mockDockerClient(key string, DockerClient ticket.DockerClientTicket) ticket.Getter {
	return mock(key, ticket.TicketDockerClientTicket{Value: DockerClient})
}
func mockB2(key string, b2 ticket.B2Ticket) ticket.Getter {
	return mock(key, ticket.TicketB2Ticket{Value: b2})
}
func mockVanadium(key string, vanadium ticket.VanadiumTicket) ticket.Getter {
	return mock(key, ticket.TicketVanadiumTicket{Value: vanadium})
}
